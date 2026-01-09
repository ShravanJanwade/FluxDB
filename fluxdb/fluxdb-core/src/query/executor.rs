//! Query executor
//!
//! Executes query plans against data points, supporting:
//! - Simple SELECT queries
//! - Aggregations
//! - Advanced filters (IN, BETWEEN, LIKE, IS NULL)
//! - DISTINCT
//! - OFFSET for pagination

use super::{
    planner::{Aggregation, AdvancedFilter, FieldSelection, QueryPlan, SortOrder},
    AggregateFunc, CompareOp, QueryResult, QueryRow, QueryValue,
};
use crate::{DataPoint, FieldValue, Result, SeriesKey, TimeRange};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

/// Query executor
pub struct QueryExecutor;

impl QueryExecutor {
    /// Execute a query plan against data points
    pub fn execute(plan: &QueryPlan, data: Vec<(SeriesKey, DataPoint)>) -> Result<QueryResult> {
        let start = Instant::now();

        // Filter by basic conditions
        let filtered: Vec<_> = data
            .into_iter()
            .filter(|(key, point)| Self::matches_basic_filters(plan, key, point))
            .filter(|(key, point)| Self::matches_advanced_filters(plan, key, point))
            .collect();

        // Group and aggregate if needed
        let result = if !plan.aggregations.is_empty() {
            Self::execute_aggregation(plan, filtered)?
        } else {
            Self::execute_select(plan, filtered)?
        };

        let execution_time_ms = start.elapsed().as_secs_f64() * 1000.0;

        Ok(QueryResult {
            columns: result.0,
            rows: result.1,
            execution_time_ms,
            rows_affected: None,
        })
    }

    fn matches_basic_filters(plan: &QueryPlan, key: &SeriesKey, point: &DataPoint) -> bool {
        // Check tag filters
        for (tag_name, tag_value) in &plan.tag_filters {
            if key.tags.get(tag_name) != Some(tag_value) {
                return false;
            }
        }

        // Check time range
        if !plan.time_range.contains(point.timestamp) {
            return false;
        }

        // Check field filters
        for filter in &plan.field_filters {
            if let Some(field_val) = point.fields.get(&filter.field) {
                if let Some(val) = field_val.as_f64() {
                    let passes = match filter.op {
                        CompareOp::Eq => (val - filter.value).abs() < f64::EPSILON,
                        CompareOp::Ne => (val - filter.value).abs() >= f64::EPSILON,
                        CompareOp::Lt => val < filter.value,
                        CompareOp::Le => val <= filter.value,
                        CompareOp::Gt => val > filter.value,
                        CompareOp::Ge => val >= filter.value,
                        _ => true, // Other ops handled differently
                    };
                    if !passes {
                        return false;
                    }
                }
            }
        }

        true
    }

    fn matches_advanced_filters(plan: &QueryPlan, _key: &SeriesKey, point: &DataPoint) -> bool {
        for filter in &plan.advanced_filters {
            match filter {
                AdvancedFilter::In { field, values, negated } => {
                    if let Some(field_val) = point.fields.get(field) {
                        let query_val = Self::field_to_query_value(field_val);
                        let found = values.contains(&query_val);
                        if *negated && found {
                            return false;
                        }
                        if !*negated && !found {
                            return false;
                        }
                    }
                }
                AdvancedFilter::Between { field, low, high, negated } => {
                    if let Some(field_val) = point.fields.get(field) {
                        if let Some(val) = field_val.as_f64() {
                            let low_f = Self::query_value_to_f64(low).unwrap_or(f64::NEG_INFINITY);
                            let high_f = Self::query_value_to_f64(high).unwrap_or(f64::INFINITY);
                            let in_range = val >= low_f && val <= high_f;
                            if *negated && in_range {
                                return false;
                            }
                            if !*negated && !in_range {
                                return false;
                            }
                        }
                    }
                }
                AdvancedFilter::Like { field, pattern, negated } => {
                    if let Some(field_val) = point.fields.get(field) {
                        if let FieldValue::String(s) = field_val {
                            let matches = Self::matches_like_pattern(s, pattern);
                            if *negated && matches {
                                return false;
                            }
                            if !*negated && !matches {
                                return false;
                            }
                        }
                    }
                }
                AdvancedFilter::IsNull { field, negated } => {
                    let is_null = point.fields.get(field).is_none();
                    if *negated && is_null {
                        return false;
                    }
                    if !*negated && !is_null {
                        return false;
                    }
                }
                AdvancedFilter::StringCompare { field, op, value } => {
                    if let Some(FieldValue::String(s)) = point.fields.get(field) {
                        let passes = match op {
                            CompareOp::Eq => s == value,
                            CompareOp::Ne => s != value,
                            CompareOp::Lt => s < value,
                            CompareOp::Le => s <= value,
                            CompareOp::Gt => s > value,
                            CompareOp::Ge => s >= value,
                            _ => true,
                        };
                        if !passes {
                            return false;
                        }
                    }
                }
            }
        }
        true
    }

    fn matches_like_pattern(s: &str, pattern: &str) -> bool {
        // Simple LIKE pattern matching with % and _ wildcards
        let regex_pattern = pattern
            .replace('%', ".*")
            .replace('_', ".");
        
        // Try to match with regex
        regex::Regex::new(&format!("^{}$", regex_pattern))
            .map(|re| re.is_match(s))
            .unwrap_or_else(|_| s.contains(&pattern.replace('%', "").replace('_', "")))
    }

    fn query_value_to_f64(val: &QueryValue) -> Option<f64> {
        match val {
            QueryValue::Float(f) => Some(*f),
            QueryValue::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    fn execute_select(
        plan: &QueryPlan,
        data: Vec<(SeriesKey, DataPoint)>,
    ) -> Result<(Vec<String>, Vec<QueryRow>)> {
        // Determine columns
        let mut columns = vec!["time".to_string(), "series".to_string()];
        
        let field_names: Vec<String> = match &plan.fields {
            FieldSelection::All => {
                // Collect all unique field names
                let mut names: Vec<String> = data
                    .iter()
                    .flat_map(|(_, dp)| dp.fields.0.keys().cloned())
                    .collect();
                names.sort();
                names.dedup();
                names
            }
            FieldSelection::Fields(fields) => fields.clone(),
            FieldSelection::QualifiedFields(fields) => {
                fields.iter().map(|(_, f)| f.clone()).collect()
            }
        };
        
        columns.extend(field_names.clone());

        // Build rows
        let mut rows: Vec<QueryRow> = data
            .into_iter()
            .map(|(key, dp)| {
                let values: Vec<QueryValue> = field_names
                    .iter()
                    .map(|name| {
                        dp.fields
                            .get(name)
                            .map(|v| Self::field_to_query_value(v))
                            .unwrap_or(QueryValue::Null)
                    })
                    .collect();

                QueryRow {
                    time: Some(dp.timestamp),
                    series: Some(key.canonical()),
                    values,
                }
            })
            .collect();

        // Apply DISTINCT
        if plan.distinct {
            let mut seen = HashSet::new();
            rows.retain(|row| {
                let key = format!("{:?}", row.values);
                seen.insert(key)
            });
        }

        // Sort if needed
        if let Some(sort) = &plan.sort {
            let field_idx = field_names.iter().position(|n| n == &sort.field);
            if sort.field == "time" {
                if sort.descending {
                    rows.sort_by(|a, b| b.time.cmp(&a.time));
                } else {
                    rows.sort_by(|a, b| a.time.cmp(&b.time));
                }
            } else if let Some(idx) = field_idx {
                rows.sort_by(|a, b| {
                    let av = a.values.get(idx).and_then(|v| v.as_f64());
                    let bv = b.values.get(idx).and_then(|v| v.as_f64());
                    if sort.descending {
                        bv.partial_cmp(&av).unwrap_or(std::cmp::Ordering::Equal)
                    } else {
                        av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal)
                    }
                });
            }
        }

        // Apply offset
        if let Some(offset) = plan.offset {
            if offset < rows.len() {
                rows = rows.into_iter().skip(offset).collect();
            } else {
                rows.clear();
            }
        }

        // Apply limit
        if let Some(limit) = plan.limit {
            rows.truncate(limit);
        }

        Ok((columns, rows))
    }

    fn execute_aggregation(
        plan: &QueryPlan,
        data: Vec<(SeriesKey, DataPoint)>,
    ) -> Result<(Vec<String>, Vec<QueryRow>)> {
        // Group data
        let mut groups: HashMap<GroupKey, Vec<(SeriesKey, DataPoint)>> = HashMap::new();

        for (key, point) in data {
            let group_key = GroupKey {
                time_bucket: plan.time_bucket.map(|b| (point.timestamp / b) * b),
                tags: plan
                    .group_by_tags
                    .iter()
                    .filter_map(|t| key.tags.get(t).map(|v| (t.clone(), v.clone())))
                    .collect(),
            };

            groups.entry(group_key).or_default().push((key, point));
        }

        // Build columns
        let mut columns = Vec::new();
        if plan.time_bucket.is_some() {
            columns.push("time".to_string());
        }
        for tag in &plan.group_by_tags {
            columns.push(tag.clone());
        }
        for agg in &plan.aggregations {
            columns.push(agg.alias.clone());
        }

        // Compute aggregates for each group
        let mut rows: Vec<QueryRow> = groups
            .into_iter()
            .map(|(group_key, points)| {
                let mut values = Vec::new();

                // Add group-by tag values
                for tag in &plan.group_by_tags {
                    let val = group_key
                        .tags
                        .iter()
                        .find(|(k, _)| k == tag)
                        .map(|(_, v)| QueryValue::String(v.clone()))
                        .unwrap_or(QueryValue::Null);
                    values.push(val);
                }

                // Compute each aggregation
                for agg in &plan.aggregations {
                    let field_values: Vec<f64> = points
                        .iter()
                        .filter_map(|(_, dp)| dp.fields.get(&agg.field))
                        .filter_map(|v| v.as_f64())
                        .collect();

                    let result = Self::compute_aggregate(agg.function, &field_values, &points);
                    values.push(result);
                }

                QueryRow {
                    time: group_key.time_bucket,
                    series: None,
                    values,
                }
            })
            .collect();

        // Sort by time if time bucketing
        if plan.time_bucket.is_some() {
            rows.sort_by(|a, b| a.time.cmp(&b.time));
        }

        // Apply offset
        if let Some(offset) = plan.offset {
            if offset < rows.len() {
                rows = rows.into_iter().skip(offset).collect();
            } else {
                rows.clear();
            }
        }

        // Apply limit
        if let Some(limit) = plan.limit {
            rows.truncate(limit);
        }

        Ok((columns, rows))
    }

    fn compute_aggregate(
        func: AggregateFunc,
        values: &[f64],
        points: &[(SeriesKey, DataPoint)],
    ) -> QueryValue {
        if values.is_empty() {
            return QueryValue::Null;
        }

        match func {
            AggregateFunc::Count => QueryValue::Integer(values.len() as i64),
            AggregateFunc::Sum => QueryValue::Float(values.iter().sum()),
            AggregateFunc::Mean => {
                QueryValue::Float(values.iter().sum::<f64>() / values.len() as f64)
            }
            AggregateFunc::Min => QueryValue::Float(
                values.iter().cloned().fold(f64::INFINITY, f64::min),
            ),
            AggregateFunc::Max => QueryValue::Float(
                values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            ),
            AggregateFunc::First => {
                // Get value with earliest timestamp
                points
                    .iter()
                    .min_by_key(|(_, dp)| dp.timestamp)
                    .and_then(|(_, dp)| dp.fields.0.values().next())
                    .and_then(|v| v.as_f64())
                    .map(QueryValue::Float)
                    .unwrap_or(QueryValue::Null)
            }
            AggregateFunc::Last => {
                // Get value with latest timestamp
                points
                    .iter()
                    .max_by_key(|(_, dp)| dp.timestamp)
                    .and_then(|(_, dp)| dp.fields.0.values().next())
                    .and_then(|v| v.as_f64())
                    .map(QueryValue::Float)
                    .unwrap_or(QueryValue::Null)
            }
            AggregateFunc::Stddev => {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                    / values.len() as f64;
                QueryValue::Float(variance.sqrt())
            }
            AggregateFunc::Variance => {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                    / values.len() as f64;
                QueryValue::Float(variance)
            }
            AggregateFunc::Median => {
                let mut sorted = values.to_vec();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let mid = sorted.len() / 2;
                if sorted.len() % 2 == 0 {
                    QueryValue::Float((sorted[mid - 1] + sorted[mid]) / 2.0)
                } else {
                    QueryValue::Float(sorted[mid])
                }
            }
            AggregateFunc::Percentile => {
                // Default to 50th percentile (median)
                let mut sorted = values.to_vec();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = (sorted.len() as f64 * 0.5) as usize;
                QueryValue::Float(sorted.get(idx).cloned().unwrap_or(0.0))
            }
        }
    }

    fn field_to_query_value(field: &FieldValue) -> QueryValue {
        match field {
            FieldValue::Float(v) => QueryValue::Float(*v),
            FieldValue::Integer(v) => QueryValue::Integer(*v),
            FieldValue::Boolean(v) => QueryValue::Boolean(*v),
            FieldValue::String(v) => QueryValue::String(v.clone()),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct GroupKey {
    time_bucket: Option<i64>,
    tags: Vec<(String, String)>,
}
