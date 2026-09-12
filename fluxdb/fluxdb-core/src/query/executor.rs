//! Query executor
//!
//! Executes query plans against data points, supporting:
//! - Simple SELECT queries
//! - Aggregations
//! - Advanced filters (IN, BETWEEN, LIKE, IS NULL)
//! - DISTINCT
//! - OFFSET for pagination

use super::{
    planner::{FieldSelection, QueryPlan},
    AggregateFunc, CompareOp, QueryResult, QueryRow, QueryValue,
};
use crate::{DataPoint, FieldValue, Result, SeriesKey};
use std::collections::{HashMap, HashSet};
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
            .filter(|(key, point)| {
                plan.predicate
                    .as_ref()
                    .map(|w| {
                        w.conditions
                            .iter()
                            .all(|c| Self::matches(c, key, point) == Some(true))
                    })
                    .unwrap_or(true)
            })
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

    pub fn matches(c: &super::Condition, key: &SeriesKey, point: &DataPoint) -> Option<bool> {
        use super::Condition::*;
        let value = |field: &str| -> Option<QueryValue> {
            if field == "time" {
                return Some(QueryValue::Integer(point.timestamp));
            }
            point
                .fields
                .get(field)
                .map(Self::field_to_query_value)
                .or_else(|| key.tags.get(field).cloned().map(QueryValue::String))
        };
        let compare = |a: &QueryValue, op: CompareOp, b: &QueryValue| -> Option<bool> {
            let order = match (a, b) {
                (QueryValue::String(a), QueryValue::String(b)) => Some(a.cmp(b)),
                (QueryValue::Boolean(a), QueryValue::Boolean(b)) => Some(a.cmp(b)),
                (QueryValue::Integer(a), QueryValue::Integer(b)) => Some(a.cmp(b)),
                _ => a.as_f64()?.partial_cmp(&b.as_f64()?),
            }?;
            Some(match op {
                CompareOp::Eq => order.is_eq(),
                CompareOp::Ne => !order.is_eq(),
                CompareOp::Lt => order.is_lt(),
                CompareOp::Le => !order.is_gt(),
                CompareOp::Gt => order.is_gt(),
                CompareOp::Ge => !order.is_lt(),
                _ => false,
            })
        };
        match c {
            TimeRange(r) => Some(r.contains(point.timestamp)),
            TagEquals { tag, value: v } => {
                compare(&value(tag)?, CompareOp::Eq, &QueryValue::String(v.clone()))
            }
            ValueCompare {
                field,
                op,
                value: v,
            } => compare(&value(field)?, *op, v),
            FieldCompare {
                field,
                op,
                value: v,
            } => compare(&value(field)?, *op, &QueryValue::Float(*v)),
            StringCompare {
                field,
                op,
                value: v,
            } => compare(&value(field)?, *op, &QueryValue::String(v.clone())),
            In {
                field,
                values,
                negated,
            } => {
                let v = value(field)?;
                if values
                    .iter()
                    .any(|x| compare(&v, CompareOp::Eq, x) == Some(true))
                {
                    Some(!*negated)
                } else if values
                    .iter()
                    .any(|x| compare(&v, CompareOp::Eq, x).is_none())
                {
                    None
                } else {
                    Some(*negated)
                }
            }
            Between {
                field,
                low,
                high,
                negated,
            } => {
                let v = value(field)?;
                Some(
                    (compare(&v, CompareOp::Ge, low)? && compare(&v, CompareOp::Le, high)?)
                        != *negated,
                )
            }
            Like {
                field,
                pattern,
                negated,
            } => {
                let QueryValue::String(v) = value(field)? else {
                    return None;
                };
                let pattern = regex::escape(pattern).replace('%', ".*").replace('_', ".");
                Some(
                    regex::Regex::new(&format!("(?s)^{}$", pattern))
                        .ok()?
                        .is_match(&v)
                        != *negated,
                )
            }
            IsNull { field, negated } => Some(value(field).is_none() != *negated),
            And(a, b) => match (Self::matches(a, key, point), Self::matches(b, key, point)) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            Or(a, b) => match (Self::matches(a, key, point), Self::matches(b, key, point)) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
            Not(a) => Self::matches(a, key, point).map(|v| !v),
            _ => None,
        }
    }

    fn value_order(a: &QueryValue, b: &QueryValue) -> std::cmp::Ordering {
        match (a, b) {
            (QueryValue::Integer(a), QueryValue::Integer(b)) => a.cmp(b),
            (QueryValue::String(a), QueryValue::String(b)) => a.cmp(b),
            (QueryValue::Boolean(a), QueryValue::Boolean(b)) => a.cmp(b),
            _ => a
                .as_f64()
                .partial_cmp(&b.as_f64())
                .unwrap_or(std::cmp::Ordering::Equal),
        }
    }

    fn execute_select(
        plan: &QueryPlan,
        data: Vec<(SeriesKey, DataPoint)>,
    ) -> Result<(Vec<String>, Vec<QueryRow>)> {
        let mut data = data;
        if let Some(sort) = &plan.sort {
            data.sort_by(|(ak, ap), (bk, bp)| {
                let value = |key: &SeriesKey, p: &DataPoint| {
                    p.fields
                        .get(&sort.field)
                        .map(Self::field_to_query_value)
                        .or_else(|| key.tags.get(&sort.field).cloned().map(QueryValue::String))
                        .unwrap_or(QueryValue::Null)
                };
                let order = if sort.field == "time" {
                    ap.timestamp.cmp(&bp.timestamp)
                } else if sort.field == "series" {
                    ak.cmp(bk)
                } else {
                    Self::value_order(&value(ak, ap), &value(bk, bp))
                };
                if sort.descending {
                    order.reverse()
                } else {
                    order
                }
            });
        }
        let all = matches!(plan.fields, FieldSelection::All);
        let mut columns = if all {
            vec!["time".to_string(), "series".to_string()]
        } else {
            Vec::new()
        };

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
                        if name == "time" {
                            return QueryValue::Integer(dp.timestamp);
                        }
                        if name == "series" {
                            return QueryValue::String(key.canonical());
                        }
                        dp.fields
                            .get(name)
                            .map(|v| Self::field_to_query_value(v))
                            .or_else(|| key.tags.get(name).cloned().map(QueryValue::String))
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

        if !all {
            for row in &mut rows {
                row.time = None;
                row.series = None;
            }
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
                time_bucket: plan
                    .time_bucket
                    .map(|b| point.timestamp.div_euclid(b).saturating_mul(b)),
                tags: plan
                    .group_by_tags
                    .iter()
                    .filter_map(|t| key.tags.get(t).map(|v| (t.clone(), v.clone())))
                    .collect(),
            };

            groups.entry(group_key).or_default().push((key, point));
        }

        if groups.is_empty() && plan.time_bucket.is_none() && plan.group_by_tags.is_empty() {
            groups.insert(
                GroupKey {
                    time_bucket: None,
                    tags: vec![],
                },
                vec![],
            );
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

                    let result = if agg.function == AggregateFunc::Count {
                        QueryValue::Integer(if agg.field == "*" {
                            points.len()
                        } else {
                            points
                                .iter()
                                .filter(|(_, p)| p.fields.get(&agg.field).is_some())
                                .count()
                        } as i64)
                    } else if matches!(agg.function, AggregateFunc::First | AggregateFunc::Last) {
                        let mut present: Vec<_> = points
                            .iter()
                            .filter(|(_, p)| p.fields.get(&agg.field).is_some())
                            .collect();
                        present.sort_by_key(|(_, p)| p.timestamp);
                        let point = if agg.function == AggregateFunc::First {
                            present.first()
                        } else {
                            present.last()
                        };
                        point
                            .and_then(|(_, p)| p.fields.get(&agg.field))
                            .map(Self::field_to_query_value)
                            .unwrap_or(QueryValue::Null)
                    } else {
                        Self::compute_aggregate(agg.function, &field_values, &points)
                    };
                    values.push(result);
                }

                QueryRow {
                    time: group_key.time_bucket,
                    series: None,
                    values,
                }
            })
            .collect();

        if let Some(sort) = &plan.sort {
            if sort.field == "time" {
                rows.sort_by_key(|r| r.time);
            } else if let Some(index) = columns
                .iter()
                .filter(|c| *c != "time" || plan.time_bucket.is_none())
                .position(|c| c == &sort.field)
            {
                rows.sort_by(|a, b| Self::value_order(&a.values[index], &b.values[index]));
            }
            if sort.descending {
                rows.reverse();
            }
        } else {
            rows.sort_by(|a, b| {
                a.time
                    .cmp(&b.time)
                    .then(format!("{:?}", a.values).cmp(&format!("{:?}", b.values)))
            });
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
            AggregateFunc::Min => {
                QueryValue::Float(values.iter().cloned().fold(f64::INFINITY, f64::min))
            }
            AggregateFunc::Max => {
                QueryValue::Float(values.iter().cloned().fold(f64::NEG_INFINITY, f64::max))
            }
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
                let variance =
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                QueryValue::Float(variance.sqrt())
            }
            AggregateFunc::Variance => {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance =
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
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
