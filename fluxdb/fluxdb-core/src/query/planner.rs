//! Query planner for FluxDB
//!
//! Creates execution plans from parsed queries, handling:
//! - Simple SELECTs
//! - JOINs
//! - Aggregations
//! - Time-based queries

use super::{
    Query, SelectItem, Condition, GroupBy, AggregateFunc, FromClause, 
    JoinClause, JoinType, QueryValue,
};
use crate::{Result, SeriesKey, TimeRange};
use std::collections::HashSet;

/// Query execution plan
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Plan type
    pub plan_type: PlanType,
    /// Source measurement (for simple queries)
    pub measurement: String,
    /// Time range to query
    pub time_range: TimeRange,
    /// Tag filters
    pub tag_filters: Vec<(String, String)>,
    /// Field filters
    pub field_filters: Vec<FieldFilter>,
    /// Advanced filters (IN, BETWEEN, LIKE, etc.)
    pub advanced_filters: Vec<AdvancedFilter>,
    /// Fields to select
    pub fields: FieldSelection,
    /// Aggregations to perform
    pub aggregations: Vec<Aggregation>,
    /// Time bucket for grouping (nanoseconds)
    pub time_bucket: Option<i64>,
    /// Tags to group by
    pub group_by_tags: Vec<String>,
    /// Sort order
    pub sort: Option<SortOrder>,
    /// Result limit
    pub limit: Option<usize>,
    /// Result offset
    pub offset: Option<usize>,
    /// DISTINCT modifier
    pub distinct: bool,
}

/// Plan type
#[derive(Debug, Clone)]
pub enum PlanType {
    /// Simple table scan
    TableScan,
    /// Join operation
    Join(JoinPlan),
    /// Subquery
    Subquery(Box<QueryPlan>),
}

/// Join execution plan
#[derive(Debug, Clone)]
pub struct JoinPlan {
    pub join_type: JoinType,
    pub left: Box<QueryPlan>,
    pub right: Box<QueryPlan>,
    pub on_condition: Option<JoinOnCondition>,
}

/// Join ON condition for execution
#[derive(Debug, Clone)]
pub struct JoinOnCondition {
    pub left_field: String,
    pub right_field: String,
}

/// Field selection
#[derive(Debug, Clone)]
pub enum FieldSelection {
    All,
    Fields(Vec<String>),
    QualifiedFields(Vec<(Option<String>, String)>), // (table, field)
}

/// Field filter
#[derive(Debug, Clone)]
pub struct FieldFilter {
    pub field: String,
    pub op: super::CompareOp,
    pub value: f64,
}

/// Advanced filter types
#[derive(Debug, Clone)]
pub enum AdvancedFilter {
    In {
        field: String,
        values: Vec<QueryValue>,
        negated: bool,
    },
    Between {
        field: String,
        low: QueryValue,
        high: QueryValue,
        negated: bool,
    },
    Like {
        field: String,
        pattern: String,
        negated: bool,
    },
    IsNull {
        field: String,
        negated: bool,
    },
    StringCompare {
        field: String,
        op: super::CompareOp,
        value: String,
    },
}

/// Aggregation specification
#[derive(Debug, Clone)]
pub struct Aggregation {
    pub function: AggregateFunc,
    pub field: String,
    pub alias: String,
}

/// Sort order
#[derive(Debug, Clone)]
pub struct SortOrder {
    pub field: String,
    pub descending: bool,
}

/// Query planner
pub struct QueryPlanner;

impl QueryPlanner {
    /// Create an execution plan from a parsed query
    pub fn plan(query: &Query) -> Result<QueryPlan> {
        let mut time_range = TimeRange::new(i64::MIN, i64::MAX);
        let mut tag_filters = Vec::new();
        let mut field_filters = Vec::new();
        let mut advanced_filters = Vec::new();

        // Extract conditions
        if let Some(where_clause) = &query.where_clause {
            for condition in &where_clause.conditions {
                Self::extract_conditions(
                    condition,
                    &mut time_range,
                    &mut tag_filters,
                    &mut field_filters,
                    &mut advanced_filters,
                );
            }
        }

        // Determine plan type and measurement
        let (plan_type, measurement) = match &query.from {
            FromClause::Table(name) => (PlanType::TableScan, name.clone()),
            FromClause::Join(join_clause) => {
                let join_plan = Self::plan_join(join_clause)?;
                let measurement = Self::get_measurement_from_join(join_clause);
                (PlanType::Join(join_plan), measurement)
            }
            FromClause::Subquery(subquery, _alias) => {
                let sub_plan = Self::plan(subquery)?;
                (PlanType::Subquery(Box::new(sub_plan)), "subquery".to_string())
            }
        };

        // Parse SELECT
        let (fields, aggregations) = Self::extract_select_items(&query.select)?;

        // Parse GROUP BY
        let (time_bucket, group_by_tags) = match &query.group_by {
            Some(gb) => (gb.time_bucket, gb.tags.clone()),
            None => (None, Vec::new()),
        };

        // Parse ORDER BY
        let sort = query.order_by.as_ref().map(|ob| {
            let (field, descending) = if let Some(first) = ob.items.first() {
                (first.field.clone(), first.descending)
            } else {
                ("time".to_string(), false)
            };
            SortOrder { field, descending }
        });

        Ok(QueryPlan {
            plan_type,
            measurement,
            time_range,
            tag_filters,
            field_filters,
            advanced_filters,
            fields,
            aggregations,
            time_bucket,
            group_by_tags,
            sort,
            limit: query.limit,
            offset: query.offset,
            distinct: query.distinct,
        })
    }

    fn plan_join(join: &JoinClause) -> Result<JoinPlan> {
        let left = Self::plan_from_clause(&join.left)?;
        let right = Self::plan_from_clause(&join.right)?;

        // Extract join condition
        let on_condition = match &join.on {
            super::JoinCondition::On(cond) => Self::extract_join_condition(cond),
            super::JoinCondition::Using(cols) => {
                cols.first().map(|col| JoinOnCondition {
                    left_field: col.clone(),
                    right_field: col.clone(),
                })
            }
            super::JoinCondition::Natural => None,
        };

        Ok(JoinPlan {
            join_type: join.join_type,
            left: Box::new(left),
            right: Box::new(right),
            on_condition,
        })
    }

    fn plan_from_clause(from: &FromClause) -> Result<QueryPlan> {
        match from {
            FromClause::Table(name) => Ok(QueryPlan {
                plan_type: PlanType::TableScan,
                measurement: name.clone(),
                time_range: TimeRange::new(i64::MIN, i64::MAX),
                tag_filters: Vec::new(),
                field_filters: Vec::new(),
                advanced_filters: Vec::new(),
                fields: FieldSelection::All,
                aggregations: Vec::new(),
                time_bucket: None,
                group_by_tags: Vec::new(),
                sort: None,
                limit: None,
                offset: None,
                distinct: false,
            }),
            FromClause::Join(join) => {
                let join_plan = Self::plan_join(join)?;
                let measurement = Self::get_measurement_from_join(join);
                Ok(QueryPlan {
                    plan_type: PlanType::Join(join_plan),
                    measurement,
                    time_range: TimeRange::new(i64::MIN, i64::MAX),
                    tag_filters: Vec::new(),
                    field_filters: Vec::new(),
                    advanced_filters: Vec::new(),
                    fields: FieldSelection::All,
                    aggregations: Vec::new(),
                    time_bucket: None,
                    group_by_tags: Vec::new(),
                    sort: None,
                    limit: None,
                    offset: None,
                    distinct: false,
                })
            }
            FromClause::Subquery(query, _) => Self::plan(query),
        }
    }

    fn get_measurement_from_join(join: &JoinClause) -> String {
        match &join.left {
            FromClause::Table(name) => name.clone(),
            FromClause::Join(inner) => Self::get_measurement_from_join(inner),
            FromClause::Subquery(_, alias) => alias.clone(),
        }
    }

    fn extract_join_condition(condition: &Condition) -> Option<JoinOnCondition> {
        match condition {
            Condition::TagEquals { tag, value } => Some(JoinOnCondition {
                left_field: tag.clone(),
                right_field: value.clone(),
            }),
            Condition::FieldCompare { field, value, .. } => Some(JoinOnCondition {
                left_field: field.clone(),
                right_field: value.to_string(),
            }),
            _ => None,
        }
    }

    fn extract_select_items(items: &[SelectItem]) -> Result<(FieldSelection, Vec<Aggregation>)> {
        let mut field_names = Vec::new();
        let mut aggregations = Vec::new();
        let mut has_all = false;

        for item in items {
            match item {
                SelectItem::All => {
                    has_all = true;
                }
                SelectItem::QualifiedAll(_table) => {
                    has_all = true;
                }
                SelectItem::Field(name) => {
                    field_names.push(name.clone());
                }
                SelectItem::QualifiedField { table: _, field } => {
                    field_names.push(field.clone());
                }
                SelectItem::Aggregate { function, field, alias } => {
                    let alias = alias.clone().unwrap_or_else(|| {
                        format!("{}_{}", Self::func_name(*function), field)
                    });
                    aggregations.push(Aggregation {
                        function: *function,
                        field: field.clone(),
                        alias,
                    });
                }
                SelectItem::Expression { .. } => {
                    // Expression handling would go here
                }
            }
        }

        let fields = if has_all || field_names.is_empty() {
            FieldSelection::All
        } else {
            FieldSelection::Fields(field_names)
        };

        Ok((fields, aggregations))
    }

    fn extract_conditions(
        condition: &Condition,
        time_range: &mut TimeRange,
        tag_filters: &mut Vec<(String, String)>,
        field_filters: &mut Vec<FieldFilter>,
        advanced_filters: &mut Vec<AdvancedFilter>,
    ) {
        match condition {
            Condition::TimeRange(tr) => {
                *time_range = TimeRange::new(
                    time_range.start.max(tr.start),
                    time_range.end.min(tr.end),
                );
            }
            Condition::TagEquals { tag, value } => {
                tag_filters.push((tag.clone(), value.clone()));
            }
            Condition::FieldCompare { field, op, value } => {
                field_filters.push(FieldFilter {
                    field: field.clone(),
                    op: *op,
                    value: *value,
                });
            }
            Condition::StringCompare { field, op, value } => {
                advanced_filters.push(AdvancedFilter::StringCompare {
                    field: field.clone(),
                    op: *op,
                    value: value.clone(),
                });
            }
            Condition::In { field, values, negated } => {
                advanced_filters.push(AdvancedFilter::In {
                    field: field.clone(),
                    values: values.clone(),
                    negated: *negated,
                });
            }
            Condition::Between { field, low, high, negated } => {
                advanced_filters.push(AdvancedFilter::Between {
                    field: field.clone(),
                    low: low.clone(),
                    high: high.clone(),
                    negated: *negated,
                });
            }
            Condition::Like { field, pattern, negated } => {
                advanced_filters.push(AdvancedFilter::Like {
                    field: field.clone(),
                    pattern: pattern.clone(),
                    negated: *negated,
                });
            }
            Condition::IsNull { field, negated } => {
                advanced_filters.push(AdvancedFilter::IsNull {
                    field: field.clone(),
                    negated: *negated,
                });
            }
            Condition::And(left, right) => {
                Self::extract_conditions(left, time_range, tag_filters, field_filters, advanced_filters);
                Self::extract_conditions(right, time_range, tag_filters, field_filters, advanced_filters);
            }
            Condition::Or(left, right) => {
                // For OR conditions we process both sides
                Self::extract_conditions(left, time_range, tag_filters, field_filters, advanced_filters);
                Self::extract_conditions(right, time_range, tag_filters, field_filters, advanced_filters);
            }
            Condition::Not(inner) => {
                Self::extract_conditions(inner, time_range, tag_filters, field_filters, advanced_filters);
            }
            Condition::Exists { .. } | Condition::SubqueryCompare { .. } => {
                // Subquery conditions would need special handling
            }
        }
    }

    fn func_name(func: AggregateFunc) -> &'static str {
        match func {
            AggregateFunc::Count => "count",
            AggregateFunc::Sum => "sum",
            AggregateFunc::Mean => "mean",
            AggregateFunc::Min => "min",
            AggregateFunc::Max => "max",
            AggregateFunc::First => "first",
            AggregateFunc::Last => "last",
            AggregateFunc::Stddev => "stddev",
            AggregateFunc::Variance => "variance",
            AggregateFunc::Median => "median",
            AggregateFunc::Percentile => "percentile",
        }
    }
}
