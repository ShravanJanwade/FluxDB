//! Query engine for FluxDB
//! 
//! Supports:
//! - SELECT with aggregations, DISTINCT
//! - JOIN operations (INNER, LEFT, RIGHT, FULL OUTER)
//! - Set operations (UNION, INTERSECT, EXCEPT)
//! - UPDATE and DELETE statements
//! - Advanced conditions (IN, BETWEEN, LIKE, IS NULL)

mod parser;
mod planner;
mod executor;
mod aggregates;

pub use parser::QueryParser;
pub use planner::{QueryPlan, QueryPlanner};
pub use executor::QueryExecutor;
pub use aggregates::*;

use crate::{DataPoint, Result, SeriesKey, TimeRange, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Query Result Types
// ============================================================================

/// Query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Result rows
    pub rows: Vec<QueryRow>,
    /// Execution time in milliseconds
    pub execution_time_ms: f64,
    /// Number of rows affected (for UPDATE/DELETE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<usize>,
}

impl Default for QueryResult {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            execution_time_ms: 0.0,
            rows_affected: None,
        }
    }
}

/// A single result row
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRow {
    /// Timestamp (if applicable)
    pub time: Option<Timestamp>,
    /// Series key 
    pub series: Option<String>,
    /// Values (matching columns)
    pub values: Vec<QueryValue>,
}

/// Query value types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum QueryValue {
    Null,
    Float(f64),
    Integer(i64),
    String(String),
    Boolean(bool),
}

impl QueryValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            QueryValue::Float(v) => Some(*v),
            QueryValue::Integer(v) => Some(*v as f64),
            _ => None,
        }
    }
    
    pub fn as_string(&self) -> Option<String> {
        match self {
            QueryValue::String(s) => Some(s.clone()),
            QueryValue::Float(v) => Some(v.to_string()),
            QueryValue::Integer(v) => Some(v.to_string()),
            QueryValue::Boolean(v) => Some(v.to_string()),
            QueryValue::Null => None,
        }
    }
    
    pub fn is_null(&self) -> bool {
        matches!(self, QueryValue::Null)
    }
}

// ============================================================================
// Statement Types (Top-level SQL statements)
// ============================================================================

/// SQL statement types
#[derive(Debug, Clone)]
pub enum Statement {
    /// SELECT query
    Select(Query),
    /// INSERT statement
    Insert(InsertStatement),
    /// UPDATE statement
    Update(UpdateStatement),
    /// DELETE statement
    Delete(DeleteStatement),
    /// Set operation (UNION, INTERSECT, EXCEPT)
    SetOperation(SetOperation),
}

/// INSERT statement
#[derive(Debug, Clone)]
pub struct InsertStatement {
    /// Target measurement
    pub measurement: String,
    /// Columns to insert
    pub columns: Vec<String>,
    /// Values to insert (multiple rows)
    pub values: Vec<Vec<QueryValue>>,
    /// Tags for the inserted data
    pub tags: HashMap<String, String>,
}

/// UPDATE statement
#[derive(Debug, Clone)]
pub struct UpdateStatement {
    /// Target measurement
    pub measurement: String,
    /// Field assignments (field_name -> new_value)
    pub assignments: Vec<Assignment>,
    /// WHERE conditions
    pub where_clause: Option<WhereClause>,
}

/// DELETE statement
#[derive(Debug, Clone)]
pub struct DeleteStatement {
    /// Target measurement
    pub measurement: String,
    /// WHERE conditions (required for safety)
    pub where_clause: WhereClause,
}

/// Assignment in UPDATE
#[derive(Debug, Clone)]
pub struct Assignment {
    pub field: String,
    pub value: QueryValue,
}

// ============================================================================
// Set Operations
// ============================================================================

/// Set operation (UNION, INTERSECT, EXCEPT)
#[derive(Debug, Clone)]
pub struct SetOperation {
    pub op: SetOpType,
    pub left: Box<Statement>,
    pub right: Box<Statement>,
    pub all: bool, // UNION ALL vs UNION
}

/// Set operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpType {
    Union,
    Intersect,
    Except,
}

// ============================================================================
// SELECT Query Types
// ============================================================================

/// Parsed SELECT query
#[derive(Debug, Clone)]
pub struct Query {
    /// DISTINCT modifier
    pub distinct: bool,
    /// SELECT fields
    pub select: Vec<SelectItem>,
    /// FROM clause (measurement or join)
    pub from: FromClause,
    /// WHERE conditions
    pub where_clause: Option<WhereClause>,
    /// GROUP BY
    pub group_by: Option<GroupBy>,
    /// HAVING conditions (for aggregate filtering)
    pub having: Option<WhereClause>,
    /// ORDER BY
    pub order_by: Option<OrderBy>,
    /// LIMIT
    pub limit: Option<usize>,
    /// OFFSET
    pub offset: Option<usize>,
}

/// FROM clause - can be a simple table or a JOIN
#[derive(Debug, Clone)]
pub enum FromClause {
    /// Simple table reference
    Table(String),
    /// JOIN operation
    Join(Box<JoinClause>),
    /// Subquery
    Subquery(Box<Query>, String), // Query and alias
}

/// JOIN clause
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub left: FromClause,
    pub right: FromClause,
    pub on: JoinCondition,
}

/// JOIN type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    FullOuter,
    Cross,
}

/// JOIN condition
#[derive(Debug, Clone)]
pub enum JoinCondition {
    /// ON clause with conditions
    On(Condition),
    /// USING clause with column names
    Using(Vec<String>),
    /// NATURAL join (auto-match columns)
    Natural,
}

/// SELECT item
#[derive(Debug, Clone)]
pub enum SelectItem {
    /// All fields (*)
    All,
    /// Qualified wildcard (table.*)
    QualifiedAll(String),
    /// Field name
    Field(String),
    /// Qualified field (table.field)
    QualifiedField { table: String, field: String },
    /// Aggregate function
    Aggregate {
        function: AggregateFunc,
        field: String,
        alias: Option<String>,
    },
    /// Expression with alias
    Expression {
        expr: Box<Expr>,
        alias: Option<String>,
    },
}

/// Expression for computed columns
#[derive(Debug, Clone)]
pub enum Expr {
    /// Column reference
    Column(String),
    /// Qualified column (table.column)
    QualifiedColumn { table: String, column: String },
    /// Literal value
    Literal(QueryValue),
    /// Binary operation
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    /// Function call
    Function {
        name: String,
        args: Vec<Expr>,
    },
    /// CASE expression
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    /// Subquery
    Subquery(Box<Query>),
}

/// Binary operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

/// Aggregate function
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Mean,
    Min,
    Max,
    First,
    Last,
    Stddev,
    Variance,
    Median,
    Percentile,
}

impl AggregateFunc {
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "count" => Some(AggregateFunc::Count),
            "sum" => Some(AggregateFunc::Sum),
            "mean" | "avg" | "average" => Some(AggregateFunc::Mean),
            "min" => Some(AggregateFunc::Min),
            "max" => Some(AggregateFunc::Max),
            "first" => Some(AggregateFunc::First),
            "last" => Some(AggregateFunc::Last),
            "stddev" | "stdev" => Some(AggregateFunc::Stddev),
            "variance" | "var" => Some(AggregateFunc::Variance),
            "median" => Some(AggregateFunc::Median),
            "percentile" => Some(AggregateFunc::Percentile),
            _ => None,
        }
    }
}

// ============================================================================
// WHERE Clause and Conditions
// ============================================================================

/// WHERE clause
#[derive(Debug, Clone)]
pub struct WhereClause {
    pub conditions: Vec<Condition>,
}

/// Condition (enhanced with more operators)
#[derive(Debug, Clone)]
pub enum Condition {
    /// Time range filter
    TimeRange(TimeRange),
    /// Tag equals value
    TagEquals { tag: String, value: String },
    /// Field comparison
    FieldCompare { field: String, op: CompareOp, value: f64 },
    /// String field comparison
    StringCompare { field: String, op: CompareOp, value: String },
    /// IN operator (field IN (value1, value2, ...))
    In { field: String, values: Vec<QueryValue>, negated: bool },
    /// BETWEEN operator
    Between { field: String, low: QueryValue, high: QueryValue, negated: bool },
    /// LIKE operator for pattern matching
    Like { field: String, pattern: String, negated: bool },
    /// IS NULL / IS NOT NULL
    IsNull { field: String, negated: bool },
    /// EXISTS subquery
    Exists { subquery: Box<Query>, negated: bool },
    /// Subquery comparison (field op (SELECT ...))
    SubqueryCompare { field: String, op: CompareOp, subquery: Box<Query> },
    /// AND combination
    And(Box<Condition>, Box<Condition>),
    /// OR combination
    Or(Box<Condition>, Box<Condition>),
    /// NOT negation
    Not(Box<Condition>),
}

/// Comparison operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,      // =
    Ne,      // != or <>
    Lt,      // <
    Le,      // <=
    Gt,      // >
    Ge,      // >=
    Like,    // LIKE
    NotLike, // NOT LIKE
    RegexMatch, // ~
    RegexNotMatch, // !~
}

// ============================================================================
// GROUP BY Clause
// ============================================================================

/// GROUP BY clause
#[derive(Debug, Clone)]
pub struct GroupBy {
    /// Time bucket interval (for time-series grouping)
    pub time_bucket: Option<i64>,
    /// Tag columns to group by
    pub tags: Vec<String>,
    /// FILL option for time grouping
    pub fill: Option<FillOption>,
}

/// Fill option for missing time buckets
#[derive(Debug, Clone)]
pub enum FillOption {
    /// Fill with NULL
    Null,
    /// Fill with previous value
    Previous,
    /// Fill with specific value
    Value(f64),
    /// Linear interpolation
    Linear,
    /// No fill (skip missing)
    None,
}

// ============================================================================
// ORDER BY Clause
// ============================================================================

/// ORDER BY clause
#[derive(Debug, Clone)]
pub struct OrderBy {
    pub items: Vec<OrderByItem>,
}

/// Single ORDER BY item
#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub field: String,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

impl OrderBy {
    /// Create a simple single-field order by (legacy)
    pub fn simple(field: String, descending: bool) -> Self {
        Self {
            items: vec![OrderByItem { 
                field, 
                descending, 
                nulls_first: None 
            }],
        }
    }
    
    /// Get the first field (for backward compatibility)
    pub fn field(&self) -> Option<&str> {
        self.items.first().map(|i| i.field.as_str())
    }
    
    /// Check if first field is descending
    pub fn descending(&self) -> bool {
        self.items.first().map(|i| i.descending).unwrap_or(false)
    }
}
