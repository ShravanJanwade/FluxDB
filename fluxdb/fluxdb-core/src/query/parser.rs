//! SQL parser for FluxDB queries
//!
//! Supports:
//! - SELECT with DISTINCT, aggregations, expressions
//! - JOIN operations (INNER, LEFT, RIGHT, FULL OUTER)
//! - Set operations (UNION, INTERSECT, EXCEPT)
//! - UPDATE and DELETE statements
//! - Advanced conditions (IN, BETWEEN, LIKE, IS NULL)

use super::{
    AggregateFunc, Assignment, CompareOp, Condition, DeleteStatement, FromClause, 
    GroupBy, JoinClause, JoinCondition, JoinType, OrderBy, OrderByItem, Query, 
    QueryValue, SelectItem, SetOpType, SetOperation, Statement, UpdateStatement, 
    WhereClause,
};
use crate::{FluxError, Result, TimeRange};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident,
    Join, JoinConstraint, JoinOperator, Query as SqlQuery, Select, 
    SelectItem as SqlSelectItem, SetExpr, SetOperator, Statement as SqlStatement, 
    TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// SQL query parser
pub struct QueryParser;

impl QueryParser {
    /// Parse a SQL query string into a Statement
    pub fn parse_statement(sql: &str) -> Result<Statement> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| FluxError::SqlParse(e.to_string()))?;

        if statements.is_empty() {
            return Err(FluxError::SqlParse("Empty query".into()));
        }

        match &statements[0] {
            SqlStatement::Query(query) => {
                Self::parse_query_to_statement(query)
            }
            SqlStatement::Update { table, assignments, selection, .. } => {
                Self::parse_update(table, assignments, selection)
            }
            SqlStatement::Delete { from, selection, .. } => {
                Self::parse_delete(from, selection)
            }
            _ => Err(FluxError::SqlParse(
                "Only SELECT, UPDATE, and DELETE statements are supported".into()
            )),
        }
    }

    /// Parse a SQL query string (legacy method for backward compatibility)
    pub fn parse(sql: &str) -> Result<Query> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| FluxError::SqlParse(e.to_string()))?;

        if statements.is_empty() {
            return Err(FluxError::SqlParse("Empty query".into()));
        }

        match &statements[0] {
            SqlStatement::Query(query) => Self::parse_query(query),
            _ => Err(FluxError::SqlParse("Only SELECT queries are supported".into())),
        }
    }

    fn parse_query_to_statement(query: &SqlQuery) -> Result<Statement> {
        // Check for set operations
        match query.body.as_ref() {
            SetExpr::SetOperation { op, set_quantifier, left, right, .. } => {
                let left_stmt = Self::parse_set_expr(left)?;
                let right_stmt = Self::parse_set_expr(right)?;
                
                let set_op_type = match op {
                    SetOperator::Union => SetOpType::Union,
                    SetOperator::Intersect => SetOpType::Intersect,
                    SetOperator::Except => SetOpType::Except,
                };
                
                let all = matches!(set_quantifier, sqlparser::ast::SetQuantifier::All);
                
                Ok(Statement::SetOperation(SetOperation {
                    op: set_op_type,
                    left: Box::new(left_stmt),
                    right: Box::new(right_stmt),
                    all,
                }))
            }
            _ => {
                let q = Self::parse_query(query)?;
                Ok(Statement::Select(q))
            }
        }
    }

    fn parse_set_expr(expr: &SetExpr) -> Result<Statement> {
        match expr {
            SetExpr::Select(select) => {
                // Create a minimal query for this select
                let query = Self::parse_select_to_query(select, None, None, None)?;
                Ok(Statement::Select(query))
            }
            SetExpr::Query(query) => {
                Self::parse_query_to_statement(query)
            }
            SetExpr::SetOperation { op, set_quantifier, left, right, .. } => {
                let left_stmt = Self::parse_set_expr(left)?;
                let right_stmt = Self::parse_set_expr(right)?;
                
                let set_op_type = match op {
                    SetOperator::Union => SetOpType::Union,
                    SetOperator::Intersect => SetOpType::Intersect,
                    SetOperator::Except => SetOpType::Except,
                };
                
                let all = matches!(set_quantifier, sqlparser::ast::SetQuantifier::All);
                
                Ok(Statement::SetOperation(SetOperation {
                    op: set_op_type,
                    left: Box::new(left_stmt),
                    right: Box::new(right_stmt),
                    all,
                }))
            }
            _ => Err(FluxError::SqlParse("Unsupported set expression".into())),
        }
    }

    fn parse_query(query: &SqlQuery) -> Result<Query> {
        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            SetExpr::SetOperation { .. } => {
                return Err(FluxError::SqlParse(
                    "Use parse_statement for set operations".into()
                ));
            }
            _ => return Err(FluxError::SqlParse("Unsupported query type".into())),
        };

        let order_by = Self::parse_order_by(query)?;
        let limit = Self::parse_limit(query)?;
        let offset = Self::parse_offset(query)?;
        
        Self::parse_select_to_query(select, order_by, limit, offset)
    }

    fn parse_select_to_query(
        select: &Select, 
        order_by: Option<OrderBy>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Query> {
        let from = Self::parse_from(select)?;
        let select_items = Self::parse_select_items(select)?;
        let where_clause = Self::parse_where(select)?;
        let group_by = Self::parse_group_by(select)?;
        let having = Self::parse_having(select)?;
        let distinct = select.distinct.is_some();

        Ok(Query {
            distinct,
            select: select_items,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_from(select: &Select) -> Result<FromClause> {
        if select.from.is_empty() {
            return Err(FluxError::SqlParse("Missing FROM clause".into()));
        }

        let table_with_joins = &select.from[0];
        Self::parse_table_with_joins(table_with_joins)
    }

    fn parse_table_with_joins(twj: &TableWithJoins) -> Result<FromClause> {
        let base = Self::parse_table_factor(&twj.relation)?;
        
        if twj.joins.is_empty() {
            return Ok(base);
        }

        // Process joins left to right
        let mut result = base;
        for join in &twj.joins {
            result = Self::parse_join(result, join)?;
        }

        Ok(result)
    }

    fn parse_table_factor(tf: &TableFactor) -> Result<FromClause> {
        match tf {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                // If there's an alias, we still just use the table name for now
                let _ = alias;
                Ok(FromClause::Table(table_name))
            }
            TableFactor::Derived { subquery, alias, .. } => {
                let query = Self::parse_query(subquery)?;
                let alias_name = alias.as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "subquery".to_string());
                Ok(FromClause::Subquery(Box::new(query), alias_name))
            }
            TableFactor::NestedJoin { table_with_joins, .. } => {
                Self::parse_table_with_joins(table_with_joins)
            }
            _ => Err(FluxError::SqlParse("Unsupported table factor".into())),
        }
    }

    fn parse_join(left: FromClause, join: &Join) -> Result<FromClause> {
        let right = Self::parse_table_factor(&join.relation)?;
        
        let (join_type, on) = match &join.join_operator {
            JoinOperator::Inner(constraint) => {
                (JoinType::Inner, Self::parse_join_constraint(constraint)?)
            }
            JoinOperator::LeftOuter(constraint) => {
                (JoinType::Left, Self::parse_join_constraint(constraint)?)
            }
            JoinOperator::RightOuter(constraint) => {
                (JoinType::Right, Self::parse_join_constraint(constraint)?)
            }
            JoinOperator::FullOuter(constraint) => {
                (JoinType::FullOuter, Self::parse_join_constraint(constraint)?)
            }
            JoinOperator::CrossJoin => {
                (JoinType::Cross, JoinCondition::Natural)
            }
            _ => return Err(FluxError::SqlParse("Unsupported join type".into())),
        };

        Ok(FromClause::Join(Box::new(JoinClause {
            join_type,
            left,
            right,
            on,
        })))
    }

    fn parse_join_constraint(constraint: &JoinConstraint) -> Result<JoinCondition> {
        match constraint {
            JoinConstraint::On(expr) => {
                let condition = Self::parse_condition(expr)?;
                Ok(JoinCondition::On(condition))
            }
            JoinConstraint::Using(columns) => {
                let cols: Vec<String> = columns.iter().map(|i| i.value.clone()).collect();
                Ok(JoinCondition::Using(cols))
            }
            JoinConstraint::Natural => Ok(JoinCondition::Natural),
            JoinConstraint::None => Ok(JoinCondition::Natural),
        }
    }

    fn parse_select_items(select: &Select) -> Result<Vec<SelectItem>> {
        let mut items = Vec::new();

        for item in &select.projection {
            match item {
                SqlSelectItem::Wildcard(_) => {
                    items.push(SelectItem::All);
                }
                SqlSelectItem::QualifiedWildcard(name, _) => {
                    items.push(SelectItem::QualifiedAll(name.to_string()));
                }
                SqlSelectItem::UnnamedExpr(expr) => {
                    items.push(Self::parse_select_expr(expr)?);
                }
                SqlSelectItem::ExprWithAlias { expr, alias } => {
                    let mut item = Self::parse_select_expr(expr)?;
                    if let SelectItem::Aggregate { alias: ref mut a, .. } = item {
                        *a = Some(alias.value.clone());
                    }
                    items.push(item);
                }
            }
        }

        Ok(items)
    }

    fn parse_select_expr(expr: &Expr) -> Result<SelectItem> {
        match expr {
            Expr::Identifier(ident) => Ok(SelectItem::Field(ident.value.clone())),
            Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
                Ok(SelectItem::QualifiedField {
                    table: idents[0].value.clone(),
                    field: idents[1].value.clone(),
                })
            }
            Expr::Function(func) => Self::parse_function(func),
            _ => Err(FluxError::SqlParse(format!(
                "Unsupported expression in SELECT: {:?}",
                expr
            ))),
        }
    }

    fn parse_function(func: &Function) -> Result<SelectItem> {
        let name = func.name.to_string().to_lowercase();
        let agg_func = AggregateFunc::from_name(&name)
            .ok_or_else(|| FluxError::SqlParse(format!("Unknown function: {}", name)))?;

        let field = if func.args.is_empty() {
            "*".to_string()
        } else {
            match &func.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                    ident.value.clone()
                }
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => "*".to_string(),
                _ => {
                    return Err(FluxError::SqlParse(
                        "Unsupported function argument".into(),
                    ))
                }
            }
        };

        Ok(SelectItem::Aggregate {
            function: agg_func,
            field,
            alias: None,
        })
    }

    fn parse_where(select: &Select) -> Result<Option<WhereClause>> {
        let selection = match &select.selection {
            Some(expr) => expr,
            None => return Ok(None),
        };

        let conditions = Self::parse_condition(selection)?;
        Ok(Some(WhereClause {
            conditions: vec![conditions],
        }))
    }

    fn parse_having(select: &Select) -> Result<Option<WhereClause>> {
        let having = match &select.having {
            Some(expr) => expr,
            None => return Ok(None),
        };

        let conditions = Self::parse_condition(having)?;
        Ok(Some(WhereClause {
            conditions: vec![conditions],
        }))
    }

    fn parse_condition(expr: &Expr) -> Result<Condition> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        let left_cond = Self::parse_condition(left)?;
                        let right_cond = Self::parse_condition(right)?;
                        Ok(Condition::And(Box::new(left_cond), Box::new(right_cond)))
                    }
                    BinaryOperator::Or => {
                        let left_cond = Self::parse_condition(left)?;
                        let right_cond = Self::parse_condition(right)?;
                        Ok(Condition::Or(Box::new(left_cond), Box::new(right_cond)))
                    }
                    _ => Self::parse_comparison(left, op, right),
                }
            }
            Expr::Between { expr, low, high, negated } => {
                let field = Self::extract_field_name(expr)?;
                let low_val = Self::parse_value_expr(low)?;
                let high_val = Self::parse_value_expr(high)?;
                Ok(Condition::Between {
                    field,
                    low: low_val,
                    high: high_val,
                    negated: *negated,
                })
            }
            Expr::InList { expr, list, negated } => {
                let field = Self::extract_field_name(expr)?;
                let values: Result<Vec<QueryValue>> = list.iter()
                    .map(|e| Self::parse_value_expr(e))
                    .collect();
                Ok(Condition::In {
                    field,
                    values: values?,
                    negated: *negated,
                })
            }
            Expr::IsNull(expr) => {
                let field = Self::extract_field_name(expr)?;
                Ok(Condition::IsNull { field, negated: false })
            }
            Expr::IsNotNull(expr) => {
                let field = Self::extract_field_name(expr)?;
                Ok(Condition::IsNull { field, negated: true })
            }
            Expr::Like { expr, pattern, negated, .. } => {
                let field = Self::extract_field_name(expr)?;
                let pattern_str = match pattern.as_ref() {
                    Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
                    _ => return Err(FluxError::SqlParse("LIKE pattern must be a string".into())),
                };
                Ok(Condition::Like {
                    field,
                    pattern: pattern_str,
                    negated: *negated,
                })
            }
            Expr::UnaryOp { op, expr } => {
                use sqlparser::ast::UnaryOperator;
                match op {
                    UnaryOperator::Not => {
                        let inner = Self::parse_condition(expr)?;
                        Ok(Condition::Not(Box::new(inner)))
                    }
                    _ => Err(FluxError::SqlParse(format!("Unsupported unary operator: {:?}", op))),
                }
            }
            Expr::Nested(inner) => Self::parse_condition(inner),
            _ => Err(FluxError::SqlParse(format!(
                "Unsupported WHERE expression: {:?}",
                expr
            ))),
        }
    }

    fn extract_field_name(expr: &Expr) -> Result<String> {
        match expr {
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            Expr::CompoundIdentifier(idents) => {
                Ok(idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."))
            }
            _ => Err(FluxError::SqlParse("Expected field name".into())),
        }
    }

    fn parse_value_expr(expr: &Expr) -> Result<QueryValue> {
        match expr {
            Expr::Value(val) => Self::parse_value(val),
            Expr::UnaryOp { op, expr } => {
                use sqlparser::ast::UnaryOperator;
                match op {
                    UnaryOperator::Minus => {
                        let inner = Self::parse_value_expr(expr)?;
                        match inner {
                            QueryValue::Float(f) => Ok(QueryValue::Float(-f)),
                            QueryValue::Integer(i) => Ok(QueryValue::Integer(-i)),
                            _ => Err(FluxError::SqlParse("Cannot negate non-numeric value".into())),
                        }
                    }
                    _ => Err(FluxError::SqlParse("Unsupported unary operator in value".into())),
                }
            }
            _ => Err(FluxError::SqlParse(format!("Unsupported value expression: {:?}", expr))),
        }
    }

    fn parse_value(val: &Value) -> Result<QueryValue> {
        match val {
            Value::Number(n, _) => {
                if n.contains('.') {
                    Ok(QueryValue::Float(
                        n.parse().map_err(|_| FluxError::SqlParse("Invalid float".into()))?
                    ))
                } else {
                    Ok(QueryValue::Integer(
                        n.parse().map_err(|_| FluxError::SqlParse("Invalid integer".into()))?
                    ))
                }
            }
            Value::SingleQuotedString(s) => Ok(QueryValue::String(s.clone())),
            Value::DoubleQuotedString(s) => Ok(QueryValue::String(s.clone())),
            Value::Boolean(b) => Ok(QueryValue::Boolean(*b)),
            Value::Null => Ok(QueryValue::Null),
            _ => Err(FluxError::SqlParse(format!("Unsupported value type: {:?}", val))),
        }
    }

    fn parse_comparison(left: &Expr, op: &BinaryOperator, right: &Expr) -> Result<Condition> {
        let field = match left {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(idents) => {
                idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
            }
            _ => return Err(FluxError::SqlParse("Left side must be identifier".into())),
        };

        let compare_op = match op {
            BinaryOperator::Eq => CompareOp::Eq,
            BinaryOperator::NotEq => CompareOp::Ne,
            BinaryOperator::Lt => CompareOp::Lt,
            BinaryOperator::LtEq => CompareOp::Le,
            BinaryOperator::Gt => CompareOp::Gt,
            BinaryOperator::GtEq => CompareOp::Ge,
            _ => return Err(FluxError::SqlParse(format!("Unsupported operator: {:?}", op))),
        };

        // Check if it's a time comparison
        if field.to_lowercase() == "time" {
            let ts = Self::parse_timestamp_value(right)?;
            let range = match compare_op {
                CompareOp::Gt | CompareOp::Ge => TimeRange::new(ts, i64::MAX),
                CompareOp::Lt | CompareOp::Le => TimeRange::new(i64::MIN, ts),
                _ => return Err(FluxError::SqlParse("Unsupported time comparison".into())),
            };
            return Ok(Condition::TimeRange(range));
        }

        // Check if it's a string comparison (tag)
        if let Expr::Value(Value::SingleQuotedString(s)) = right {
            return Ok(Condition::TagEquals {
                tag: field,
                value: s.clone(),
            });
        }

        // Field comparison (numeric)
        let value = match right {
            Expr::Value(Value::Number(n, _)) => n.parse::<f64>()
                .map_err(|_| FluxError::SqlParse("Invalid number".into()))?,
            _ => return Err(FluxError::SqlParse("Unsupported value type".into())),
        };

        Ok(Condition::FieldCompare {
            field,
            op: compare_op,
            value,
        })
    }

    fn parse_timestamp_value(expr: &Expr) -> Result<i64> {
        match expr {
            Expr::Value(Value::Number(n, _)) => n.parse::<i64>()
                .map_err(|_| FluxError::SqlParse("Invalid timestamp".into())),
            Expr::Value(Value::SingleQuotedString(s)) => {
                // Try to parse as ISO 8601
                chrono::DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.timestamp_nanos_opt().unwrap_or(0))
                    .map_err(|_| FluxError::SqlParse("Invalid timestamp format".into()))
            }
            _ => Err(FluxError::SqlParse("Unsupported timestamp expression".into())),
        }
    }

    fn parse_group_by(select: &Select) -> Result<Option<GroupBy>> {
        use sqlparser::ast::GroupByExpr;
        
        let expressions = match &select.group_by {
            GroupByExpr::Expressions(exprs) => exprs,
            GroupByExpr::All => return Ok(None),
        };

        if expressions.is_empty() {
            return Ok(None);
        }

        let mut time_bucket = None;
        let mut tags = Vec::new();

        for expr in expressions {
            match expr {
                Expr::Function(func) if func.name.to_string().to_lowercase() == "time" => {
                    // time(1h) style grouping
                    if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                        Value::SingleQuotedString(interval),
                    )))) = func.args.first()
                    {
                        time_bucket = Some(Self::parse_interval(interval)?);
                    }
                }
                Expr::Identifier(ident) => {
                    tags.push(ident.value.clone());
                }
                _ => {}
            }
        }

        Ok(Some(GroupBy { 
            time_bucket, 
            tags,
            fill: None,
        }))
    }

    fn parse_interval(s: &str) -> Result<i64> {
        let s = s.trim();
        let (num_str, unit) = s
            .find(|c: char| !c.is_numeric())
            .map(|i| s.split_at(i))
            .unwrap_or((s, "ns"));

        let num: i64 = num_str
            .parse()
            .map_err(|_| FluxError::SqlParse("Invalid interval number".into()))?;

        let multiplier = match unit.to_lowercase().as_str() {
            "ns" | "" => 1,
            "us" | "µs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            "d" => 86_400_000_000_000,
            _ => return Err(FluxError::SqlParse(format!("Unknown time unit: {}", unit))),
        };

        Ok(num * multiplier)
    }

    fn parse_order_by(query: &SqlQuery) -> Result<Option<OrderBy>> {
        if query.order_by.is_empty() {
            return Ok(None);
        }

        let mut items = Vec::new();
        for order_expr in &query.order_by {
            let field = match &order_expr.expr {
                Expr::Identifier(ident) => ident.value.clone(),
                _ => return Err(FluxError::SqlParse("Unsupported ORDER BY expression".into())),
            };

            let descending = order_expr.asc.map(|asc| !asc).unwrap_or(false);
            let nulls_first = order_expr.nulls_first;

            items.push(OrderByItem {
                field,
                descending,
                nulls_first,
            });
        }

        Ok(Some(OrderBy { items }))
    }

    fn parse_limit(query: &SqlQuery) -> Result<Option<usize>> {
        match &query.limit {
            Some(Expr::Value(Value::Number(n, _))) => {
                let limit = n.parse::<usize>()
                    .map_err(|_| FluxError::SqlParse("Invalid LIMIT value".into()))?;
                Ok(Some(limit))
            }
            Some(_) => Err(FluxError::SqlParse("Unsupported LIMIT expression".into())),
            None => Ok(None),
        }
    }

    fn parse_offset(query: &SqlQuery) -> Result<Option<usize>> {
        match &query.offset {
            Some(offset) => {
                match &offset.value {
                    Expr::Value(Value::Number(n, _)) => {
                        let off = n.parse::<usize>()
                            .map_err(|_| FluxError::SqlParse("Invalid OFFSET value".into()))?;
                        Ok(Some(off))
                    }
                    _ => Err(FluxError::SqlParse("Unsupported OFFSET expression".into())),
                }
            }
            None => Ok(None),
        }
    }

    // ========================================================================
    // UPDATE parsing
    // ========================================================================

    fn parse_update(
        table: &sqlparser::ast::TableWithJoins,
        assignments: &[sqlparser::ast::Assignment],
        selection: &Option<Expr>,
    ) -> Result<Statement> {
        let measurement = match &table.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(FluxError::SqlParse("Invalid table in UPDATE".into())),
        };

        let parsed_assignments: Result<Vec<Assignment>> = assignments
            .iter()
            .map(|a| {
                // In sqlparser 0.41.0, Assignment has `id` field (Vec<Ident>), not `target`
                let field = a.id.iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                let value = Self::parse_value_expr(&a.value)?;
                Ok(Assignment { field, value })
            })
            .collect();

        let where_clause = if let Some(expr) = selection {
            let cond = Self::parse_condition(expr)?;
            Some(WhereClause { conditions: vec![cond] })
        } else {
            None
        };

        Ok(Statement::Update(UpdateStatement {
            measurement,
            assignments: parsed_assignments?,
            where_clause,
        }))
    }

    // ========================================================================
    // DELETE parsing
    // ========================================================================

    fn parse_delete(
        from: &Vec<sqlparser::ast::TableWithJoins>,
        selection: &Option<Expr>,
    ) -> Result<Statement> {
        // In sqlparser 0.41.0, DELETE uses Vec<TableWithJoins> directly
        let measurement = if !from.is_empty() {
            match &from[0].relation {
                TableFactor::Table { name, .. } => name.to_string(),
                _ => return Err(FluxError::SqlParse("Invalid table in DELETE".into())),
            }
        } else {
            return Err(FluxError::SqlParse("Missing FROM in DELETE".into()));
        };

        let where_clause = match selection {
            Some(expr) => {
                let cond = Self::parse_condition(expr)?;
                WhereClause { conditions: vec![cond] }
            }
            None => {
                return Err(FluxError::SqlParse(
                    "DELETE requires a WHERE clause for safety".into()
                ));
            }
        };

        Ok(Statement::Delete(DeleteStatement {
            measurement,
            where_clause,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let query = QueryParser::parse("SELECT * FROM temperature").unwrap();
        match &query.from {
            FromClause::Table(name) => assert_eq!(name, "temperature"),
            _ => panic!("Expected table"),
        }
        assert!(matches!(query.select[0], SelectItem::All));
    }

    #[test]
    fn test_parse_aggregate() {
        let query = QueryParser::parse("SELECT mean(value), max(value) FROM temperature").unwrap();
        assert_eq!(query.select.len(), 2);
        
        if let SelectItem::Aggregate { function, field, .. } = &query.select[0] {
            assert!(matches!(function, AggregateFunc::Mean));
            assert_eq!(field, "value");
        } else {
            panic!("Expected aggregate");
        }
    }

    #[test]
    fn test_parse_where() {
        let query = QueryParser::parse(
            "SELECT * FROM temperature WHERE sensor_id = 'sensor-1' AND value > 20"
        ).unwrap();
        
        assert!(query.where_clause.is_some());
    }

    #[test]
    fn test_parse_limit() {
        let query = QueryParser::parse("SELECT * FROM temperature LIMIT 100").unwrap();
        assert_eq!(query.limit, Some(100));
    }

    #[test]
    fn test_parse_distinct() {
        let query = QueryParser::parse("SELECT DISTINCT sensor_id FROM temperature").unwrap();
        assert!(query.distinct);
    }

    #[test]
    fn test_parse_join() {
        let query = QueryParser::parse(
            "SELECT t.value, s.name FROM temperature t INNER JOIN sensors s ON t.sensor_id = s.id"
        ).unwrap();
        
        match &query.from {
            FromClause::Join(join) => {
                assert!(matches!(join.join_type, JoinType::Inner));
            }
            _ => panic!("Expected join"),
        }
    }

    #[test]
    fn test_parse_in() {
        let query = QueryParser::parse(
            "SELECT * FROM temperature WHERE sensor_id IN ('s1', 's2', 's3')"
        ).unwrap();
        
        assert!(query.where_clause.is_some());
        if let Some(wc) = &query.where_clause {
            if let Condition::In { field, values, negated } = &wc.conditions[0] {
                assert_eq!(field, "sensor_id");
                assert_eq!(values.len(), 3);
                assert!(!negated);
            } else {
                panic!("Expected IN condition");
            }
        }
    }

    #[test]
    fn test_parse_between() {
        let query = QueryParser::parse(
            "SELECT * FROM temperature WHERE value BETWEEN 20 AND 30"
        ).unwrap();
        
        assert!(query.where_clause.is_some());
        if let Some(wc) = &query.where_clause {
            if let Condition::Between { field, low, high, negated } = &wc.conditions[0] {
                assert_eq!(field, "value");
                assert_eq!(low, &QueryValue::Integer(20));
                assert_eq!(high, &QueryValue::Integer(30));
                assert!(!negated);
            } else {
                panic!("Expected BETWEEN condition");
            }
        }
    }

    #[test]
    fn test_parse_like() {
        let query = QueryParser::parse(
            "SELECT * FROM temperature WHERE sensor_id LIKE 'sensor-%'"
        ).unwrap();
        
        assert!(query.where_clause.is_some());
    }

    #[test]
    fn test_parse_is_null() {
        let query = QueryParser::parse(
            "SELECT * FROM temperature WHERE value IS NULL"
        ).unwrap();
        
        assert!(query.where_clause.is_some());
        if let Some(wc) = &query.where_clause {
            if let Condition::IsNull { field, negated } = &wc.conditions[0] {
                assert_eq!(field, "value");
                assert!(!negated);
            } else {
                panic!("Expected IS NULL condition");
            }
        }
    }
}
