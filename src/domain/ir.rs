use super::{DebugSummary, Identifier, Selector, ast};

/// Lowered representation that highlights dataflow between operations.
#[derive(Clone, Debug, PartialEq)]
pub struct OperationGraph {
    pub operations: Vec<OperationNode>,
    pub result: ResultBinding,
}

impl DebugSummary for OperationGraph {
    fn debug_summary(&self) -> String {
        let mut lines = Vec::new();

        lines.push("OperationGraph".to_string());
        lines.push(format!("  operations: {}", self.operations.len()));
        for (idx, op) in self.operations.iter().enumerate() {
            lines.push(format!(
                "    op {idx}: {} -> [{}]",
                op.name,
                op.outputs
                    .iter()
                    .map(|o| o.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));

            match &op.stage {
                OperationStage::IterationInput { selector } => {
                    lines.push(format!(
                        "      stage: iteration_input({})",
                        describe_selector(*selector)
                    ));
                }
                OperationStage::Map { inputs, lambda } => {
                    lines.push(format!(
                        "      stage: map inputs=[{}]",
                        inputs
                            .iter()
                            .map(|i| i.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                    lines.push(format!("      lambda: {}", describe_ir_lambda(lambda)));
                }
                OperationStage::Filter { inputs, lambda } => {
                    lines.push(format!(
                        "      stage: filter inputs=[{}]",
                        inputs
                            .iter()
                            .map(|i| i.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                    lines.push(format!("      lambda: {}", describe_ir_lambda(lambda)));
                }
                OperationStage::Reduce {
                    key,
                    values,
                    lambda,
                } => {
                    lines.push(format!(
                        "      stage: reduce key={} values=[{}]",
                        key,
                        values
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                    lines.push(format!("      lambda: {}", describe_ir_lambda(lambda)));
                }
            }
        }

        lines.push(format!(
            "  result: {} as {}",
            self.result.value, self.result.property
        ));

        lines.join("\n")
    }
}

/// Node within the operation graph.
#[derive(Clone, Debug, PartialEq)]
pub struct OperationNode {
    pub name: Identifier,
    pub stage: OperationStage,
    pub outputs: Vec<Identifier>,
}

/// Captures the operation class and its lambda-level logic.
#[derive(Clone, Debug, PartialEq)]
pub enum OperationStage {
    IterationInput {
        selector: Selector,
    },
    Map {
        inputs: Vec<Identifier>,
        lambda: IrLambda,
    },
    Filter {
        inputs: Vec<Identifier>,
        lambda: IrLambda,
    },
    Reduce {
        key: Identifier,
        values: Vec<Identifier>,
        lambda: IrLambda,
    },
}

/// Describes how the final result vector maps back to node properties.
#[derive(Clone, Debug, PartialEq)]
pub struct ResultBinding {
    pub value: Identifier,
    pub property: Identifier,
}

/// Lambda IR used by map, filter, and reduce.
#[derive(Clone, Debug, PartialEq)]
pub struct IrLambda {
    pub params: Vec<Identifier>,
    pub body: IrExpr,
}

/// Expression tree for lambda bodies in the IR.
#[derive(Clone, Debug, PartialEq)]
pub enum IrExpr {
    Identifier(Identifier),
    Literal(ast::Literal),
    MemberAccess {
        target: Box<IrExpr>,
        access: ast::Accessor,
    },
    Call {
        function: Identifier,
        args: Vec<IrExpr>,
    },
    Binary {
        op: ast::BinaryOp,
        left: Box<IrExpr>,
        right: Box<IrExpr>,
    },
    Unary {
        op: ast::UnaryOp,
        expr: Box<IrExpr>,
    },
    Ternary {
        condition: Box<IrExpr>,
        then_expr: Box<IrExpr>,
        else_expr: Box<IrExpr>,
    },
}

impl From<ast::Lambda> for IrLambda {
    fn from(value: ast::Lambda) -> Self {
        Self {
            params: value.params,
            body: value.body.into(),
        }
    }
}

fn describe_ir_lambda(lambda: &IrLambda) -> String {
    let params = lambda
        .params
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!("lambda ({params}) => {}", describe_ir_expr(&lambda.body))
}

fn describe_ir_expr(expr: &IrExpr) -> String {
    match expr {
        IrExpr::Identifier(id) => id.to_string(),
        IrExpr::Literal(lit) => describe_literal(lit),
        IrExpr::MemberAccess { target, access } => match access {
            ast::Accessor::Property(prop) => format!("{}.{}", describe_ir_expr(target), prop),
            ast::Accessor::Index(idx) => format!("{}[{idx}]", describe_ir_expr(target)),
        },
        IrExpr::Call { function, args } => format!(
            "{}({})",
            function,
            args.iter()
                .map(describe_ir_expr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        IrExpr::Binary { op, left, right } => format!(
            "({} {} {})",
            describe_ir_expr(left),
            describe_binary_op(*op),
            describe_ir_expr(right)
        ),
        IrExpr::Unary { op, expr } => {
            format!("({}{})", describe_unary_op(*op), describe_ir_expr(expr))
        }
        IrExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => format!(
            "({} ? {} : {})",
            describe_ir_expr(condition),
            describe_ir_expr(then_expr),
            describe_ir_expr(else_expr)
        ),
    }
}

fn describe_literal(lit: &ast::Literal) -> String {
    match lit {
        ast::Literal::Int(v) => v.to_string(),
        ast::Literal::Float(v) => v.to_string(),
        ast::Literal::Bool(v) => v.to_string(),
    }
}

fn describe_selector(selector: Selector) -> &'static str {
    match selector {
        Selector::Nodes => "G.NODES",
        Selector::Edges => "G.EDGES",
    }
}

fn describe_binary_op(op: ast::BinaryOp) -> &'static str {
    match op {
        ast::BinaryOp::Add => "+",
        ast::BinaryOp::Sub => "-",
        ast::BinaryOp::Mul => "*",
        ast::BinaryOp::Div => "/",
        ast::BinaryOp::Eq => "==",
        ast::BinaryOp::Ne => "!=",
        ast::BinaryOp::Gt => ">",
        ast::BinaryOp::Lt => "<",
        ast::BinaryOp::Ge => ">=",
        ast::BinaryOp::Le => "<=",
        ast::BinaryOp::BitAnd => "&",
        ast::BinaryOp::BitOr => "|",
        ast::BinaryOp::And => "&&",
        ast::BinaryOp::Or => "||",
    }
}

fn describe_unary_op(op: ast::UnaryOp) -> &'static str {
    match op {
        ast::UnaryOp::Not => "!",
        ast::UnaryOp::BitNot => "~",
    }
}

impl From<ast::Expr> for IrExpr {
    fn from(value: ast::Expr) -> Self {
        match value {
            ast::Expr::Identifier(id) => IrExpr::Identifier(id),
            ast::Expr::Literal(lit) => IrExpr::Literal(lit),
            ast::Expr::MemberAccess { target, access } => IrExpr::MemberAccess {
                target: Box::new((*target).into()),
                access,
            },
            ast::Expr::Call { function, args } => IrExpr::Call {
                function,
                args: args.into_iter().map(|a| a.into()).collect(),
            },
            ast::Expr::Binary { op, left, right } => IrExpr::Binary {
                op,
                left: Box::new((*left).into()),
                right: Box::new((*right).into()),
            },
            ast::Expr::Unary { op, expr } => IrExpr::Unary {
                op,
                expr: Box::new((*expr).into()),
            },
            ast::Expr::Ternary {
                condition,
                then_expr,
                else_expr,
            } => IrExpr::Ternary {
                condition: Box::new((*condition).into()),
                then_expr: Box::new((*then_expr).into()),
                else_expr: Box::new((*else_expr).into()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ast::{BinaryOp, Expr, Identifier, Lambda, Literal};

    #[test]
    fn converts_lambda_into_ir() {
        let lambda = Lambda {
            params: vec![Identifier::new("e")],
            body: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Identifier(Identifier::new("e"))),
                right: Box::new(Expr::Literal(Literal::Int(1))),
            },
        };

        let ir_lambda: IrLambda = lambda.into();
        match ir_lambda.body {
            IrExpr::Binary { op, .. } => assert_eq!(op, BinaryOp::Add),
            other => panic!("unexpected IR expr {other:?}"),
        }
    }
}
