use crate::domain::{
    ast::{self, Identifier},
    gas::{GasProgram, GasType},
    hls_ops::{
        ApplyOp, GatherOp, KernelOpBundle, OperatorBinary, OperatorExpr, OperatorOperand,
        OperatorUnary, ReducerIdentity, ReducerKind, ScatterOp,
    },
    ir::{IrExpr, IrLambda},
};
use thiserror::Error;

/// Errors raised while validating/extracting operator shapes from GAS.
#[derive(Debug, Error, PartialEq)]
pub enum GasToHlsOpsError {
    #[error("scatter key must be edge.dst")]
    ScatterKeyNotDst,
    #[error("scatter value must be scalar numeric; found {0:?}")]
    ScatterValueNot32Bit(GasType),
    #[error("gather input must be scalar numeric; found {0:?}")]
    GatherValueNot32Bit(GasType),
    #[error("scatter expression uses unsupported fields or operators")]
    ScatterUnsupported,
    #[error("gather reducer must take two 32-bit params; found {0} params")]
    GatherArityMismatch(usize),
    #[error("gather reducer must be associative/commutative min/max/sum; unsupported shape")]
    GatherUnsupported,
    #[error("apply input must be scalar numeric; found {0:?}")]
    ApplyInputNot32Bit(GasType),
    #[error("apply expression uses unsupported fields or operators")]
    ApplyUnsupported,
}

/// Validates a GAS program against the fixed S→G→A shape and extracts the operators.
pub fn extract_kernel_ops(gas: &GasProgram) -> Result<KernelOpBundle, GasToHlsOpsError> {
    enforce_dst_key(&gas.scatter.key_lambda)?;
    ensure_numeric_scalar(
        &gas.scatter.value_type,
        GasToHlsOpsError::ScatterValueNot32Bit,
    )?;
    ensure_numeric_scalar(
        &gas.gather.input_value_type,
        GasToHlsOpsError::GatherValueNot32Bit,
    )?;
    ensure_numeric_scalar(&gas.apply.input_type, GasToHlsOpsError::ApplyInputNot32Bit)?;

    let scatter = ScatterOp {
        expr: convert_scatter_expr(&gas.scatter.value_lambda)?,
    };
    let gather = convert_gather(&gas.gather.reducer)?;
    let apply = convert_apply(&gas.apply.lambda, gas.apply.target_property.as_str())?;

    Ok(KernelOpBundle {
        scatter,
        gather,
        apply,
    })
}

fn enforce_dst_key(lambda: &IrLambda) -> Result<(), GasToHlsOpsError> {
    if lambda.params.len() != 1 {
        return Err(GasToHlsOpsError::ScatterKeyNotDst);
    }
    let param = lambda.params[0].as_str();
    if matches_dst(&lambda.body, param) {
        Ok(())
    } else {
        Err(GasToHlsOpsError::ScatterKeyNotDst)
    }
}

fn matches_dst(expr: &IrExpr, edge_param: &str) -> bool {
    match expr {
        IrExpr::MemberAccess { target, access } => match (target.as_ref(), access) {
            (IrExpr::Identifier(id), ast::Accessor::Property(prop))
                if id.as_str() == edge_param && prop.as_str() == "dst" =>
            {
                true
            }
            _ => false,
        },
        _ => false,
    }
}

fn ensure_numeric_scalar<F, E>(ty: &GasType, mk_err: F) -> Result<(), E>
where
    F: Fn(GasType) -> E,
{
    match ty {
        GasType::Int(_)
        | GasType::Float
        | GasType::Fixed { .. }
        | GasType::Bool
        | GasType::Unknown => Ok(()),
        other => Err(mk_err(other.clone())),
    }
}

fn convert_scatter_expr(lambda: &IrLambda) -> Result<OperatorExpr, GasToHlsOpsError> {
    let edge_param = lambda
        .params
        .get(0)
        .ok_or(GasToHlsOpsError::ScatterUnsupported)?;
    convert_expr(&lambda.body, ExprContext::scatter(edge_param))
        .map_err(|_| GasToHlsOpsError::ScatterUnsupported)
}

fn convert_apply(
    lambda: &Option<IrLambda>,
    target_property: &str,
) -> Result<ApplyOp, GasToHlsOpsError> {
    let expr = match lambda {
        None => OperatorExpr::Operand(OperatorOperand::GatherValue),
        Some(l) => convert_expr(&l.body, ExprContext::apply(l, target_property))
            .map_err(|_| GasToHlsOpsError::ApplyUnsupported)?,
    };
    Ok(ApplyOp { expr })
}

fn convert_gather(lambda: &IrLambda) -> Result<GatherOp, GasToHlsOpsError> {
    if lambda.params.len() != 2 {
        return Err(GasToHlsOpsError::GatherArityMismatch(lambda.params.len()));
    }
    let lhs = lambda.params[0].as_str();
    let rhs = lambda.params[1].as_str();

    match classify_reducer(&lambda.body, lhs, rhs) {
        Some(ReducerKind::Min) => Ok(GatherOp {
            kind: ReducerKind::Min,
            identity: ReducerIdentity::PositiveInfinity,
        }),
        Some(ReducerKind::Max) => Ok(GatherOp {
            kind: ReducerKind::Max,
            identity: ReducerIdentity::NegativeInfinity,
        }),
        Some(ReducerKind::Sum) => Ok(GatherOp {
            kind: ReducerKind::Sum,
            identity: ReducerIdentity::Zero,
        }),
        Some(ReducerKind::Or) => Ok(GatherOp {
            kind: ReducerKind::Or,
            identity: ReducerIdentity::Zero,
        }),
        Some(ReducerKind::MaskedMinIgnoreZero { active_mask }) => Ok(GatherOp {
            kind: ReducerKind::MaskedMinIgnoreZero { active_mask },
            identity: ReducerIdentity::Zero,
        }),
        None => Err(GasToHlsOpsError::GatherUnsupported),
    }
}

fn classify_reducer(expr: &IrExpr, acc: &str, incoming: &str) -> Option<ReducerKind> {
    match expr {
        IrExpr::Binary {
            op: ast::BinaryOp::Add,
            left,
            right,
        } => {
            if matches_param_pair(left, right, acc, incoming) {
                Some(ReducerKind::Sum)
            } else {
                None
            }
        }
        IrExpr::Binary {
            op: ast::BinaryOp::BitOr,
            left,
            right,
        } => {
            if matches_param_pair(left, right, acc, incoming) {
                Some(ReducerKind::Or)
            } else {
                None
            }
        }
        IrExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => classify_bfs_masked_min(condition, then_expr, else_expr, acc, incoming)
            .or_else(|| classify_min_max(condition, then_expr, else_expr, acc, incoming)),
        _ => None,
    }
}

fn classify_bfs_masked_min(
    cond: &IrExpr,
    then_expr: &IrExpr,
    else_expr: &IrExpr,
    acc: &str,
    incoming: &str,
) -> Option<ReducerKind> {
    // Match:
    //   incoming == 0 ? acc :
    //     (acc == 0 ? incoming :
    //       ((acc & low_mask) > (incoming & low_mask) ? incoming : acc))
    if !matches_eq_zero(cond, incoming) {
        return None;
    }
    if !matches_param(then_expr, acc) {
        return None;
    }

    let IrExpr::Ternary {
        condition: cond2,
        then_expr: then2,
        else_expr: else2,
    } = else_expr
    else {
        return None;
    };
    if !matches_eq_zero(cond2, acc) {
        return None;
    }
    if !matches_param(then2, incoming) {
        return None;
    }

    let IrExpr::Ternary {
        condition: cond3,
        then_expr: then3,
        else_expr: else3,
    } = else2.as_ref()
    else {
        return None;
    };
    if !matches_param(then3, incoming) || !matches_param(else3, acc) {
        return None;
    }

    let IrExpr::Binary {
        op: ast::BinaryOp::Gt,
        left,
        right,
    } = cond3.as_ref()
    else {
        return None;
    };

    let Some(mask_left) = matches_bitand_mask(left, acc) else {
        return None;
    };
    let Some(mask_right) = matches_bitand_mask(right, incoming) else {
        return None;
    };
    if mask_left != mask_right {
        return None;
    }

    let low_mask = mask_left;
    if low_mask < 0 {
        return None;
    }
    let low_mask = low_mask as u64;
    let active_mask = low_mask + 1;
    if active_mask == 0 || (active_mask & (active_mask - 1)) != 0 {
        return None;
    }
    if low_mask != active_mask - 1 {
        return None;
    }
    if active_mask > u32::MAX as u64 {
        return None;
    }

    Some(ReducerKind::MaskedMinIgnoreZero {
        active_mask: active_mask as u32,
    })
}

fn matches_eq_zero(expr: &IrExpr, param: &str) -> bool {
    match expr {
        IrExpr::Binary {
            op: ast::BinaryOp::Eq,
            left,
            right,
        } => {
            (matches_param(left, param) && matches_int_literal(right, 0))
                || (matches_int_literal(left, 0) && matches_param(right, param))
        }
        _ => false,
    }
}

fn matches_int_literal(expr: &IrExpr, value: i64) -> bool {
    matches!(expr, IrExpr::Literal(ast::Literal::Int(v)) if *v == value)
}

fn matches_bitand_mask(expr: &IrExpr, param: &str) -> Option<i64> {
    match expr {
        IrExpr::Binary {
            op: ast::BinaryOp::BitAnd,
            left,
            right,
        } => {
            if matches_param(left, param) {
                if let IrExpr::Literal(ast::Literal::Int(mask)) = right.as_ref() {
                    return Some(*mask);
                }
            }
            if matches_param(right, param) {
                if let IrExpr::Literal(ast::Literal::Int(mask)) = left.as_ref() {
                    return Some(*mask);
                }
            }
            None
        }
        _ => None,
    }
}

fn classify_min_max(
    cond: &IrExpr,
    then_expr: &IrExpr,
    else_expr: &IrExpr,
    acc: &str,
    incoming: &str,
) -> Option<ReducerKind> {
    match cond {
        IrExpr::Binary {
            op: ast::BinaryOp::Gt,
            left,
            right,
        } => {
            if matches_param(left, acc) && matches_param(right, incoming) {
                if matches_param(then_expr, incoming) && matches_param(else_expr, acc) {
                    return Some(ReducerKind::Min);
                }
                if matches_param(then_expr, acc) && matches_param(else_expr, incoming) {
                    return Some(ReducerKind::Max);
                }
            }
            None
        }
        IrExpr::Binary {
            op: ast::BinaryOp::Lt,
            left,
            right,
        } => {
            if matches_param(left, acc) && matches_param(right, incoming) {
                if matches_param(then_expr, acc) && matches_param(else_expr, incoming) {
                    return Some(ReducerKind::Min);
                }
                if matches_param(then_expr, incoming) && matches_param(else_expr, acc) {
                    return Some(ReducerKind::Max);
                }
            }
            None
        }
        _ => None,
    }
}

fn matches_param(expr: &IrExpr, param: &str) -> bool {
    matches!(expr, IrExpr::Identifier(id) if id.as_str() == param)
}

fn matches_param_pair(lhs: &IrExpr, rhs: &IrExpr, a: &str, b: &str) -> bool {
    (matches_param(lhs, a) && matches_param(rhs, b))
        || (matches_param(lhs, b) && matches_param(rhs, a))
}

#[derive(Clone, Copy)]
enum ExprContext<'a> {
    Scatter {
        edge_param: &'a Identifier,
    },
    Apply {
        gather_params: &'a [Identifier],
        target_property: &'a str,
    },
}

impl<'a> ExprContext<'a> {
    fn scatter(edge_param: &'a Identifier) -> Self {
        ExprContext::Scatter { edge_param }
    }

    fn apply(lambda: &'a IrLambda, target_property: &'a str) -> Self {
        ExprContext::Apply {
            gather_params: lambda.params.as_slice(),
            target_property,
        }
    }
}

fn convert_expr(expr: &IrExpr, ctx: ExprContext<'_>) -> Result<OperatorExpr, ()> {
    match expr {
        IrExpr::Identifier(id) => convert_identifier(id, ctx),
        IrExpr::Literal(ast::Literal::Int(v)) => {
            Ok(OperatorExpr::Operand(OperatorOperand::ConstInt(*v)))
        }
        IrExpr::Literal(ast::Literal::Float(v)) => {
            let parsed = v.parse::<f64>().map_err(|_| ())?;
            Ok(OperatorExpr::Operand(OperatorOperand::ConstFloat(parsed)))
        }
        IrExpr::Literal(ast::Literal::Bool(_)) => Err(()),
        IrExpr::Unary { op, expr } => {
            let unary = match op {
                ast::UnaryOp::Not => OperatorUnary::LogicalNot,
                ast::UnaryOp::BitNot => OperatorUnary::BitNot,
            };
            Ok(OperatorExpr::Unary {
                op: unary,
                expr: Box::new(convert_expr(expr, ctx)?),
            })
        }
        IrExpr::Binary { op, left, right } => convert_binary(op, left, right, ctx),
        IrExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => Ok(OperatorExpr::Ternary {
            condition: Box::new(convert_expr(condition, ctx)?),
            then_expr: Box::new(convert_expr(then_expr, ctx)?),
            else_expr: Box::new(convert_expr(else_expr, ctx)?),
        }),
        IrExpr::MemberAccess { .. } => convert_member(expr, ctx),
        IrExpr::Call { .. } => Err(()),
    }
}

fn convert_identifier(id: &Identifier, ctx: ExprContext<'_>) -> Result<OperatorExpr, ()> {
    match ctx {
        ExprContext::Scatter { .. } => Err(()),
        ExprContext::Apply { gather_params, .. } if gather_params.iter().any(|p| p == id) => {
            Ok(OperatorExpr::Operand(OperatorOperand::GatherValue))
        }
        ExprContext::Apply { .. } => Err(()),
    }
}

fn convert_member(expr: &IrExpr, ctx: ExprContext<'_>) -> Result<OperatorExpr, ()> {
    match ctx {
        ExprContext::Scatter { edge_param } => match classify_edge_access(expr, edge_param) {
            Some(EdgeAccess::SrcProp) => Ok(OperatorExpr::Operand(OperatorOperand::ScatterSrcProp)),
            Some(EdgeAccess::SrcId) => Ok(OperatorExpr::Operand(OperatorOperand::ScatterSrcId)),
            Some(EdgeAccess::DstId) => Ok(OperatorExpr::Operand(OperatorOperand::ScatterDstId)),
            Some(EdgeAccess::EdgeWeight) => {
                Ok(OperatorExpr::Operand(OperatorOperand::ScatterEdgeWeight))
            }
            None => Err(()),
        },
        ExprContext::Apply {
            target_property, ..
        } => {
            if let Some(prop) = self_property_name(expr) {
                if prop == target_property {
                    Ok(OperatorExpr::Operand(OperatorOperand::OldProp))
                } else {
                    Ok(OperatorExpr::Operand(OperatorOperand::OldAux))
                }
            } else {
                Err(())
            }
        }
    }
}

fn convert_binary(
    op: &ast::BinaryOp,
    left: &IrExpr,
    right: &IrExpr,
    ctx: ExprContext<'_>,
) -> Result<OperatorExpr, ()> {
    let binop = match op {
        ast::BinaryOp::Add => OperatorBinary::Add,
        ast::BinaryOp::Sub => OperatorBinary::Sub,
        ast::BinaryOp::Mul => OperatorBinary::Mul,
        ast::BinaryOp::Div => OperatorBinary::Div,
        ast::BinaryOp::Lt => OperatorBinary::Lt,
        ast::BinaryOp::Gt => OperatorBinary::Gt,
        ast::BinaryOp::Eq => OperatorBinary::Eq,
        ast::BinaryOp::Ne => OperatorBinary::Ne,
        ast::BinaryOp::Le => OperatorBinary::Le,
        ast::BinaryOp::Ge => OperatorBinary::Ge,
        ast::BinaryOp::And => OperatorBinary::And,
        ast::BinaryOp::Or => OperatorBinary::Or,
        ast::BinaryOp::BitAnd => OperatorBinary::BitAnd,
        ast::BinaryOp::BitOr => OperatorBinary::BitOr,
    };

    Ok(OperatorExpr::Binary {
        op: binop,
        left: Box::new(convert_expr(left, ctx)?),
        right: Box::new(convert_expr(right, ctx)?),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EdgeAccess {
    SrcProp,
    SrcId,
    DstId,
    EdgeWeight,
}

fn classify_edge_access(expr: &IrExpr, edge_param: &Identifier) -> Option<EdgeAccess> {
    match expr {
        IrExpr::MemberAccess { target, access } => match (target.as_ref(), access) {
            (IrExpr::Identifier(id), ast::Accessor::Property(prop)) if id == edge_param => {
                match prop.as_str() {
                    "dst" => Some(EdgeAccess::DstId),
                    "src" => None,
                    "weight" => Some(EdgeAccess::EdgeWeight),
                    _ => None,
                }
            }
            (
                IrExpr::MemberAccess {
                    target: inner_target,
                    access: inner_access,
                },
                ast::Accessor::Property(prop),
            ) => match (inner_target.as_ref(), inner_access) {
                (IrExpr::Identifier(id), ast::Accessor::Property(parent_prop))
                    if id == edge_param
                        && parent_prop.as_str() == "src"
                        && prop.as_str() == "id" =>
                {
                    Some(EdgeAccess::SrcId)
                }
                (IrExpr::Identifier(id), ast::Accessor::Property(parent_prop))
                    if id == edge_param && parent_prop.as_str() == "src" =>
                {
                    Some(EdgeAccess::SrcProp)
                }
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

fn self_property_name(expr: &IrExpr) -> Option<&str> {
    match expr {
        IrExpr::MemberAccess {
            target,
            access: ast::Accessor::Property(prop),
        } => match target.as_ref() {
            IrExpr::Identifier(id) if id.as_str() == "self" => Some(prop.as_str()),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{gas_lower::lower_to_gas, ir_builder::LoweredProgram};
    use std::fs;
    use std::path::PathBuf;

    fn load_gas(app: &str) -> GasProgram {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let source_path = manifest.join("apps").join(format!("{app}.dsl"));
        let source = fs::read_to_string(&source_path).expect("app source");
        let lowered = LoweredProgram::parse_and_lower(&source).expect("lower");
        lower_to_gas(&lowered.ast, &lowered.ir).expect("gas")
    }

    #[test]
    fn extracts_sssp_operators() {
        let gas = load_gas("sssp");
        let ops = extract_kernel_ops(&gas).expect("extract");

        assert!(matches!(
            ops.scatter.expr,
            OperatorExpr::Binary {
                op: OperatorBinary::Add,
                ..
            }
        ));
        assert_eq!(ops.gather.kind, ReducerKind::Min);
        assert!(matches!(ops.apply.expr, OperatorExpr::Ternary { .. }));
    }

    #[test]
    fn extracts_cc_operators_without_apply_lambda() {
        let gas = load_gas("connected_components");
        let ops = extract_kernel_ops(&gas).expect("extract");
        assert!(matches!(
            ops.scatter.expr,
            OperatorExpr::Operand(OperatorOperand::ScatterSrcProp)
        ));
        assert_eq!(ops.gather.kind, ReducerKind::Min);
        assert!(matches!(
            ops.apply.expr,
            OperatorExpr::Operand(OperatorOperand::GatherValue)
        ));
    }

    fn uses_operand(expr: &OperatorExpr, needle: &OperatorOperand) -> bool {
        match expr {
            OperatorExpr::Operand(opnd) => opnd == needle,
            OperatorExpr::Unary { expr, .. } => uses_operand(expr, needle),
            OperatorExpr::Binary { left, right, .. } => {
                uses_operand(left, needle) || uses_operand(right, needle)
            }
            OperatorExpr::Ternary {
                condition,
                then_expr,
                else_expr,
            } => {
                uses_operand(condition, needle)
                    || uses_operand(then_expr, needle)
                    || uses_operand(else_expr, needle)
            }
        }
    }

    #[test]
    fn extracts_pagerank_uses_aux_for_out_degree() {
        let gas = load_gas("pagerank");
        let ops = extract_kernel_ops(&gas).expect("extract");

        assert!(matches!(
            ops.scatter.expr,
            OperatorExpr::Operand(OperatorOperand::ScatterSrcProp)
        ));
        assert_eq!(ops.gather.kind, ReducerKind::Sum);
        assert!(
            uses_operand(&ops.apply.expr, &OperatorOperand::OldAux),
            "pagerank apply must read out-degree via OldAux"
        );
        assert!(
            !uses_operand(&ops.apply.expr, &OperatorOperand::OldProp),
            "pagerank apply should not treat the target property as the out-degree"
        );
    }

    #[test]
    fn extracts_bfs_masked_min_reducer() {
        let gas = load_gas("bfs");
        let ops = extract_kernel_ops(&gas).expect("extract");

        assert!(matches!(
            ops.gather.kind,
            ReducerKind::MaskedMinIgnoreZero {
                active_mask: 2147483648
            }
        ));
        assert_eq!(ops.gather.identity, ReducerIdentity::Zero);
    }

    fn expr_has_unary(expr: &OperatorExpr, want: OperatorUnary) -> bool {
        match expr {
            OperatorExpr::Unary { op, expr } => *op == want || expr_has_unary(expr, want),
            OperatorExpr::Binary { left, right, .. } => {
                expr_has_unary(left, want) || expr_has_unary(right, want)
            }
            OperatorExpr::Ternary {
                condition,
                then_expr,
                else_expr,
            } => {
                expr_has_unary(condition, want)
                    || expr_has_unary(then_expr, want)
                    || expr_has_unary(else_expr, want)
            }
            OperatorExpr::Operand(_) => false,
        }
    }

    fn expr_has_binary(expr: &OperatorExpr, want: OperatorBinary) -> bool {
        match expr {
            OperatorExpr::Binary { op, left, right } => {
                *op == want || expr_has_binary(left, want) || expr_has_binary(right, want)
            }
            OperatorExpr::Unary { expr, .. } => expr_has_binary(expr, want),
            OperatorExpr::Ternary {
                condition,
                then_expr,
                else_expr,
            } => {
                expr_has_binary(condition, want)
                    || expr_has_binary(then_expr, want)
                    || expr_has_binary(else_expr, want)
            }
            OperatorExpr::Operand(_) => false,
        }
    }

    #[test]
    fn extracts_bitwise_and_unary_ops() {
        let source = r#"
{
    Node: { dist: int<32> }
    Edge: {}
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    updates = map([edges], lambda e: (e.src.dist & 3) | 1)
    mins = reduce(key=dst_ids, values=[updates], function=lambda x, y: x > y ? y : x)
    out = map([mins], lambda d: (self.dist & ~1) | (d & 1))
    return out as result_node_prop.dist
}
"#;
        let lowered = LoweredProgram::parse_and_lower(source).expect("lower");
        let gas = lower_to_gas(&lowered.ast, &lowered.ir).expect("gas");
        let ops = extract_kernel_ops(&gas).expect("extract");

        assert!(expr_has_binary(&ops.scatter.expr, OperatorBinary::BitAnd));
        assert!(expr_has_binary(&ops.scatter.expr, OperatorBinary::BitOr));
        assert!(expr_has_unary(&ops.apply.expr, OperatorUnary::BitNot));
        assert!(expr_has_binary(&ops.apply.expr, OperatorBinary::BitAnd));
        assert!(expr_has_binary(&ops.apply.expr, OperatorBinary::BitOr));
    }
}
