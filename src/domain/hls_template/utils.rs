use crate::domain::{
    hls::{
        HlsBinaryOp, HlsExpr, HlsForLoop, HlsIdentifier, HlsLiteral, HlsStatement, HlsType,
        HlsVarDecl, LoopIncrement, LoopInitializer, LoopLabel,
    },
    hls_ops::{
        OperatorBinary, OperatorExpr, OperatorOperand, OperatorUnary, ReducerIdentity, ReducerKind,
    },
};

use super::HlsTemplateError;

pub(crate) fn ident(name: &str) -> Result<HlsIdentifier, HlsTemplateError> {
    Ok(HlsIdentifier::new(name)?)
}

pub(crate) fn custom(name: &str) -> HlsType {
    HlsType::Custom(name.to_string())
}

pub(crate) fn literal_bool(value: bool) -> HlsExpr {
    HlsExpr::Literal(HlsLiteral::Bool(value))
}

pub(crate) fn literal_int(value: i64) -> HlsExpr {
    HlsExpr::Literal(HlsLiteral::Int(value))
}

pub(crate) fn literal_uint(value: u64) -> HlsExpr {
    HlsExpr::Literal(HlsLiteral::UInt(value))
}

pub(crate) fn binary(op: HlsBinaryOp, left: HlsExpr, right: HlsExpr) -> HlsExpr {
    HlsExpr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

pub(crate) fn assignment(target: HlsExpr, value: HlsExpr) -> HlsStatement {
    HlsStatement::Assignment { target, value }
}

pub(crate) fn method_call(
    target: HlsExpr,
    method: &str,
    args: Vec<HlsExpr>,
) -> Result<HlsExpr, HlsTemplateError> {
    Ok(HlsExpr::MethodCall {
        target: Box::new(target),
        method: ident(method)?,
        args,
    })
}

pub(crate) fn member_expr(target: HlsExpr, field: &str) -> Result<HlsExpr, HlsTemplateError> {
    Ok(HlsExpr::Member {
        target: Box::new(target),
        field: ident(field)?,
    })
}

pub(crate) fn range_method(
    target: HlsExpr,
    high: HlsExpr,
    low: HlsExpr,
) -> Result<HlsExpr, HlsTemplateError> {
    method_call(target, "range", vec![high, low])
}

pub(crate) fn index_ident(name: &str, idx: HlsExpr) -> Result<HlsExpr, HlsTemplateError> {
    Ok(HlsExpr::Index {
        target: Box::new(HlsExpr::Identifier(ident(name)?)),
        index: Box::new(idx),
    })
}

pub(crate) fn int_decl(name: &str, init: HlsExpr) -> Result<HlsVarDecl, HlsTemplateError> {
    Ok(HlsVarDecl {
        name: ident(name)?,
        ty: HlsType::Int32,
        init: Some(init),
    })
}

pub(crate) fn raw(text: &str) -> HlsStatement {
    HlsStatement::Raw(text.to_string())
}

pub(crate) fn render_operator_expr(
    expr: &OperatorExpr,
    leaf_mapper: &mut dyn FnMut(&OperatorOperand) -> Option<HlsExpr>,
) -> Result<HlsExpr, HlsTemplateError> {
    match expr {
        OperatorExpr::Operand(opnd) => leaf_mapper(opnd)
            .ok_or_else(|| HlsTemplateError::UnsupportedOperator("missing operand mapping")),
        OperatorExpr::Unary { op, expr } => Ok(HlsExpr::Unary {
            op: match op {
                OperatorUnary::LogicalNot => crate::domain::hls::HlsUnaryOp::LogicalNot,
                OperatorUnary::BitNot => crate::domain::hls::HlsUnaryOp::BitNot,
            },
            expr: Box::new(render_operator_expr(expr, leaf_mapper)?),
        }),
        OperatorExpr::Binary {
            op: OperatorBinary::Div,
            left,
            right,
        } => {
            let left_expr = render_operator_expr(left, leaf_mapper)?;
            let right_expr = render_operator_expr(right, leaf_mapper)?;

            // Guard division-by-zero without forcing the division result to be
            // truncated to `distance_t` before it participates in larger
            // expressions. Casting the `(left / right)` branch to `distance_t`
            // changes fixed-point rounding (e.g., PageRank) and can cause
            // FPGA-vs-host mismatches.
            //
            // Use `decltype` so both branches of the ternary share the natural
            // widened type produced by `ap_fixed` operators.
            let left_rendered = left_expr.render();
            let right_rendered = right_expr.render();
            Ok(HlsExpr::Raw(format!(
                "((({r}) == decltype(({r}) + 0)(0)) ? decltype(({l}) / ({r}))(0) : (({l}) / ({r})))",
                l = left_rendered,
                r = right_rendered,
            )))
        }
        OperatorExpr::Binary { op, left, right } => Ok(HlsExpr::Binary {
            op: map_operator_binary(*op)?,
            left: Box::new(render_operator_expr(left, leaf_mapper)?),
            right: Box::new(render_operator_expr(right, leaf_mapper)?),
        }),
        OperatorExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => Ok(HlsExpr::Ternary {
            condition: Box::new(render_operator_expr(condition, leaf_mapper)?),
            then_expr: Box::new(render_operator_expr(then_expr, leaf_mapper)?),
            else_expr: Box::new(render_operator_expr(else_expr, leaf_mapper)?),
        }),
    }
}

/// Ensures every ternary expression has both branches cast to the same target type.
///
/// This avoids ambiguous conditional-expression typing in Vitis HLS when mixed-width
/// `ap_int`/`ap_uint` intermediate expressions appear under `?:`.
pub(crate) fn cast_ternary_branches(expr: HlsExpr, target_type: HlsType) -> HlsExpr {
    fn cast_if_needed(expr: HlsExpr, target_type: &HlsType) -> HlsExpr {
        match &expr {
            HlsExpr::Cast {
                target_type: existing,
                ..
            } if existing == target_type => expr,
            _ => HlsExpr::Cast {
                target_type: target_type.clone(),
                expr: Box::new(expr),
            },
        }
    }

    match expr {
        HlsExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => HlsExpr::Ternary {
            condition: Box::new(cast_ternary_branches(*condition, target_type.clone())),
            then_expr: Box::new(cast_if_needed(
                cast_ternary_branches(*then_expr, target_type.clone()),
                &target_type,
            )),
            else_expr: Box::new(cast_if_needed(
                cast_ternary_branches(*else_expr, target_type.clone()),
                &target_type,
            )),
        },
        HlsExpr::Unary { op, expr } => HlsExpr::Unary {
            op,
            expr: Box::new(cast_ternary_branches(*expr, target_type)),
        },
        HlsExpr::Binary { op, left, right } => HlsExpr::Binary {
            op,
            left: Box::new(cast_ternary_branches(*left, target_type.clone())),
            right: Box::new(cast_ternary_branches(*right, target_type)),
        },
        HlsExpr::Member { target, field } => HlsExpr::Member {
            target: Box::new(cast_ternary_branches(*target, target_type)),
            field,
        },
        HlsExpr::Index { target, index } => HlsExpr::Index {
            target: Box::new(cast_ternary_branches(*target, target_type.clone())),
            index: Box::new(cast_ternary_branches(*index, target_type)),
        },
        HlsExpr::Cast {
            target_type: cast_ty,
            expr,
        } => HlsExpr::Cast {
            target_type: cast_ty,
            expr: Box::new(cast_ternary_branches(*expr, target_type)),
        },
        HlsExpr::MethodCall {
            target,
            method,
            args,
        } => HlsExpr::MethodCall {
            target: Box::new(cast_ternary_branches(*target, target_type.clone())),
            method,
            args: args
                .into_iter()
                .map(|a| cast_ternary_branches(a, target_type.clone()))
                .collect(),
        },
        HlsExpr::ReinterpretCast {
            target_type: cast_ty,
            expr,
        } => HlsExpr::ReinterpretCast {
            target_type: cast_ty,
            expr: Box::new(cast_ternary_branches(*expr, target_type)),
        },
        other => other,
    }
}

pub(crate) fn expr_uses_operand(expr: &OperatorExpr, needle: &OperatorOperand) -> bool {
    match expr {
        OperatorExpr::Operand(opnd) => opnd == needle,
        OperatorExpr::Unary { expr, .. } => expr_uses_operand(expr, needle),
        OperatorExpr::Binary { left, right, .. } => {
            expr_uses_operand(left, needle) || expr_uses_operand(right, needle)
        }
        OperatorExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => {
            expr_uses_operand(condition, needle)
                || expr_uses_operand(then_expr, needle)
                || expr_uses_operand(else_expr, needle)
        }
    }
}

fn map_operator_binary(op: OperatorBinary) -> Result<HlsBinaryOp, HlsTemplateError> {
    match op {
        OperatorBinary::Add => Ok(HlsBinaryOp::Add),
        OperatorBinary::Sub => Ok(HlsBinaryOp::Sub),
        OperatorBinary::Mul => Ok(HlsBinaryOp::Mul),
        OperatorBinary::Div => Ok(HlsBinaryOp::Div),
        OperatorBinary::Lt => Ok(HlsBinaryOp::Lt),
        OperatorBinary::Gt => Ok(HlsBinaryOp::Gt),
        OperatorBinary::Eq => Ok(HlsBinaryOp::Eq),
        OperatorBinary::Ne => Ok(HlsBinaryOp::Ne),
        OperatorBinary::Le => Ok(HlsBinaryOp::Le),
        OperatorBinary::Ge => Ok(HlsBinaryOp::Ge),
        OperatorBinary::And => Ok(HlsBinaryOp::And),
        OperatorBinary::Or => Ok(HlsBinaryOp::Or),
        OperatorBinary::BitAnd => Ok(HlsBinaryOp::BitAnd),
        OperatorBinary::BitOr => Ok(HlsBinaryOp::BitOr),
    }
}

pub(crate) fn reducer_identity_expr(
    identity: ReducerIdentity,
) -> Result<HlsExpr, HlsTemplateError> {
    Ok(match identity {
        ReducerIdentity::Zero => literal_uint(0),
        ReducerIdentity::PositiveInfinity => HlsExpr::Identifier(ident("INFINITY_POD")?),
        ReducerIdentity::NegativeInfinity => HlsExpr::Identifier(ident("NEG_INFINITY_POD")?),
    })
}

pub(crate) fn reducer_combine_expr(
    kind: ReducerKind,
    current: HlsExpr,
    incoming: HlsExpr,
    identity: HlsExpr,
    result_type: Option<HlsType>,
) -> HlsExpr {
    let equals_identity = binary(HlsBinaryOp::Eq, current.clone(), identity.clone());
    let incoming_equals_identity = binary(HlsBinaryOp::Eq, incoming.clone(), identity);

    let cast_if_needed = |expr: HlsExpr| match &result_type {
        Some(ty) => HlsExpr::Cast {
            target_type: ty.clone(),
            expr: Box::new(expr),
        },
        None => expr,
    };

    match kind {
        // OR: identity is 0 and (0 | x) == x, so no identity check needed.
        ReducerKind::Or => cast_if_needed(binary(HlsBinaryOp::BitOr, current, incoming)),
        ReducerKind::Sum => HlsExpr::Ternary {
            condition: Box::new(equals_identity.clone()),
            then_expr: Box::new(cast_if_needed(incoming.clone())),
            else_expr: Box::new(cast_if_needed(binary(HlsBinaryOp::Add, current, incoming))),
        },
        ReducerKind::Min => {
            let incoming_for_identity = incoming.clone();
            let choose = HlsExpr::Ternary {
                condition: Box::new(binary(HlsBinaryOp::Lt, incoming.clone(), current.clone())),
                then_expr: Box::new(cast_if_needed(incoming)),
                else_expr: Box::new(cast_if_needed(current.clone())),
            };
            HlsExpr::Ternary {
                condition: Box::new(equals_identity),
                then_expr: Box::new(cast_if_needed(incoming_for_identity)),
                else_expr: Box::new(choose),
            }
        }
        ReducerKind::Max => {
            let incoming_for_identity = incoming.clone();
            let choose = HlsExpr::Ternary {
                condition: Box::new(binary(HlsBinaryOp::Gt, incoming.clone(), current.clone())),
                then_expr: Box::new(cast_if_needed(incoming)),
                else_expr: Box::new(cast_if_needed(current.clone())),
            };
            HlsExpr::Ternary {
                condition: Box::new(equals_identity),
                then_expr: Box::new(cast_if_needed(incoming_for_identity)),
                else_expr: Box::new(choose),
            }
        }
        ReducerKind::MaskedMinIgnoreZero { active_mask } => {
            let low_mask = literal_uint(active_mask.saturating_sub(1) as u64);
            let masked_current = binary(HlsBinaryOp::BitAnd, current.clone(), low_mask.clone());
            let masked_incoming = binary(HlsBinaryOp::BitAnd, incoming.clone(), low_mask);

            let choose = HlsExpr::Ternary {
                condition: Box::new(binary(HlsBinaryOp::Gt, masked_current, masked_incoming)),
                then_expr: Box::new(cast_if_needed(incoming.clone())),
                else_expr: Box::new(cast_if_needed(current.clone())),
            };

            // If incoming is identity (0), keep current.
            // Else if current is identity (0), take incoming.
            // Else choose min by masked low bits (active bit ignored).
            HlsExpr::Ternary {
                condition: Box::new(incoming_equals_identity),
                then_expr: Box::new(cast_if_needed(current.clone())),
                else_expr: Box::new(HlsExpr::Ternary {
                    condition: Box::new(equals_identity),
                    then_expr: Box::new(cast_if_needed(incoming)),
                    else_expr: Box::new(choose),
                }),
            }
        }
    }
}

/// Zero-sentinel combine expression.
///
/// `check_incoming = true`  (mergers):  `(incoming != 0) ? op(current, incoming) : current`
///   — incoming can be empty (0); skip zero updates, keep current.
///
/// `check_incoming = false` (reduce):   `(current != 0) ? op(current, incoming) : incoming`
///   — URAM is zero-initialized; 0 means "empty slot", first write replaces it.
pub(crate) fn reducer_combine_expr_zero_sentinel(
    kind: ReducerKind,
    current: HlsExpr,
    incoming: HlsExpr,
    result_type: Option<HlsType>,
    check_incoming: bool,
) -> HlsExpr {
    let cast_if_needed = |expr: HlsExpr| match &result_type {
        Some(ty) => HlsExpr::Cast {
            target_type: ty.clone(),
            expr: Box::new(expr),
        },
        None => expr,
    };

    let op_expr = match kind {
        ReducerKind::Or => binary(HlsBinaryOp::BitOr, current.clone(), incoming.clone()),
        ReducerKind::Sum => binary(HlsBinaryOp::Add, current.clone(), incoming.clone()),
        ReducerKind::Min => HlsExpr::Ternary {
            condition: Box::new(binary(HlsBinaryOp::Lt, current.clone(), incoming.clone())),
            then_expr: Box::new(current.clone()),
            else_expr: Box::new(incoming.clone()),
        },
        ReducerKind::Max => HlsExpr::Ternary {
            condition: Box::new(binary(HlsBinaryOp::Gt, current.clone(), incoming.clone())),
            then_expr: Box::new(current.clone()),
            else_expr: Box::new(incoming.clone()),
        },
        ReducerKind::MaskedMinIgnoreZero { active_mask } => {
            let low_mask = literal_uint(active_mask.saturating_sub(1) as u64);
            let masked_current = binary(HlsBinaryOp::BitAnd, current.clone(), low_mask.clone());
            let masked_incoming = binary(HlsBinaryOp::BitAnd, incoming.clone(), low_mask);
            HlsExpr::Ternary {
                condition: Box::new(binary(HlsBinaryOp::Gt, masked_current, masked_incoming)),
                then_expr: Box::new(incoming.clone()),
                else_expr: Box::new(current.clone()),
            }
        }
    };

    if check_incoming {
        // Mergers: (incoming != 0) ? op(current, incoming) : current
        let nonzero = binary(HlsBinaryOp::Ne, incoming.clone(), literal_uint(0));
        HlsExpr::Ternary {
            condition: Box::new(nonzero),
            then_expr: Box::new(cast_if_needed(op_expr)),
            else_expr: Box::new(cast_if_needed(current)),
        }
    } else {
        // Reduce: (current != 0) ? op(current, incoming) : incoming
        let nonzero = binary(HlsBinaryOp::Ne, current.clone(), literal_uint(0));
        HlsExpr::Ternary {
            condition: Box::new(nonzero),
            then_expr: Box::new(cast_if_needed(op_expr)),
            else_expr: Box::new(cast_if_needed(incoming)),
        }
    }
}

pub(crate) struct HlsForLoopBuilder {
    label: LoopLabel,
    init: Option<LoopInitializer>,
    condition: Option<HlsExpr>,
    increment: Option<LoopIncrement>,
    body: Option<Vec<HlsStatement>>,
}

impl HlsForLoopBuilder {
    pub(crate) fn new(label: &str) -> Result<Self, HlsTemplateError> {
        Ok(Self {
            label: LoopLabel::new(label)?,
            init: None,
            condition: None,
            increment: None,
            body: None,
        })
    }

    pub(crate) fn init(mut self, decl: HlsVarDecl) -> Self {
        self.init = Some(LoopInitializer::Declaration(decl));
        self
    }

    pub(crate) fn condition(mut self, expr: HlsExpr) -> Self {
        self.condition = Some(expr);
        self
    }

    pub(crate) fn increment(mut self, incr: LoopIncrement) -> Self {
        self.increment = Some(incr);
        self
    }

    pub(crate) fn body(mut self, body: Vec<HlsStatement>) -> Self {
        self.body = Some(body);
        self
    }

    pub(crate) fn build(self) -> HlsStatement {
        HlsStatement::ForLoop(HlsForLoop {
            label: self.label,
            init: self.init.unwrap_or(LoopInitializer::Empty),
            condition: self.condition.unwrap_or_else(|| literal_bool(true)),
            increment: self.increment.unwrap_or(LoopIncrement::Empty),
            body: self.body.unwrap_or_default(),
        })
    }
}
