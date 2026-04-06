/// Represents a scalar value used inside scatter/gather/apply expressions.
#[derive(Clone, Debug, PartialEq)]
pub enum OperatorOperand {
    ScatterSrcProp,
    ScatterSrcId,
    ScatterDstId,
    ScatterEdgeWeight,
    GatherValue,
    OldProp,
    /// Optional auxiliary per-node input (e.g., out-degree).
    OldAux,
    ConstInt(i64),
    ConstFloat(f64),
}

/// Supported binary operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperatorBinary {
    Add,
    Sub,
    Mul,
    Div,
    Lt,
    Gt,
    Eq,
    Ne,
    Le,
    Ge,
    And,
    Or,
    BitAnd,
    BitOr,
}

/// Supported unary operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperatorUnary {
    LogicalNot,
    BitNot,
}

/// Expression tree built from the constrained operator operands.
#[derive(Clone, Debug, PartialEq)]
pub enum OperatorExpr {
    Operand(OperatorOperand),
    Unary {
        op: OperatorUnary,
        expr: Box<OperatorExpr>,
    },
    Binary {
        op: OperatorBinary,
        left: Box<OperatorExpr>,
        right: Box<OperatorExpr>,
    },
    Ternary {
        condition: Box<OperatorExpr>,
        then_expr: Box<OperatorExpr>,
        else_expr: Box<OperatorExpr>,
    },
}

/// Supported reducer shapes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReducerKind {
    Min,
    Max,
    Sum,
    /// Bitwise OR reduction (CC bitmask propagation).
    Or,
    /// BFS-style reduction:
    /// - treat `0` as "no update"
    /// - otherwise pick the update with the smallest low bits (active bit ignored)
    ///
    /// The active bit is given by `active_mask` and the low mask is
    /// `active_mask - 1`.
    MaskedMinIgnoreZero {
        active_mask: u32,
    },
}

/// Identity value category for a reducer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReducerIdentity {
    Zero,
    PositiveInfinity,
    NegativeInfinity,
}

/// Scatter arithmetic descriptor.
#[derive(Clone, Debug, PartialEq)]
pub struct ScatterOp {
    pub expr: OperatorExpr,
}

/// Gather reduction descriptor.
#[derive(Clone, Debug, PartialEq)]
pub struct GatherOp {
    pub kind: ReducerKind,
    pub identity: ReducerIdentity,
}

/// Apply arithmetic descriptor.
#[derive(Clone, Debug, PartialEq)]
pub struct ApplyOp {
    pub expr: OperatorExpr,
}

/// Collected operators that parameterize the HLS templates.
#[derive(Clone, Debug, PartialEq)]
pub struct KernelOpBundle {
    pub scatter: ScatterOp,
    pub gather: GatherOp,
    pub apply: ApplyOp,
}

impl KernelOpBundle {
    /// Returns the operator bundle matching the legacy SSSP templates.
    pub fn sssp_default() -> Self {
        let scatter_expr = OperatorExpr::Binary {
            op: OperatorBinary::Add,
            left: Box::new(OperatorExpr::Operand(OperatorOperand::ScatterSrcProp)),
            right: Box::new(OperatorExpr::Operand(OperatorOperand::ScatterEdgeWeight)),
        };
        let apply_expr = OperatorExpr::Ternary {
            condition: Box::new(OperatorExpr::Binary {
                op: OperatorBinary::Lt,
                left: Box::new(OperatorExpr::Operand(OperatorOperand::OldProp)),
                right: Box::new(OperatorExpr::Operand(OperatorOperand::GatherValue)),
            }),
            then_expr: Box::new(OperatorExpr::Operand(OperatorOperand::OldProp)),
            else_expr: Box::new(OperatorExpr::Operand(OperatorOperand::GatherValue)),
        };
        KernelOpBundle {
            scatter: ScatterOp { expr: scatter_expr },
            gather: GatherOp {
                kind: ReducerKind::Min,
                identity: ReducerIdentity::PositiveInfinity,
            },
            apply: ApplyOp { expr: apply_expr },
        }
    }

    /// Connected Components: scatter src label, min reducer, passthrough apply.
    pub fn cc_default() -> Self {
        KernelOpBundle {
            scatter: ScatterOp {
                expr: OperatorExpr::Operand(OperatorOperand::ScatterSrcProp),
            },
            gather: GatherOp {
                kind: ReducerKind::Min,
                identity: ReducerIdentity::PositiveInfinity,
            },
            apply: ApplyOp {
                expr: OperatorExpr::Operand(OperatorOperand::GatherValue),
            },
        }
    }

    /// PageRank-style bundle: scatter uses source rank, gather sums, apply divides by out-degree and applies damping.
    pub fn pagerank_default() -> Self {
        let scatter_expr = OperatorExpr::Operand(OperatorOperand::ScatterSrcProp);
        let apply_expr = OperatorExpr::Binary {
            op: OperatorBinary::Add,
            left: Box::new(OperatorExpr::Operand(OperatorOperand::ConstFloat(0.15))),
            right: Box::new(OperatorExpr::Binary {
                op: OperatorBinary::Mul,
                left: Box::new(OperatorExpr::Operand(OperatorOperand::ConstFloat(0.85))),
                right: Box::new(OperatorExpr::Binary {
                    op: OperatorBinary::Div,
                    left: Box::new(OperatorExpr::Operand(OperatorOperand::GatherValue)),
                    right: Box::new(OperatorExpr::Operand(OperatorOperand::OldAux)),
                }),
            }),
        };
        KernelOpBundle {
            scatter: ScatterOp { expr: scatter_expr },
            gather: GatherOp {
                kind: ReducerKind::Sum,
                identity: ReducerIdentity::Zero,
            },
            apply: ApplyOp { expr: apply_expr },
        }
    }
}
