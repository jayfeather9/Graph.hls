use crate::domain::hls_ops::{KernelOpBundle, OperatorBinary, OperatorExpr, OperatorOperand, ReducerKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HlsAlgorithmKind {
    Sssp,
    ConnectedComponents,
    Pagerank,
    Bfs,
    ArticleRank,
    Wcc,
    /// Placeholder for future algorithms (BFS/AR/WCC) once the backend supports them.
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HlsConvergenceMode {
    MinImprove,
    EqualityStable,
    DeltaThreshold,
    FixedIterations,
    NewlyDiscoveredZero,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HlsNumericKind {
    Fixed,
    Float,
    Int,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HlsUpdateMode {
    Min,
    Max,
    Overwrite,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HlsAlgorithmSpec {
    pub kind: HlsAlgorithmKind,
    pub target_property: String,
    pub numeric_kind: HlsNumericKind,
    pub bitwidth: u32,
    pub int_width: u32,
    pub convergence_mode: HlsConvergenceMode,
    pub delta_threshold: f32,
    pub max_iterations: u32,
    pub needs_edge_weight: bool,
    pub needs_out_degree: bool,
    pub update_mode: HlsUpdateMode,
    pub active_mask: u32,
    pub inf_value: u32,
}

impl HlsAlgorithmSpec {
    pub fn classify(
        target_property: &str,
        ops: &KernelOpBundle,
        needs_edge_weight: bool,
        numeric_kind: HlsNumericKind,
        bitwidth: u32,
        int_width: u32,
    ) -> Self {
        let is_unweighted_sssp = !needs_edge_weight
            && target_property == "dist"
            && matches!(ops.gather.kind, ReducerKind::Min)
            && scatter_is_unit_distance(&ops.scatter.expr)
            && apply_is_min_keep_old(&ops.apply.expr);

        let (
            kind,
            convergence_mode,
            delta_threshold,
            max_iterations,
            needs_out_degree,
            update_mode,
            active_mask,
            inf_value,
        ) = if needs_edge_weight || is_unweighted_sssp {
            (
                HlsAlgorithmKind::Sssp,
                HlsConvergenceMode::MinImprove,
                0.0,
                0,
                false,
                HlsUpdateMode::Min,
                0,
                0,
            )
        } else {
            match ops.gather.kind {
                ReducerKind::Sum => match numeric_kind {
                    HlsNumericKind::Float => (
                        HlsAlgorithmKind::Pagerank,
                        HlsConvergenceMode::FixedIterations,
                        1e-4,
                        16,
                        true,
                        HlsUpdateMode::Overwrite,
                        0,
                        0,
                    ),
                    _ => (
                        HlsAlgorithmKind::ArticleRank,
                        HlsConvergenceMode::FixedIterations,
                        0.0,
                        10,
                        true,
                        HlsUpdateMode::Overwrite,
                        0,
                        0,
                    ),
                },
                ReducerKind::MaskedMinIgnoreZero { active_mask } => (
                    HlsAlgorithmKind::Bfs,
                    HlsConvergenceMode::NewlyDiscoveredZero,
                    0.0,
                    0,
                    false,
                    HlsUpdateMode::Overwrite,
                    active_mask,
                    active_mask.saturating_sub(2),
                ),
                ReducerKind::Max => (
                    HlsAlgorithmKind::Wcc,
                    HlsConvergenceMode::EqualityStable,
                    0.0,
                    0,
                    false,
                    HlsUpdateMode::Max,
                    0,
                    0,
                ),
                ReducerKind::Or => (
                    HlsAlgorithmKind::ConnectedComponents,
                    HlsConvergenceMode::EqualityStable,
                    0.0,
                    0,
                    false,
                    HlsUpdateMode::Overwrite,
                    0,
                    0,
                ),
                _ => (
                    HlsAlgorithmKind::ConnectedComponents,
                    HlsConvergenceMode::EqualityStable,
                    0.0,
                    0,
                    false,
                    HlsUpdateMode::Min,
                    0,
                    0,
                ),
            }
        };

        Self {
            kind,
            target_property: target_property.to_string(),
            numeric_kind,
            bitwidth,
            int_width,
            convergence_mode,
            delta_threshold,
            max_iterations,
            needs_edge_weight,
            needs_out_degree,
            update_mode,
            active_mask,
            inf_value,
        }
    }
}

fn scatter_is_unit_distance(expr: &OperatorExpr) -> bool {
    match expr {
        OperatorExpr::Binary { op: OperatorBinary::Add, left, right } => {
            matches!(
                (&**left, &**right),
                (
                    OperatorExpr::Operand(OperatorOperand::ScatterSrcProp),
                    OperatorExpr::Operand(OperatorOperand::ConstInt(1))
                ) | (
                    OperatorExpr::Operand(OperatorOperand::ConstInt(1)),
                    OperatorExpr::Operand(OperatorOperand::ScatterSrcProp)
                )
            )
        }
        _ => false,
    }
}

fn apply_is_min_keep_old(expr: &OperatorExpr) -> bool {
    match expr {
        OperatorExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => {
            matches!(
                (&**condition, &**then_expr, &**else_expr),
                (
                    OperatorExpr::Binary {
                        op: OperatorBinary::Gt,
                        left,
                        right
                    },
                    OperatorExpr::Operand(OperatorOperand::GatherValue),
                    OperatorExpr::Operand(OperatorOperand::OldProp)
                ) if matches!(
                    (&**left, &**right),
                    (
                        OperatorExpr::Operand(OperatorOperand::OldProp),
                        OperatorExpr::Operand(OperatorOperand::GatherValue)
                    )
                )
            ) || matches!(
                (&**condition, &**then_expr, &**else_expr),
                (
                    OperatorExpr::Binary {
                        op: OperatorBinary::Lt,
                        left,
                        right
                    },
                    OperatorExpr::Operand(OperatorOperand::OldProp),
                    OperatorExpr::Operand(OperatorOperand::GatherValue)
                ) if matches!(
                    (&**left, &**right),
                    (
                        OperatorExpr::Operand(OperatorOperand::OldProp),
                        OperatorExpr::Operand(OperatorOperand::GatherValue)
                    )
                )
            )
        }
        _ => false,
    }
}
