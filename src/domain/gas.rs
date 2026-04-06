use std::collections::HashMap;

use super::{Identifier, IrLambda, OperationGraph};

/// Simplified type used when expressing the GAS lowering output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GasType {
    Int(u32),
    Float,
    Fixed {
        width: u32,
        int_width: u32,
    },
    Bool,
    Tuple(Vec<GasType>),
    Set(Box<GasType>),
    Array(Box<GasType>),
    EdgeRecord {
        props: HashMap<String, GasType>,
    },
    NodeRecord {
        props: HashMap<String, GasType>,
    },
    NodeRef,
    Vector {
        element: Box<GasType>,
        len: u32,
    },
    Matrix {
        element: Box<GasType>,
        rows: u32,
        cols: u32,
    },
    Unknown,
}

impl GasType {
    /// Builds a shallow tuple type.
    pub fn tuple(items: impl Into<Vec<GasType>>) -> Self {
        GasType::Tuple(items.into())
    }

    pub fn fmt_inline(&self) -> String {
        match self {
            GasType::Int(w) => format!("int<{w}>"),
            GasType::Float => "float".to_string(),
            GasType::Fixed { width, int_width } => format!("fixed<{width},{int_width}>"),
            GasType::Bool => "bool".to_string(),
            GasType::Tuple(items) => {
                let inner = items
                    .iter()
                    .map(GasType::fmt_inline)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("tuple<{inner}>")
            }
            GasType::Set(inner) => format!("set<{}>", inner.fmt_inline()),
            GasType::Array(inner) => format!("array<{}>", inner.fmt_inline()),
            GasType::EdgeRecord { props } => {
                let entries = fmt_props(props);
                format!("Edge{{{entries}}}")
            }
            GasType::NodeRecord { props } => {
                let entries = fmt_props(props);
                format!("Node{{{entries}}}")
            }
            GasType::NodeRef => "NodeRef".to_string(),
            GasType::Vector { element, len } => format!("vector<{}, {len}>", element.fmt_inline()),
            GasType::Matrix {
                element,
                rows,
                cols,
            } => {
                format!("matrix<{}, {rows}, {cols}>", element.fmt_inline())
            }
            GasType::Unknown => "unknown".to_string(),
        }
    }
}

fn fmt_props(props: &HashMap<String, GasType>) -> String {
    let mut entries = props
        .iter()
        .map(|(k, v)| format!("{k}: {}", v.fmt_inline()))
        .collect::<Vec<_>>();
    entries.sort();
    entries.join(", ")
}

/// Describes the scatter stage in a GAS iteration.
#[derive(Clone, Debug, PartialEq)]
pub struct GasScatterStage {
    pub edge_input: Identifier,
    pub key_binding: Identifier,
    pub key_lambda: IrLambda,
    pub key_type: GasType,
    pub value_binding: Identifier,
    pub value_lambda: IrLambda,
    pub value_type: GasType,
}

/// Describes the gather (reduce) stage.
#[derive(Clone, Debug, PartialEq)]
pub struct GasGatherStage {
    pub input_value_type: GasType,
    pub reducer: IrLambda,
    pub output_type: GasType,
}

/// Describes the apply stage that produces the final node property updates.
#[derive(Clone, Debug, PartialEq)]
pub struct GasApplyStage {
    pub input_binding: Identifier,
    pub input_type: GasType,
    pub lambda: Option<IrLambda>,
    pub output_binding: Identifier,
    pub output_type: GasType,
    pub target_property: Identifier,
}

/// Fully lowered GAS program consisting of the three ordered stages.
#[derive(Clone, Debug, PartialEq)]
pub struct GasProgram {
    pub scatter: GasScatterStage,
    pub gather: GasGatherStage,
    pub apply: GasApplyStage,
}

impl crate::domain::DebugSummary for GasProgram {
    fn debug_summary(&self) -> String {
        let mut lines = Vec::new();
        lines.push("GAS Program".to_string());
        lines.push("  Scatter:".to_string());
        lines.push(format!("    edge_input: {}", self.scatter.edge_input));
        lines.push(format!(
            "    key: {} -> {}",
            self.scatter.key_binding,
            self.scatter.key_type.fmt_inline()
        ));
        lines.push(format!("    key_lambda: {:?}", self.scatter.key_lambda));
        lines.push(format!(
            "    value: {} -> {}",
            self.scatter.value_binding,
            self.scatter.value_type.fmt_inline()
        ));
        lines.push(format!("    value_lambda: {:?}", self.scatter.value_lambda));

        lines.push("  Gather:".to_string());
        lines.push(format!(
            "    input_type: {}",
            self.gather.input_value_type.fmt_inline()
        ));
        lines.push(format!(
            "    output_type: {}",
            self.gather.output_type.fmt_inline()
        ));
        lines.push(format!("    reducer: {:?}", self.gather.reducer));

        lines.push("  Apply:".to_string());
        lines.push(format!(
            "    input_binding: {} ({})",
            self.apply.input_binding,
            self.apply.input_type.fmt_inline()
        ));
        lines.push(format!(
            "    output_binding: {} -> {}",
            self.apply.output_binding,
            self.apply.output_type.fmt_inline()
        ));
        lines.push(format!(
            "    target_property: {}",
            self.apply.target_property
        ));
        if let Some(lambda) = &self.apply.lambda {
            lines.push(format!("    apply_lambda: {:?}", lambda));
        }

        lines.join("\n")
    }
}

impl GasProgram {
    /// Builds a GAS program from the three phases.
    pub fn new(scatter: GasScatterStage, gather: GasGatherStage, apply: GasApplyStage) -> Self {
        Self {
            scatter,
            gather,
            apply,
        }
    }
}

/// Captures the GAS-lowerable portion of an `OperationGraph`.
#[derive(Clone, Debug, PartialEq)]
pub struct GasLoweringSummary {
    pub source_graph: OperationGraph,
    pub gas: GasProgram,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_tuple_type() {
        let tuple = GasType::tuple(vec![GasType::Int(32), GasType::Bool]);
        match tuple {
            GasType::Tuple(items) => {
                assert_eq!(items.len(), 2, "expected two tuple items");
                assert!(matches!(items[0], GasType::Int(32)));
                assert!(matches!(items[1], GasType::Bool));
            }
            other => panic!("unexpected type {other:?}"),
        }
    }

    #[test]
    fn formats_vector_type() {
        let ty = GasType::Vector {
            element: Box::new(GasType::Float),
            len: 4,
        };
        assert_eq!(ty.fmt_inline(), "vector<float, 4>");
    }
}
