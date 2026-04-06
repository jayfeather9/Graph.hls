use std::collections::HashSet;

use crate::{
    domain::{
        GraphyflowError, IrError,
        ast::{Operation, Program},
        ir::{IrLambda, OperationGraph, OperationNode, OperationStage, ResultBinding},
    },
    services::parser::parse_program,
};

/// Convenience wrapper that bundles the parsed AST with the generated IR.
pub struct LoweredProgram {
    pub ast: Program,
    pub ir: OperationGraph,
}

impl LoweredProgram {
    /// Parses the source string and lowers it into the IR graph.
    pub fn parse_and_lower(source: &str) -> Result<Self, GraphyflowError> {
        let ast = parse_program(source)?;
        let ir = build_ir(&ast)?;
        Ok(Self { ast, ir })
    }
}

/// Converts an AST program into the IR graph representation.
pub fn build_ir(program: &Program) -> Result<OperationGraph, IrError> {
    let mut operations = Vec::new();
    let mut bindings = HashSet::new();

    for statement in &program.algorithm.statements {
        match &statement.operation {
            Operation::IterationInput(selector) => {
                operations.push(OperationNode {
                    name: statement.target.clone(),
                    stage: OperationStage::IterationInput {
                        selector: *selector,
                    },
                    outputs: vec![statement.target.clone()],
                });
            }
            Operation::Map(map) => {
                ensure_all_defined(&map.inputs, &bindings)?;
                operations.push(OperationNode {
                    name: statement.target.clone(),
                    stage: OperationStage::Map {
                        inputs: map.inputs.clone(),
                        lambda: IrLambda::from(map.lambda.clone()),
                    },
                    outputs: vec![statement.target.clone()],
                });
            }
            Operation::Filter(filter) => {
                ensure_all_defined(&filter.inputs, &bindings)?;
                operations.push(OperationNode {
                    name: statement.target.clone(),
                    stage: OperationStage::Filter {
                        inputs: filter.inputs.clone(),
                        lambda: IrLambda::from(filter.lambda.clone()),
                    },
                    outputs: vec![statement.target.clone()],
                });
            }
            Operation::Reduce(reduce) => {
                ensure_binding(&reduce.key, &bindings).map_err(|_| IrError::UnknownReduceKey {
                    key: reduce.key.as_str().to_string(),
                })?;
                ensure_all_defined(&reduce.values, &bindings)?;
                operations.push(OperationNode {
                    name: statement.target.clone(),
                    stage: OperationStage::Reduce {
                        key: reduce.key.clone(),
                        values: reduce.values.clone(),
                        lambda: IrLambda::from(reduce.function.clone()),
                    },
                    outputs: vec![statement.target.clone()],
                });
            }
        }

        bindings.insert(statement.target.as_str().to_string());
    }

    ensure_binding(&program.algorithm.return_stmt.value, &bindings)?;

    Ok(OperationGraph {
        operations,
        result: ResultBinding {
            value: program.algorithm.return_stmt.value.clone(),
            property: program.algorithm.return_stmt.property.clone(),
        },
    })
}

fn ensure_all_defined(
    inputs: &[crate::domain::ast::Identifier],
    bindings: &HashSet<String>,
) -> Result<(), IrError> {
    for input in inputs {
        ensure_binding(input, bindings)?;
    }
    Ok(())
}

fn ensure_binding(
    ident: &crate::domain::ast::Identifier,
    bindings: &HashSet<String>,
) -> Result<(), IrError> {
    if bindings.contains(ident.as_str()) {
        Ok(())
    } else {
        Err(IrError::UnknownBinding {
            binding: ident.as_str().to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::parser::parse_program;

    const SAMPLE: &str = r"{
    Node: {
        dist: int<32>
    }
    Edge: {
        weight: int<32>
    }
}

{
    edges = iteration_input(G.EDGES)
    dst_ids = map([edges], lambda e: e.dst)
    updates = map([edges], lambda e: e.src.dist + e.weight)
    min_dists = reduce(key=dst_ids, values=[updates], function=lambda x, y: x > y ? y : x)
    return min_dists as result_node_prop.dist
}
";

    #[test]
    fn lowers_program_into_operation_graph() {
        let ast = parse_program(SAMPLE).expect("valid program");
        let ir = build_ir(&ast).expect("ir");
        assert_eq!(ir.operations.len(), 4);
        assert_eq!(ir.result.property.as_str(), "dist");
    }

    #[test]
    fn detects_unknown_binding() {
        let ast = parse_program(
            "{ Node: {} Edge: {} } { bogus = map([missing], lambda x: x) return bogus as result_node_prop.id }",
        )
        .expect("parsed");
        let err = build_ir(&ast).expect_err("should fail");
        assert!(matches!(err, IrError::UnknownBinding { .. }));
    }
}
