use std::collections::{HashMap, HashSet};

use crate::domain::{
    ast::{self, Identifier, Literal, Program, TypeExpr},
    gas::{GasApplyStage, GasGatherStage, GasProgram, GasScatterStage, GasType},
    ir::{IrExpr, IrLambda, OperationGraph, OperationNode, OperationStage},
};
use thiserror::Error;

/// Errors that can occur while lowering an `OperationGraph` into a GAS program.
#[derive(Debug, Error)]
pub enum GasLowerError {
    #[error("no reduce operation found; GAS requires a gather stage")]
    MissingReduce,
    #[error("multiple reduce operations found; only a single gather stage is supported")]
    MultipleReduce,
    #[error("GAS lowering currently supports only edge iteration inputs")]
    NonEdgeIteration,
    #[error("binding '{binding}' referenced but not produced before reduce")]
    MissingBinding { binding: String },
    #[error("scatter key binding '{binding}' must be produced by a map before the reduce")]
    UnsupportedScatterKey { binding: String },
    #[error("scatter value binding '{binding}' must be produced by a map before the reduce")]
    UnsupportedScatterValue { binding: String },
    #[error("gather values must contain exactly one binding, found {len}")]
    UnsupportedGatherValueArity { len: usize },
    #[error(
        "operations before reduce are not exclusively used by the reduce (found stray output '{binding}')"
    )]
    ScatterNotContiguous { binding: String },
    #[error(
        "operations after reduce are not exclusively used to produce the result binding '{binding}'"
    )]
    ApplyNotContiguous { binding: String },
    #[error("scatter lambda reads dst node properties, which is forbidden")]
    ScatterReadsDstProperties,
    #[error("gather lambda accesses properties; only scatter outputs are permitted")]
    GatherReadsProperties,
    #[error(
        "apply chain does not produce the requested result binding '{expected}' (found '{found}')"
    )]
    ApplyBindingMismatch { expected: String, found: String },
}

/// Entry point: lower a parsed and IR-lowered program into GAS stages.
pub fn lower_to_gas(
    program: &Program,
    graph: &OperationGraph,
) -> Result<GasProgram, GasLowerError> {
    let reduce_idx = locate_reduce(graph)?;
    let gather_node = graph
        .operations
        .get(reduce_idx)
        .ok_or(GasLowerError::MissingReduce)?;

    let scatter_ops = &graph.operations[..reduce_idx];
    let apply_ops = &graph.operations[reduce_idx + 1..];

    let type_env = TypeEnv::from_program(program);

    let mut binding_types: HashMap<String, GasType> = HashMap::new();
    let mut map_lambdas: HashMap<String, (IrLambda, Vec<Identifier>)> = HashMap::new();

    track_bindings(scatter_ops, &type_env, &mut binding_types, &mut map_lambdas)?;

    let (scatter, gather) = build_scatter_and_gather(
        scatter_ops,
        gather_node,
        &binding_types,
        &map_lambdas,
        &type_env,
    )?;

    if let Some(gather_output) = gather_node.outputs.first() {
        binding_types.insert(
            gather_output.as_str().to_string(),
            gather.output_type.clone(),
        );
    }

    let apply = build_apply(
        apply_ops,
        &graph.result.value,
        &graph.result.property,
        &binding_types,
        &type_env,
        gather_node,
    )?;

    Ok(GasProgram::new(scatter, gather, apply))
}

fn locate_reduce(graph: &OperationGraph) -> Result<usize, GasLowerError> {
    let reduce_indices: Vec<usize> = graph
        .operations
        .iter()
        .enumerate()
        .filter_map(|(idx, op)| matches!(op.stage, OperationStage::Reduce { .. }).then_some(idx))
        .collect();

    match reduce_indices.len() {
        0 => Err(GasLowerError::MissingReduce),
        1 => Ok(reduce_indices[0]),
        _ => Err(GasLowerError::MultipleReduce),
    }
}

fn track_bindings(
    ops: &[OperationNode],
    type_env: &TypeEnv,
    binding_types: &mut HashMap<String, GasType>,
    map_lambdas: &mut HashMap<String, (IrLambda, Vec<Identifier>)>,
) -> Result<(), GasLowerError> {
    if ops.is_empty() {
        return Err(GasLowerError::MissingReduce);
    }

    if !matches!(
        ops[0].stage,
        OperationStage::IterationInput {
            selector: ast::Selector::Edges
        }
    ) {
        return Err(GasLowerError::NonEdgeIteration);
    }

    let first_binding = ops[0]
        .outputs
        .first()
        .map(|id| id.as_str().to_string())
        .ok_or(GasLowerError::MissingBinding {
            binding: "iteration_input".to_string(),
        })?;
    binding_types.insert(first_binding, type_env.edge_record());

    for op in ops.iter().skip(1) {
        match &op.stage {
            OperationStage::Map { inputs, lambda } => {
                let param_types = collect_param_types(inputs, &lambda.params, binding_types);
                let output_type = infer_lambda_type(lambda, &param_types, type_env);
                map_lambdas.insert(
                    op.name.as_str().to_string(),
                    (lambda.clone(), inputs.clone()),
                );
                binding_types.insert(op.name.as_str().to_string(), output_type);
            }
            OperationStage::Filter { inputs, lambda } => {
                let param_types = collect_param_types(inputs, &lambda.params, binding_types);
                // Filter preserves the stream element type; use first input type if present.
                let preserved_type = inputs
                    .first()
                    .and_then(|id| binding_types.get(id.as_str()))
                    .cloned()
                    .unwrap_or(GasType::Unknown);
                let _ = infer_lambda_type(lambda, &param_types, type_env);
                binding_types.insert(op.name.as_str().to_string(), preserved_type);
            }
            OperationStage::IterationInput { .. } => return Err(GasLowerError::NonEdgeIteration),
            OperationStage::Reduce { .. } => {}
        }
    }

    Ok(())
}

fn build_scatter_and_gather(
    scatter_ops: &[OperationNode],
    gather_node: &OperationNode,
    binding_types: &HashMap<String, GasType>,
    map_lambdas: &HashMap<String, (IrLambda, Vec<Identifier>)>,
    type_env: &TypeEnv,
) -> Result<(GasScatterStage, GasGatherStage), GasLowerError> {
    let OperationStage::Reduce {
        key,
        values,
        lambda,
    } = &gather_node.stage
    else {
        return Err(GasLowerError::MissingReduce);
    };

    if values.len() != 1 {
        return Err(GasLowerError::UnsupportedGatherValueArity { len: values.len() });
    }

    let key_binding = key.as_str().to_string();
    let value_binding = values[0].as_str().to_string();

    ensure_contiguous_scatter(scatter_ops, &key_binding, &value_binding)?;

    let (key_lambda, _key_inputs) =
        map_lambdas
            .get(&key_binding)
            .cloned()
            .ok_or(GasLowerError::UnsupportedScatterKey {
                binding: key_binding.clone(),
            })?;

    let (value_lambda, _value_inputs) =
        map_lambdas
            .get(&value_binding)
            .cloned()
            .ok_or(GasLowerError::UnsupportedScatterValue {
                binding: value_binding.clone(),
            })?;

    enforce_scatter_rules(&key_lambda)?;
    enforce_scatter_rules(&value_lambda)?;

    let key_type = binding_types
        .get(&key_binding)
        .cloned()
        .unwrap_or(GasType::Unknown);
    let value_type = binding_types
        .get(&value_binding)
        .cloned()
        .unwrap_or(GasType::Unknown);

    let scatter_value_type = value_type.clone();

    let scatter = GasScatterStage {
        edge_input: scatter_ops
            .first()
            .and_then(|op| op.outputs.first())
            .cloned()
            .unwrap_or_else(|| Identifier::new("edge_stream")),
        key_binding: key.clone(),
        key_lambda,
        key_type,
        value_binding: values[0].clone(),
        value_lambda,
        value_type: scatter_value_type,
    };

    let gather_input_type = value_type.clone();
    let reducer_param_types = HashMap::from_iter(
        lambda
            .params
            .iter()
            .map(|p| (p.as_str().to_string(), gather_input_type.clone())),
    );
    let gather_output_type = infer_lambda_type(lambda, &reducer_param_types, type_env);

    ensure_gather_rules(lambda)?;

    let gather = GasGatherStage {
        input_value_type: gather_input_type,
        reducer: lambda.clone(),
        output_type: gather_output_type,
    };

    Ok((scatter, gather))
}

fn build_apply(
    apply_ops: &[OperationNode],
    result_binding: &Identifier,
    target_property: &Identifier,
    binding_types: &HashMap<String, GasType>,
    type_env: &TypeEnv,
    gather_node: &OperationNode,
) -> Result<GasApplyStage, GasLowerError> {
    let gather_output = gather_node
        .outputs
        .first()
        .ok_or(GasLowerError::MissingReduce)?
        .clone();

    if apply_ops.is_empty() {
        let output_type = binding_types
            .get(gather_output.as_str())
            .cloned()
            .unwrap_or(GasType::Unknown);
        if gather_output != *result_binding {
            return Err(GasLowerError::ApplyBindingMismatch {
                expected: result_binding.as_str().to_string(),
                found: gather_output.as_str().to_string(),
            });
        }

        return Ok(GasApplyStage {
            input_binding: gather_output.clone(),
            input_type: output_type.clone(),
            lambda: None,
            output_binding: result_binding.clone(),
            output_type,
            target_property: target_property.clone(),
        });
    }

    if apply_ops.len() != 1 {
        return Err(GasLowerError::ApplyNotContiguous {
            binding: result_binding.as_str().to_string(),
        });
    }

    let apply_op = &apply_ops[0];
    let (lambda, inputs) = match &apply_op.stage {
        OperationStage::Map { inputs, lambda } => (Some(lambda), inputs),
        OperationStage::Filter { .. } => {
            return Err(GasLowerError::ApplyNotContiguous {
                binding: result_binding.as_str().to_string(),
            });
        }
        OperationStage::IterationInput { .. } => {
            return Err(GasLowerError::ApplyNotContiguous {
                binding: result_binding.as_str().to_string(),
            });
        }
        OperationStage::Reduce { .. } => {
            return Err(GasLowerError::ApplyNotContiguous {
                binding: result_binding.as_str().to_string(),
            });
        }
    };

    if apply_op.name != *result_binding {
        return Err(GasLowerError::ApplyBindingMismatch {
            expected: result_binding.as_str().to_string(),
            found: apply_op.name.as_str().to_string(),
        });
    }

    if !inputs.iter().any(|i| i == &gather_output) {
        return Err(GasLowerError::ApplyNotContiguous {
            binding: result_binding.as_str().to_string(),
        });
    }

    let param_types = collect_param_types(
        inputs,
        lambda.as_ref().map(|l| l.params.as_slice()).unwrap_or(&[]),
        binding_types,
    );
    let output_type = lambda
        .as_ref()
        .map(|l| infer_lambda_type(l, &param_types, type_env))
        .unwrap_or(GasType::Unknown);

    Ok(GasApplyStage {
        input_binding: gather_output,
        input_type: binding_types
            .get(inputs.first().map(Identifier::as_str).unwrap_or(""))
            .cloned()
            .unwrap_or(GasType::Unknown),
        lambda: lambda.cloned(),
        output_binding: result_binding.clone(),
        output_type,
        target_property: target_property.clone(),
    })
}

fn ensure_contiguous_scatter(
    scatter_ops: &[OperationNode],
    key_binding: &str,
    value_binding: &str,
) -> Result<(), GasLowerError> {
    let mut needed: HashSet<String> = [key_binding.to_string(), value_binding.to_string()]
        .into_iter()
        .collect();

    for op in scatter_ops.iter().rev() {
        let produced = op
            .outputs
            .first()
            .map(|id| id.as_str().to_string())
            .unwrap_or_default();

        if !needed.contains(&produced) {
            return Err(GasLowerError::ScatterNotContiguous { binding: produced });
        }

        match &op.stage {
            OperationStage::Map { inputs, .. } | OperationStage::Filter { inputs, .. } => {
                for input in inputs {
                    needed.insert(input.as_str().to_string());
                }
            }
            OperationStage::IterationInput { .. } => {}
            OperationStage::Reduce { .. } => {}
        }
    }

    Ok(())
}

fn enforce_scatter_rules(lambda: &IrLambda) -> Result<(), GasLowerError> {
    let edge_param_names: HashSet<String> = lambda
        .params
        .iter()
        .map(|p| p.as_str().to_string())
        .collect();
    if reads_dst_properties(&lambda.body, &edge_param_names) {
        return Err(GasLowerError::ScatterReadsDstProperties);
    }
    Ok(())
}

fn ensure_gather_rules(lambda: &IrLambda) -> Result<(), GasLowerError> {
    if contains_member_access(&lambda.body) {
        return Err(GasLowerError::GatherReadsProperties);
    }
    Ok(())
}

fn collect_param_types(
    inputs: &[Identifier],
    params: &[Identifier],
    bindings: &HashMap<String, GasType>,
) -> HashMap<String, GasType> {
    inputs
        .iter()
        .zip(params.iter())
        .filter_map(|(input, param)| {
            bindings
                .get(input.as_str())
                .cloned()
                .map(|ty| (param.as_str().to_string(), ty))
        })
        .collect()
}

fn infer_lambda_type(
    lambda: &IrLambda,
    params: &HashMap<String, GasType>,
    type_env: &TypeEnv,
) -> GasType {
    infer_expr_type(&lambda.body, params, type_env)
}

fn infer_expr_type(
    expr: &IrExpr,
    params: &HashMap<String, GasType>,
    type_env: &TypeEnv,
) -> GasType {
    match expr {
        IrExpr::Identifier(id) => {
            if id.as_str() == "self" {
                GasType::NodeRef
            } else {
                params.get(id.as_str()).cloned().unwrap_or(GasType::Unknown)
            }
        }
        IrExpr::Literal(lit) => match lit {
            Literal::Int(_) => GasType::Int(64),
            Literal::Float(_) => GasType::Float,
            Literal::Bool(_) => GasType::Bool,
        },
        IrExpr::MemberAccess { target, access } => {
            let target_ty = infer_expr_type(target, params, type_env);
            match (target_ty, access) {
                (GasType::EdgeRecord { props }, ast::Accessor::Property(name)) => {
                    if name.as_str() == "src" || name.as_str() == "dst" {
                        GasType::NodeRef
                    } else {
                        props
                            .get(name.as_str())
                            .cloned()
                            .unwrap_or(GasType::Unknown)
                    }
                }
                (GasType::NodeRecord { props }, ast::Accessor::Property(name)) => props
                    .get(name.as_str())
                    .cloned()
                    .unwrap_or(GasType::Unknown),
                (GasType::NodeRef, ast::Accessor::Property(name)) => type_env
                    .node_props
                    .get(name.as_str())
                    .cloned()
                    .unwrap_or(GasType::Unknown),
                (GasType::Array(inner), ast::Accessor::Index(_)) => (*inner).clone(),
                (GasType::Tuple(items), ast::Accessor::Index(idx)) => items
                    .get(*idx as usize)
                    .cloned()
                    .unwrap_or(GasType::Unknown),
                _ => GasType::Unknown,
            }
        }
        IrExpr::Call { function, args } => {
            let arg_types = args
                .iter()
                .map(|arg| infer_expr_type(arg, params, type_env))
                .collect::<Vec<_>>();
            infer_call_type(function.as_str(), &arg_types)
        }
        IrExpr::Binary { left, right, .. } => unify_types(
            infer_expr_type(left, params, type_env),
            infer_expr_type(right, params, type_env),
        ),
        IrExpr::Unary { expr, .. } => infer_expr_type(expr, params, type_env),
        IrExpr::Ternary {
            condition: _,
            then_expr,
            else_expr,
        } => unify_types(
            infer_expr_type(then_expr, params, type_env),
            infer_expr_type(else_expr, params, type_env),
        ),
    }
}

fn unify_types(left: GasType, right: GasType) -> GasType {
    if left == right {
        left
    } else {
        GasType::Unknown
    }
}

fn infer_call_type(name: &str, args: &[GasType]) -> GasType {
    match (name, args) {
        ("make_set", [inner]) => GasType::Set(Box::new(inner.clone())),
        ("set_union", [lhs, rhs]) => unify_types(lhs.clone(), rhs.clone()),
        ("set_insert", [set_ty, value_ty]) => match set_ty {
            GasType::Set(inner) if **inner == *value_ty => GasType::Set(inner.clone()),
            _ => GasType::Unknown,
        },
        ("mex", [GasType::Set(inner)]) => match &**inner {
            GasType::Int(width) => GasType::Int(*width),
            _ => GasType::Unknown,
        },
        ("outer_product", [GasType::Vector { element, len }, GasType::Vector { .. }]) => {
            GasType::Matrix {
                element: element.clone(),
                rows: *len,
                cols: *len,
            }
        }
        ("vector_scale", [GasType::Vector { element, len }, scalar]) => {
            if matches!(
                scalar,
                GasType::Int(_) | GasType::Float | GasType::Fixed { .. }
            ) {
                GasType::Vector {
                    element: element.clone(),
                    len: *len,
                }
            } else {
                GasType::Unknown
            }
        }
        ("vector_add", [GasType::Vector { element, len }, GasType::Vector { .. }]) => {
            GasType::Vector {
                element: element.clone(),
                len: *len,
            }
        }
        (
            "matrix_add",
            [
                GasType::Matrix {
                    element,
                    rows,
                    cols,
                },
                GasType::Matrix { .. },
            ],
        ) => GasType::Matrix {
            element: element.clone(),
            rows: *rows,
            cols: *cols,
        },
        (
            "solve_linear",
            [
                GasType::Matrix {
                    element,
                    rows,
                    cols,
                },
                GasType::Vector { .. },
            ],
        ) => GasType::Vector {
            element: element.clone(),
            len: *rows.max(cols),
        },
        ("pair", [lhs, rhs]) => GasType::Tuple(vec![lhs.clone(), rhs.clone()]),
        _ => GasType::Unknown,
    }
}

fn reads_dst_properties(expr: &IrExpr, edge_params: &HashSet<String>) -> bool {
    fn classify(expr: &IrExpr, edge_params: &HashSet<String>) -> (AccessOrigin, bool) {
        match expr {
            IrExpr::Identifier(id) => {
                if edge_params.contains(id.as_str()) {
                    (AccessOrigin::EdgeParam, false)
                } else {
                    (AccessOrigin::Other, false)
                }
            }
            IrExpr::MemberAccess { target, access } => {
                let (origin, violation_child) = classify(target, edge_params);
                let mut violation = violation_child;
                let next_origin = match (origin, access) {
                    (AccessOrigin::EdgeParam, ast::Accessor::Property(name))
                        if name.as_str() == "dst" =>
                    {
                        AccessOrigin::DstRef
                    }
                    (AccessOrigin::EdgeParam, ast::Accessor::Property(name))
                        if name.as_str() == "src" =>
                    {
                        AccessOrigin::SrcRef
                    }
                    (AccessOrigin::DstRef, ast::Accessor::Property(_)) => {
                        violation = true;
                        AccessOrigin::Other
                    }
                    _ => AccessOrigin::Other,
                };
                (next_origin, violation)
            }
            IrExpr::Binary { left, right, .. } => {
                let (_, violation_left) = classify(left, edge_params);
                let (_, violation_right) = classify(right, edge_params);
                (AccessOrigin::Other, violation_left || violation_right)
            }
            IrExpr::Unary { expr, .. } => classify(expr, edge_params),
            IrExpr::Ternary {
                condition,
                then_expr,
                else_expr,
            } => {
                let (_, v1) = classify(condition, edge_params);
                let (_, v2) = classify(then_expr, edge_params);
                let (_, v3) = classify(else_expr, edge_params);
                (AccessOrigin::Other, v1 || v2 || v3)
            }
            IrExpr::Call { args, .. } => {
                let violation = args.iter().any(|arg| classify(arg, edge_params).1);
                (AccessOrigin::Other, violation)
            }
            IrExpr::Literal(_) => (AccessOrigin::Other, false),
        }
    }

    let (_, violation) = classify(expr, edge_params);
    violation
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AccessOrigin {
    EdgeParam,
    DstRef,
    SrcRef,
    Other,
}

fn contains_member_access(expr: &IrExpr) -> bool {
    match expr {
        IrExpr::MemberAccess { target, access } => match access {
            ast::Accessor::Property(_) => true,
            ast::Accessor::Index(_) => contains_member_access(target),
        },
        IrExpr::Binary { left, right, .. } => {
            contains_member_access(left) || contains_member_access(right)
        }
        IrExpr::Unary { expr, .. } => contains_member_access(expr),
        IrExpr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => {
            contains_member_access(condition)
                || contains_member_access(then_expr)
                || contains_member_access(else_expr)
        }
        IrExpr::Call { args, .. } => args.iter().any(contains_member_access),
        IrExpr::Identifier(_) | IrExpr::Literal(_) => false,
    }
}

struct TypeEnv {
    node_props: HashMap<String, GasType>,
    edge_props: HashMap<String, GasType>,
}

impl TypeEnv {
    fn from_program(program: &Program) -> Self {
        Self {
            node_props: program
                .schema
                .node
                .as_ref()
                .map(|entity| collect_props(entity))
                .unwrap_or_default(),
            edge_props: program
                .schema
                .edge
                .as_ref()
                .map(|entity| collect_props(entity))
                .unwrap_or_default(),
        }
    }

    fn edge_record(&self) -> GasType {
        GasType::EdgeRecord {
            props: self.edge_props.clone(),
        }
    }
}

fn collect_props(entity: &crate::domain::ast::EntityDef) -> HashMap<String, GasType> {
    entity
        .properties
        .iter()
        .map(|prop| {
            (
                prop.name.as_str().to_string(),
                typeexpr_to_gastype(&prop.ty),
            )
        })
        .collect()
}

pub fn typeexpr_to_gastype(ty: &TypeExpr) -> GasType {
    match ty {
        TypeExpr::Int { width } => GasType::Int(*width),
        TypeExpr::Float => GasType::Float,
        TypeExpr::Fixed { width, int_width } => GasType::Fixed {
            width: *width,
            int_width: *int_width,
        },
        TypeExpr::Bool => GasType::Bool,
        TypeExpr::Set(inner) => GasType::Set(Box::new(typeexpr_to_gastype(inner))),
        TypeExpr::Tuple(items) => GasType::Tuple(items.iter().map(typeexpr_to_gastype).collect()),
        TypeExpr::Array(inner) => GasType::Array(Box::new(typeexpr_to_gastype(inner))),
        TypeExpr::Vector { element, len } => GasType::Vector {
            element: Box::new(typeexpr_to_gastype(element)),
            len: *len,
        },
        TypeExpr::Matrix {
            element,
            rows,
            cols,
        } => GasType::Matrix {
            element: Box::new(typeexpr_to_gastype(element)),
            rows: *rows,
            cols: *cols,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::ir_builder::LoweredProgram;

    fn lowered_sample() -> LoweredProgram {
        let source = include_str!("../../test.dsl");
        LoweredProgram::parse_and_lower(source).expect("sample lowers")
    }

    #[test]
    fn lowers_sample_program_to_gas() {
        let lowered = lowered_sample();
        let gas = lower_to_gas(&lowered.ast, &lowered.ir).expect("gas lowering");

        assert_eq!(gas.scatter.key_binding.as_str(), "dst_ids");
        assert_eq!(gas.scatter.value_binding.as_str(), "updates");
        assert!(matches!(gas.gather.output_type, GasType::Int(_))); // min distance remains int
        assert_eq!(gas.apply.output_binding.as_str(), "min_dists");
        assert_eq!(gas.apply.target_property.as_str(), "dist");
    }

    #[test]
    fn errors_when_multiple_reduces() {
        let lowered = lowered_sample();
        let dup = lowered.ir.operations[3].clone();
        let mut ops = lowered.ir.operations.clone();
        ops.insert(4, dup);
        let graph = OperationGraph {
            operations: ops,
            result: lowered.ir.result.clone(),
        };

        let err = lower_to_gas(&lowered.ast, &graph).expect_err("should fail");
        assert!(matches!(err, GasLowerError::MultipleReduce));
    }

    #[test]
    fn rejects_scatter_accessing_dst_props() {
        let mut lowered = lowered_sample();
        // Force dst_ids map to access dst property chain: lambda e: e.dst.dist
        if let OperationStage::Map { ref mut lambda, .. } = lowered.ir.operations[1].stage {
            lambda.body = IrExpr::MemberAccess {
                target: Box::new(IrExpr::MemberAccess {
                    target: Box::new(IrExpr::Identifier(Identifier::new("e"))),
                    access: crate::domain::ast::Accessor::Property(Identifier::new("dst")),
                }),
                access: crate::domain::ast::Accessor::Property(Identifier::new("dist")),
            };
        }

        let err =
            lower_to_gas(&lowered.ast, &lowered.ir).expect_err("dst property should be rejected");
        assert!(matches!(err, GasLowerError::ScatterReadsDstProperties));
    }

    fn lowered_app(name: &str) -> LoweredProgram {
        let source = std::fs::read_to_string(format!("apps/{name}.dsl")).expect("app file exists");
        LoweredProgram::parse_and_lower(&source).expect("app lowers")
    }

    #[test]
    fn lowers_pagerank_to_gas() {
        let lowered = lowered_app("pagerank");
        let gas = lower_to_gas(&lowered.ast, &lowered.ir).expect("pagerank lowers");
        assert_eq!(gas.apply.target_property.as_str(), "rank");
    }

    #[test]
    fn lowers_connected_components_to_gas() {
        let lowered = lowered_app("connected_components");
        let gas = lower_to_gas(&lowered.ast, &lowered.ir).expect("cc lowers");
        assert_eq!(gas.apply.target_property.as_str(), "label");
    }

    #[test]
    fn lowers_graph_coloring_to_gas() {
        let lowered = lowered_app("graph_coloring");
        let gas = lower_to_gas(&lowered.ast, &lowered.ir).expect("coloring lowers");
        assert_eq!(gas.apply.target_property.as_str(), "color");
    }

    #[test]
    fn lowers_als_to_gas() {
        let lowered = lowered_app("als");
        let gas = lower_to_gas(&lowered.ast, &lowered.ir).expect("als lowers");
        assert_eq!(gas.apply.target_property.as_str(), "vec");
    }
}
