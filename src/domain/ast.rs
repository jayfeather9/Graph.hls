use super::DebugSummary;
use std::fmt;

/// Root node produced by the parser.
#[derive(Clone, Debug, PartialEq)]
pub struct Program {
    pub schema: SchemaBlock,
    pub hls: Option<HlsConfigBlock>,
    pub algorithm: AlgoBlock,
}

impl DebugSummary for Program {
    fn debug_summary(&self) -> String {
        let mut lines = Vec::new();

        lines.push("Program".to_string());
        lines.push("  schema:".to_string());
        lines.push(format!(
            "    node: {}",
            describe_entity(self.schema.node.as_ref())
        ));
        lines.push(format!(
            "    edge: {}",
            describe_entity(self.schema.edge.as_ref())
        ));

        lines.push("  algorithm:".to_string());
        for (idx, stmt) in self.algorithm.statements.iter().enumerate() {
            lines.push(format!(
                "    stmt {idx}: {} = {}",
                stmt.target,
                describe_operation(&stmt.operation)
            ));
        }
        lines.push(format!(
            "    return: {} as {}",
            self.algorithm.return_stmt.value, self.algorithm.return_stmt.property
        ));

        if let Some(hls) = &self.hls {
            lines.push("  hls:".to_string());
            if let Some(topo) = &hls.topology {
                lines.push(format!(
                    "    topology: apply_slr={}, hbm_writer_slr={}, cross_slr_fifo_depth={}",
                    topo.apply_slr, topo.hbm_writer_slr, topo.cross_slr_fifo_depth
                ));
                lines.push(format!(
                    "    topology: little_groups={}, big_groups={}",
                    topo.little_groups.len(),
                    topo.big_groups.len()
                ));
            } else {
                lines.push("    <none>".to_string());
            }
        }

        lines.join("\n")
    }
}

fn describe_entity(entity: Option<&EntityDef>) -> String {
    match entity {
        Some(def) if !def.properties.is_empty() => def
            .properties
            .iter()
            .map(|prop| format!("{}: {}", prop.name, describe_type(&prop.ty)))
            .collect::<Vec<_>>()
            .join(", "),
        Some(_) => "<empty>".to_string(),
        None => "<missing>".to_string(),
    }
}

fn describe_type(ty: &TypeExpr) -> String {
    match ty {
        TypeExpr::Int { width } => format!("int<{width}>"),
        TypeExpr::Float => "float".to_string(),
        TypeExpr::Fixed { width, int_width } => format!("fixed<{width},{int_width}>"),
        TypeExpr::Bool => "bool".to_string(),
        TypeExpr::Set(inner) => format!("set<{}>", describe_type(inner)),
        TypeExpr::Tuple(items) => format!(
            "tuple<{}>",
            items
                .iter()
                .map(describe_type)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        TypeExpr::Array(inner) => format!("array<{}>", describe_type(inner)),
        TypeExpr::Vector { element, len } => format!("vector<{}, {}>", describe_type(element), len),
        TypeExpr::Matrix {
            element,
            rows,
            cols,
        } => {
            format!("matrix<{}, {}, {}>", describe_type(element), rows, cols)
        }
    }
}

fn describe_operation(op: &Operation) -> String {
    match op {
        Operation::IterationInput(selector) => {
            format!("iteration_input({})", describe_selector(*selector))
        }
        Operation::Map(map) => format!(
            "map(inputs=[{}], {})",
            map.inputs
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            describe_lambda(&map.lambda)
        ),
        Operation::Filter(filter) => format!(
            "filter(inputs=[{}], {})",
            filter
                .inputs
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            describe_lambda(&filter.lambda)
        ),
        Operation::Reduce(reduce) => format!(
            "reduce(key={}, values=[{}], {})",
            reduce.key,
            reduce
                .values
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            describe_lambda(&reduce.function)
        ),
    }
}

fn describe_lambda(lambda: &Lambda) -> String {
    let params = lambda
        .params
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!("lambda ({params}) => {}", describe_expr(&lambda.body))
}

fn describe_expr(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(id) => id.to_string(),
        Expr::Literal(lit) => describe_literal(lit),
        Expr::MemberAccess { target, access } => match access {
            Accessor::Property(prop) => format!("{}.{}", describe_expr(target), prop),
            Accessor::Index(idx) => format!("{}[{idx}]", describe_expr(target)),
        },
        Expr::Call { function, args } => format!(
            "{}({})",
            function,
            args.iter()
                .map(describe_expr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Binary { op, left, right } => format!(
            "({} {} {})",
            describe_expr(left),
            describe_binary_op(*op),
            describe_expr(right)
        ),
        Expr::Unary { op, expr } => format!("({}{})", describe_unary_op(*op), describe_expr(expr)),
        Expr::Ternary {
            condition,
            then_expr,
            else_expr,
        } => format!(
            "({} ? {} : {})",
            describe_expr(condition),
            describe_expr(then_expr),
            describe_expr(else_expr)
        ),
    }
}

fn describe_literal(lit: &Literal) -> String {
    match lit {
        Literal::Int(v) => v.to_string(),
        Literal::Float(v) => v.to_string(),
        Literal::Bool(v) => v.to_string(),
    }
}

fn describe_selector(selector: Selector) -> &'static str {
    match selector {
        Selector::Nodes => "G.NODES",
        Selector::Edges => "G.EDGES",
    }
}

fn describe_binary_op(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Eq => "==",
        BinaryOp::Ne => "!=",
        BinaryOp::Gt => ">",
        BinaryOp::Lt => "<",
        BinaryOp::Ge => ">=",
        BinaryOp::Le => "<=",
        BinaryOp::BitAnd => "&",
        BinaryOp::BitOr => "|",
        BinaryOp::And => "&&",
        BinaryOp::Or => "||",
    }
}

fn describe_unary_op(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Not => "!",
        UnaryOp::BitNot => "~",
    }
}

/// Schema declaration describing node and edge properties.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaBlock {
    pub node: Option<EntityDef>,
    pub edge: Option<EntityDef>,
}

/// Memory backend for the emitted HLS project.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryBackend {
    /// High-Bandwidth Memory (default, e.g. Xilinx U55C).
    Hbm,
    /// DDR memory (e.g. Xilinx U250/U280).
    Ddr,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        MemoryBackend::Hbm
    }
}

/// Optional HLS emission configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsConfigBlock {
    pub topology: Option<HlsTopologyConfig>,
    pub memory: MemoryBackend,
    /// Bit width for compressed local destination IDs. Default 32 (no compression).
    /// When < 32, edges use `ap_uint<local_id_bits>` for dst_id, saving bits per edge.
    pub local_id_bits: u32,
    /// When true (default), the reduce pipeline uses 0 as the "empty" sentinel,
    /// relying on URAM power-on-reset to zero. Requires edge weights >= 1 so
    /// that 0 never appears as a valid gathered value.
    pub zero_sentinel: bool,
    /// When true, use simplified 2-partition preprocess without throughput-based
    /// repartitioning (no L1 optimization). Incompatible with multi-group topologies.
    pub no_l1_preprocess: bool,
}

/// Explicit multi-merger topology configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HlsTopologyConfig {
    pub apply_slr: u8,
    pub hbm_writer_slr: u8,
    pub cross_slr_fifo_depth: u32,
    pub little_groups: Vec<HlsKernelGroupConfig>,
    pub big_groups: Vec<HlsKernelGroupConfig>,
}

/// A kernel group (pipelines + a merger), used for both little and big partitions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HlsKernelGroupConfig {
    pub pipelines: u32,
    pub merger_slr: u8,
    pub pipeline_slr: Vec<u8>,
}

/// Definition shared by node and edge declarations.
#[derive(Clone, Debug, PartialEq)]
pub struct EntityDef {
    pub properties: Vec<PropertyDefinition>,
}

/// A single property entry.
#[derive(Clone, Debug, PartialEq)]
pub struct PropertyDefinition {
    pub name: Identifier,
    pub ty: TypeExpr,
}

/// Strongly typed identifier used throughout the AST.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Identifier(pub String);

impl Identifier {
    /// Creates a new identifier from a string-like input.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the inner string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Type expressions supported by the DSL.
#[derive(Clone, Debug, PartialEq)]
pub enum TypeExpr {
    Int {
        width: u32,
    },
    Float,
    Fixed {
        width: u32,
        int_width: u32,
    },
    Bool,
    Set(Box<TypeExpr>),
    Tuple(Vec<TypeExpr>),
    Array(Box<TypeExpr>),
    Vector {
        element: Box<TypeExpr>,
        len: u32,
    },
    Matrix {
        element: Box<TypeExpr>,
        rows: u32,
        cols: u32,
    },
}

/// Algorithm block containing statements and final return binding.
#[derive(Clone, Debug, PartialEq)]
pub struct AlgoBlock {
    pub statements: Vec<Statement>,
    pub return_stmt: ReturnStmt,
}

/// Assignment statement binding an operation to a symbol.
#[derive(Clone, Debug, PartialEq)]
pub struct Statement {
    pub target: Identifier,
    pub operation: Operation,
}

/// Supported high-level operations.
#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    IterationInput(Selector),
    Map(MapOp),
    Filter(FilterOp),
    Reduce(ReduceOp),
}

/// Selector for iteration inputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Selector {
    Nodes,
    Edges,
}

/// Representation of a map invocation.
#[derive(Clone, Debug, PartialEq)]
pub struct MapOp {
    pub inputs: Vec<Identifier>,
    pub lambda: Lambda,
}

/// Representation of a filter invocation.
#[derive(Clone, Debug, PartialEq)]
pub struct FilterOp {
    pub inputs: Vec<Identifier>,
    pub lambda: Lambda,
}

/// Representation of a reduce invocation.
#[derive(Clone, Debug, PartialEq)]
pub struct ReduceOp {
    pub key: Identifier,
    pub values: Vec<Identifier>,
    pub function: Lambda,
}

/// Return statement describing the node property update.
#[derive(Clone, Debug, PartialEq)]
pub struct ReturnStmt {
    pub value: Identifier,
    pub property: Identifier,
}

/// Lambda expression wrapper.
#[derive(Clone, Debug, PartialEq)]
pub struct Lambda {
    pub params: Vec<Identifier>,
    pub body: Expr,
}

/// Expression AST for lambda bodies.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Identifier(Identifier),
    Literal(Literal),
    MemberAccess {
        target: Box<Expr>,
        access: Accessor,
    },
    Call {
        function: Identifier,
        args: Vec<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Ternary {
        condition: Box<Expr>,
        then_expr: Box<Expr>,
        else_expr: Box<Expr>,
    },
}

/// Member access target (either a property name or tuple index).
#[derive(Clone, Debug, PartialEq)]
pub enum Accessor {
    Property(Identifier),
    Index(u32),
}

/// Literal variants supported inside expressions.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Int(i64),
    Float(String),
    Bool(bool),
}

/// Binary operators ordered by precedence rules in the DSL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    BitAnd,
    BitOr,
    And,
    Or,
}

/// Unary operators on expressions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    BitNot,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_display_round_trip() {
        let ident = Identifier::new("sample_name");
        assert_eq!(ident.to_string(), "sample_name");
        assert_eq!(ident.as_str(), "sample_name");
    }
}
