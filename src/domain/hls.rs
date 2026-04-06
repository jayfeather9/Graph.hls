use std::fmt;

use crate::domain::ast::Identifier;
use thiserror::Error;

/// Errors that can occur while constructing or rendering HLS C++ code.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum HlsCodegenError {
    #[error("identifier cannot be empty")]
    EmptyIdentifier,
    #[error(
        "identifier '{0}' contains invalid characters; only [A-Za-z_][A-Za-z0-9_]* are allowed"
    )]
    InvalidIdentifier(String),
    #[error("loop label cannot be empty")]
    EmptyLabel,
    #[error("pragma content cannot be empty")]
    EmptyPragma,
    #[error("include target cannot be empty")]
    EmptyInclude,
    #[error("define target cannot be empty")]
    EmptyDefine,
    #[error("custom type name cannot be empty")]
    EmptyCustomType,
    #[error("array dimensions cannot be empty")]
    EmptyArrayDimensions,
    #[error("array dimension at index {index} must be non-zero")]
    ZeroArrayDimension { index: usize },
}

fn is_valid_identifier(candidate: &str) -> bool {
    let mut chars = candidate.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Identifier validated for C++ emission.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HlsIdentifier(Identifier);

impl HlsIdentifier {
    /// Builds a new identifier after validating C/C++ rules.
    pub fn new(name: impl Into<String>) -> Result<Self, HlsCodegenError> {
        let raw = name.into();
        if raw.is_empty() {
            return Err(HlsCodegenError::EmptyIdentifier);
        }
        if !is_valid_identifier(&raw) {
            return Err(HlsCodegenError::InvalidIdentifier(raw));
        }
        Ok(Self(Identifier::new(raw)))
    }

    /// Returns the inner identifier.
    pub fn as_identifier(&self) -> &Identifier {
        &self.0
    }

    /// Returns the name as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for HlsIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Loop label that must always be present for loops.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LoopLabel(String);

impl LoopLabel {
    /// Creates a label, rejecting empty or invalid identifiers.
    pub fn new(label: impl Into<String>) -> Result<Self, HlsCodegenError> {
        let raw = label.into();
        if raw.is_empty() {
            return Err(HlsCodegenError::EmptyLabel);
        }
        if !is_valid_identifier(&raw) {
            return Err(HlsCodegenError::InvalidIdentifier(raw));
        }
        Ok(Self(raw))
    }

    /// Returns the label as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LoopLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Parameter passing modes for generated functions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassingStyle {
    Value,
    Reference,
    ConstReference,
}

/// A single array dimension that can be a literal extent or a symbolic expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArrayDimension {
    Literal(usize),
    Expr(String),
}

impl ArrayDimension {
    fn validate(&self, index: usize) -> Result<(), HlsCodegenError> {
        match self {
            ArrayDimension::Literal(0) => Err(HlsCodegenError::ZeroArrayDimension { index }),
            ArrayDimension::Literal(_) => Ok(()),
            ArrayDimension::Expr(expr) if expr.trim().is_empty() => {
                Err(HlsCodegenError::ZeroArrayDimension { index })
            }
            ArrayDimension::Expr(_) => Ok(()),
        }
    }

    pub(crate) fn render(&self) -> String {
        match self {
            ArrayDimension::Literal(v) => format!("[{v}]"),
            ArrayDimension::Expr(expr) => format!("[{expr}]").to_string(),
        }
    }
}

/// Validated dimensions for arrays (each dimension must be present; literal dims must be > 0).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArrayDimensions(Vec<ArrayDimension>);

impl ArrayDimensions {
    /// Creates dimensions ensuring non-empty and non-zero extents for literal values.
    pub fn new(dimensions: impl Into<Vec<usize>>) -> Result<Self, HlsCodegenError> {
        let dims = dimensions
            .into()
            .into_iter()
            .map(ArrayDimension::Literal)
            .collect::<Vec<_>>();
        Self::from_dimensions(dims)
    }

    /// Builds dimensions from a mix of literals and expressions.
    pub fn from_dimensions(
        dimensions: impl Into<Vec<ArrayDimension>>,
    ) -> Result<Self, HlsCodegenError> {
        let dims = dimensions.into();
        if dims.is_empty() {
            return Err(HlsCodegenError::EmptyArrayDimensions);
        }
        for (idx, dim) in dims.iter().enumerate() {
            dim.validate(idx)?;
        }
        Ok(Self(dims))
    }

    /// Returns the raw slice of dimensions.
    pub fn as_slice(&self) -> &[ArrayDimension] {
        &self.0
    }
}

/// HLS types supported by the renderer.
#[derive(Clone, Debug, PartialEq)]
pub enum HlsType {
    Void,
    Int32,
    UInt32,
    Bool,
    Float,
    ApFixed {
        width: u32,
        int_width: u32,
    },
    ApUInt {
        width: u32,
    },
    ApInt {
        width: u32,
    },
    Stream(Box<HlsType>),
    Array {
        element: Box<HlsType>,
        dimensions: ArrayDimensions,
    },
    Pointer(Box<HlsType>),
    ConstPointer(Box<HlsType>),
    Custom(String),
}

impl HlsType {
    /// Creates a custom-named type after verifying the name is non-empty.
    pub fn custom(name: impl Into<String>) -> Result<Self, HlsCodegenError> {
        let raw = name.into();
        if raw.trim().is_empty() {
            return Err(HlsCodegenError::EmptyCustomType);
        }
        Ok(Self::Custom(raw))
    }

    /// Creates an array type with validated dimensions.
    pub fn array(
        element: HlsType,
        dimensions: impl Into<Vec<usize>>,
    ) -> Result<Self, HlsCodegenError> {
        let dims = ArrayDimensions::new(dimensions)?;
        Ok(HlsType::Array {
            element: Box::new(element),
            dimensions: dims,
        })
    }

    /// Creates an array type using symbolic or mixed dimensions.
    pub fn array_with_dims(
        element: HlsType,
        dimensions: ArrayDimensions,
    ) -> Result<Self, HlsCodegenError> {
        Ok(HlsType::Array {
            element: Box::new(element),
            dimensions,
        })
    }

    /// Creates an array type from string expressions for each dimension.
    pub fn array_with_exprs(
        element: HlsType,
        dimensions: impl Into<Vec<String>>,
    ) -> Result<Self, HlsCodegenError> {
        let dims = dimensions
            .into()
            .into_iter()
            .map(ArrayDimension::Expr)
            .collect::<Vec<_>>();
        let validated = ArrayDimensions::from_dimensions(dims)?;
        Ok(HlsType::Array {
            element: Box::new(element),
            dimensions: validated,
        })
    }

    /// Renders the type in C++ syntax (without a declarator name).
    pub fn to_code(&self) -> String {
        match self {
            HlsType::Void => "void".to_string(),
            HlsType::Int32 => "int32_t".to_string(),
            HlsType::UInt32 => "uint32_t".to_string(),
            HlsType::Bool => "bool".to_string(),
            HlsType::Float => "float".to_string(),
            HlsType::ApFixed { width, int_width } => {
                format!("ap_fixed<{width}, {int_width}>").to_string()
            }
            HlsType::ApUInt { width } => format!("ap_uint<{width}>").to_string(),
            HlsType::ApInt { width } => format!("ap_int<{width}>").to_string(),
            HlsType::Stream(inner) => format!("hls::stream<{}>", inner.to_code()),
            HlsType::Array {
                element,
                dimensions,
            } => {
                let dims = render_dimensions(dimensions.as_slice());
                format!("{}{}", element.to_code(), dims)
            }
            HlsType::Pointer(inner) => format!("{}*", inner.to_code()),
            HlsType::ConstPointer(inner) => format!("const {}*", inner.to_code()),
            HlsType::Custom(name) => name.clone(),
        }
    }

    fn format_declarator(&self, name: &HlsIdentifier, passing: Option<PassingStyle>) -> String {
        match self {
            HlsType::Array {
                element,
                dimensions,
            } => {
                let dims = render_dimensions(dimensions.as_slice());
                match passing {
                    Some(PassingStyle::Reference) => {
                        format!("{} (&{}){}", element.to_code(), name, dims)
                    }
                    Some(PassingStyle::ConstReference) => {
                        format!("const {} (&{}){}", element.to_code(), name, dims)
                    }
                    _ => format!("{} {}{}", element.to_code(), name, dims),
                }
            }
            HlsType::Pointer(inner) => {
                let base = inner.to_code();
                format!("{base} *{name}")
            }
            HlsType::ConstPointer(inner) => {
                let base = inner.to_code();
                format!("const {base} *{name}")
            }
            _ => {
                let base = self.to_code();
                match passing {
                    None | Some(PassingStyle::Value) => format!("{base} {name}"),
                    Some(PassingStyle::Reference) => format!("{base} &{name}"),
                    Some(PassingStyle::ConstReference) => format!("const {base} &{name}"),
                }
            }
        }
    }
}

fn render_dimensions(dims: &[ArrayDimension]) -> String {
    dims.iter()
        .map(ArrayDimension::render)
        .collect::<Vec<_>>()
        .join("")
}

/// Literal values used in expressions.
#[derive(Clone, Debug, PartialEq)]
pub enum HlsLiteral {
    Int(i64),
    UInt(u64),
    Bool(bool),
    Float(f64),
    Str(String),
}

impl HlsLiteral {
    pub(crate) fn render(&self) -> String {
        match self {
            HlsLiteral::Int(v) => {
                if *v > i32::MAX as i64 {
                    format!("{v}u")
                } else {
                    v.to_string()
                }
            }
            HlsLiteral::UInt(v) => format!("{v}u"),
            HlsLiteral::Bool(v) => v.to_string(),
            HlsLiteral::Float(v) => format!("{v}f"),
            HlsLiteral::Str(s) => format!("\"{}\"", s.escape_default()),
        }
    }
}

/// Unary operators supported in expressions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HlsUnaryOp {
    Negate,
    LogicalNot,
    BitNot,
    PreIncrement,
    PreDecrement,
    AddressOf,
    Dereference,
}

impl HlsUnaryOp {
    fn render(&self) -> &'static str {
        match self {
            HlsUnaryOp::Negate => "-",
            HlsUnaryOp::LogicalNot => "!",
            HlsUnaryOp::BitNot => "~",
            HlsUnaryOp::PreIncrement => "++",
            HlsUnaryOp::PreDecrement => "--",
            HlsUnaryOp::AddressOf => "&",
            HlsUnaryOp::Dereference => "*",
        }
    }
}

/// Binary operators supported in expressions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HlsBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Shl,
    Shr,
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    And,
    Or,
    BitAnd,
    BitOr,
    BitXor,
}

impl HlsBinaryOp {
    fn render(&self) -> &'static str {
        match self {
            HlsBinaryOp::Add => "+",
            HlsBinaryOp::Sub => "-",
            HlsBinaryOp::Mul => "*",
            HlsBinaryOp::Div => "/",
            HlsBinaryOp::Mod => "%",
            HlsBinaryOp::Shl => "<<",
            HlsBinaryOp::Shr => ">>",
            HlsBinaryOp::Eq => "==",
            HlsBinaryOp::Ne => "!=",
            HlsBinaryOp::Lt => "<",
            HlsBinaryOp::Gt => ">",
            HlsBinaryOp::Le => "<=",
            HlsBinaryOp::Ge => ">=",
            HlsBinaryOp::And => "&&",
            HlsBinaryOp::Or => "||",
            HlsBinaryOp::BitAnd => "&",
            HlsBinaryOp::BitOr => "|",
            HlsBinaryOp::BitXor => "^",
        }
    }
}

/// Expression tree for HLS C++ rendering.
#[derive(Clone, Debug, PartialEq)]
pub enum HlsExpr {
    Identifier(HlsIdentifier),
    Literal(HlsLiteral),
    Call {
        function: HlsIdentifier,
        args: Vec<HlsExpr>,
    },
    Binary {
        op: HlsBinaryOp,
        left: Box<HlsExpr>,
        right: Box<HlsExpr>,
    },
    Unary {
        op: HlsUnaryOp,
        expr: Box<HlsExpr>,
    },
    Ternary {
        condition: Box<HlsExpr>,
        then_expr: Box<HlsExpr>,
        else_expr: Box<HlsExpr>,
    },
    Index {
        target: Box<HlsExpr>,
        index: Box<HlsExpr>,
    },
    Member {
        target: Box<HlsExpr>,
        field: HlsIdentifier,
    },
    MethodCall {
        target: Box<HlsExpr>,
        method: HlsIdentifier,
        args: Vec<HlsExpr>,
    },
    Cast {
        target_type: HlsType,
        expr: Box<HlsExpr>,
    },
    ReinterpretCast {
        target_type: HlsType,
        expr: Box<HlsExpr>,
    },
    SizeOfType(HlsType),
    SizeOfExpr(Box<HlsExpr>),
    Raw(String),
}

impl HlsExpr {
    pub(crate) fn render(&self) -> String {
        match self {
            HlsExpr::Identifier(id) => id.to_string(),
            HlsExpr::Literal(lit) => lit.render(),
            HlsExpr::Call { function, args } => {
                let rendered_args = args
                    .iter()
                    .map(HlsExpr::render)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", function, rendered_args)
            }
            HlsExpr::Binary { op, left, right } => {
                format!("({} {} {})", left.render(), op.render(), right.render())
            }
            HlsExpr::Unary { op, expr } => format!("({}{})", op.render(), expr.render()),
            HlsExpr::Ternary {
                condition,
                then_expr,
                else_expr,
            } => format!(
                "({} ? {} : {})",
                condition.render(),
                then_expr.render(),
                else_expr.render()
            ),
            HlsExpr::Index { target, index } => format!("{}[{}]", target.render(), index.render()),
            HlsExpr::Member { target, field } => format!("{}.{}", target.render(), field),
            HlsExpr::MethodCall {
                target,
                method,
                args,
            } => {
                let rendered_args = args
                    .iter()
                    .map(HlsExpr::render)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}.{}({})", target.render(), method, rendered_args)
            }
            HlsExpr::Cast { target_type, expr } => {
                format!("static_cast<{}>({})", target_type.to_code(), expr.render())
            }
            HlsExpr::ReinterpretCast { target_type, expr } => {
                format!(
                    "reinterpret_cast<{}>({})",
                    target_type.to_code(),
                    expr.render()
                )
            }
            HlsExpr::SizeOfType(target) => format!("sizeof({})", target.to_code()),
            HlsExpr::SizeOfExpr(expr) => format!("sizeof({})", expr.render()),
            HlsExpr::Raw(text) => text.clone(),
        }
    }
}

/// Variable declaration statement, optionally with an initializer.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsVarDecl {
    pub name: HlsIdentifier,
    pub ty: HlsType,
    pub init: Option<HlsExpr>,
}

impl HlsVarDecl {
    /// Renders the declaration line.
    pub fn render(&self) -> String {
        let declarator = self
            .ty
            .format_declarator(&self.name, Some(PassingStyle::Value));
        match &self.init {
            Some(init) => format!("{declarator} = {};", init.render()),
            None => format!("{declarator};"),
        }
    }
}

/// Function parameter definition.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsParameter {
    pub name: HlsIdentifier,
    pub ty: HlsType,
    pub passing: PassingStyle,
}

impl HlsParameter {
    /// Renders the parameter as it appears in a signature.
    pub fn render(&self) -> String {
        self.ty.format_declarator(&self.name, Some(self.passing))
    }
}

/// HLS pragma line.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsPragma(String);

impl HlsPragma {
    /// Creates a pragma ensuring it is non-empty.
    pub fn new(text: impl Into<String>) -> Result<Self, HlsCodegenError> {
        let raw = text.into();
        if raw.trim().is_empty() {
            return Err(HlsCodegenError::EmptyPragma);
        }
        Ok(Self(raw))
    }

    fn render(&self) -> String {
        format!("#pragma {}", self.0)
    }
}

/// Include directive.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsInclude {
    pub target: String,
    pub angled: bool,
}

impl HlsInclude {
    /// Builds an include directive; `angled` controls <...> vs "...".
    pub fn new(target: impl Into<String>, angled: bool) -> Result<Self, HlsCodegenError> {
        let raw = target.into();
        if raw.trim().is_empty() {
            return Err(HlsCodegenError::EmptyInclude);
        }
        Ok(Self {
            target: raw,
            angled,
        })
    }

    fn render(&self) -> String {
        if self.angled {
            format!("#include <{}>", self.target)
        } else {
            format!("#include \"{}\"", self.target)
        }
    }
}

/// Preprocessor define directive.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsDefine {
    pub name: HlsIdentifier,
    pub value: Option<String>,
}

impl HlsDefine {
    /// Creates a define directive with an optional value.
    pub fn new(name: HlsIdentifier, value: Option<String>) -> Result<Self, HlsCodegenError> {
        if name.as_str().is_empty() {
            return Err(HlsCodegenError::EmptyDefine);
        }
        Ok(Self { name, value })
    }

    fn render(&self) -> String {
        match &self.value {
            Some(v) => format!("#define {} {}", self.name, v),
            None => format!("#define {}", self.name),
        }
    }
}

/// Loop initializer clause.
#[derive(Clone, Debug, PartialEq)]
pub enum LoopInitializer {
    Declaration(HlsVarDecl),
    Assignment { target: HlsExpr, value: HlsExpr },
    Empty,
}

impl LoopInitializer {
    fn render(&self) -> String {
        match self {
            LoopInitializer::Declaration(decl) => decl.render().trim_end_matches(';').to_string(),
            LoopInitializer::Assignment { target, value } => {
                format!("{} = {}", target.render(), value.render())
            }
            LoopInitializer::Empty => String::new(),
        }
    }
}

/// Loop increment clause.
#[derive(Clone, Debug, PartialEq)]
pub enum LoopIncrement {
    Assignment { target: HlsExpr, value: HlsExpr },
    Unary(HlsUnaryOp, HlsExpr),
    Empty,
}

impl LoopIncrement {
    fn render(&self) -> String {
        match self {
            LoopIncrement::Assignment { target, value } => {
                format!("{} = {}", target.render(), value.render())
            }
            LoopIncrement::Unary(op, expr) => format!("{}{}", op.render(), expr.render()),
            LoopIncrement::Empty => String::new(),
        }
    }
}

/// If/else composite statement.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsIfElse {
    pub condition: HlsExpr,
    pub then_body: Vec<HlsStatement>,
    pub else_body: Vec<HlsStatement>,
}

/// For-loop statement with mandatory label.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsForLoop {
    pub label: LoopLabel,
    pub init: LoopInitializer,
    pub condition: HlsExpr,
    pub increment: LoopIncrement,
    pub body: Vec<HlsStatement>,
}

/// While-loop statement with mandatory label.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsWhileLoop {
    pub label: LoopLabel,
    pub condition: HlsExpr,
    pub body: Vec<HlsStatement>,
}

/// Struct field definition.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsField {
    pub name: HlsIdentifier,
    pub ty: HlsType,
}

impl HlsField {
    fn render(&self, indent: usize) -> String {
        let pad = indent_str(indent);
        format!(
            "{pad}{};",
            self.ty
                .format_declarator(&self.name, Some(PassingStyle::Value))
        )
    }
}

/// Plain C++ struct useful for bundling HLS data.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsStruct {
    pub name: HlsIdentifier,
    pub fields: Vec<HlsField>,
    pub attributes: Vec<String>,
}

impl HlsStruct {
    fn render(&self, indent: usize) -> String {
        let pad = indent_str(indent);
        let mut lines = Vec::new();
        if self.attributes.is_empty() {
            lines.push(format!("{pad}struct {} {{", self.name));
        } else {
            let attrs = self.attributes.join(" ");
            lines.push(format!("{pad}struct {attrs} {} {{", self.name));
        }
        for field in &self.fields {
            lines.push(field.render(indent + 1));
        }
        lines.push(format!("{pad}}};"));
        lines.join("\n")
    }
}

/// Supported statements that can appear in functions or translation units.
#[derive(Clone, Debug, PartialEq)]
pub enum HlsStatement {
    Declaration(HlsVarDecl),
    Assignment {
        target: HlsExpr,
        value: HlsExpr,
    },
    Expr(HlsExpr),
    IfElse(HlsIfElse),
    ForLoop(HlsForLoop),
    WhileLoop(HlsWhileLoop),
    Pragma(HlsPragma),
    Include(HlsInclude),
    Define(HlsDefine),
    Comment(String),
    StreamRead {
        stream: HlsIdentifier,
        target: HlsIdentifier,
    },
    StreamWrite {
        stream: HlsIdentifier,
        value: HlsExpr,
    },
    Block(Vec<HlsStatement>),
    Return(Option<HlsExpr>),
    Break,
    Continue,
    Struct(HlsStruct),
    UsingAlias {
        name: HlsIdentifier,
        ty: HlsType,
    },
    IfDef {
        symbol: String,
        then_body: Vec<HlsStatement>,
        else_body: Vec<HlsStatement>,
    },
    StaticAssert {
        condition: HlsExpr,
        message: Option<String>,
    },
    FunctionPrototype(HlsFunctionPrototype),
    FunctionDef(HlsFunction),
    Raw(String),
}

impl HlsStatement {
    fn render_block(body: &[HlsStatement], indent: usize) -> String {
        let mut lines = Vec::new();
        let pad = indent_str(indent);
        lines.push(format!("{pad}{{"));
        for stmt in body {
            lines.push(stmt.render(indent + 1));
        }
        lines.push(format!("{pad}}}"));
        lines.join("\n")
    }

    /// Renders the statement with the provided indentation depth.
    pub fn render(&self, indent: usize) -> String {
        let pad = indent_str(indent);
        match self {
            HlsStatement::Declaration(decl) => format!("{pad}{}", decl.render()),
            HlsStatement::Assignment { target, value } => {
                format!("{pad}{} = {};", target.render(), value.render())
            }
            HlsStatement::Expr(expr) => format!("{pad}{};", expr.render()),
            HlsStatement::IfElse(HlsIfElse {
                condition,
                then_body,
                else_body,
            }) => {
                let mut lines = Vec::new();
                lines.push(format!("{pad}if ({}) {{", condition.render()));
                for stmt in then_body {
                    lines.push(stmt.render(indent + 1));
                }
                lines.push(format!("{pad}}}"));
                if !else_body.is_empty() {
                    lines.push(format!("{pad}else {{"));
                    for stmt in else_body {
                        lines.push(stmt.render(indent + 1));
                    }
                    lines.push(format!("{pad}}}"));
                }
                lines.join("\n")
            }
            HlsStatement::ForLoop(HlsForLoop {
                label,
                init,
                condition,
                increment,
                body,
            }) => {
                let init_rendered = init.render();
                let incr_rendered = increment.render();
                let header = format!(
                    "{pad}{}: for ({}; {}; {}) {{",
                    label,
                    init_rendered,
                    condition.render(),
                    incr_rendered
                );
                let mut lines = vec![header];
                for stmt in body {
                    lines.push(stmt.render(indent + 1));
                }
                lines.push(format!("{pad}}}"));
                lines.join("\n")
            }
            HlsStatement::WhileLoop(HlsWhileLoop {
                label,
                condition,
                body,
            }) => {
                let mut lines = Vec::new();
                lines.push(format!("{pad}{}: while ({}) {{", label, condition.render()));
                for stmt in body {
                    lines.push(stmt.render(indent + 1));
                }
                lines.push(format!("{pad}}}"));
                lines.join("\n")
            }
            HlsStatement::Pragma(pragma) => format!("{pad}{}", pragma.render()),
            HlsStatement::Include(include) => format!("{pad}{}", include.render()),
            HlsStatement::Define(define) => format!("{pad}{}", define.render()),
            HlsStatement::Comment(text) => format!("{pad}// {}", text),
            HlsStatement::StreamRead { stream, target } => {
                format!("{pad}{} = {}.read();", target, stream)
            }
            HlsStatement::StreamWrite { stream, value } => {
                format!("{pad}{}.write({});", stream, value.render())
            }
            HlsStatement::Block(body) => Self::render_block(body, indent),
            HlsStatement::Return(expr) => match expr {
                Some(e) => format!("{pad}return {};", e.render()),
                None => format!("{pad}return;"),
            },
            HlsStatement::Break => format!("{pad}break;"),
            HlsStatement::Continue => format!("{pad}continue;"),
            HlsStatement::Struct(def) => def.render(indent),
            HlsStatement::UsingAlias { name, ty } => {
                format!("{pad}using {} = {};", name, ty.to_code())
            }
            HlsStatement::IfDef {
                symbol,
                then_body,
                else_body,
            } => {
                let mut lines = Vec::new();
                lines.push(format!("{pad}#ifdef {symbol}"));
                for stmt in then_body {
                    lines.push(stmt.render(indent + 1));
                }
                if !else_body.is_empty() {
                    lines.push(format!("{pad}#else"));
                    for stmt in else_body {
                        lines.push(stmt.render(indent + 1));
                    }
                }
                lines.push(format!("{pad}#endif"));
                lines.join("\n")
            }
            HlsStatement::StaticAssert { condition, message } => {
                if let Some(msg) = message {
                    format!(
                        "{pad}static_assert({}, \"{}\");",
                        condition.render(),
                        msg.escape_default()
                    )
                } else {
                    format!("{pad}static_assert({});", condition.render())
                }
            }
            HlsStatement::FunctionPrototype(proto) => proto.render(indent),
            HlsStatement::FunctionDef(func) => func.to_code(),
            HlsStatement::Raw(text) => format!("{pad}{text}"),
        }
    }
}

/// `extern` function declaration rendered outside of a definition.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsFunctionPrototype {
    pub linkage: Option<&'static str>,
    pub return_type: HlsType,
    pub name: HlsIdentifier,
    pub params: Vec<HlsParameter>,
}

impl HlsFunctionPrototype {
    fn render(&self, indent: usize) -> String {
        let pad = indent_str(indent);
        let params = self
            .params
            .iter()
            .map(HlsParameter::render)
            .collect::<Vec<_>>()
            .join(", ");
        let prefix = self.linkage.unwrap_or("");
        if prefix.is_empty() {
            format!(
                "{pad}{} {}({});",
                self.return_type.to_code(),
                self.name,
                params
            )
        } else {
            format!(
                "{pad}{} {} {}({});",
                prefix,
                self.return_type.to_code(),
                self.name,
                params
            )
        }
    }
}

/// Function definition with typed parameters and a body.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsFunction {
    pub linkage: Option<&'static str>,
    pub name: HlsIdentifier,
    pub return_type: HlsType,
    pub params: Vec<HlsParameter>,
    pub body: Vec<HlsStatement>,
}

impl HlsFunction {
    /// Renders the function into C++ code.
    pub fn to_code(&self) -> String {
        let params = self
            .params
            .iter()
            .map(HlsParameter::render)
            .collect::<Vec<_>>()
            .join(", ");
        let mut lines = Vec::new();
        let prefix = self.linkage.unwrap_or("");
        if prefix.is_empty() {
            lines.push(format!(
                "{} {}({}) {{",
                self.return_type.to_code(),
                self.name,
                params
            ));
        } else {
            lines.push(format!(
                "{prefix} {} {}({}) {{",
                self.return_type.to_code(),
                self.name,
                params
            ));
        }
        for stmt in &self.body {
            lines.push(stmt.render(1));
        }
        lines.push("}".to_string());
        lines.join("\n")
    }
}

/// Translation unit comprising includes, defines, globals, and functions.
#[derive(Clone, Debug, PartialEq)]
pub struct HlsCompilationUnit {
    pub includes: Vec<HlsInclude>,
    pub defines: Vec<HlsDefine>,
    pub globals: Vec<HlsStatement>,
    pub functions: Vec<HlsFunction>,
}

impl HlsCompilationUnit {
    /// Renders the entire unit to compilable HLS C++ code.
    pub fn to_code(&self) -> String {
        let mut lines = Vec::new();
        for include in &self.includes {
            lines.push(include.render());
        }
        if !self.includes.is_empty() {
            lines.push(String::new());
        }
        for define in &self.defines {
            lines.push(define.render());
        }
        if !self.defines.is_empty() {
            lines.push(String::new());
        }
        for global in &self.globals {
            lines.push(global.render(0));
        }
        if !self.globals.is_empty() {
            lines.push(String::new());
        }
        for (idx, func) in self.functions.iter().enumerate() {
            lines.push(func.to_code());
            if idx + 1 != self.functions.len() {
                lines.push(String::new());
            }
        }
        lines.join("\n")
    }
}

fn indent_str(level: usize) -> String {
    const INDENT: usize = 4;
    " ".repeat(level * INDENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn rejects_empty_identifier() {
        let result = HlsIdentifier::new("");
        assert!(matches!(result, Err(HlsCodegenError::EmptyIdentifier)));
    }

    #[test]
    fn rejects_zero_dim_array() {
        let result = HlsType::array(HlsType::Float, vec![4, 0]);
        assert!(matches!(
            result,
            Err(HlsCodegenError::ZeroArrayDimension { index: 1 })
        ));
    }

    #[rstest]
    #[case(HlsType::Int32, "int32_t")]
    #[case(HlsType::UInt32, "uint32_t")]
    #[case(HlsType::Bool, "bool")]
    #[case(HlsType::Float, "float")]
    #[case(HlsType::ApFixed { width: 16, int_width: 8 }, "ap_fixed<16, 8>")]
    #[case(HlsType::ApUInt { width: 12 }, "ap_uint<12>")]
    #[case(HlsType::Stream(Box::new(HlsType::Int32)), "hls::stream<int32_t>")]
    fn renders_basic_types(#[case] ty: HlsType, #[case] expected: &str) {
        assert_eq!(ty.to_code(), expected);
    }

    #[test]
    fn renders_array_declaration() -> Result<(), HlsCodegenError> {
        let name = HlsIdentifier::new("buffer")?;
        let decl = HlsVarDecl {
            name,
            ty: HlsType::array(HlsType::Float, vec![4, 8])?,
            init: None,
        };
        assert_eq!(decl.render(), "float buffer[4][8];");
        Ok(())
    }

    #[test]
    fn renders_array_reference_parameter() -> Result<(), HlsCodegenError> {
        let param = HlsParameter {
            name: HlsIdentifier::new("weights")?,
            ty: HlsType::array(HlsType::Float, vec![4, 8])?,
            passing: PassingStyle::ConstReference,
        };
        assert_eq!(param.render(), "const float (&weights)[4][8]");
        Ok(())
    }

    #[test]
    fn renders_stream_read_and_write() -> Result<(), HlsCodegenError> {
        let stream = HlsIdentifier::new("axis")?;
        let target = HlsIdentifier::new("word")?;
        let write_val = HlsExpr::Literal(HlsLiteral::UInt(42));
        let read_stmt = HlsStatement::StreamRead {
            stream: stream.clone(),
            target: target.clone(),
        };
        let write_stmt = HlsStatement::StreamWrite {
            stream,
            value: write_val,
        };
        assert_eq!(read_stmt.render(1), "    word = axis.read();");
        assert_eq!(write_stmt.render(1), "    axis.write(42u);");
        Ok(())
    }

    #[test]
    fn renders_function_with_control_flow() -> Result<(), HlsCodegenError> {
        let func = HlsFunction {
            linkage: None,
            name: HlsIdentifier::new("compute")?,
            return_type: HlsType::Void,
            params: vec![HlsParameter {
                name: HlsIdentifier::new("iter")?,
                ty: HlsType::UInt32,
                passing: PassingStyle::Value,
            }],
            body: vec![
                HlsStatement::Pragma(HlsPragma::new("HLS inline")?),
                HlsStatement::ForLoop(HlsForLoop {
                    label: LoopLabel::new("loop0")?,
                    init: LoopInitializer::Declaration(HlsVarDecl {
                        name: HlsIdentifier::new("i")?,
                        ty: HlsType::Int32,
                        init: Some(HlsExpr::Literal(HlsLiteral::Int(0))),
                    }),
                    condition: HlsExpr::Binary {
                        op: HlsBinaryOp::Lt,
                        left: Box::new(HlsExpr::Identifier(HlsIdentifier::new("i")?)),
                        right: Box::new(HlsExpr::Identifier(HlsIdentifier::new("iter")?)),
                    },
                    increment: LoopIncrement::Unary(
                        HlsUnaryOp::PreIncrement,
                        HlsExpr::Identifier(HlsIdentifier::new("i")?),
                    ),
                    body: vec![HlsStatement::Comment("loop body".to_string())],
                }),
                HlsStatement::Return(None),
            ],
        };

        let rendered = func.to_code();
        assert!(rendered.contains("loop0: for (int32_t i = 0; (i < iter); ++i) {"));
        assert!(rendered.contains("// loop body"));
        Ok(())
    }

    #[test]
    fn builds_translation_unit_with_structs_and_aliases() -> Result<(), HlsCodegenError> {
        let include = HlsInclude::new("ap_int.h", true)?;
        let define = HlsDefine::new(HlsIdentifier::new("DEPTH")?, Some("64".to_string()))?;
        let payload = HlsStruct {
            name: HlsIdentifier::new("Payload")?,
            fields: vec![
                HlsField {
                    name: HlsIdentifier::new("id")?,
                    ty: HlsType::UInt32,
                },
                HlsField {
                    name: HlsIdentifier::new("value")?,
                    ty: HlsType::ApFixed {
                        width: 16,
                        int_width: 8,
                    },
                },
            ],
            attributes: Vec::new(),
        };
        let alias = HlsStatement::UsingAlias {
            name: HlsIdentifier::new("AxisWord")?,
            ty: HlsType::Custom("ap_axiu<32,0,0,0>".to_string()),
        };
        let func = HlsFunction {
            linkage: None,
            name: HlsIdentifier::new("top")?,
            return_type: HlsType::Void,
            params: vec![HlsParameter {
                name: HlsIdentifier::new("data")?,
                ty: HlsType::Stream(Box::new(HlsType::Custom("AxisWord".to_string()))),
                passing: PassingStyle::Reference,
            }],
            body: vec![
                HlsStatement::Struct(payload),
                alias,
                HlsStatement::StaticAssert {
                    condition: HlsExpr::Literal(HlsLiteral::Bool(true)),
                    message: Some("payload is valid".to_string()),
                },
                HlsStatement::Return(None),
            ],
        };

        let unit = HlsCompilationUnit {
            includes: vec![include],
            defines: vec![define],
            globals: Vec::new(),
            functions: vec![func],
        };

        let code = unit.to_code();
        assert!(code.starts_with("#include <ap_int.h>"));
        assert!(code.contains("#define DEPTH 64"));
        assert!(code.contains("struct Payload"));
        assert!(code.contains("using AxisWord = ap_axiu<32,0,0,0>;"));
        assert!(code.contains("static_assert(true, \"payload is valid\");"));
        Ok(())
    }
    #[test]
    fn renders_method_call_and_break() -> Result<(), HlsCodegenError> {
        let stream_id = HlsIdentifier::new("axis")?;
        let pkt = HlsIdentifier::new("pkt")?;
        let method_call = HlsExpr::MethodCall {
            target: Box::new(HlsExpr::Identifier(stream_id.clone())),
            method: HlsIdentifier::new("read_nb")?,
            args: vec![HlsExpr::Identifier(pkt.clone())],
        };
        let func = HlsFunction {
            linkage: Some(r#"extern "C""#),
            name: HlsIdentifier::new("consume")?,
            return_type: HlsType::Bool,
            params: vec![HlsParameter {
                name: stream_id.clone(),
                ty: HlsType::Stream(Box::new(HlsType::Custom("axis_pkt_t".to_string()))),
                passing: PassingStyle::Reference,
            }],
            body: vec![
                HlsStatement::IfElse(HlsIfElse {
                    condition: method_call,
                    then_body: vec![HlsStatement::Return(Some(HlsExpr::Literal(
                        HlsLiteral::Bool(true),
                    )))],
                    else_body: vec![HlsStatement::Break],
                }),
                HlsStatement::Return(Some(HlsExpr::Literal(HlsLiteral::Bool(false)))),
            ],
        };
        let rendered = func.to_code();
        assert!(rendered.contains(r#"extern "C" bool consume("#));
        assert!(rendered.contains("axis.read_nb(pkt)"));
        assert!(rendered.contains("break;"));
        Ok(())
    }
}
