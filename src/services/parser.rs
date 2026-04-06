use chumsky::{
    Stream,
    error::{Simple, SimpleReason},
    prelude::*,
};

use crate::domain::{
    ast::{
        Accessor, AlgoBlock, BinaryOp, EntityDef, Expr, FilterOp, HlsConfigBlock,
        HlsKernelGroupConfig, HlsTopologyConfig, Identifier, Lambda, Literal, MapOp, MemoryBackend,
        Operation, Program, PropertyDefinition, ReduceOp, ReturnStmt, SchemaBlock, Selector,
        Statement, TypeExpr, UnaryOp,
    },
    errors::{ParseError, Span},
};

type TokParser<'a, O> = BoxedParser<'a, Token, O, Simple<Token>>;

/// Parses source text into an AST `Program`.
pub fn parse_program(source: &str) -> Result<Program, ParseError> {
    let tokens = lexer()
        .parse(source)
        .map_err(|errs| map_char_errors(errs))?;

    let token_stream = Stream::from_iter(source.len()..source.len() + 1, tokens.into_iter());

    program_parser()
        .parse(token_stream)
        .map_err(|errs| map_token_errors(errs))
}

fn map_char_errors(errors: Vec<Simple<char>>) -> ParseError {
    errors
        .into_iter()
        .next()
        .map(|err| {
            let span = err.span();
            match err.reason() {
                SimpleReason::Unexpected => ParseError::with_span(
                    format!("unexpected character {:?}", err.found().copied()),
                    span,
                ),
                SimpleReason::Unclosed { span: inner, .. } => {
                    ParseError::with_span("unclosed delimiter", inner.clone())
                }
                SimpleReason::Custom(msg) => ParseError::with_span(msg.clone(), span),
            }
        })
        .unwrap_or_else(|| ParseError::Lexer("unknown lexer error".into()))
}

fn map_token_errors(errors: Vec<Simple<Token>>) -> ParseError {
    errors
        .into_iter()
        .next()
        .map(|err| {
            let span = err.span();
            match err.reason() {
                SimpleReason::Unexpected => {
                    let expected = err
                        .expected()
                        .map(|e| match e {
                            Some(token) => format!("{token:?}"),
                            None => "end of input".to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    ParseError::with_span(
                        format!(
                            "unexpected token {:?}, expected one of: {expected}",
                            err.found().cloned()
                        ),
                        span,
                    )
                }
                SimpleReason::Unclosed { span: inner, .. } => {
                    ParseError::with_span("unclosed token group", inner.clone())
                }
                SimpleReason::Custom(msg) => ParseError::with_span(msg.clone(), span),
            }
        })
        .unwrap_or_else(|| ParseError::Lexer("unknown parser error".into()))
}

fn lexer() -> impl Parser<char, Vec<(Token, Span)>, Error = Simple<char>> {
    let bool_lit = choice((
        just("true").to(Token::BoolLiteral(true)),
        just("false").to(Token::BoolLiteral(false)),
    ));

    let float = chomsky_float();

    let int = chumsky::text::int(10).map(Token::IntLiteral);

    let ident = chumsky::text::ident().map(|ident: String| match ident.as_str() {
        "HlsConfig" => Token::Keyword(Keyword::HlsConfig),
        "HierarchicalParam" | "HierachicalParam" => Token::Keyword(Keyword::HierarchicalParam),
        "GraphConfig" => Token::Keyword(Keyword::GraphConfig),
        "GraphSet" => Token::Keyword(Keyword::GraphSet),
        "Iteration" => Token::Keyword(Keyword::Iteration),
        "map" => Token::Keyword(Keyword::Map),
        "filter" => Token::Keyword(Keyword::Filter),
        "reduce" => Token::Keyword(Keyword::Reduce),
        "iteration_input" => Token::Keyword(Keyword::IterationInput),
        "Node" => Token::Keyword(Keyword::Node),
        "Edge" => Token::Keyword(Keyword::Edge),
        "return" => Token::Keyword(Keyword::Return),
        "as" => Token::Keyword(Keyword::As),
        "result_node_prop" => Token::Keyword(Keyword::ResultNodeProp),
        "key" => Token::Keyword(Keyword::Key),
        "values" => Token::Keyword(Keyword::Values),
        "function" => Token::Keyword(Keyword::Function),
        "lambda" => Token::Keyword(Keyword::Lambda),
        "int" => Token::Keyword(Keyword::Int),
        "float" => Token::Keyword(Keyword::Float),
        "fixed" => Token::Keyword(Keyword::Fixed),
        "bool" => Token::Keyword(Keyword::Bool),
        "set" => Token::Keyword(Keyword::Set),
        "tuple" => Token::Keyword(Keyword::Tuple),
        "array" => Token::Keyword(Keyword::Array),
        "vector" => Token::Keyword(Keyword::Vector),
        "matrix" => Token::Keyword(Keyword::Matrix),
        other => Token::Ident(other.to_string()),
    });

    // Split punctuation into smaller `choice` sets to avoid exceeding chumsky's tuple limits.
    let delimiters = choice((
        just("{").to(Token::Symbol(Symbol::LBrace)),
        just("}").to(Token::Symbol(Symbol::RBrace)),
        just("[").to(Token::Symbol(Symbol::LBracket)),
        just("]").to(Token::Symbol(Symbol::RBracket)),
        just("(").to(Token::Symbol(Symbol::LParen)),
        just(")").to(Token::Symbol(Symbol::RParen)),
    ));
    let separators = choice((
        just(":").to(Token::Symbol(Symbol::Colon)),
        just(",").to(Token::Symbol(Symbol::Comma)),
        just(".").to(Token::Symbol(Symbol::Dot)),
        just("?").to(Token::Symbol(Symbol::Question)),
        just("=").to(Token::Symbol(Symbol::Assign)),
    ));
    let arithmetic = choice((
        just("+").to(Token::Symbol(Symbol::Plus)),
        just("-").to(Token::Symbol(Symbol::Minus)),
        just("*").to(Token::Symbol(Symbol::Star)),
        just("/").to(Token::Symbol(Symbol::Slash)),
    ));
    let comparisons = choice((
        just("==").to(Token::Symbol(Symbol::EqEq)),
        just("!=").to(Token::Symbol(Symbol::Neq)),
        just("<=").to(Token::Symbol(Symbol::Le)),
        just(">=").to(Token::Symbol(Symbol::Ge)),
        just("<").to(Token::Symbol(Symbol::Lt)),
        just(">").to(Token::Symbol(Symbol::Gt)),
    ));
    // Ensure multi-character operators are attempted before single-character ones.
    let logic = choice((
        just("&&").to(Token::Symbol(Symbol::AmpAmp)),
        just("||").to(Token::Symbol(Symbol::PipePipe)),
        just("&").to(Token::Symbol(Symbol::Ampersand)),
        just("|").to(Token::Symbol(Symbol::Pipe)),
    ));
    let unary_ops = choice((
        just("!").to(Token::Symbol(Symbol::Bang)),
        just("~").to(Token::Symbol(Symbol::Tilde)),
    ));
    let punctuation = choice((
        delimiters,
        comparisons,
        logic,
        separators,
        arithmetic,
        unary_ops,
    ));

    let padding = skip_ws_or_comment().repeated().boxed();

    choice((bool_lit, float, int, ident, punctuation))
        .map_with_span(|tok, span| (tok, span))
        .padded_by(padding.clone())
        .repeated()
        .then_ignore(padding)
        .then_ignore(end())
}

fn chomsky_float() -> impl Parser<char, Token, Error = Simple<char>> {
    chumsky::text::int(10)
        .then_ignore(just('.'))
        .then(chumsky::text::digits(10))
        .map(|(whole, frac): (String, String)| Token::FloatLiteral(format!("{whole}.{frac}")))
}

fn skip_ws_or_comment() -> impl Parser<char, (), Error = Simple<char>> {
    let whitespace = filter(|c: &char| c.is_whitespace())
        .repeated()
        .at_least(1)
        .ignored();

    whitespace.or(just("//")
        .then(take_until(just('\n').ignored().or(end())))
        .ignored())
}

fn program_parser() -> impl Parser<Token, Program, Error = Simple<Token>> {
    schema_block()
        .then(hls_config_block().or_not())
        .then(hierarchical_param_block().or_not())
        .then(algo_block())
        .map(|(((schema, hls), _hierarchical), algorithm)| Program {
            schema,
            hls,
            algorithm,
        })
}

fn schema_block() -> impl Parser<Token, SchemaBlock, Error = Simple<Token>> {
    let body = sym(Symbol::LBrace)
        .ignore_then(schema_entries())
        .then_ignore(sym(Symbol::RBrace))
        .boxed();

    keyword(Keyword::GraphConfig)
        .or(keyword(Keyword::GraphSet))
        .ignore_then(body.clone())
        .or(body)
}

fn schema_entries() -> impl Parser<Token, SchemaBlock, Error = Simple<Token>> {
    schema_entry()
        .repeated()
        .at_least(1)
        .validate(|entries, span, emit| {
            let mut node = None;
            let mut edge = None;
            for entry in entries {
                match entry {
                    SchemaEntry::Node(def) => {
                        if node.replace(def).is_some() {
                            emit(Simple::custom(span.clone(), "duplicate Node definition"));
                        }
                    }
                    SchemaEntry::Edge(def) => {
                        if edge.replace(def).is_some() {
                            emit(Simple::custom(span.clone(), "duplicate Edge definition"));
                        }
                    }
                }
            }
            SchemaBlock { node, edge }
        })
}

fn schema_entry() -> impl Parser<Token, SchemaEntry, Error = Simple<Token>> {
    node_def()
        .map(SchemaEntry::Node)
        .or(edge_def().map(SchemaEntry::Edge))
}

fn node_def() -> impl Parser<Token, EntityDef, Error = Simple<Token>> {
    keyword(Keyword::Node)
        .ignore_then(sym(Symbol::Colon))
        .ignore_then(sym(Symbol::LBrace))
        .ignore_then(property_list())
        .then_ignore(sym(Symbol::RBrace))
        .map(|properties| EntityDef { properties })
}

fn edge_def() -> impl Parser<Token, EntityDef, Error = Simple<Token>> {
    keyword(Keyword::Edge)
        .ignore_then(sym(Symbol::Colon))
        .ignore_then(sym(Symbol::LBrace))
        .ignore_then(property_list())
        .then_ignore(sym(Symbol::RBrace))
        .map(|properties| EntityDef { properties })
        .try_map(|entity, span| {
            let reserved = entity
                .properties
                .iter()
                .find(|prop| matches!(prop.name.as_str(), "src" | "dst"));
            if let Some(prop) = reserved {
                Err(Simple::custom(
                    span,
                    format!(
                        "edge property '{}' is reserved and must not be declared",
                        prop.name
                    ),
                ))
            } else {
                Ok(entity)
            }
        })
}

fn property_list() -> impl Parser<Token, Vec<PropertyDefinition>, Error = Simple<Token>> {
    property_def().repeated()
}

fn property_def() -> impl Parser<Token, PropertyDefinition, Error = Simple<Token>> {
    identifier()
        .then_ignore(sym(Symbol::Colon))
        .then(type_expr())
        .map(|(name, ty)| PropertyDefinition { name, ty })
}

fn hls_config_block() -> impl Parser<Token, HlsConfigBlock, Error = Simple<Token>> {
    keyword(Keyword::HlsConfig)
        .ignore_then(sym(Symbol::LBrace))
        .ignore_then(hls_config_entries())
        .then_ignore(sym(Symbol::RBrace))
}

fn hierarchical_param_block() -> impl Parser<Token, (), Error = Simple<Token>> {
    keyword(Keyword::HierarchicalParam).ignore_then(ignored_brace_block())
}

fn ignored_brace_block() -> impl Parser<Token, (), Error = Simple<Token>> {
    recursive(|block| {
        let non_brace = filter(|tok: &Token| {
            !matches!(
                tok,
                Token::Symbol(Symbol::LBrace) | Token::Symbol(Symbol::RBrace)
            )
        })
        .ignored();

        sym(Symbol::LBrace)
            .ignore_then(choice((block, non_brace)).repeated())
            .then_ignore(sym(Symbol::RBrace))
            .ignored()
    })
}

fn hls_config_entries() -> impl Parser<Token, HlsConfigBlock, Error = Simple<Token>> {
    hls_config_entry()
        .then_ignore(sym(Symbol::Comma).or_not())
        .repeated()
        .validate(|entries, span, emit| {
            let mut topology = None;
            let mut memory = MemoryBackend::default();
            let mut memory_set = false;
            let mut local_id_bits: u32 = 32;
            let mut zero_sentinel: bool = true;
            let mut no_l1_preprocess: bool = false;
            for entry in entries {
                match entry {
                    HlsConfigEntry::Topology(topo) => {
                        if topology.replace(topo).is_some() {
                            emit(Simple::custom(span.clone(), "duplicate topology entry"));
                        }
                    }
                    HlsConfigEntry::Memory(mem) => {
                        if memory_set {
                            emit(Simple::custom(span.clone(), "duplicate memory entry"));
                        }
                        memory = mem;
                        memory_set = true;
                    }
                    HlsConfigEntry::LocalIdBits(bits) => {
                        local_id_bits = bits;
                    }
                    HlsConfigEntry::ZeroSentinel(val) => {
                        zero_sentinel = val;
                    }
                    HlsConfigEntry::NoL1Preprocess(val) => {
                        no_l1_preprocess = val;
                    }
                }
            }
            HlsConfigBlock {
                topology,
                memory,
                local_id_bits,
                zero_sentinel,
                no_l1_preprocess,
            }
        })
}

fn hls_config_entry() -> impl Parser<Token, HlsConfigEntry, Error = Simple<Token>> {
    let topology_entry = identifier()
        .then_ignore(sym(Symbol::Colon))
        .then(topology_value())
        .try_map(|(key, topo), span| {
            if key.as_str() != "topology" {
                return Err(Simple::custom(
                    span,
                    format!("expected 'topology', got '{}'", key.as_str()),
                ));
            }
            Ok(HlsConfigEntry::Topology(topo))
        });

    let memory_entry = identifier()
        .then_ignore(sym(Symbol::Colon))
        .then(identifier())
        .try_map(|(key, value), span| {
            if key.as_str() != "memory" {
                return Err(Simple::custom(
                    span,
                    format!("expected 'memory', got '{}'", key.as_str()),
                ));
            }
            match value.as_str() {
                "hbm" => Ok(HlsConfigEntry::Memory(MemoryBackend::Hbm)),
                "ddr" => Ok(HlsConfigEntry::Memory(MemoryBackend::Ddr)),
                other => Err(Simple::custom(
                    span,
                    format!("memory must be 'hbm' or 'ddr', got '{other}'"),
                )),
            }
        });

    let local_id_bits_entry = identifier()
        .then_ignore(sym(Symbol::Colon))
        .then(u32_literal())
        .try_map(|(key, value), span| {
            if key.as_str() != "local_id_bits" {
                return Err(Simple::custom(
                    span,
                    format!("expected 'local_id_bits', got '{}'", key.as_str()),
                ));
            }
            if value == 0 || value > 32 {
                return Err(Simple::custom(span, "local_id_bits must be in [1, 32]"));
            }
            Ok(HlsConfigEntry::LocalIdBits(value))
        });

    let zero_sentinel_entry = identifier()
        .then_ignore(sym(Symbol::Colon))
        .then(select! { Token::BoolLiteral(b) => b })
        .try_map(|(key, value), span| {
            if key.as_str() != "zero_sentinel" {
                return Err(Simple::custom(
                    span,
                    format!("expected 'zero_sentinel', got '{}'", key.as_str()),
                ));
            }
            Ok(HlsConfigEntry::ZeroSentinel(value))
        });

    let no_l1_preprocess_entry = identifier()
        .then_ignore(sym(Symbol::Colon))
        .then(select! { Token::BoolLiteral(b) => b })
        .try_map(|(key, value), span| {
            if key.as_str() != "no_l1_preprocess" {
                return Err(Simple::custom(
                    span,
                    format!("expected 'no_l1_preprocess', got '{}'", key.as_str()),
                ));
            }
            Ok(HlsConfigEntry::NoL1Preprocess(value))
        });

    topology_entry
        .or(memory_entry)
        .or(local_id_bits_entry)
        .or(zero_sentinel_entry)
        .or(no_l1_preprocess_entry)
}

fn topology_value() -> impl Parser<Token, HlsTopologyConfig, Error = Simple<Token>> {
    sym(Symbol::LBrace)
        .ignore_then(topology_entries())
        .then_ignore(sym(Symbol::RBrace))
}

fn topology_entries() -> impl Parser<Token, HlsTopologyConfig, Error = Simple<Token>> {
    topology_entry()
        .then_ignore(sym(Symbol::Comma).or_not())
        .repeated()
        .validate(|entries, span, emit| {
            let mut apply_slr: Option<u8> = None;
            let mut hbm_writer_slr: Option<u8> = None;
            let mut cross_slr_fifo_depth: Option<u32> = None;
            let mut little_groups: Option<Vec<HlsKernelGroupConfig>> = None;
            let mut big_groups: Option<Vec<HlsKernelGroupConfig>> = None;

            for entry in entries {
                match entry {
                    TopologyEntry::ApplySlr(v) => {
                        if apply_slr.replace(v).is_some() {
                            emit(Simple::custom(span.clone(), "duplicate apply_slr"));
                        }
                    }
                    TopologyEntry::HbmWriterSlr(v) => {
                        if hbm_writer_slr.replace(v).is_some() {
                            emit(Simple::custom(span.clone(), "duplicate hbm_writer_slr"));
                        }
                    }
                    TopologyEntry::CrossSlrFifoDepth(v) => {
                        if cross_slr_fifo_depth.replace(v).is_some() {
                            emit(Simple::custom(
                                span.clone(),
                                "duplicate cross_slr_fifo_depth",
                            ));
                        }
                    }
                    TopologyEntry::LittleGroups(v) => {
                        if little_groups.replace(v).is_some() {
                            emit(Simple::custom(span.clone(), "duplicate little_groups"));
                        }
                    }
                    TopologyEntry::BigGroups(v) => {
                        if big_groups.replace(v).is_some() {
                            emit(Simple::custom(span.clone(), "duplicate big_groups"));
                        }
                    }
                }
            }

            let apply_slr = apply_slr.unwrap_or(1);
            let hbm_writer_slr = hbm_writer_slr.unwrap_or(0);
            let cross_slr_fifo_depth = cross_slr_fifo_depth.unwrap_or(16);
            let little_groups = little_groups.unwrap_or_default();
            let big_groups = big_groups.unwrap_or_default();

            if little_groups.is_empty() && big_groups.is_empty() {
                emit(Simple::custom(
                    span.clone(),
                    "topology must configure at least one kernel group",
                ));
            }

            HlsTopologyConfig {
                apply_slr,
                hbm_writer_slr,
                cross_slr_fifo_depth,
                little_groups,
                big_groups,
            }
        })
}

fn topology_entry() -> impl Parser<Token, TopologyEntry, Error = Simple<Token>> {
    identifier()
        .then_ignore(sym(Symbol::Colon))
        .then(choice((
            u32_literal().map(TopologyValue::U32),
            group_list().map(TopologyValue::Groups),
        )))
        .try_map(|(key, value), span| match key.as_str() {
            "apply_slr" => match value {
                TopologyValue::U32(v) if v <= u8::MAX as u32 => {
                    Ok(TopologyEntry::ApplySlr(v as u8))
                }
                _ => Err(Simple::custom(span, "apply_slr must be <= 255")),
            },
            "hbm_writer_slr" => match value {
                TopologyValue::U32(v) if v <= u8::MAX as u32 => {
                    Ok(TopologyEntry::HbmWriterSlr(v as u8))
                }
                _ => Err(Simple::custom(span, "hbm_writer_slr must be <= 255")),
            },
            "cross_slr_fifo_depth" => match value {
                TopologyValue::U32(v) => Ok(TopologyEntry::CrossSlrFifoDepth(v)),
                _ => Err(Simple::custom(span, "cross_slr_fifo_depth must be int")),
            },
            "little_groups" => match value {
                TopologyValue::Groups(v) => Ok(TopologyEntry::LittleGroups(v)),
                _ => Err(Simple::custom(span, "little_groups must be [ {..}, .. ]")),
            },
            "big_groups" => match value {
                TopologyValue::Groups(v) => Ok(TopologyEntry::BigGroups(v)),
                _ => Err(Simple::custom(span, "big_groups must be [ {..}, .. ]")),
            },
            other => Err(Simple::custom(
                span,
                format!("unknown topology entry '{other}'"),
            )),
        })
}

fn group_list() -> impl Parser<Token, Vec<HlsKernelGroupConfig>, Error = Simple<Token>> {
    sym(Symbol::LBracket)
        .ignore_then(
            group_object()
                .then_ignore(sym(Symbol::Comma).or_not())
                .repeated()
                .collect::<Vec<_>>(),
        )
        .then_ignore(sym(Symbol::RBracket))
}

fn group_object() -> impl Parser<Token, HlsKernelGroupConfig, Error = Simple<Token>> {
    sym(Symbol::LBrace)
        .ignore_then(group_entries())
        .then_ignore(sym(Symbol::RBrace))
}

fn group_entries() -> impl Parser<Token, HlsKernelGroupConfig, Error = Simple<Token>> {
    group_entry()
        .then_ignore(sym(Symbol::Comma).or_not())
        .repeated()
        .validate(|entries, span, emit| {
            let mut pipelines: Option<u32> = None;
            let mut merger_slr: Option<u8> = None;
            let mut pipeline_slr: Option<Vec<u8>> = None;

            for entry in entries {
                match entry {
                    GroupEntry::Pipelines(v) => {
                        if pipelines.replace(v).is_some() {
                            emit(Simple::custom(span.clone(), "duplicate pipelines"));
                        }
                    }
                    GroupEntry::MergerSlr(v) => {
                        if merger_slr.replace(v).is_some() {
                            emit(Simple::custom(span.clone(), "duplicate merger_slr"));
                        }
                    }
                    GroupEntry::PipelineSlr(v) => {
                        if pipeline_slr.replace(v).is_some() {
                            emit(Simple::custom(span.clone(), "duplicate pipeline_slr"));
                        }
                    }
                }
            }

            let pipelines = pipelines.unwrap_or(0);
            if pipelines == 0 {
                emit(Simple::custom(span.clone(), "group.pipelines must be >= 1"));
            }

            let merger_slr = merger_slr.unwrap_or(0);
            let pipeline_slr = pipeline_slr.unwrap_or_else(|| vec![merger_slr; pipelines as usize]);

            if pipeline_slr.len() != pipelines as usize {
                emit(Simple::custom(
                    span.clone(),
                    format!(
                        "group.pipeline_slr length ({}) must equal pipelines ({pipelines})",
                        pipeline_slr.len()
                    ),
                ));
            }

            HlsKernelGroupConfig {
                pipelines,
                merger_slr,
                pipeline_slr,
            }
        })
}

fn group_entry() -> impl Parser<Token, GroupEntry, Error = Simple<Token>> {
    identifier()
        .then_ignore(sym(Symbol::Colon))
        .then(choice((
            u32_literal().map(GroupValue::U32),
            u8_list().map(GroupValue::U8List),
        )))
        .try_map(|(key, value), span| match key.as_str() {
            "pipelines" => match value {
                GroupValue::U32(v) => Ok(GroupEntry::Pipelines(v)),
                _ => Err(Simple::custom(span, "pipelines must be int")),
            },
            "merger_slr" => match value {
                GroupValue::U32(v) if v <= u8::MAX as u32 => Ok(GroupEntry::MergerSlr(v as u8)),
                _ => Err(Simple::custom(span, "merger_slr must be <= 255")),
            },
            "pipeline_slr" => match value {
                GroupValue::U8List(v) => Ok(GroupEntry::PipelineSlr(v)),
                _ => Err(Simple::custom(span, "pipeline_slr must be [..]")),
            },
            other => Err(Simple::custom(
                span,
                format!("unknown group entry '{other}'"),
            )),
        })
}

fn u8_list() -> impl Parser<Token, Vec<u8>, Error = Simple<Token>> {
    sym(Symbol::LBracket)
        .ignore_then(
            u32_literal()
                .separated_by(sym(Symbol::Comma))
                .allow_trailing()
                .collect::<Vec<_>>()
                .validate(|vals, span, emit| {
                    let mut out = Vec::with_capacity(vals.len());
                    for v in vals {
                        if v > u8::MAX as u32 {
                            emit(Simple::custom(span.clone(), "SLR must be <= 255"));
                        } else {
                            out.push(v as u8);
                        }
                    }
                    out
                }),
        )
        .then_ignore(sym(Symbol::RBracket))
}

fn u32_literal() -> impl Parser<Token, u32, Error = Simple<Token>> {
    select! { Token::IntLiteral(raw) => raw }.try_map(|raw, span| {
        raw.parse::<u32>()
            .map_err(|e| Simple::custom(span, format!("expected u32 literal, got '{raw}': {e}")))
    })
}

fn type_expr() -> impl Parser<Token, TypeExpr, Error = Simple<Token>> {
    recursive(|ty| {
        let int_ty = keyword(Keyword::Int)
            .ignore_then(sym(Symbol::Lt))
            .ignore_then(integer_literal())
            .then_ignore(sym(Symbol::Gt))
            .map(|width| TypeExpr::Int {
                width: width as u32,
            });

        let float_ty = keyword(Keyword::Float).to(TypeExpr::Float);

        let fixed_ty = keyword(Keyword::Fixed)
            .ignore_then(sym(Symbol::Lt))
            .ignore_then(integer_literal())
            .then_ignore(sym(Symbol::Comma))
            .then(integer_literal())
            .then_ignore(sym(Symbol::Gt))
            .map(|(width, int_width)| TypeExpr::Fixed {
                width: width as u32,
                int_width: int_width as u32,
            });

        let bool_ty = keyword(Keyword::Bool).to(TypeExpr::Bool);

        let set_ty = keyword(Keyword::Set)
            .ignore_then(sym(Symbol::Lt))
            .ignore_then(ty.clone())
            .then_ignore(sym(Symbol::Gt))
            .map(|inner| TypeExpr::Set(Box::new(inner)));

        let array_ty = keyword(Keyword::Array)
            .ignore_then(sym(Symbol::Lt))
            .ignore_then(ty.clone())
            .then_ignore(sym(Symbol::Gt))
            .map(|inner| TypeExpr::Array(Box::new(inner)));

        let vector_ty = keyword(Keyword::Vector)
            .ignore_then(sym(Symbol::Lt))
            .ignore_then(ty.clone())
            .then_ignore(sym(Symbol::Comma))
            .then(integer_literal())
            .then_ignore(sym(Symbol::Gt))
            .map(|(element, len)| TypeExpr::Vector {
                element: Box::new(element),
                len: len as u32,
            });

        let matrix_ty = keyword(Keyword::Matrix)
            .ignore_then(sym(Symbol::Lt))
            .ignore_then(ty.clone())
            .then_ignore(sym(Symbol::Comma))
            .then(integer_literal())
            .then_ignore(sym(Symbol::Comma))
            .then(integer_literal())
            .then_ignore(sym(Symbol::Gt))
            .map(|((element, rows), cols)| TypeExpr::Matrix {
                element: Box::new(element),
                rows: rows as u32,
                cols: cols as u32,
            });

        let tuple_ty = keyword(Keyword::Tuple)
            .ignore_then(sym(Symbol::Lt))
            .ignore_then(ty.clone().separated_by(sym(Symbol::Comma)))
            .then_ignore(sym(Symbol::Gt))
            .map(TypeExpr::Tuple);

        choice((
            int_ty, float_ty, fixed_ty, bool_ty, set_ty, tuple_ty, array_ty, vector_ty, matrix_ty,
        ))
    })
}

fn algo_block() -> impl Parser<Token, AlgoBlock, Error = Simple<Token>> {
    let body = sym(Symbol::LBrace)
        .ignore_then(statement().repeated())
        .then(return_stmt())
        .then_ignore(sym(Symbol::RBrace))
        .map(|(statements, return_stmt)| AlgoBlock {
            statements,
            return_stmt,
        })
        .boxed();

    keyword(Keyword::Iteration)
        .ignore_then(body.clone())
        .or(body)
}

fn statement() -> impl Parser<Token, Statement, Error = Simple<Token>> {
    identifier()
        .then_ignore(sym(Symbol::Assign))
        .then(operation())
        .map(|(target, operation)| Statement { target, operation })
}

fn operation() -> impl Parser<Token, Operation, Error = Simple<Token>> {
    choice((
        iteration_input().map(Operation::IterationInput),
        map_op().map(Operation::Map),
        filter_op().map(Operation::Filter),
        reduce_op().map(Operation::Reduce),
    ))
}

fn iteration_input() -> impl Parser<Token, Selector, Error = Simple<Token>> {
    keyword(Keyword::IterationInput)
        .ignore_then(sym(Symbol::LParen))
        .ignore_then(selector())
        .then_ignore(sym(Symbol::RParen))
}

fn map_op() -> impl Parser<Token, MapOp, Error = Simple<Token>> {
    keyword(Keyword::Map)
        .ignore_then(sym(Symbol::LParen))
        .ignore_then(sym(Symbol::LBracket))
        .ignore_then(arg_list())
        .then_ignore(sym(Symbol::RBracket))
        .then_ignore(sym(Symbol::Comma))
        .then(lambda_expr())
        .then_ignore(sym(Symbol::RParen))
        .map(|(inputs, lambda)| MapOp { inputs, lambda })
}

fn filter_op() -> impl Parser<Token, FilterOp, Error = Simple<Token>> {
    keyword(Keyword::Filter)
        .ignore_then(sym(Symbol::LParen))
        .ignore_then(sym(Symbol::LBracket))
        .ignore_then(arg_list())
        .then_ignore(sym(Symbol::RBracket))
        .then_ignore(sym(Symbol::Comma))
        .then(lambda_expr())
        .then_ignore(sym(Symbol::RParen))
        .map(|(inputs, lambda)| FilterOp { inputs, lambda })
}

fn reduce_op() -> impl Parser<Token, ReduceOp, Error = Simple<Token>> {
    keyword(Keyword::Reduce)
        .ignore_then(sym(Symbol::LParen))
        .ignore_then(keyword(Keyword::Key))
        .ignore_then(sym(Symbol::Assign))
        .ignore_then(identifier())
        .then_ignore(sym(Symbol::Comma))
        .then_ignore(keyword(Keyword::Values))
        .then_ignore(sym(Symbol::Assign))
        .then_ignore(sym(Symbol::LBracket))
        .then(arg_list())
        .then_ignore(sym(Symbol::RBracket))
        .then_ignore(sym(Symbol::Comma))
        .then_ignore(keyword(Keyword::Function))
        .then_ignore(sym(Symbol::Assign))
        .then(lambda_expr())
        .then_ignore(sym(Symbol::RParen))
        .map(|((key, values), function)| ReduceOp {
            key,
            values,
            function,
        })
}

fn return_stmt() -> impl Parser<Token, ReturnStmt, Error = Simple<Token>> {
    keyword(Keyword::Return)
        .ignore_then(identifier())
        .then_ignore(keyword(Keyword::As))
        .then_ignore(keyword(Keyword::ResultNodeProp))
        .then_ignore(sym(Symbol::Dot))
        .then(identifier())
        .map(|(value, property)| ReturnStmt { value, property })
}

fn arg_list() -> impl Parser<Token, Vec<Identifier>, Error = Simple<Token>> {
    identifier().separated_by(sym(Symbol::Comma)).at_least(1)
}

fn lambda_expr() -> impl Parser<Token, Lambda, Error = Simple<Token>> {
    keyword(Keyword::Lambda)
        .ignore_then(identifier().separated_by(sym(Symbol::Comma)).at_least(1))
        .then_ignore(sym(Symbol::Colon))
        .then(expr_parser())
        .map(|(params, body)| Lambda { params, body })
}

fn selector() -> impl Parser<Token, Selector, Error = Simple<Token>> {
    identifier()
        .try_map(|ident, span| {
            if ident.as_str() == "G" {
                Ok(ident)
            } else {
                Err(Simple::custom(span, "selector must start with 'G'"))
            }
        })
        .ignore_then(sym(Symbol::Dot))
        .ignore_then(identifier().try_map(|ident, span| match ident.as_str() {
            "NODES" => Ok(Selector::Nodes),
            "EDGES" => Ok(Selector::Edges),
            _ => Err(Simple::custom(span, "unknown selector")),
        }))
}

fn expr_parser() -> TokParser<'static, Expr> {
    recursive(|expr| {
        let literal = literal_parser();
        let grouped = sym(Symbol::LParen)
            .ignore_then(expr.clone())
            .then_ignore(sym(Symbol::RParen))
            .boxed();

        let call = identifier()
            .then(
                sym(Symbol::LParen)
                    .ignore_then(expr.clone().separated_by(sym(Symbol::Comma)))
                    .then_ignore(sym(Symbol::RParen)),
            )
            .map(|(function, args)| Expr::Call { function, args })
            .boxed();

        let variable = identifier().map(Expr::Identifier).boxed();

        let base = choice((grouped, literal.clone(), call.clone(), variable.clone())).boxed();

        let member = base
            .clone()
            .then(sym(Symbol::Dot).ignore_then(member_accessor()).repeated())
            .map(|(root, accesses)| {
                accesses
                    .into_iter()
                    .fold(root, |target, access| Expr::MemberAccess {
                        target: Box::new(target),
                        access,
                    })
            })
            .boxed();

        let unary = prefix_ops()
            .then(member.clone())
            .map(|(ops, expr)| {
                ops.into_iter().rev().fold(expr, |acc, op| Expr::Unary {
                    op,
                    expr: Box::new(acc),
                })
            })
            .boxed();

        let multiplicative = binary_layer(
            unary.clone(),
            &[
                (Symbol::Star, BinaryOp::Mul),
                (Symbol::Slash, BinaryOp::Div),
            ],
        );

        let additive = binary_layer(
            multiplicative.clone(),
            &[
                (Symbol::Plus, BinaryOp::Add),
                (Symbol::Minus, BinaryOp::Sub),
            ],
        );

        let relational = binary_layer(
            additive.clone(),
            &[
                (Symbol::Lt, BinaryOp::Lt),
                (Symbol::Le, BinaryOp::Le),
                (Symbol::Gt, BinaryOp::Gt),
                (Symbol::Ge, BinaryOp::Ge),
            ],
        );

        let equality = binary_layer(
            relational.clone(),
            &[(Symbol::EqEq, BinaryOp::Eq), (Symbol::Neq, BinaryOp::Ne)],
        );

        let bit_and_layer =
            binary_layer(equality.clone(), &[(Symbol::Ampersand, BinaryOp::BitAnd)]);

        let bit_or_layer = binary_layer(bit_and_layer.clone(), &[(Symbol::Pipe, BinaryOp::BitOr)]);

        let logical_and_layer =
            binary_layer(bit_or_layer.clone(), &[(Symbol::AmpAmp, BinaryOp::And)]);

        let logical_or_layer = binary_layer(
            logical_and_layer.clone(),
            &[(Symbol::PipePipe, BinaryOp::Or)],
        );

        logical_or_layer
            .clone()
            .then(
                sym(Symbol::Question)
                    .ignore_then(expr.clone())
                    .then_ignore(sym(Symbol::Colon))
                    .then(expr.clone())
                    .or_not(),
            )
            .map(|(condition, branches)| {
                if let Some((then_expr, else_expr)) = branches {
                    Expr::Ternary {
                        condition: Box::new(condition),
                        then_expr: Box::new(then_expr),
                        else_expr: Box::new(else_expr),
                    }
                } else {
                    condition
                }
            })
            .boxed()
    })
    .boxed()
}

fn literal_parser() -> TokParser<'static, Expr> {
    choice((
        select! { Token::IntLiteral(raw) => raw }.try_map(|raw, span| {
            raw.parse::<i64>()
                .map(|value| Expr::Literal(Literal::Int(value)))
                .map_err(|_| Simple::custom(span, "integer literal out of range"))
        }),
        select! { Token::FloatLiteral(raw) => Expr::Literal(Literal::Float(raw)) },
        select! { Token::BoolLiteral(value) => Expr::Literal(Literal::Bool(value)) },
    ))
    .boxed()
}

fn member_accessor() -> TokParser<'static, Accessor> {
    choice((
        identifier().map(Accessor::Property),
        select! { Token::IntLiteral(raw) => raw }.try_map(|raw, span| {
            raw.parse::<u32>()
                .map(Accessor::Index)
                .map_err(|_| Simple::custom(span, "tuple index must fit u32"))
        }),
    ))
    .boxed()
}

fn integer_literal() -> impl Parser<Token, i64, Error = Simple<Token>> {
    select! { Token::IntLiteral(raw) => raw }.try_map(|raw, span| {
        raw.parse::<i64>()
            .map_err(|_| Simple::custom(span, "integer literal out of range"))
    })
}

fn prefix_ops() -> impl Parser<Token, Vec<UnaryOp>, Error = Simple<Token>> {
    choice((
        sym(Symbol::Bang).to(UnaryOp::Not),
        sym(Symbol::Tilde).to(UnaryOp::BitNot),
    ))
    .repeated()
}

fn binary_layer(
    lower: TokParser<'static, Expr>,
    ops: &[(Symbol, BinaryOp)],
) -> TokParser<'static, Expr> {
    if ops.is_empty() {
        return lower;
    }

    let (first_symbol, first_op) = ops[0];
    let mut op_parser = sym(first_symbol).to(first_op).boxed();
    for (symbol, op) in ops[1..].iter().copied() {
        op_parser = op_parser.or(sym(symbol).to(op)).boxed();
    }

    let repeated = op_parser.then(lower.clone()).repeated();

    lower
        .clone()
        .then(repeated)
        .map(|(first, rest)| {
            rest.into_iter()
                .fold(first, |left, (op, right)| Expr::Binary {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                })
        })
        .boxed()
}

fn identifier() -> impl Parser<Token, Identifier, Error = Simple<Token>> {
    select! { Token::Ident(name) => Identifier::new(name) }
}

fn keyword(keyword: Keyword) -> impl Parser<Token, (), Error = Simple<Token>> {
    just(Token::Keyword(keyword)).ignored()
}

fn sym(symbol: Symbol) -> impl Parser<Token, (), Error = Simple<Token>> {
    just(Token::Symbol(symbol)).ignored()
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Token {
    Keyword(Keyword),
    Ident(String),
    IntLiteral(String),
    FloatLiteral(String),
    BoolLiteral(bool),
    Symbol(Symbol),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Keyword {
    Map,
    Filter,
    Reduce,
    IterationInput,
    GraphConfig,
    GraphSet,
    Iteration,
    HierarchicalParam,
    Node,
    Edge,
    Return,
    As,
    ResultNodeProp,
    Key,
    Values,
    Function,
    Lambda,
    HlsConfig,
    Int,
    Float,
    Fixed,
    Bool,
    Set,
    Tuple,
    Array,
    Vector,
    Matrix,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Symbol {
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    LParen,
    RParen,
    Colon,
    Comma,
    Dot,
    Assign,
    Plus,
    Minus,
    Star,
    Slash,
    EqEq,
    Neq,
    Lt,
    Gt,
    Le,
    Ge,
    Ampersand,
    Pipe,
    AmpAmp,
    PipePipe,
    Question,
    Bang,
    Tilde,
}

enum SchemaEntry {
    Node(EntityDef),
    Edge(EntityDef),
}

enum HlsConfigEntry {
    Topology(HlsTopologyConfig),
    Memory(MemoryBackend),
    LocalIdBits(u32),
    ZeroSentinel(bool),
    NoL1Preprocess(bool),
}

enum TopologyEntry {
    ApplySlr(u8),
    HbmWriterSlr(u8),
    CrossSlrFifoDepth(u32),
    LittleGroups(Vec<HlsKernelGroupConfig>),
    BigGroups(Vec<HlsKernelGroupConfig>),
}

enum TopologyValue {
    U32(u32),
    Groups(Vec<HlsKernelGroupConfig>),
}

enum GroupEntry {
    Pipelines(u32),
    MergerSlr(u8),
    PipelineSlr(Vec<u8>),
}

enum GroupValue {
    U32(u32),
    U8List(Vec<u8>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::DebugSummary;
    use rstest::rstest;

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
    fn parses_sample_program() {
        let program = parse_program(SAMPLE).expect("failed to parse sample");
        assert_eq!(
            program.schema.node.as_ref().map(|n| n.properties.len()),
            Some(1)
        );
        assert_eq!(
            program.schema.edge.as_ref().map(|e| e.properties.len()),
            Some(1)
        );
        assert!(program.hls.is_none());
        assert_eq!(program.algorithm.statements.len(), 4);
        let summary = program.debug_summary();
        assert!(
            summary.contains("stmt 0:"),
            "missing stmt 0 in summary: {summary}"
        );
        assert!(
            summary.contains("stmt 3:"),
            "missing stmt 3 in summary: {summary}"
        );
        assert!(
            summary.contains("return:"),
            "missing return line in summary: {summary}"
        );
    }

    #[rstest]
    fn rejects_reserved_edge_property() {
        let dsl = r"{
            Edge: { src: int<32> }
        }
        { return foo as result_node_prop.bar }
        ";
        let err = parse_program(dsl).expect_err("expected failure");
        assert!(err.to_string().contains("reserved"));
    }

    #[test]
    fn parses_graph_coloring_app() {
        let source = include_str!("../../apps/graph_coloring.dsl");
        let program = parse_program(source).expect("graph coloring parses");
        assert_eq!(program.algorithm.statements.len(), 5);
    }

    #[test]
    fn parses_bitwise_and_expression() {
        let dsl = r"{
            Node: { dist: int<32> }
            Edge: { }
        }
        {
            edges = iteration_input(G.EDGES)
            vals = map([edges], lambda e: 1 & 3)
            return vals as result_node_prop.dist
        }";
        let program = parse_program(dsl).expect("parse");
        let stmt = &program.algorithm.statements[1];
        let Operation::Map(map) = &stmt.operation else {
            panic!("expected map");
        };
        let Expr::Binary { op, .. } = &map.lambda.body else {
            panic!("expected binary expr");
        };
        assert_eq!(*op, BinaryOp::BitAnd);
    }

    #[test]
    fn parses_logical_and_expression() {
        let dsl = r"{
            Node: { active: bool }
            Edge: { }
        }
        {
            edges = iteration_input(G.EDGES)
            vals = map([edges], lambda e: true && false)
            return vals as result_node_prop.active
        }";
        let program = parse_program(dsl).expect("parse");
        let stmt = &program.algorithm.statements[1];
        let Operation::Map(map) = &stmt.operation else {
            panic!("expected map");
        };
        let Expr::Binary { op, .. } = &map.lambda.body else {
            panic!("expected binary expr");
        };
        assert_eq!(*op, BinaryOp::And);
    }

    #[test]
    fn parses_hls_config_topology_block() {
        let dsl = r"{
            Node: { dist: int<32> }
            Edge: { weight: int<32> }
        }

        HlsConfig {
            topology: {
                apply_slr: 1
                hbm_writer_slr: 0
                cross_slr_fifo_depth: 16
                little_groups: [
                    { pipelines: 6 merger_slr: 0 pipeline_slr: [0,0,0,0,1,1] },
                    { pipelines: 6 merger_slr: 1 pipeline_slr: [1,1,1,2,2,2] }
                ]
                big_groups: [
                    { pipelines: 2 merger_slr: 2 pipeline_slr: [2,2] }
                ]
            }
        }

        { return foo as result_node_prop.dist }
        ";

        let program = parse_program(dsl).expect("should parse");
        let topo = program
            .hls
            .as_ref()
            .and_then(|h| h.topology.as_ref())
            .expect("expected topology");
        assert_eq!(topo.apply_slr, 1);
        assert_eq!(topo.hbm_writer_slr, 0);
        assert_eq!(topo.cross_slr_fifo_depth, 16);
        assert_eq!(topo.little_groups.len(), 2);
        assert_eq!(topo.big_groups.len(), 1);
        assert_eq!(topo.little_groups[0].pipelines, 6);
        assert_eq!(topo.little_groups[1].pipeline_slr.len(), 6);
    }

    #[test]
    fn parses_graphconfig_iteration_with_hierarchical_param() {
        let dsl = r"
        GraphConfig {
            Node: { dist: int<32> }
            Edge: { weight: int<32> }
        }

        HierarchicalParam {
            L1: { URAM_SIZE: 262144 }
            L3: {
                pipe_partition: [
                    { type: big number: 2 },
                    { type: little number: 3 },
                    { type: little number: 3 }
                ]
            }
        }

        Iteration {
            edges = iteration_input(G.EDGES)
            dst_ids = map([edges], lambda e: e.dst)
            updates = map([edges], lambda e: e.src.dist + e.weight)
            min_dists = reduce(key=dst_ids, values=[updates], function=lambda x, y: x > y ? y : x)
            return min_dists as result_node_prop.dist
        }
        ";

        let program = parse_program(dsl).expect("should parse GraphConfig/Iteration");
        assert_eq!(program.algorithm.statements.len(), 4);
        assert_eq!(program.algorithm.return_stmt.property.as_str(), "dist");
        assert!(program.hls.is_none());
    }

    #[test]
    fn parses_misspelled_hierachical_param_alias() {
        let dsl = r"
        GraphSet { Node: { dist: int<32> } Edge: { weight: int<32> } }
        HierachicalParam { L1: { URAM_SIZE: 1 } }
        Iteration { return foo as result_node_prop.dist }
        ";

        let program = parse_program(dsl).expect("should parse misspelled alias");
        assert_eq!(program.algorithm.return_stmt.property.as_str(), "dist");
    }
}
