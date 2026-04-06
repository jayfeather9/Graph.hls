use std::ops::Range;
use thiserror::Error;

pub type Span = Range<usize>;

/// Top-level error for the crate.
#[derive(Debug, Error)]
pub enum GraphyflowError {
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),
    #[error("ir error: {0}")]
    Ir(#[from] IrError),
}

/// Parser/lexer failures.
#[derive(Debug, Error, Clone)]
pub enum ParseError {
    #[error("{message} at {span:?}")]
    WithSpan { message: String, span: Span },
    #[error("lexer error: {0}")]
    Lexer(String),
}

impl ParseError {
    pub fn with_span(message: impl Into<String>, span: Span) -> Self {
        Self::WithSpan {
            message: message.into(),
            span,
        }
    }
}

/// IR lowering failures, typically due to semantic inconsistencies.
#[derive(Debug, Error)]
pub enum IrError {
    #[error("unknown binding '{binding}' referenced")]
    UnknownBinding { binding: String },
    #[error("reduce key '{key}' references undefined binding")]
    UnknownReduceKey { key: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_error_displays_span() {
        let err = ParseError::with_span("boom", 3..6);
        assert!(format!("{err}").contains("3") && format!("{err}").contains("6"));
    }
}
