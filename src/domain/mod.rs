pub mod ast;
pub mod errors;
pub mod gas;
pub mod hls;
pub mod hls_ops;
pub mod hls_template;
pub mod host_template;
pub mod ir;

pub use ast::*;
pub use errors::*;
pub use gas::*;
pub use hls::*;
pub use hls_template::*;
pub use host_template::*;
pub use ir::*;

/// Provides a short textual description useful for debugging.
pub trait DebugSummary {
    /// Returns a multi-line string describing the structure.
    fn debug_summary(&self) -> String;
}
