//! Host template bundle described via Rust HLS structures.

#![allow(dead_code)]

use thiserror::Error;

use crate::domain::hls::{HlsCodegenError, HlsCompilationUnit};

mod graph_loader;

pub use graph_loader::graph_loader_unit;

#[derive(Debug, Error)]
pub enum HostTemplateError {
    #[error("{0}")]
    Codegen(#[from] HlsCodegenError),
}

pub fn host_unit() -> Result<Vec<HlsCompilationUnit>, HostTemplateError> {
    Ok(vec![graph_loader_unit()?])
}
