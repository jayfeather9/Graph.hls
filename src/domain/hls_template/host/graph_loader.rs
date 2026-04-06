use crate::domain::hls::{
    HlsCompilationUnit, HlsFunction, HlsIdentifier, HlsInclude, HlsParameter, HlsStatement,
    HlsType, PassingStyle,
};

use super::super::utils::custom;
use super::HostTemplateError;

pub fn graph_loader_unit() -> Result<HlsCompilationUnit, HostTemplateError> {
    Ok(HlsCompilationUnit {
        includes: vec![
            HlsInclude::new("graph_loader.h", false)?,
            HlsInclude::new("fpga_executor.h", false)?,
        ],
        defines: Vec::new(),
        globals: Vec::new(),
        functions: vec![stub_function()?],
    })
}

fn stub_function() -> Result<HlsFunction, HostTemplateError> {
    Ok(HlsFunction {
        linkage: None,
        name: HlsIdentifier::new("graph_loader_stub")?,
        return_type: HlsType::Void,
        params: vec![HlsParameter {
            name: HlsIdentifier::new("arg")?,
            ty: custom("int"),
            passing: PassingStyle::Value,
        }],
        body: vec![HlsStatement::Comment("TODO: implement host logic".into())],
    })
}
