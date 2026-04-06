#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct HostPreprocessSpec {}

const GRAPH_PREPROCESS_CPP_TEMPLATE: &str =
    include_str!("templates/graph_preprocess.cpp.tpl");

const GRAPH_PREPROCESS_NO_L1_CPP_TEMPLATE: &str =
    include_str!("templates/graph_preprocess_no_l1.cpp.tpl");

pub fn render_graph_preprocess_cpp(_spec: &HostPreprocessSpec) -> String {
    GRAPH_PREPROCESS_CPP_TEMPLATE.to_string()
}

pub fn render_graph_preprocess_no_l1_cpp() -> String {
    GRAPH_PREPROCESS_NO_L1_CPP_TEMPLATE.to_string()
}
