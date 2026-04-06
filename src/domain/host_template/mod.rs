pub mod graph_preprocess;

pub use graph_preprocess::{HostPreprocessSpec, render_graph_preprocess_cpp, render_graph_preprocess_no_l1_cpp};

#[cfg(test)]
mod tests {
    use super::*;

    fn normalize_whitespace(input: &str) -> String {
        input.split_whitespace().collect()
    }

    #[test]
    fn graph_preprocess_cpp_matches_golden() {
        let rendered = render_graph_preprocess_cpp(&HostPreprocessSpec::default());
        let golden =
            include_str!("../../hls_assets/scripts/host/graph_preprocess/graph_preprocess.cpp");
        assert_eq!(
            normalize_whitespace(&rendered),
            normalize_whitespace(golden),
            "graph_preprocess.cpp rendering diverged from golden"
        );
    }
}
