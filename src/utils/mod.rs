/// Indents each line in the provided text block by the given number of spaces.
pub fn indent_block(input: &str, spaces: usize) -> String {
    let padding = " ".repeat(spaces);
    input
        .lines()
        .map(|line| format!("{padding}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

pub mod graph_converter;
pub mod graph_generator;
pub mod graph_metadata;
pub mod grouping_predictor;
pub mod grouping_predictor_32;
pub mod reference_calcs;

#[cfg(test)]
mod tests {
    use super::indent_block;

    #[test]
    fn indents_each_line() {
        let result = indent_block("a\nb", 2);
        assert_eq!(result, "  a\n  b");
    }
}
