use std::fs;
use std::path::{Path, PathBuf};

use refactor_Graphyflow::domain::host_template::{HostPreprocessSpec, render_graph_preprocess_cpp};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    write_graph_preprocess_golden(&manifest_dir, &HostPreprocessSpec::default())?;
    eprintln!("Regenerated host golden sources under src/hls_assets/.");
    Ok(())
}

fn write_graph_preprocess_golden(
    manifest_dir: &Path,
    spec: &HostPreprocessSpec,
) -> Result<(), Box<dyn std::error::Error>> {
    let dest = manifest_dir
        .join("src")
        .join("hls_assets")
        .join("scripts")
        .join("host")
        .join("graph_preprocess")
        .join("graph_preprocess.cpp");
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(dest, render_graph_preprocess_cpp(spec))?;
    Ok(())
}
