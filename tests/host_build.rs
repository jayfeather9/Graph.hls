use std::process::Command;

/// Opt-in build check for the generated host code.
///
/// Set `RUN_HOST_BUILD_TEST=1` to enable. This will source the external
/// environment script and run `make cleanall && make exe TARGET=hw` in the host
/// scripts directory.
#[test]
fn host_build_smoke() {
    if std::env::var("RUN_HOST_BUILD_TEST").unwrap_or_default() != "1" {
        eprintln!("skipping host build smoke test (set RUN_HOST_BUILD_TEST=1 to run)");
        return;
    }

    // Run from src/hls_assets so includes like global_para.mk resolve correctly.
    let script = "cd src/hls_assets && source /path/to/vitis/settings64.sh && make -f scripts/main.mk cleanall && make -f scripts/main.mk exe TARGET=hw";
    let status = Command::new("bash")
        .arg("-lc")
        .arg(script)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("failed to spawn host build command");

    assert!(
        status.success(),
        "host build command failed with status {:?}",
        status.code()
    );
}
