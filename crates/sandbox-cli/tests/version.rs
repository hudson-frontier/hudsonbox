//! `--version` reports the product name and the workspace version from VERSION.
#![allow(clippy::unwrap_used)]

#[test]
fn version_flag_reports_hudsonbox_product_version() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sandbox-api-test-client"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("hudsonbox {}\n", env!("CARGO_PKG_VERSION"))
    );
    let product = include_str!("../../../VERSION");
    assert_eq!(product.trim_end(), env!("CARGO_PKG_VERSION"));
}
