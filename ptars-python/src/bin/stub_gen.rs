use pyo3_stub_gen::Result;
use std::path::Path;

fn main() -> Result<()> {
    // Set CARGO_MANIFEST_DIR env var for pyo3-stub-gen's runtime use
    // Point to project root where pyproject.toml lives
    let manifest_dir: &Path = env!("CARGO_MANIFEST_DIR").as_ref();
    let project_root = manifest_dir.parent().unwrap();
    std::env::set_var("CARGO_MANIFEST_DIR", project_root);

    // Generate stubs using the gathered info
    let stub = ptars_python::stub_info()?;
    stub.generate()?;
    Ok(())
}
