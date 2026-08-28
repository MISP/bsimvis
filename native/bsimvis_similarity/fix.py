import sys

with open("src/lib.rs", "r") as f:
    content = f.read()

start = content.find("#[pymodule]")
end = content.find("Ok(())", start)

new_module = """#[pymodule]
fn bsimvis_similarity_native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ExactScorer>()?;
    module.add_function(wrap_pyfunction!(
        compact_edge_delta_from_summaries_native,
        module
    )?)?;
    #[cfg(feature = "gpu")]
    module.add_function(wrap_pyfunction!(wgpu_adapter_info, module)?)?;
    """

content = content[:start] + new_module + content[end:]
with open("src/lib.rs", "w") as f:
    f.write(content)
