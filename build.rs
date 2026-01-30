fn main() {
    // Fix mac problems
    // https://pyo3.rs/v0.15.1/building_and_distribution#macos
    pyo3_build_config::add_extension_module_link_args();
}
