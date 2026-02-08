use pyo3::prelude::*;

pub mod db;
mod index;
mod page;
mod page_collection;
mod page_directory;
mod page_range;
pub mod query;
pub mod table;

/// A Python module implemented in Rust. The name of this module must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
mod _core {
    use pyo3::prelude::*;

    #[pyfunction]
    fn hello_from_bin() -> String {
        "Hello from lstore!".to_string()
    }
}
