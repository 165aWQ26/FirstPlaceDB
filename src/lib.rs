use pyo3::prelude::*;

pub mod db;
pub mod error;
pub mod index;
mod page;
mod page_collection;
mod page_directory;
mod page_range;
pub mod query;
pub mod table;

#[cfg(test)]
mod tests;

mod bindings;

/// A Python module implemented in Rust. The name of this module must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
mod _core {
    #[pymodule_export]
    use crate::bindings::CoreQuery;
}
