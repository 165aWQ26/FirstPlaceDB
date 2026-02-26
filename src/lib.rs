use pyo3::prelude::*;

mod bufferpool;
pub mod db;
pub mod db_error;
pub mod index;
mod page;
mod page_directory;
mod page_range;
pub mod query;
pub mod table;

#[cfg(test)]
mod tests;

mod bindings;
mod bufferpool_context;
mod transaction;
mod transaction_worker;
mod io_helper;

/// A Python module implemented in Rust. The name of this module must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
mod _core {
    #[pymodule_export]
    use crate::bindings::CoreDatabase;
    #[pymodule_export]
    use crate::bindings::CoreQuery;
}
