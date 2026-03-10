use crate::bindings::CoreQuery;
use crate::transaction::{QueryOp, Transaction};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

#[pyclass]
pub struct CoreTransaction {
    pub(crate) inner: Transaction,
}

#[pymethods]
impl CoreTransaction {
    #[new]
    pub fn new() -> Self {
        Self { inner: Transaction::new() }
    }

    //Extract Python objects into plain Rust types while the GIL is held.
    #[pyo3(signature = (query_fn, _table, *args))]
    pub fn add_query(
        &mut self,
        py: Python,
        query_fn: &Bound<PyAny>,
        _table: &Bound<PyAny>,
        args: &Bound<PyTuple>,
    ) -> PyResult<()> {
        let result = (|| -> PyResult<()> {
            let fn_name: String = query_fn.getattr("__name__")?.extract()?;

            let core_q = query_fn
                .getattr("__self__")?
                .getattr("_core")?
                .cast_into::<CoreQuery>()?;
            let table = core_q.borrow().inner.table.clone();

            let op = match fn_name.as_str() {
                "insert" => {
                    let raw: Vec<i64> = args.extract()?;
                    QueryOp::Insert { table, args: raw.into_iter().map(Some).collect() }
                }
                "update" => {
                    let key: i64 = args.get_item(0)?.extract()?;
                    let cols = (1..args.len())
                        .map(|i| args.get_item(i)?.extract::<Option<i64>>())
                        .collect::<PyResult<_>>()?;
                    QueryOp::Update { table, key, cols }
                }
                "delete" => {
                    QueryOp::Delete { table, key: args.get_item(0)?.extract()? }
                }
                "select" => QueryOp::Select {
                    table,
                    key:        args.get_item(0)?.extract()?,
                    search_col: args.get_item(1)?.extract()?,
                    proj:       args.get_item(2)?.extract()?,
                },
                "select_version" => QueryOp::SelectVersion {
                    table,
                    key:        args.get_item(0)?.extract()?,
                    search_col: args.get_item(1)?.extract()?,
                    proj:       args.get_item(2)?.extract()?,
                    version:    args.get_item(3)?.extract()?,
                },
                "sum" => QueryOp::Sum {
                    table,
                    start: args.get_item(0)?.extract()?,
                    end:   args.get_item(1)?.extract()?,
                    col:   args.get_item(2)?.extract()?,
                },
                "sum_version" => QueryOp::SumVersion {
                    table,
                    start:   args.get_item(0)?.extract()?,
                    end:     args.get_item(1)?.extract()?,
                    col:     args.get_item(2)?.extract()?,
                    version: args.get_item(3)?.extract()?,
                },
                "increment" => QueryOp::Increment {
                    table,
                    key: args.get_item(0)?.extract()?,
                    col: args.get_item(1)?.extract()?,
                },
                other => return Err(PyRuntimeError::new_err(format!("unknown query op: {}", other))),
            };

            self.inner.add_op(op);
            Ok(())
        })();

        if let Err(ref e) = result {
            eprintln!("add_query error: {e}");
        }
        result
    }

    pub fn run(&self) -> bool {
        self.inner.run()
    }
}