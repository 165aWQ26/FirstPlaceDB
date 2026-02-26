use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use crate::table::TableError;

pub(crate) fn write_i64(
    val: impl Into<Option<i64>>,
    writer: &mut BufWriter<File>,
) -> Result<(), TableError> {
    const SENTINEL: i64 = i64::MAX;
    let bytes = match val.into() {
        Some(val) => val.to_be_bytes(),
        None => SENTINEL.to_be_bytes(),
    };
    writer.write_all(&bytes).map_err(|_| TableError::WriteFail)
}

pub(crate) fn read_i64(buf: &mut [u8; 8], reader: &mut BufReader<File>) -> Result<i64, TableError> {
    reader.read_exact(buf).map_err(|_| TableError::ReadFail)?;
    Ok(i64::from_be_bytes(*buf))
}

pub(crate) fn read_usize(
    buf: &mut [u8; 8],
    reader: &mut BufReader<File>,
) -> Result<usize, TableError> {
    Ok(read_i64(buf, reader)? as usize)
}