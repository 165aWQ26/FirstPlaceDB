use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use dashmap::DashMap;
use crate::bufferpool::DiskError;
use crate::page::{Page};
use crate::page_collection::PageId;
use crate::table::Table;

pub struct DiskManager {
    base_path: PathBuf,
}

impl DiskManager {
    pub fn new<P: Into<PathBuf>>(base_path: P) -> Result<Self, DiskError> {
        let base_path = base_path.into();
        fs::create_dir_all(&base_path)?;
        Ok(Self { base_path })
    }

    pub fn set_path(&mut self, path: Option<PathBuf>) -> Result<(), DiskError> {
        self.base_path = path.unwrap();
        fs::create_dir_all(&self.base_path)?;
        Ok(())
    }

    fn page_path(&self, pid: PageId) -> PathBuf {
        self.base_path
            .join("table")
            .join(pid.table_id.to_string())
            .join(pid.page_num.to_string())
    }

    pub fn read_page(&self, pid: PageId) -> Result<Page, DiskError> {
        let path = self.page_path(pid);

        if !path.exists() {
            return Err(DiskError::PageNotFound(pid));
        }

        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);

        // Read all bytes
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        // Deserialize
        self.deserialize_page(&buffer)
    }

    pub fn write_page(&self, pid: PageId, page: &Page) -> Result<(), DiskError> {
        let path = self.page_path(pid);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let data = self.serialize_page(page)?;

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        let mut writer = BufWriter::new(file);
        writer.write_all(&data)?;
        writer.flush()?;

        Ok(())
    }

    pub fn delete_page(&self, pid: PageId) -> Result<(), DiskError> {
        let path = self.page_path(pid);

        if path.exists() {
            fs::remove_file(path)?;
        }

        Ok(())
    }

    pub fn page_exists(&self, pid: PageId) -> bool {
        self.page_path(pid).exists()
    }

    fn serialize_page(&self, page: &Page) -> Result<Vec<u8>, DiskError> {
        let mut buffer = Vec::new();

        let len = page.len() as u64;
        buffer.extend_from_slice(&len.to_be_bytes());

        for i in 0..page.len() {
            let value = page.read(i).map_err(|_| DiskError::SerializationError)?;

            match value {
                Some(val) => {
                    buffer.push(1);  // Tag: Some(i64)
                    buffer.extend_from_slice(&val.to_be_bytes());
                }
                None => {
                    buffer.push(0);  // Tag: None
                }
            }
        }
        Ok(buffer)
    }

    fn deserialize_page(&self, data: &[u8]) -> Result<Page, DiskError> {
        if data.len() < 8 {
            return Err(DiskError::CorruptedPage("Data too short".into()));
        }

        let len_bytes: [u8; 8] = data[0..8].try_into()
            .map_err(|_| DiskError::CorruptedPage("Invalid length bytes".into()))?;
        let len = u64::from_be_bytes(len_bytes) as usize;

        if len > Page::PAGE_SIZE {
            return Err(DiskError::CorruptedPage(
                format!("Invalid page length: {}", len)
            ));
        }

        let mut page = Page::default();
        let mut file_offset = 8;

        for slot in 0..len {
            if file_offset >= data.len() {
                return Err(DiskError::CorruptedPage("Unexpected end of data".into()));
            }

            let tag = data[file_offset];
            file_offset += 1;

            let value = if tag == 1 {
                if file_offset + 8 > data.len() {
                    return Err(DiskError::CorruptedPage("Missing value bytes".into()));
                }
                let val_bytes: [u8; 8] = data[file_offset..file_offset + 8].try_into()
                    .map_err(|_| DiskError::CorruptedPage("Invalid value bytes".into()))?;
                let val = i64::from_be_bytes(val_bytes);
                file_offset += 8;
                Some(val)
            } else if tag == 0 {
                None
            } else {
                return Err(DiskError::CorruptedPage(format!("Invalid tag: {}", tag)));
            };

            page.write(value, slot).map_err(|e| DiskError::PageError(e))?;
        }

        Ok(page)
    }

    pub fn write_tables(&self, tables: &DashMap<usize, Arc<Table>>, next_table_id: usize) -> Result<(), DiskError> {
        let path = self.base_path.join("catalog.bin");

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(next_table_id as u64).to_be_bytes());
        buffer.extend_from_slice(&(tables.len() as u64).to_be_bytes());
        for entry in tables.iter() {
            let t = entry.value();
            buffer.extend_from_slice(&(t.table_id as u64).to_be_bytes());
            buffer.extend_from_slice(&(t.num_data_columns as u64).to_be_bytes());
            buffer.extend_from_slice(&(t.key_index as u64).to_be_bytes());
            buffer.extend_from_slice(&t.rid.current().to_be_bytes());
        }

        let file = OpenOptions::new().write(true).create(true).truncate(true).open(&path)?;
        let mut w = BufWriter::new(file);
        w.write_all(&buffer)?;
        w.flush()?;
        Ok(())
    }
    pub fn write_table_names(&self, table_names: &DashMap<String, usize>) -> Result<(), DiskError> {
        let path = self.base_path.join("table_names.bin");

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(table_names.len() as u64).to_be_bytes());
        for entry in table_names.iter() {
            let name_bytes = entry.key().as_bytes();
            buffer.extend_from_slice(&(name_bytes.len() as u64).to_be_bytes());
            buffer.extend_from_slice(name_bytes);
            buffer.extend_from_slice(&(*entry.value() as u64).to_be_bytes());
        }

        let file = OpenOptions::new().write(true).create(true).truncate(true).open(&path)?;
        let mut w = BufWriter::new(file);
        w.write_all(&buffer)?;
        w.flush()?;
        Ok(())
    }

    pub fn read_tables(&self) -> Result<(Vec<TableMeta>, usize), DiskError> {
        let path = self.base_path.join("catalog.bin");
        if !path.exists() {
            return Ok((vec![], 0));
        }

        let mut data = Vec::new();
        BufReader::new(File::open(&path)?).read_to_end(&mut data)?;

        if data.len() < 16 {
            return Err(DiskError::CorruptedPage("Catalog too short".into()));
        }

        let mut file_offset = 0;

        let next_table_id = u64::from_be_bytes(
            data[file_offset..file_offset+8].try_into()
                .map_err(|_| DiskError::CorruptedPage("Invalid next_table_id".into()))?
        ) as usize;
        file_offset += 8;

        let count = u64::from_be_bytes(
            data[file_offset..file_offset+8].try_into()
                .map_err(|_| DiskError::CorruptedPage("Invalid count".into()))?
        ) as usize;
        file_offset += 8;

        let mut tables = Vec::with_capacity(count);
        for _ in 0..count {
            if file_offset + 32 > data.len() {
                return Err(DiskError::CorruptedPage("Unexpected end of catalog".into()));
            }
            let table_id = u64::from_be_bytes(data[file_offset..file_offset+8].try_into()
                .map_err(|_| DiskError::CorruptedPage("Invalid table_id".into()))?) as usize;
            file_offset += 8;
            let num_data_columns = u64::from_be_bytes(data[file_offset..file_offset+8].try_into()
                .map_err(|_| DiskError::CorruptedPage("Invalid num_data_columns".into()))?) as usize;
            file_offset += 8;
            let key_index = u64::from_be_bytes(data[file_offset..file_offset+8].try_into()
                .map_err(|_| DiskError::CorruptedPage("Invalid key_index".into()))?) as usize;
            file_offset += 8;
            let next_rid = i64::from_be_bytes(data[file_offset..file_offset+8].try_into()
                .map_err(|_| DiskError::CorruptedPage("Invalid next_rid".into()))?);
            file_offset += 8;
            tables.push(TableMeta { table_id, num_data_columns, key_index, next_rid, name: String::new() });
        }

        Ok((tables, next_table_id))
    }

    pub fn read_table_names(&self) -> Result<Vec<(String, usize)>, DiskError> {
        let path = self.base_path.join("table_names.bin");
        if !path.exists() {
            return Ok(vec![]);
        }

        let mut data = Vec::new();
        BufReader::new(File::open(&path)?).read_to_end(&mut data)?;

        if data.len() < 8 {
            return Err(DiskError::CorruptedPage("Table names too short".into()));
        }

        let mut file_offset = 0;

        let count = u64::from_be_bytes(
            data[file_offset..file_offset+8].try_into()
                .map_err(|_| DiskError::CorruptedPage("Invalid count".into()))?
        ) as usize;
        file_offset += 8;

        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            if file_offset + 8 > data.len() {
                return Err(DiskError::CorruptedPage("Unexpected end of table names".into()));
            }
            let name_len = u64::from_be_bytes(
                data[file_offset..file_offset+8].try_into()
                    .map_err(|_| DiskError::CorruptedPage("Invalid name length".into()))?
            ) as usize;
            file_offset += 8;

            if file_offset + name_len + 8 > data.len() {
                return Err(DiskError::CorruptedPage("Unexpected end of name bytes".into()));
            }
            let name = String::from_utf8(data[file_offset..file_offset+name_len].to_vec())
                .map_err(|_| DiskError::CorruptedPage("Invalid table name".into()))?;
            file_offset += name_len;

            let id = u64::from_be_bytes(
                data[file_offset..file_offset+8].try_into()
                    .map_err(|_| DiskError::CorruptedPage("Invalid table id".into()))?
            ) as usize;
            file_offset += 8;

            result.push((name, id));
        }

        Ok(result)
    }
}

pub struct TableMeta {
    pub table_id: usize,
    pub num_data_columns: usize,
    pub key_index: usize,
    pub next_rid: i64,
    pub name: String,
}

