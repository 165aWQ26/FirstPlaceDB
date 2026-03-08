use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use crate::bufferpool::DiskError;
use crate::page::{Page, PageError};
use crate::page_collection::PageId;

pub struct DiskManager {
    base_path: PathBuf,
}

impl DiskManager {
    pub fn new<P: Into<PathBuf>>(base_path: P) -> Result<Self, DiskError> {
        let base_path = base_path.into();
        fs::create_dir_all(&base_path)?;

        Ok(Self { base_path })
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

    /// Each entry: [tag: 1 byte][value: 8 bytes if Some]
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
}

