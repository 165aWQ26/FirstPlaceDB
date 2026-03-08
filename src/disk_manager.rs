use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write, BufReader, BufWriter};
use std::path::PathBuf;
use crate::page::{Page, PageError};
use crate::page_collection::{PageId, PageId};

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
        let mut offset = 8;

        for _ in 0..len {
            if offset >= data.len() {
                return Err(DiskError::CorruptedPage("Unexpected end of data".into()));
            }

            let tag = data[offset];
            offset += 1;

            let value = if tag == 1 {
                if offset + 8 > data.len() {
                    return Err(DiskError::CorruptedPage("Missing value bytes".into()));
                }

                let val_bytes: [u8; 8] = data[offset..offset + 8].try_into()
                    .map_err(|_| DiskError::CorruptedPage("Invalid value bytes".into()))?;
                let val = i64::from_be_bytes(val_bytes);
                offset += 8;
                Some(val)
            } else if tag == 0 {
                None
            } else {
                return Err(DiskError::CorruptedPage(format!("Invalid tag: {}", tag)));
            };

            //Todo write needs an offset or add an append page function to page
            page.write(value).map_err(|e| DiskError::PageError(e))?;
        }

        Ok(page)
    }
}

#[derive(Debug)]
pub enum DiskError {
    PageNotFound(PageId),
    IoError(std::io::Error),
    SerializationError,
    CorruptedPage(String),
    PageError(PageError),
}

impl std::fmt::Display for DiskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskError::PageNotFound(pid) => {
                write!(f, "Page not found: table_id={}, page_num={}",
                       pid.table_id, pid.page_num)
            }
            DiskError::IoError(e) => write!(f, "I/O error: {}", e),
            DiskError::SerializationError => write!(f, "Serialization error"),
            DiskError::CorruptedPage(msg) => write!(f, "Corrupted page: {}", msg),
            DiskError::PageError(e) => write!(f, "Page error: {:?}", e),
        }
    }
}

impl std::error::Error for DiskError {}

impl From<std::io::Error> for DiskError {
    fn from(e: std::io::Error) -> Self {
        DiskError::IoError(e)
    }
}

impl From<PageError> for DiskError {
    fn from(e: PageError) -> Self {
        DiskError::PageError(e)
    }
}