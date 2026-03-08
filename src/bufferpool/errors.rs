use crate::page::PageError;
use crate::page_collection::PageId;

#[derive(Debug)]
pub enum BufferPoolError {
    PageNotFound,

    NoVictim,

    Disk(DiskError),

    Page(PageError),

    BackgroundWorkerDead,

    AllFramesPinned,

    PidNotInFrame,
}

impl std::fmt::Display for BufferPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BufferPoolError::PageNotFound => write!(f, "Page not found in buffer pool"),
            BufferPoolError::NoVictim => write!(f, "No victim available for eviction"),
            BufferPoolError::Disk(e) => write!(f, "Disk error: {}", e),
            BufferPoolError::Page(e) => write!(f, "Page error: {:?}", e),
            BufferPoolError::BackgroundWorkerDead => write!(f, "Background worker thread has died"),
            BufferPoolError::AllFramesPinned => write!(f, "Every frame is pinned"),
            BufferPoolError::PidNotInFrame => write!(f, "Pid "),
        }
    }
}

impl From<PageError> for BufferPoolError {
    fn from(e: PageError) -> Self {
        BufferPoolError::Page(e)
    }
}

impl From<DiskError> for BufferPoolError {
    fn from(e: DiskError) -> Self {
        BufferPoolError::Disk(e)
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