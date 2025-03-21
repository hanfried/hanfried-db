use crate::file_management::block_id::DbFilename;
use crate::file_management::file_manager::{FileManager, IoError};
use crate::memory_management::buffer_manager::BufferManager;
use crate::memory_management::log_manager::LogManager;
use std::num::NonZeroUsize;
use std::time::Duration;

#[derive(Debug)]
pub struct HanfriedDb {
    pub file_manager: FileManager,
    pub log_manager: LogManager,
    pub buffer_manager: BufferManager,
}

impl HanfriedDb {
    pub fn new(
        db_directory: String,
        block_size: usize,
        log_file: String,
        pool_size: usize,
        max_open_files: usize,
    ) -> Result<Self, IoError> {
        let fm = FileManager::new(
            db_directory,
            NonZeroUsize::new(block_size).unwrap(),
            NonZeroUsize::new(max_open_files).unwrap(),
        )
        .unwrap();
        let lm = LogManager::new(&fm, &DbFilename::from(log_file))?;
        let bm = BufferManager::new(&fm, &lm, pool_size, Duration::from_secs(10));
        Ok(Self {
            file_manager: fm,
            log_manager: lm,
            buffer_manager: bm,
        })
    }
}
