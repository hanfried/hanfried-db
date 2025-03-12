use crate::file_management::file_manager::{FileManager, IoError};
use crate::memory_management::buffer_manager::BufferManager;
use crate::memory_management::log_manager::LogManager;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug)]
pub struct HanfriedDb {
    pub file_manager: Arc<FileManager>,
    pub log_manager: Arc<Mutex<LogManager>>,
    pub buffer_manager: Arc<BufferManager>,
}

impl HanfriedDb {
    pub fn new(
        db_directory: String,
        block_size: usize,
        log_file: String,
        pool_size: usize,
        max_open_files: usize,
    ) -> Result<Self, IoError> {
        let fm = Arc::new(FileManager::new(
            db_directory,
            NonZeroUsize::new(block_size).unwrap(),
            NonZeroUsize::new(max_open_files).unwrap(),
        )?);
        let lm = Arc::new(Mutex::new(LogManager::new(
            fm.clone(),
            log_file.to_string(),
        )?));
        let bm = Arc::new(BufferManager::new(
            fm.clone(),
            lm.clone(),
            pool_size,
            Duration::from_secs(10),
        ));
        Ok(Self {
            file_manager: fm.clone(),
            log_manager: lm.clone(),
            buffer_manager: bm.clone(),
        })
    }
}
