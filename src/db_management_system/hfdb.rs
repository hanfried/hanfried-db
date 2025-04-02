use crate::file_management::block_id::DbFilename;
use crate::file_management::file_manager::{FileManager, FileManagerBuilder, IoError};
use crate::memory_management::buffer_manager::{BufferManager, BufferManagerBuilder};
use crate::memory_management::log_manager::{LogManager, LogManagerBuilder};
use std::num::NonZeroUsize;
use std::time::Duration;

#[derive(Debug)]
pub struct HanfriedDb {
    pub file_manager: FileManager,
    pub log_manager: LogManager,
    pub buffer_manager: BufferManager,
}

pub struct HanfriedDbBuilder {
    file_manager_builder: FileManagerBuilder,
    log_manager_builder: LogManagerBuilder,
    buffer_manager_builder: BufferManagerBuilder,
}

impl HanfriedDbBuilder {
    pub fn new(db_directory: String) -> Self {
        Self {
            file_manager_builder: FileManagerBuilder::new(db_directory),
            log_manager_builder: LogManagerBuilder::new(),
            buffer_manager_builder: BufferManagerBuilder::new(),
        }
    }

    pub fn unittest(sub_directory_name: &str) -> Self {
        Self {
            file_manager_builder: FileManagerBuilder::unittest(sub_directory_name),
            log_manager_builder: LogManagerBuilder::unittest(),
            buffer_manager_builder: BufferManagerBuilder::unittest(),
        }
    }

    pub fn file_manager(mut self, config: fn(FileManagerBuilder) -> FileManagerBuilder) -> Self {
        self.file_manager_builder = config(self.file_manager_builder);
        self
    }

    pub fn log_manager(mut self, config: fn(LogManagerBuilder) -> LogManagerBuilder) -> Self {
        self.log_manager_builder = config(self.log_manager_builder);
        self
    }

    pub fn buffer_manager(
        mut self,
        config: fn(BufferManagerBuilder) -> BufferManagerBuilder,
    ) -> Self {
        self.buffer_manager_builder = config(self.buffer_manager_builder);
        self
    }

    pub fn build(self) -> HanfriedDb {
        let file_manager = self.file_manager_builder.build().unwrap();
        let log_manager = self.log_manager_builder.build(&file_manager).unwrap();
        let buffer_manager = self
            .buffer_manager_builder
            .build(&file_manager, &log_manager);
        HanfriedDb {
            file_manager,
            log_manager,
            buffer_manager,
        }
    }
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
