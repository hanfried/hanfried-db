use std::cell::RefCell;
use crate::file_management::block_id::BlockId;
use crate::file_management::file_manager::{FileManager, IoError};
use crate::file_management::page::Page;
use crate::memory_management::log_manager::{LogManager, LogSequenceNumber};
use log::debug;
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct TransactionNumber(NonZeroUsize);

impl From<u64> for TransactionNumber {
    fn from(nr: u64) -> Self {
        TransactionNumber(NonZeroUsize::new(nr as usize).unwrap())
    }
}

#[derive(Debug, Clone)]
pub struct Buffer {
    file_manager: FileManager,
    log_manager: LogManager,
    page: Page,
    block: Option<BlockId>,
    pins_count: usize,
    transaction_number: Option<TransactionNumber>,
    log_sequence_number: Option<LogSequenceNumber>,
}

impl Display for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Buffer block={:?} pins_count={:?} transaction_number={:?} log_sequence_number={:?}",
            self.block, self.pins_count, self.transaction_number, self.log_sequence_number
        )
    }
}

impl Buffer {
    pub fn new(file_manager: &FileManager, log_manager: &LogManager) -> Buffer {
        Buffer {
            file_manager: file_manager.clone(),
            log_manager: log_manager.clone(),
            page: Page::new(file_manager.block_size),
            block: None,
            pins_count: 0,
            transaction_number: None,
            log_sequence_number: None,
        }
    }

    pub fn page(&self) -> &Page {
        &self.page
    }

    pub fn block(&self) -> Option<BlockId> {
        self.block.clone()
    }

    pub fn set_modified(
        &mut self,
        transaction_number: TransactionNumber,
        log_sequence_number: Option<LogSequenceNumber>,
    ) {
        debug!(
            "Buffer: Set modified for block {:?} {:?} {:?}",
            self.block, transaction_number, log_sequence_number
        );
        self.transaction_number = Some(transaction_number);
        self.log_sequence_number = log_sequence_number;
    }

    pub fn is_pinned(&self) -> bool {
        self.pins_count > 0
    }

    pub fn is_not_pinned(&self) -> bool {
        !self.is_pinned()
    }

    pub fn modifying_transaction_number(&self) -> Option<TransactionNumber> {
        self.transaction_number
    }

    pub fn assign_to_block(&mut self, block_id: BlockId) -> Result<(), IoError> {
        debug!(
            "Buffer: Assigning to block {:?} <- block {:?}",
            self.block, block_id
        );
        self.flush()?;
        self.block = Some(block_id.clone());
        debug!(
            "Buffer: Assigning to block={:?}, read file_manager={:?} contents={:?}",
            &block_id, self.file_manager, self.page
        );
        self.file_manager.read(&block_id, &self.page)?;
        debug!(
            "Buffer: Assigning to block={:?}, set pins_count=0",
            &block_id
        );
        self.pins_count = 0;
        Ok(())
    }

    // TODO: maybe not public
    pub fn flush(&mut self) -> Result<(), IoError> {
        if self.transaction_number.is_some() {
            debug!("Buffer: Flush {}", self);
            if let Some(lsn) = self.log_sequence_number {
                self.log_manager.flush(lsn)?;
            }
            self.file_manager
                .write(&self.block().unwrap(), &self.page)?;
            self.transaction_number = None;
        }
        Ok(())
    }

    pub fn pin(&mut self) {
        self.pins_count += 1;
        debug!(
            "Buffer: Pinned block {:?} new count {:?}",
            self.block, self.pins_count
        );
    }

    pub fn unpin(&mut self) {
        self.pins_count -= 1;
        debug!(
            "Buffer: Unpinned block {:?} new count {:?}",
            self.block, self.pins_count
        );
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use crate::file_management::block_id::DbFilename;
    use crate::file_management::file_manager::FileManagerBuilder;
    use crate::memory_management::buffer::Buffer;
    use crate::memory_management::log_manager::LogManager;

    #[test]
    fn test_buffer_cloning() {
        let file_manager = FileManagerBuilder::unittest("buffer_test_cloning").block_size(NonZeroUsize::new(100 as usize).unwrap()).build().unwrap();
        let log_manager = LogManager::new(&file_manager, &DbFilename::from("test_buffer_cloning.log")).unwrap();
        let mut buffer = Buffer::new(&file_manager, &log_manager);
        let mut buffer_clone = buffer.clone();

        buffer.page().set_i32(0, 100);
        assert_eq!(buffer.page().get_contents(), buffer_clone.page().get_contents());
    }
}
