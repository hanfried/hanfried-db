use crate::file_management::block_id::BlockId;
use crate::file_management::file_manager::{FileManager, IoError};
use crate::file_management::page::Page;
use crate::memory_management::log_manager::{LogManager, LogSequenceNumber};
use log::debug;
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct TransactionNumber(NonZeroUsize);

impl From<u64> for TransactionNumber {
    fn from(nr: u64) -> Self {
        TransactionNumber(NonZeroUsize::new(nr as usize).unwrap())
    }
}

#[derive(Debug)]
struct BufferData {
    page: Page,
    block: Option<BlockId>,
    transaction: Option<TransactionNumber>,
    log_sequence_number: Option<LogSequenceNumber>,
    pins_count: usize,
}

#[derive(Debug, Clone)]
pub struct Buffer {
    file_manager: FileManager,
    log_manager: LogManager,
    data: Arc<Mutex<BufferData>>,
}

impl Display for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Buffer data={:?}", self.data)
    }
}

impl Buffer {
    pub fn new(file_manager: &FileManager, log_manager: &LogManager) -> Buffer {
        Buffer {
            file_manager: file_manager.clone(),
            log_manager: log_manager.clone(),
            data: Arc::new(Mutex::new(BufferData {
                page: Page::new(file_manager.block_size),
                block: None,
                transaction: None,
                log_sequence_number: None,
                pins_count: 0,
            })),
        }
    }

    pub fn page(&self) -> Page {
        self.data.lock().unwrap().page.clone()
    }

    pub fn block(&self) -> Option<BlockId> {
        self.data.lock().unwrap().block.clone()
    }

    pub fn modify_page<R>(
        &mut self,
        modifier: fn(&mut Page) -> R,
        transaction_number: TransactionNumber,
        log_sequence_number: Option<LogSequenceNumber>,
    ) -> R {
        let mut data_guard = self.data.lock().unwrap();
        let data = data_guard.deref_mut();
        debug!(
            "modifying page block={:?} transaction_number={:?}",
            data.block, transaction_number
        );
        let result = modifier(&mut data.page);
        data.transaction = Some(transaction_number);
        data.log_sequence_number = log_sequence_number;
        result
    }

    pub fn is_pinned(&self) -> bool {
        self.data.lock().unwrap().pins_count > 0
    }

    pub fn is_not_pinned(&self) -> bool {
        !self.is_pinned()
    }

    pub fn modifying_transaction_number(&self) -> Option<TransactionNumber> {
        self.data.lock().unwrap().transaction
    }

    pub fn assign_to_block(&mut self, block_id: BlockId) -> Result<(), IoError> {
        let mut data_guard = self.data.lock().unwrap();
        let locked_data = data_guard.deref_mut();
        debug!(
            "Buffer: Assigning block {:?} (previous: {:?}) buffer {:?}",
            block_id, locked_data.block, self
        );

        self._flush(locked_data)?;

        locked_data.block = Some(block_id.clone());
        debug!(
            "Buffer: Assigning to block={:?}, read file_manager={:?} contents={:?}",
            &block_id, self.file_manager, locked_data.page
        );
        self.file_manager.read(&block_id, &locked_data.page)?;
        debug!(
            "Buffer: Assigning to block={:?}, set pins_count=0",
            &block_id
        );

        locked_data.pins_count = 0;
        Ok(())
    }

    fn _flush(&self, locked_data: &mut BufferData) -> Result<(), IoError> {
        if locked_data.transaction.is_some() {
            debug!("Buffer: Flush {}", self);
            let block = locked_data
                .block
                .clone()
                .expect("Buffer: Block not set when trying to flush");
            if let Some(lsn) = locked_data.log_sequence_number {
                self.log_manager.flush(lsn)?;
            }
            self.file_manager.write(&block, &locked_data.page)?;
            locked_data.transaction = None;
        } else {
            debug!("Flushing? No transaction number => no flush")
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), IoError> {
        let mut data_guard = self.data.lock().unwrap();
        let locked_data = data_guard.deref_mut();
        self._flush(locked_data)
    }

    pub fn increment_pins_count(&self) {
        let mut data_guard = self.data.lock().unwrap();
        let data = data_guard.deref_mut();
        data.pins_count += 1;
        debug!(
            "Buffer: Pinned block {:?} new count {:?}",
            data.block, data.pins_count
        );
    }

    pub fn decrement_pins_count(&self) {
        let mut data_guard = self.data.lock().unwrap();
        let data = data_guard.deref_mut();
        data.pins_count -= 1;
        debug!(
            "Buffer: Unpinned block {:?} new count {:?}",
            data.block, data.pins_count
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::datatypes::varint::Varint;
    use crate::file_management::block_id::DbFilename;
    use crate::file_management::file_manager::FileManagerBuilder;
    use crate::memory_management::buffer::{Buffer, TransactionNumber};
    use crate::memory_management::log_manager::LogManager;
    use std::num::NonZeroUsize;

    #[test]
    fn test_buffer_cloning() {
        let file_manager = FileManagerBuilder::unittest("buffer_test_cloning")
            .block_size(NonZeroUsize::new(100 as usize).unwrap())
            .build()
            .unwrap();
        let log_manager =
            LogManager::new(&file_manager, &DbFilename::from("test_buffer_cloning.log")).unwrap();
        let mut buffer = Buffer::new(&file_manager, &log_manager);
        let buffer_clone = buffer.clone();

        buffer.modify_page(
            |page| page.set(0, &Varint::from(100)),
            TransactionNumber::from(1),
            None,
        );
        // buffer.page().set_i32(0, 100);
        assert_eq!(
            buffer.page().get_contents(),
            buffer_clone.page().get_contents()
        );
    }
}
