use crate::file_management::block_id::BlockId;
use crate::file_management::file_manager::{FileManager, IoError};
use crate::file_management::page::Page;
use crate::memory_management::log_manager::{LogManager, LogSequenceNumber};
use log::debug;
use std::cell::RefCell;
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::rc::Rc;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct TransactionNumber(NonZeroUsize);

impl TransactionNumber {
    pub fn from(nr: u64) -> Self {
        TransactionNumber(NonZeroUsize::new(nr as usize).unwrap())
    }
}

#[derive(Debug)]
pub struct Buffer<'managers, 'blocks> {
    file_manager: Rc<RefCell<FileManager>>,
    log_manager: Rc<RefCell<LogManager<'managers>>>,
    contents: Page,
    block: Option<BlockId<'blocks>>,
    pins_count: usize,
    transaction_number: Option<TransactionNumber>,
    log_sequence_number: Option<LogSequenceNumber>,
}

impl Display for Buffer<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Buffer block={:?} pins_count={:?} transaction_number={:?} log_sequence_number={:?}",
            self.block, self.pins_count, self.transaction_number, self.log_sequence_number
        )
    }
}

impl<'managers, 'blocks> Buffer<'managers, 'blocks> {
    pub fn new(
        file_manager: Rc<RefCell<FileManager>>,
        log_manager: Rc<RefCell<LogManager<'managers>>>,
    ) -> Buffer<'managers, 'blocks> {
        Buffer {
            file_manager: file_manager.clone(),
            log_manager,
            contents: Page::new(file_manager.borrow().block_size),
            block: None,
            pins_count: 0,
            transaction_number: None,
            log_sequence_number: None,
        }
    }

    pub fn contents(&self) -> &Page {
        &self.contents
    }

    pub fn contents_mut(&mut self) -> &mut Page {
        &mut self.contents
    }

    pub fn block(&self) -> Option<BlockId<'blocks>> {
        self.block
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

    pub fn assign_to_block(&mut self, block_id: BlockId<'blocks>) -> Result<(), IoError> {
        debug!(
            "Buffer: Assigning to block {:?} <- block {:?}",
            self.block, block_id
        );
        self.flush()?;
        self.block = Some(block_id);
        debug!(
            "Buffer: Assigning to block={:?}, read file_manager={:?} contents={:?}",
            block_id, self.file_manager, self.contents
        );
        self.file_manager
            .borrow_mut()
            .read(&block_id, &mut self.contents)?;
        debug!(
            "Buffer: Assigning to block={:?}, set pins_count=0",
            block_id
        );
        self.pins_count = 0;
        Ok(())
    }

    // TODO: maybe not public
    pub fn flush(&mut self) -> Result<(), IoError> {
        if self.transaction_number.is_some() {
            debug!("Buffer: Flush {}", self);
            if let Some(lsn) = self.log_sequence_number {
                self.log_manager.borrow_mut().flush(lsn)?;
            }
            self.file_manager
                .borrow_mut()
                .write(&self.block().unwrap(), &self.contents)?;
            self.transaction_number = None;
        }
        Ok(())
    }

    // TODO: maybe not public
    pub fn pin(&mut self) {
        self.pins_count += 1;
        debug!(
            "Buffer: Pinned block {:?} new count {:?}",
            self.block, self.pins_count
        );
    }

    // TODO: maybe not public
    pub fn unpin(&mut self) {
        self.pins_count -= 1;
        debug!(
            "Buffer: Unpinned block {:?} new count {:?}",
            self.block, self.pins_count
        );
    }
}
