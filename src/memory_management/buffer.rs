use crate::file_management::block_id::BlockId;
use crate::file_management::file_manager::FileManager;
use crate::file_management::page::Page;
use crate::memory_management::log_manager::LogManager;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;

#[derive(Debug)]
pub struct Buffer<'a> {
    file_manager: Rc<RefCell<FileManager<'a>>>,
    log_manager: Rc<RefCell<LogManager<'a>>>,
    contents: Page,
    block: Option<&'a BlockId<'a>>,
    pins_count: usize,
    transaction_number: Option<NonZeroUsize>,
    log_sequence_number: Option<usize>,
}

impl<'a> Buffer<'a> {
    pub fn new(
        file_manager: Rc<RefCell<FileManager<'a>>>,
        log_manager: Rc<RefCell<LogManager<'a>>>,
    ) -> Buffer<'a> {
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

    pub fn block(&self) -> Option<&BlockId<'a>> {
        self.block
    }

    pub fn set_modified(
        &mut self,
        transaction_number: NonZeroUsize,
        log_sequence_number: Option<usize>,
    ) {
        self.transaction_number = Some(transaction_number);
        self.log_sequence_number = log_sequence_number;
    }

    pub fn is_pinned(&self) -> bool {
        self.pins_count > 0
    }

    pub fn modifying_transaction_number(&self) -> Option<NonZeroUsize> {
        self.transaction_number
    }

    pub fn assign_to_block(&mut self, block_id: &'a BlockId<'a>) -> Result<(), std::io::Error> {
        self.flush()?;
        self.block = Some(block_id);
        self.file_manager
            .borrow_mut()
            .read(block_id, &mut self.contents)?;
        self.pins_count = 0;
        Ok(())
    }

    // TODO: maybe not public
    pub fn flush(&mut self) -> Result<(), std::io::Error> {
        if self.transaction_number.is_some() {
            if let Some(lsn) = self.log_sequence_number {
                self.log_manager.borrow_mut().flush(lsn)?;
            }
            self.file_manager
                .borrow_mut()
                .write(self.block.unwrap(), &self.contents)?;
            self.transaction_number = None;
        }
        Ok(())
    }

    // TODO: maybe not public
    pub fn pin(&mut self) {
        self.pins_count += 1;
    }

    // TODO: maybe not public
    pub fn unpin(&mut self) {
        self.pins_count -= 1;
    }
}
