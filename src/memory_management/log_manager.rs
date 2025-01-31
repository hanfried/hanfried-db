use crate::file_management::block_id::BlockId;
use crate::file_management::file_manager::FileManager;
use crate::file_management::page::Page;
use log::debug;
use std::cell::RefCell;
use std::fmt::Display;
use std::ops::DerefMut;
use std::rc::Rc;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct LogSequenceNumber(usize);

impl LogSequenceNumber {
    pub fn from(nr: u64) -> Self {
        LogSequenceNumber(nr as usize)
    }

    fn increment(mut self) {
        self.0 += 1;
    }
}

impl Display for LogSequenceNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct LogManager<'a> {
    file_manager: Rc<RefCell<FileManager<'a>>>,
    log_file: &'a str,
    log_page: Page,
    current_block: BlockId<'a>,
    log_sequence_number_latest: LogSequenceNumber,
    log_sequence_number_last_saved: LogSequenceNumber,
}

impl<'a> LogManager<'a> {
    pub fn new(
        file_manager: Rc<RefCell<FileManager<'a>>>,
        log_file: &'a str,
    ) -> Result<LogManager<'a>, std::io::Error> {
        debug!(
            "Create new log manager, file_manager={:?}, log_file={:?}",
            file_manager, log_file
        );
        let mut fm_binding = file_manager.borrow_mut();
        let fm = fm_binding.deref_mut();
        let mut log_page = Page::new(fm.block_size);
        let current_block: BlockId = match fm.block_length(log_file)? {
            0 => Self::append_new_block(log_file, fm, &mut log_page)?,
            log_size => {
                let block_id = BlockId::new(log_file, log_size - 1);
                fm.read(&block_id, &mut log_page)?;
                block_id
            }
        };

        let log_manager = LogManager {
            file_manager: file_manager.clone(),
            log_file,
            log_page,
            current_block,
            log_sequence_number_latest: LogSequenceNumber(0),
            log_sequence_number_last_saved: LogSequenceNumber(0),
        };
        debug!("log_manager={:?}", log_manager);
        Ok(log_manager)
    }

    fn append_new_block(
        log_file: &'a str,
        fm: &mut FileManager<'a>,
        log_page: &mut Page,
    ) -> Result<BlockId<'a>, std::io::Error> {
        let block_id = fm.append(log_file)?;
        log_page.set_i32(0, fm.block_size as i32);
        fm.write(&block_id, log_page)?;
        debug!(
            "Append new block_id={:?}, log_file={:?}, log_page={:?}",
            &block_id, log_file, log_page
        );
        Ok(block_id)
    }

    pub fn flush(&mut self, log_sequence_number: LogSequenceNumber) -> Result<(), std::io::Error> {
        if log_sequence_number >= self.log_sequence_number_last_saved {
            self._flush()?;
        }
        Ok(())
    }

    fn _flush(&mut self) -> Result<(), std::io::Error> {
        let mut fm_binding = self.file_manager.borrow_mut();
        let fm = fm_binding.deref_mut();
        fm.write(&self.current_block, &self.log_page)?;
        self.log_sequence_number_last_saved = self.log_sequence_number_latest;
        Ok(())
    }

    pub fn append(&mut self, log_record: &[u8]) -> Result<LogSequenceNumber, std::io::Error> {
        let mut boundary = self.log_page.get_i32(0);
        let record_size = log_record.len();
        let bytes_needed = record_size + 4;
        if (boundary as usize) < bytes_needed + 4 {
            self._flush()?;
            self.current_block = Self::append_new_block(
                self.log_file,
                self.file_manager.borrow_mut().deref_mut(),
                &mut self.log_page,
            )?;
            boundary = self.log_page.get_i32(0);
        }
        let record_pos = boundary - bytes_needed as i32;
        self.log_page.set_bytes(record_pos as usize, log_record);
        self.log_page.set_i32(0, record_pos);
        self.log_sequence_number_latest.increment();
        Ok(self.log_sequence_number_latest)
    }

    pub fn iter(&self) -> Result<LogManagerIter<'a>, std::io::Error> {
        let mut fm = self.file_manager.borrow_mut();
        let mut page = Page::new(fm.block_size);
        fm.read(&self.current_block, &mut page)?;
        let boundary = page.get_i32(0);

        Ok(LogManagerIter {
            file_manager: self.file_manager.clone(),
            block: self.current_block,
            page,
            pos_current: boundary as usize,
            boundary: boundary as usize,
        })
    }
}

pub struct LogManagerIter<'a> {
    file_manager: Rc<RefCell<FileManager<'a>>>,
    block: BlockId<'a>,
    // page: Rc<RefCell<Page>>,
    page: Page,
    pos_current: usize,
    boundary: usize,
}

impl Iterator for LogManagerIter<'_> {
    type Item = Result<Vec<u8>, std::io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let block_size = self.file_manager.borrow().block_size;
        let has_next = (self.pos_current < block_size) || (self.block.block_number > 0);
        if !has_next {
            return None;
        };
        if self.pos_current == block_size {
            self.block = BlockId::new(self.block.filename, self.block.block_number - 1);
            if let Err(read_block_result) = self
                .file_manager
                .borrow_mut()
                .read(&self.block, &mut self.page)
            {
                return Some(Err(read_block_result));
            }
            self.boundary = self.page.get_i32(0) as usize;
            self.pos_current = self.boundary;
        }
        let record = self.page.get_bytes(self.pos_current);
        self.pos_current += 4 + record.len();
        Some(Ok(record.to_vec()))
    }
}
