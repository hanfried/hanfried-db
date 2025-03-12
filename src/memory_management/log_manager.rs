use crate::file_management::block_id::BlockId;
use crate::file_management::file_manager::{FileManager, IoError};
use crate::file_management::page::Page;
use log::debug;
use std::fmt::Display;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct LogSequenceNumber(usize);

impl LogSequenceNumber {
    pub fn from(nr: u64) -> Self {
        LogSequenceNumber(nr as usize)
    }

    fn next(&self) -> Self {
        LogSequenceNumber(self.0 + 1)
    }
}

impl Display for LogSequenceNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct LogPosition {
    pub latest: LogSequenceNumber,
    pub last_saved: LogSequenceNumber,
}

#[derive(Debug, Clone)]
struct LogHead {
    page: Page,
    block: BlockId,
    position: LogPosition,
}

#[derive(Debug, Clone)]
pub struct LogManager {
    file_manager: Arc<FileManager>,
    log_file: String,
    head: Arc<Mutex<LogHead>>,
    // log_page: Page,
    // current_block: BlockId<'a>,
    // log_sequences: Arc<Mutex<LogPosition>>,
}

impl LogManager {
    pub fn new(file_manager: Arc<FileManager>, log_file: String) -> Result<LogManager, IoError> {
        debug!(
            "Create new log manager, file_manager={:?}, log_file={:?}",
            file_manager, log_file
        );
        // let mut fm_binding = file_manager.borrow_mut();
        // let fm = fm_binding.deref_mut();
        let fm = file_manager.clone();
        let mut log_page = Page::new(fm.block_size);
        let current_block: BlockId = match fm.block_length(log_file.as_str())? {
            0 => Self::append_new_block(log_file.as_str(), fm, &mut log_page)?,
            log_size => {
                let block_id = BlockId::new(log_file.as_str(), log_size - 1);
                fm.read(&block_id, &mut log_page)?;
                block_id
            }
        };

        let log_manager = LogManager {
            file_manager,
            log_file: log_file.to_string(),
            head: Arc::new(Mutex::new(LogHead {
                page: log_page,
                block: current_block,
                position: LogPosition {
                    latest: LogSequenceNumber(0),
                    last_saved: LogSequenceNumber(0),
                },
            })),
        };
        debug!("created log_manager={:?}", log_manager);
        Ok(log_manager)
    }

    fn append_new_block(
        log_file: &str,
        fm: Arc<FileManager>,
        log_page: &mut Page,
    ) -> Result<BlockId, IoError> {
        let block_id = fm.append(log_file)?;
        log_page.set_i32(0, usize::from(fm.block_size) as i32);
        fm.write(&block_id, log_page)?;
        debug!(
            "Append new block_id={:?}, log_file={:?}, log_page={:?}",
            &block_id, log_file, log_page
        );
        Ok(block_id)
    }

    pub fn flush(&self, log_sequence_number: LogSequenceNumber) -> Result<(), IoError> {
        let mut head = self.head.lock().unwrap();
        if log_sequence_number >= head.position.latest {
            self._flush(&mut head)?;
        }
        Ok(())
    }

    fn _flush(&self, head_lock_guard: &mut MutexGuard<LogHead>) -> Result<(), IoError> {
        // println!("Flushing {}", self.log_file);
        // let mut fm_binding = self.file_manager.borrow_mut();
        // let fm = fm_binding.deref_mut();
        // let mut head = self.head.lock().unwrap();
        self.file_manager
            .write(&head_lock_guard.block, &head_lock_guard.page)?;
        head_lock_guard.position.last_saved = head_lock_guard.position.latest;
        Ok(())
    }

    pub fn append(&self, log_record: &[u8]) -> Result<LogPosition, IoError> {
        // println!("Append log record: {:?} current head {:?}", log_record, self.head.lock().unwrap());
        let mut head = self.head.lock().unwrap();
        let mut boundary = head.page.get_i32(0);
        let record_size = log_record.len();
        let bytes_needed = record_size + 4;
        if (boundary as usize) < bytes_needed + 4 {
            self._flush(&mut head)?;
            head.block = Self::append_new_block(
                self.log_file.as_str(),
                self.file_manager.clone(),
                &mut head.page,
            )?;
            boundary = head.page.get_i32(0);
        }
        let record_pos = boundary - bytes_needed as i32;
        head.page.set_bytes(record_pos as usize, log_record);
        head.page.set_i32(0, record_pos);
        head.position.latest = head.position.latest.next();
        // println!("Position now {:?} after appending log record: {:?}", head.position, log_record);
        Ok(head.position.clone())
    }

    pub fn iter(&self) -> Result<LogManagerIter, IoError> {
        let fm = self.file_manager.clone();
        let mut page = Page::new(fm.block_size);
        let head = self.head.lock().unwrap();
        fm.read(&head.block, &mut page)?;
        let boundary = page.get_i32(0);

        Ok(LogManagerIter {
            file_manager: fm,
            block: head.block.clone(),
            page,
            pos_current: boundary as usize,
            boundary: boundary as usize,
        })
    }
}

pub struct LogManagerIter {
    file_manager: Arc<FileManager>,
    block: BlockId,
    // page: Rc<RefCell<Page>>,
    page: Page,
    pos_current: usize,
    boundary: usize,
}

impl Iterator for LogManagerIter {
    type Item = Result<Vec<u8>, IoError>;

    fn next(&mut self) -> Option<Self::Item> {
        let block_size = self.file_manager.block_size;
        let has_next =
            (self.pos_current < usize::from(block_size)) || (self.block.block_number > 0);
        if !has_next {
            return None;
        };
        if self.pos_current == usize::from(block_size) {
            self.block = BlockId::new(self.block.filename.as_str(), self.block.block_number - 1);
            if let Err(read_block_result) = self.file_manager.read(&self.block, &mut self.page) {
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

#[cfg(test)]
mod tests {
    use crate::file_management::file_manager::FileManagerBuilder;
    use crate::file_management::page::Page;
    use crate::memory_management::log_manager::{LogManager, LogPosition, LogSequenceNumber};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::thread;

    fn create_log_record(s: &str, n: i32) -> Vec<u8> {
        let n_pos = s.len() + 4;
        let mut p = Page::new(NonZeroUsize::new(n_pos + 4).unwrap());
        p.set_string(0, s);
        p.set_i32(n_pos, n);
        p.get_contents().to_vec()
    }

    fn assert_create_records_parallel(log_manager: LogManager, start: i32, end: i32) {
        println!("assert create records parallel {start} {end}");
        let lm = log_manager.clone();
        let log_records: Vec<Vec<u8>> = (start..end)
            .map(|record_nr| {
                create_log_record(format!("record{}", record_nr).as_str(), record_nr + 100)
            })
            .collect();
        let mut create_records_threads = Vec::new();
        for log_record in log_records {
            let lm = log_manager.clone();
            create_records_threads.push(thread::spawn(move || {
                lm.append(log_record.as_slice()).unwrap()
            }));
        }
        let mut log_positions: Vec<LogPosition> = Vec::new();
        for t in create_records_threads {
            log_positions.push(t.join().unwrap());
        }
        assert_eq!(log_positions.iter().map(|p| p.latest).max().unwrap(), {
            lm.head.lock().unwrap().position.latest
        });
        let latest_positions_found_sorted = {
            let mut p = log_positions.iter().map(|p| p.latest).collect::<Vec<_>>();
            p.sort();
            p
        };
        let latest_positions_expected = (start..end)
            .map(|nr| LogSequenceNumber::from(nr as u64).next())
            .collect::<Vec<_>>();
        assert_eq!(latest_positions_found_sorted, latest_positions_expected);
    }

    #[test]
    fn test_log_manager() {
        let file_manager = Arc::new(FileManagerBuilder::unittest("log_manager").build().unwrap());
        let log_manager =
            LogManager::new(file_manager.clone(), "test_log_manager.log".to_string()).unwrap();
        // 0 .. 1
        // 1 .. 2
        // 2 .. 4
        // ...
        assert_create_records_parallel(log_manager.clone(), 0, 1);
        for n_th_power in 0..10 {
            assert_create_records_parallel(
                log_manager.clone(),
                1 << n_th_power,
                1 << (n_th_power + 1),
            );
        }
    }
}
