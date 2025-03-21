use std::fmt::{Display, Formatter};
use crate::file_management::block_id::BlockId;
use crate::file_management::file_manager::{FileManager, IoError};
use crate::memory_management::buffer::{Buffer, TransactionNumber};
use crate::memory_management::buffer_manager::BufferManagerError::{DeadLockTimeout, NoCapacity};
use crate::memory_management::log_manager::LogManager;
use log::{debug, warn};
use std::sync::{Condvar, Mutex, RwLock};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BufferManager {
    // pool: Vec<RwLock<Buffer>>,
    pool: Vec<Buffer>,
    num_available: usize,
    // buffer_available: Condvar,
    // deadlock_waiting_duration: Duration,
}

impl BufferManager {
    pub fn new(
        file_manager: &FileManager,
        log_manager: &LogManager,
        pool_size: usize,
        deadlock_waiting_duration: Duration,
    ) -> BufferManager {
        let mut pool: Vec<Buffer> = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            pool.push(Buffer::new(file_manager, log_manager));
        }
        BufferManager {
            pool,
            num_available: pool_size,
        //     buffer_available: Condvar::new(),
        //     deadlock_waiting_duration,
        }
    }

    pub fn num_available(&self) -> usize {
        self.num_available
    }

    pub fn flush_all(&self, transaction_number: TransactionNumber) {
        todo!()
    }

    pub fn unpin(&self, buffer: &Buffer) {
        todo!()
    }

    pub fn pin(&mut self, block_id: &BlockId) -> Result<Buffer, BufferManagerError>{
        self.try_to_pin(block_id)
    }

    fn try_to_pin(&mut self, block_id: &BlockId) -> Result<Buffer, BufferManagerError> {
        let existing_buffer = self.pool.iter_mut().find(|buffer| buffer.block() == Some(block_id.clone()));
        debug!("try to pin: existing_buffer: {:?} for block_id {:?}", existing_buffer, block_id);
        let mut buffer = match existing_buffer {
            Some(buffer) => buffer.clone(),
            None => {
                let mut buffer = self.choose_unpinned_buffer().unwrap();
                buffer.assign_to_block(block_id.clone()).map_err(|e| BufferManagerError::StdIoError(e))?;
                buffer.clone()
            }
        };
        if buffer.is_not_pinned() {
            self.num_available -= 1;
        }
        buffer.pin();
        Ok(buffer)
    }

    fn choose_unpinned_buffer(&mut self) -> Result<Buffer, BufferManagerError> {
        let unpinned_buffer = self.pool.iter().find(|buffer| buffer.is_not_pinned());
        debug!("Choosing unpinned buffer: {:?}", unpinned_buffer);
        Ok(unpinned_buffer.unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::thread;
    use std::time::Duration;
    use crate::file_management::block_id::{BlockId, DbFilename};
    use crate::file_management::file_manager::{FileManager, FileManagerBuilder};
    use crate::file_management::page::Page;
    use crate::memory_management::buffer::TransactionNumber;
    use crate::memory_management::buffer_manager::BufferManager;
    use crate::memory_management::log_manager::{LogManager, LogSequenceNumber};
    use crate::utils::logging::init_logging;

    #[test]
    fn test_buffers() {
        init_logging();
        
        let file_manager = FileManagerBuilder::unittest("buffer_test").block_size(NonZeroUsize::new(100 as usize).unwrap()).build().unwrap();
        let log_manager = LogManager::new(&file_manager, &DbFilename::from("test_buffers.log")).unwrap();
        let pool_size = 3;
        let deadlock_waiting_duration = Duration::from_secs(10);
        let buffer_manager = BufferManager::new(
            &file_manager,
            &log_manager,
            pool_size,
            deadlock_waiting_duration,
        );

        let block = BlockId::new(DbFilename::from("testfile"), 1);
        let mut bm = buffer_manager.clone();
        let block1 = block.clone();
        let t1 = thread::spawn(move || {
            let mut buffer = bm.pin(&block1.clone()).unwrap();
            let page = buffer.contents_mut();
            let n = page.get_i32(80);
            page.set_i32(80, n+1);
            buffer.set_modified(TransactionNumber::from(1), None);
            buffer.unpin();
            n + 1
        });
        let expected_n_in_block_1 = t1.join().unwrap();

        let other_threads = (2 ..= 4).map(|thread_nr| {
            let mut bm = buffer_manager.clone();
            let block_n = block.clone();
            thread::spawn(move || {bm.pin(&block_n.clone().with_other_block_number(thread_nr))})
        });
        let other_buffers = other_threads
            .map(|t| t.join().unwrap())
            .map(|buffer_result| buffer_result.unwrap())
            .collect::<Vec<_>>();

        let mut page1 = Page::new(file_manager.block_size);
        file_manager.read(&block, &mut page1).expect("Error reading block");
        let got_n_in_block_1 = page1.get_i32(80);
        assert_eq!(got_n_in_block_1, expected_n_in_block_1);

        buffer_manager.unpin(other_buffers.first().unwrap());
        let mut bm = buffer_manager.clone();
        let mut buffer = bm.pin(&block).unwrap();
        let mut page = buffer.contents();
        page.set_i32(80, 9999);
        buffer.set_modified(TransactionNumber::from(1), None);
        buffer_manager.unpin(&buffer);

        let mut page1 = Page::new(file_manager.block_size);
        file_manager.read(&block, &mut page1).expect("Error reading block");
        assert_eq!(page1.get_i32(80), expected_n_in_block_1, "Changes with unpinned buffer should not be written to disk");

        // Todo: Test Deadlock
    }
}

// impl BufferManager {
//     pub fn new(
//         file_manager: &FileManager,
//         log_manager: &LogManager,
//         pool_size: usize,
//         deadlock_waiting_duration: Duration,
//     ) -> BufferManager {
//         // let mut pool: Vec<RwLock<Buffer>> = Vec::with_capacity(pool_size);
//         let mut pool: Vec<Buffer> = Vec::with_capacity(pool_size);
//         for _ in 0..pool_size {
//             // pool.push(RwLock::new(Buffer::new(file_manager, log_manager)))
//             pool.push(Buffer::new(file_manager, log_manager));
//         }
//         BufferManager {
//             pool,
//             num_available: Mutex::new(pool_size),
//             buffer_available: Condvar::new(),
//             deadlock_waiting_duration,
//         }
//     }
//
//     pub fn available(&self) -> usize {
//         *self.num_available.lock().unwrap()
//     }
//
//     pub fn flush_all(&mut self, transaction_number: TransactionNumber) -> Result<(), IoError> {
//         debug!("BufferManager: Flush all for {:?}", transaction_number);
//         for buffer in self.pool.iter() {
//             // Todo: Locking just to see of a transaction number is set seems overkill
//             // let mut buffer = buffer.lock().unwrap();
//             if buffer.read().unwrap().modifying_transaction_number() == Some(transaction_number) {
//                 buffer.write().unwrap().flush()?;
//             }
//         }
//         Ok(())
//     }
//
//     pub fn unpin(&mut self, buffer: &mut Buffer) {
//         buffer.unpin();
//         debug!("BufferManager: Unpin buffer for block {:?}", buffer.block());
//         if !buffer.is_pinned() {
//             *(self.num_available.lock().unwrap()) += 1;
//             debug!("BufferManager: Unpinned globally buffer for block {:?} -> notify all waiting threads", buffer.block());
//             self.buffer_available.notify_all();
//         }
//     }
//
//     pub fn pin(&self, block: BlockId) -> Result<&RwLock<Buffer>, BufferManagerError> {
//         debug!("Called Buffermanager pin: {:?}", block);
//         loop {
//             let buffer_guard = self.try_to_pin(block.clone());
//             match buffer_guard {
//                 Ok(buffer) => {
//                     debug!("BufferManager: Pin buffer block {:?}", block);
//                     return Ok(buffer);
//                 }
//                 Err(BufferManagerError::StdIoError(io_error)) => {
//                     warn!(
//                         "BufferManager: Pinning failed for buffer block {:?}: {:?}",
//                         block, io_error
//                     );
//                     return Err(BufferManagerError::StdIoError(io_error));
//                 }
//                 _ => {}
//             }
//             debug!(
//                 "BufferManager: NoCapacity wait (trying to pin buffer for block {:?}) ...",
//                 block
//             );
//             let result = self.buffer_available.wait_timeout(
//                 self.num_available.lock().unwrap(),
//                 self.deadlock_waiting_duration,
//             );
//             if result.is_err() {
//                 warn!(
//                     "BufferManager: Deadlock Timout trying to pin buffer for block {:?}: {:?}",
//                     block,
//                     result.unwrap_err()
//                 );
//                 return Err(DeadLockTimeout);
//             }
//         }
//     }
//
//     fn find_existing_buffer(
//         &self,
//         block: BlockId,
//         // ) -> Option<MutexGuard<Buffer<'managers, 'blocks>>> {
//     ) -> Option<&RwLock<Buffer>> {
//         // Todo: Looping through whole list with locking seems overkill (map datastructure or something like that)
//         // and not sure whether data might be changeable after finding and unlocking it again
//         debug!("Find existing buffer for block {:?}", block);
//         self.pool
//             .iter()
//             .find(|b| b.read().unwrap().block() == Some(block.clone()))
//     }
//
//     fn choose_unpinned_buffer(&self) -> Option<&RwLock<Buffer>> {
//         // -> Option<MutexGuard<Buffer<'managers, 'blocks>>> {
//         debug!("Choose an unpinned buffer");
//         // Todo: Looping through whole list with locking seems overkill
//         self.pool.iter().find(|b| b.read().unwrap().is_not_pinned())
//         // .map(|b| b.lock().unwrap())
//     }
//
//     fn try_to_pin(
//         &self,
//         block: BlockId,
//         // ) -> Result<MutexGuard<Buffer<'managers, 'blocks>>, BufferManagerError> {
//     ) -> Result<&RwLock<Buffer>, BufferManagerError> {
//         debug!("BufferManager: Trying to pin block {:?}", block);
//         let buffer = self
//             .find_existing_buffer(block.clone())
//             .or_else(|| self.choose_unpinned_buffer());
//
//         if buffer.is_none() {
//             return Err(NoCapacity);
//         }
//         let found_buffer = buffer.unwrap();
//         let mut found_buffer_mut = found_buffer.write().unwrap();
//         if let Err(io_error) = found_buffer_mut.assign_to_block(block) {
//             return Err(BufferManagerError::StdIoError(io_error));
//         }
//         found_buffer_mut.pin();
//         Ok(found_buffer)
//         // let buffer_guard = self
//         //     .find_existing_buffer(block)
//         //     .or_else(|| self.choose_unpinned_buffer());
//         // debug!(
//         //     "BufferManager trying to pin: buffer_guard: {:?}",
//         //     buffer_guard
//         // );
//         // if buffer_guard.is_none() {
//         //     return Err(NoCapacity);
//         // }
//         //
//         // let mut buffer = buffer_guard.unwrap();
//         // if let Err(io_error) = buffer.assign_to_block(block) {
//         //     return Err(BufferManagerError::StdIoError(io_error));
//         // }
//         //
//         // if buffer.is_pinned() {
//         //     *(self.num_available.lock().unwrap()) -= 1;
//         // }
//         // buffer.pin();
//         // Ok(buffer)
//     }
// }

#[derive(Debug)]
pub enum BufferManagerError {
    StdIoError(IoError),
    NoCapacity,
    DeadLockTimeout,
}

impl Display for BufferManagerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StdIoError(e) => write!(f, "BufferManager IoError {}", e),
            NoCapacity => write!(f, "BufferManager: No capacity available"),
            DeadLockTimeout => write!(f, "BufferManager: DeadlockTimeout"),
        }
    }
}
