use crate::file_management::block_id::BlockId;
use crate::file_management::file_manager::{FileManager, IoError};
use crate::memory_management::buffer::{Buffer, TransactionNumber};
use crate::memory_management::buffer_manager::BufferManagerError::{DeadLockTimeout, NoCapacity};
use crate::memory_management::log_manager::LogManager;
use log::{debug, warn};
use std::fmt::{Display, Formatter};
use std::ops::DerefMut;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct BufferManager {
    pool: Vec<Buffer>,
    num_available: Arc<Mutex<usize>>,
    buffer_available: Arc<Condvar>,
    deadlock_waiting_duration: Duration,
}

pub struct BufferManagerBuilder {
    pool_size: usize,
    deadlock_waiting_duration: Duration,
}

impl Default for BufferManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferManagerBuilder {
    const DEFAULT_BUFFER_POOL_SIZE: usize = 100_000;
    const DEFAULT_DEADLOCK_WAITING_DURATION: Duration = Duration::from_secs(10);
    const UNITTEST_BUFFER_POOL_SIZE: usize = 1_000;
    const UNITTEST_DEADLOCK_WAITING_DURATION: Duration = Duration::from_millis(200);

    pub fn new() -> Self {
        Self {
            pool_size: Self::DEFAULT_BUFFER_POOL_SIZE,
            deadlock_waiting_duration: Self::DEFAULT_DEADLOCK_WAITING_DURATION,
        }
    }

    pub fn unittest() -> Self {
        Self {
            pool_size: Self::UNITTEST_BUFFER_POOL_SIZE,
            deadlock_waiting_duration: Self::UNITTEST_DEADLOCK_WAITING_DURATION,
        }
    }

    pub fn pool_size(mut self, buffer_pool_size: usize) -> Self {
        self.pool_size = buffer_pool_size;
        self
    }

    pub fn deadlock_waiting_duration(mut self, duration: Duration) -> Self {
        self.deadlock_waiting_duration = duration;
        self
    }

    pub fn build(self, file_manager: &FileManager, log_manager: &LogManager) -> BufferManager {
        BufferManager::new(
            file_manager,
            log_manager,
            self.pool_size,
            self.deadlock_waiting_duration,
        )
    }
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
            num_available: Arc::new(Mutex::new(pool_size)),
            buffer_available: Arc::new(Condvar::new()),
            deadlock_waiting_duration,
        }
    }

    pub fn num_available(&self) -> usize {
        *self
            .num_available
            .lock()
            .expect("failed to lock num_available in BufferManager:num_available")
    }

    pub fn flush_all(&self, transaction_number: TransactionNumber) -> Result<(), IoError> {
        for buffer in self
            .pool
            .iter()
            .filter(|buffer| buffer.modifying_transaction_number() == Some(transaction_number))
        {
            buffer.flush()?
        }
        Ok(())
    }

    pub fn unpin(&self, buffer: &Buffer) {
        let mut num_available_guard = self
            .num_available
            .lock()
            .expect("Locking failed for num_available in BufferManager unpine");
        let num_available = num_available_guard.deref_mut();
        debug!(
            "Unpin buffer: {:?} num_available_before={}",
            buffer, num_available
        );
        buffer.decrement_pins_count();
        *num_available += 1;
        debug!(
            "Unpinned buffer (notify other threads): {:?} num_available_after={}",
            buffer, num_available
        );
        self.buffer_available.notify_one();
    }

    pub fn pin(&self, block_id: &BlockId) -> Result<Buffer, BufferManagerError> {
        self.try_to_pin(block_id)
    }

    fn try_to_pin(&self, block_id: &BlockId) -> Result<Buffer, BufferManagerError> {
        let existing_buffer = self
            .pool
            .iter()
            .find(|buffer| buffer.block() == Some(block_id.clone()));
        debug!(
            "try to pin: existing_buffer: {:?} for block_id {:?}",
            existing_buffer, block_id
        );
        let buffer = match existing_buffer {
            Some(buffer) => buffer.clone(),
            None => {
                let mut buffer = self.choose_unpinned_buffer()?;
                buffer
                    .assign_to_block(block_id.clone())
                    .map_err(BufferManagerError::StdIoError)?;
                buffer
            }
        };
        debug!(
            "Now pin buffer: {:?} num_available={}",
            buffer,
            self.num_available.lock().unwrap()
        );
        if buffer.is_not_pinned() {
            let mut num_available_guard = self
                .num_available
                .lock()
                .expect("Locking failed for num_available in BufferManager try_to_pin");
            let num_available = num_available_guard.deref_mut();
            debug!(
                "Decrement num_available={}, buffer={:?}",
                num_available, buffer
            );
            *num_available -= 1;
        }
        buffer.increment_pins_count();
        Ok(buffer)
    }

    fn choose_unpinned_buffer(&self) -> Result<Buffer, BufferManagerError> {
        let start_time = Instant::now();
        while start_time.elapsed() <= self.deadlock_waiting_duration {
            let unpinned_buffer = self.pool.iter().find(|buffer| buffer.is_not_pinned());
            debug!("Choosing unpinned buffer: {:?}", unpinned_buffer);
            if let Some(buffer) = unpinned_buffer {
                return Ok(buffer.clone());
            }
            debug!(
                "No unpinned buffer available, wait at most {:?} to get some unpinned",
                self.deadlock_waiting_duration
            );

            let waiting_result = self.buffer_available.wait_timeout(
                self.num_available
                    .lock()
                    .expect("Locking num_available in BufferManager choose_unpinned_buffer"),
                self.deadlock_waiting_duration,
            );
            if waiting_result.is_err() {
                warn!("BufferManager: Deadlock Timout trying to choose unpinned buffer");
                return Err(DeadLockTimeout);
            }
        }
        Err(DeadLockTimeout)
    }
}

#[cfg(test)]
mod tests {
    use crate::datatypes::fixed_length_integers::Integer;
    use crate::db_management_system::hfdb::HanfriedDbBuilder;
    use crate::file_management::block_id::{BlockId, DbFilename};
    use crate::file_management::file_manager::FileManagerBuilder;
    use crate::file_management::page::Page;
    use crate::memory_management::buffer::TransactionNumber;
    use crate::memory_management::buffer_manager::BufferManager;
    use crate::memory_management::buffer_manager::BufferManagerError::DeadLockTimeout;
    use crate::memory_management::log_manager::LogManager;
    use crate::utils::logging::init_logging;
    use log::debug;
    use std::num::NonZeroUsize;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_buffers_flushing() {
        init_logging();

        let file_manager = FileManagerBuilder::unittest("buffer_test")
            .block_size(NonZeroUsize::new(100 as usize).unwrap())
            .build()
            .unwrap();
        let log_manager =
            LogManager::new(&file_manager, &DbFilename::from("test_buffers.log")).unwrap();
        let pool_size = 3;
        let deadlock_waiting_duration = Duration::from_secs(10);
        let buffer_manager = BufferManager::new(
            &file_manager,
            &log_manager,
            pool_size,
            deadlock_waiting_duration,
        );

        let block = BlockId::new(DbFilename::from("testfile"), 1);
        let bm = buffer_manager.clone();
        let block1 = block.clone();
        let t1 = thread::spawn(move || {
            debug!("Thread 1 Writing to {:?}", &block1);
            let mut buffer = bm.pin(&block1).unwrap();
            let n_plus_1 = buffer.modify_page(
                |page| {
                    // let n = page.get_i32(80);
                    // page.set_i32(80, n + 1);
                    let n = i32::from(page.get::<Integer>(80));
                    page.set(80, &Integer::from(n + 1));
                    n + 1
                },
                TransactionNumber::from(1),
                None,
            );
            bm.unpin(&buffer);
            debug!("Thread 1 Unpinned returning {}", n_plus_1);
            n_plus_1
        });
        let expected_n_in_block_1 = t1.join().unwrap();

        let other_threads = (2..=4).map(|thread_nr| {
            let bm = buffer_manager.clone();
            let block_n = block.clone().with_other_block_number(thread_nr);
            thread::spawn(move || {
                debug!("Thread {thread_nr} pinning {:?}", &block_n);
                bm.pin(&block_n)
            })
        });
        let other_buffers = other_threads
            .map(|t| t.join().unwrap())
            .map(|buffer_result| buffer_result.unwrap())
            .collect::<Vec<_>>();

        let page1 = Page::new(file_manager.block_size);
        file_manager
            .read(&block, &page1)
            .expect("Error reading block");
        let got_n_in_block_1 = i32::from(page1.get::<Integer>(80)); // page1.get_i32(80);
        assert_eq!(got_n_in_block_1, expected_n_in_block_1, "Changes of first block should be flushed after unpinning and pinning others to force flush");

        buffer_manager.unpin(other_buffers.first().unwrap());
        let bm = buffer_manager.clone();
        let mut buffer = bm.pin(&block).unwrap();
        buffer.modify_page(
            |page| page.set(80, &Integer::from(9999)), // page.set_i32(80, 9999),
            TransactionNumber::from(1),
            None,
        );
        buffer_manager.unpin(&buffer);

        let mut page1 = Page::new(file_manager.block_size);
        file_manager
            .read(&block, &mut page1)
            .expect("Error reading block");
        assert_eq!(
            // page1.get_i32(80),
            i32::from(page1.get::<Integer>(80)),
            expected_n_in_block_1,
            "Changes with unpinned buffer should not be written to disk"
        );
    }

    #[test]
    fn test_buffers_deadlock() {
        init_logging();

        let hfdb = HanfriedDbBuilder::unittest("buffer_test_deadlock")
            .file_manager(|fm| fm.block_size(NonZeroUsize::new(100 as usize).unwrap()))
            .buffer_manager(|bm| bm.pool_size(3))
            .build();

        let bm = &hfdb.buffer_manager;
        let test_filename = DbFilename::from("testfile");
        let block0 = BlockId::new(test_filename, 0);
        let block1 = block0.with_other_block_number(1);
        let block2 = block0.with_other_block_number(2);
        let block3 = block0.with_other_block_number(3);

        assert_eq!(bm.num_available(), 3);
        let _buffer0 = bm.pin(&block0).unwrap();
        let buffer1 = bm.pin(&block1).unwrap();
        let buffer2 = bm.pin(&block2).unwrap();
        assert_eq!(bm.num_available(), 0);

        bm.unpin(&buffer1);
        assert_eq!(bm.num_available(), 1);
        let _buffer3 = bm.pin(&block0.clone()).unwrap();
        let _buffer4 = bm.pin(&block1.clone()).unwrap();
        assert_eq!(bm.num_available(), 0);

        match bm.pin(&block3) {
            Err(DeadLockTimeout) => assert!(true),
            Err(other_error) => assert!(
                false,
                "Expected dead lock, but got other_error: {}",
                other_error
            ),
            Ok(buffer) => assert!(false, "Expected dead lock, but got buffer {}", buffer),
        }
        bm.unpin(&buffer2);
        bm.pin(&block3).unwrap();
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
