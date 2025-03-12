use crate::file_management::block_id::BlockId;
use crate::file_management::file_manager::{FileManager, IoError};
use crate::memory_management::buffer::{Buffer, TransactionNumber};
use crate::memory_management::buffer_manager::BufferManagerError::{DeadLockTimeout, NoCapacity};
use crate::memory_management::log_manager::LogManager;
use log::{debug, warn};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

#[derive(Debug)]
pub struct BufferManager {
    pool: Vec<RwLock<Buffer>>,
    num_available: Mutex<usize>,
    buffer_available: Condvar,
    deadlock_waiting_duration: Duration,
}

#[allow(elided_named_lifetimes)]
impl BufferManager {
    pub fn new(
        file_manager: Arc<FileManager>,
        log_manager: Arc<Mutex<LogManager>>,
        pool_size: usize,
        deadlock_waiting_duration: Duration,
    ) -> BufferManager {
        let mut pool: Vec<RwLock<Buffer>> = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            pool.push(RwLock::new(Buffer::new(
                file_manager.clone(),
                log_manager.clone(),
            )))
        }
        BufferManager {
            pool,
            num_available: Mutex::new(pool_size),
            buffer_available: Condvar::new(),
            deadlock_waiting_duration,
        }
    }

    pub fn available(&self) -> usize {
        *self.num_available.lock().unwrap()
    }

    pub fn flush_all(&mut self, transaction_number: TransactionNumber) -> Result<(), IoError> {
        debug!("BufferManager: Flush all for {:?}", transaction_number);
        for buffer in self.pool.iter() {
            // Todo: Locking just to see of a transaction number is set seems overkill
            // let mut buffer = buffer.lock().unwrap();
            if buffer.read().unwrap().modifying_transaction_number() == Some(transaction_number) {
                buffer.write().unwrap().flush()?;
            }
        }
        Ok(())
    }

    pub fn unpin(&mut self, buffer: &mut Buffer) {
        buffer.unpin();
        debug!("BufferManager: Unpin buffer for block {:?}", buffer.block());
        if !buffer.is_pinned() {
            *(self.num_available.lock().unwrap()) += 1;
            debug!("BufferManager: Unpinned globally buffer for block {:?} -> notify all waiting threads", buffer.block());
            self.buffer_available.notify_all();
        }
    }

    pub fn pin(&self, block: BlockId) -> Result<&RwLock<Buffer>, BufferManagerError> {
        debug!("Called Buffermanager pin: {:?}", block);
        loop {
            let buffer_guard = self.try_to_pin(block.clone());
            match buffer_guard {
                Ok(buffer) => {
                    debug!("BufferManager: Pin buffer block {:?}", block);
                    return Ok(buffer);
                }
                Err(BufferManagerError::StdIoError(io_error)) => {
                    warn!(
                        "BufferManager: Pinning failed for buffer block {:?}: {:?}",
                        block, io_error
                    );
                    return Err(BufferManagerError::StdIoError(io_error));
                }
                _ => {}
            }
            debug!(
                "BufferManager: NoCapacity wait (trying to pin buffer for block {:?}) ...",
                block
            );
            let result = self.buffer_available.wait_timeout(
                self.num_available.lock().unwrap(),
                self.deadlock_waiting_duration,
            );
            if result.is_err() {
                warn!(
                    "BufferManager: Deadlock Timout trying to pin buffer for block {:?}: {:?}",
                    block,
                    result.unwrap_err()
                );
                return Err(DeadLockTimeout);
            }
        }
    }

    fn find_existing_buffer(
        &self,
        block: BlockId,
        // ) -> Option<MutexGuard<Buffer<'managers, 'blocks>>> {
    ) -> Option<&RwLock<Buffer>> {
        // Todo: Looping through whole list with locking seems overkill (map datastructure or something like that)
        // and not sure whether data might be changeable after finding and unlocking it again
        debug!("Find existing buffer for block {:?}", block);
        self.pool
            .iter()
            .find(|b| b.read().unwrap().block() == Some(block.clone()))
    }

    fn choose_unpinned_buffer(&self) -> Option<&RwLock<Buffer>> {
        // -> Option<MutexGuard<Buffer<'managers, 'blocks>>> {
        debug!("Choose an unpinned buffer");
        // Todo: Looping through whole list with locking seems overkill
        self.pool.iter().find(|b| b.read().unwrap().is_not_pinned())
        // .map(|b| b.lock().unwrap())
    }

    fn try_to_pin(
        &self,
        block: BlockId,
        // ) -> Result<MutexGuard<Buffer<'managers, 'blocks>>, BufferManagerError> {
    ) -> Result<&RwLock<Buffer>, BufferManagerError> {
        debug!("BufferManager: Trying to pin block {:?}", block);
        let buffer = self
            .find_existing_buffer(block.clone())
            .or_else(|| self.choose_unpinned_buffer());

        if buffer.is_none() {
            return Err(NoCapacity);
        }
        let found_buffer = buffer.unwrap();
        let mut found_buffer_mut = found_buffer.write().unwrap();
        if let Err(io_error) = found_buffer_mut.assign_to_block(block) {
            return Err(BufferManagerError::StdIoError(io_error));
        }
        found_buffer_mut.pin();
        Ok(found_buffer)
        // let buffer_guard = self
        //     .find_existing_buffer(block)
        //     .or_else(|| self.choose_unpinned_buffer());
        // debug!(
        //     "BufferManager trying to pin: buffer_guard: {:?}",
        //     buffer_guard
        // );
        // if buffer_guard.is_none() {
        //     return Err(NoCapacity);
        // }
        //
        // let mut buffer = buffer_guard.unwrap();
        // if let Err(io_error) = buffer.assign_to_block(block) {
        //     return Err(BufferManagerError::StdIoError(io_error));
        // }
        //
        // if buffer.is_pinned() {
        //     *(self.num_available.lock().unwrap()) -= 1;
        // }
        // buffer.pin();
        // Ok(buffer)
    }
}

#[derive(Debug)]
pub enum BufferManagerError {
    StdIoError(IoError),
    NoCapacity,
    DeadLockTimeout,
}
