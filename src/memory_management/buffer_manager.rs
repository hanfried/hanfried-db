use crate::file_management::block_id::BlockId;
use crate::file_management::file_manager::FileManager;
use crate::memory_management::buffer::Buffer;
use crate::memory_management::buffer_manager::BufferManagerError::{DeadLockTimeout, NoCapacity};
use crate::memory_management::log_manager::LogManager;
use log::debug;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Duration;

#[derive(Debug)]
pub struct BufferManager<'a> {
    pool: Vec<Mutex<Buffer<'a>>>,
    num_available: Mutex<usize>,
    buffer_available: Condvar,
    deadlock_waiting_duration: Duration,
}

#[allow(elided_named_lifetimes)]
impl<'a> BufferManager<'a> {
    pub fn new(
        file_manager: Rc<RefCell<FileManager<'a>>>,
        log_manager: Rc<RefCell<LogManager<'a>>>,
        pool_size: usize,
        deadlock_waiting_duration: Duration,
    ) -> BufferManager<'a> {
        let mut pool = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            pool.push(Mutex::new(Buffer::new(
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

    pub fn flush_all(&mut self, transaction_number: NonZeroUsize) -> Result<(), std::io::Error> {
        for buffer in self.pool.iter() {
            // Todo: Locking just to see of a transaction number is set seems overkill
            let mut buffer = buffer.lock().unwrap();
            if buffer.modifying_transaction_number() == Some(transaction_number) {
                buffer.flush()?;
            }
        }
        Ok(())
    }

    pub fn unpin(&mut self, buffer: &mut Buffer) {
        buffer.unpin();
        if !buffer.is_pinned() {
            *(self.num_available.lock().unwrap()) += 1;
            // self.num_available.replace_with(|n| *n+1);
            debug!("Unpin buffer and notify all waiting threads");
            self.buffer_available.notify_all();
        }
    }

    pub fn pin(
        &'a mut self,
        block: &'a BlockId,
    ) -> Result<MutexGuard<Buffer<'a>>, BufferManagerError> {
        loop {
            let buffer_guard: Result<MutexGuard<Buffer<'a>>, BufferManagerError> =
                self.try_to_pin(block);
            match buffer_guard {
                Ok(buffer) => return Ok(buffer),
                Err(BufferManagerError::StdIoError(io_error)) => {
                    return Err(BufferManagerError::StdIoError(io_error))
                }
                _ => {}
            }
            debug!("NoCapacity wait ...");
            let result = self.buffer_available.wait_timeout(
                self.num_available.lock().unwrap(),
                self.deadlock_waiting_duration,
            );
            if result.is_err() {
                return Err(DeadLockTimeout);
            }
        }
    }

    fn find_existing_buffer(&self, block: &'a BlockId) -> Option<MutexGuard<Buffer<'a>>> {
        // Todo: Looping through whole list with locking seems overkill (map datastructure or something like that)
        // and not sure whether data might be changeable after finding and unlocking it again
        self.pool
            .iter()
            .find(|b| b.lock().unwrap().block() == Some(block))
            .map(|b| b.lock().unwrap())
    }

    fn choose_unpinned_buffer(&self) -> Option<MutexGuard<Buffer<'a>>> {
        // Todo: Looping through whole list with locking seems overkill
        self.pool
            .iter()
            .find(|b| !b.lock().unwrap().is_pinned())
            .map(|b| b.lock().unwrap())
    }

    fn try_to_pin(
        &'a self,
        block: &'a BlockId,
    ) -> Result<MutexGuard<Buffer<'a>>, BufferManagerError> {
        let buffer_guard = self
            .find_existing_buffer(block)
            .or_else(|| self.choose_unpinned_buffer());
        if buffer_guard.is_none() {
            return Err(NoCapacity);
        }

        let mut buffer = buffer_guard.unwrap();
        if let Err(io_error) = buffer.assign_to_block(block) {
            return Err(BufferManagerError::StdIoError(io_error));
        }

        if buffer.is_pinned() {
            *(self.num_available.lock().unwrap()) -= 1;
        }
        buffer.pin();
        Ok(buffer)
    }
}

pub enum BufferManagerError {
    StdIoError(std::io::Error),
    NoCapacity,
    DeadLockTimeout,
}
