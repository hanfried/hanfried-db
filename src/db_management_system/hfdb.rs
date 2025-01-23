use crate::file_management::file_manager::FileManager;
use crate::memory_management::buffer_manager::BufferManager;
use crate::memory_management::log_manager::LogManager;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

#[derive(Debug)]
pub struct HanfriedDb<'a> {
    pub file_manager: Rc<RefCell<FileManager<'a>>>,
    pub log_manager: Rc<RefCell<LogManager<'a>>>,
    pub buffer_manager: RefCell<BufferManager<'a>>,
}

impl<'a> HanfriedDb<'a> {
    pub fn new(
        db_directory: &'a str,
        block_size: usize,
        log_file: &'a str,
        pool_size: usize,
    ) -> Result<Self, std::io::Error> {
        let fm = Rc::new(RefCell::new(FileManager::new(db_directory, block_size)?));
        let lm = Rc::new(RefCell::new(LogManager::new(fm.clone(), log_file)?));
        Ok(Self {
            file_manager: fm.clone(),
            log_manager: lm.clone(),
            buffer_manager: RefCell::new(BufferManager::new(
                fm.clone(),
                lm.clone(),
                pool_size,
                Duration::from_secs(10),
            )),
        })
    }
}
