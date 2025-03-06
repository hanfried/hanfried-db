use crate::file_management::file_manager::FileManager;
use crate::memory_management::buffer_manager::BufferManager;
use crate::memory_management::log_manager::LogManager;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::Duration;

#[derive(Debug)]
pub struct HanfriedDb<'managers, 'blocks>
where
    'managers: 'blocks,
{
    pub file_manager: Rc<RefCell<FileManager<'managers>>>,
    pub log_manager: Rc<RefCell<LogManager<'managers>>>,
    pub buffer_manager: Rc<RefCell<BufferManager<'managers, 'blocks>>>,
}

impl<'managers> HanfriedDb<'managers, '_> {
    pub fn new(
        db_directory: &'managers str,
        block_size: usize,
        log_file: &'managers str,
        pool_size: usize,
        max_open_files: usize,
    ) -> Result<Self, std::io::Error> {
        let fm = Rc::new(RefCell::new(FileManager::new(
            db_directory,
            NonZeroUsize::new(block_size).unwrap(),
            NonZeroUsize::new(max_open_files).unwrap(),
        )?));
        let lm = Rc::new(RefCell::new(LogManager::new(fm.clone(), log_file)?));
        let bm = Rc::new(RefCell::new(BufferManager::new(
            fm.clone(),
            lm.clone(),
            pool_size,
            Duration::from_secs(10),
        )));
        Ok(Self {
            file_manager: fm.clone(),
            log_manager: lm.clone(),
            buffer_manager: bm.clone(),
        })
    }
}
