use crate::file_management::block_id::{BlockId, DbFilename};
use crate::file_management::page::Page;
use crate::utils::sync_resource_cache::SyncResourceCache;
use log::info;
use std::fmt::{Display, Formatter};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct FileManager {
    db_directory: String,
    pub block_size: NonZeroUsize,
    file_cache: Arc<SyncResourceCache<String, Arc<Mutex<File>>>>,
}

pub struct FileManagerBuilder {
    db_directory: String,
    block_size: NonZeroUsize,
    max_open_files: NonZeroUsize,
}

impl FileManagerBuilder {
    const DEFAULT_BLOCK_SIZE: NonZeroUsize = NonZeroUsize::new(4096).unwrap();
    const DEFAULT_MAX_OPEN_FILES: NonZeroUsize = NonZeroUsize::new(512).unwrap();

    const UNITTEST_DB_DIR: &'static str = "/data/hanfried-db-unittest";

    pub fn new(db_directory: String) -> Self {
        FileManagerBuilder {
            db_directory,
            block_size: Self::DEFAULT_BLOCK_SIZE,
            max_open_files: Self::DEFAULT_MAX_OPEN_FILES,
        }
    }

    pub fn unittest(db_sub_directory: &str) -> Self {
        let db_directory = Self::UNITTEST_DB_DIR;
        Self::new(format!("{db_directory}/{db_sub_directory}"))
    }

    pub fn block_size(mut self, block_size: NonZeroUsize) -> Self {
        self.block_size = block_size;
        self
    }

    pub fn max_open_files(mut self, max_open_files: NonZeroUsize) -> Self {
        self.max_open_files = max_open_files;
        self
    }

    pub fn build(self) -> Result<FileManager, IoError> {
        FileManager::new(self.db_directory, self.block_size, self.max_open_files)
    }
}

#[derive(Debug)]
pub struct IoError {
    error: std::io::Error,
    context: String,
}

impl Display for IoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} context: {}", self.error, self.context)
    }
}

impl FileManager {
    pub fn new(
        db_directory: String,
        block_size: NonZeroUsize,
        max_size: NonZeroUsize,
    ) -> Result<FileManager, IoError> {
        let db_root: &Path = Path::new(db_directory.as_str());
        if !db_root.exists() {
            info!("Create db root: {:?}", db_root);
            fs::create_dir_all(db_root).map_err(|error| IoError {
                error,
                context: format!("create db root {db_root:?}"),
            })?;
        }

        let temp_files: Vec<PathBuf> = fs::read_dir(db_root)
            .map_err(|error| IoError {
                error,
                context: format!("read_dir db root {db_root:?}"),
            })?
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap().path())
            .filter(|p| {
                p.as_path()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("temp")
                    || p.as_path()
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .starts_with("test")
            })
            .collect();
        if !temp_files.is_empty() {
            for t in temp_files {
                match fs::remove_file(t.clone()) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Failed to remove temp file {:?}: {:?}", t, e);
                    }
                };
            }
        }

        Ok(FileManager {
            db_directory,
            block_size,
            file_cache: Arc::new(SyncResourceCache::new(usize::from(max_size))),
        })
    }

    fn get_file(&self, filename: &DbFilename) -> Result<Arc<Mutex<File>>, IoError> {
        self.file_cache.get_or_create(filename.to_string(), || {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(Path::new(self.db_directory.as_str()).join(filename.as_str()))
                .map_err(|error| IoError {
                    error,
                    context: format!("get_file open file {}", filename),
                })?;
            Ok(Arc::new(Mutex::new(f)))
        })
    }

    pub fn open_files_count(&self) -> usize {
        self.file_cache.len_open()
    }

    pub fn read(&self, block: &BlockId, page: &Page) -> Result<(), IoError> {
        let file_binding = self.get_file(block.filename())?;
        let mut file = file_binding.lock().unwrap();
        let block_size = self.block_size;
        let seek_from =
            std::io::SeekFrom::Start((block.block_number() * usize::from(block_size)) as u64);
        file.seek(seek_from).map_err(|error| IoError {
            error,
            context: format!("read seek file block {:?}", block),
        })?;
        let mut buf: Vec<u8> = vec![0; usize::from(block_size)];
        let _bytes_read = file.read(&mut buf);
        page.set_contents(buf.as_slice());
        Ok(())
    }

    pub fn write(&self, block: &BlockId, page: &Page) -> Result<(), IoError> {
        let file_binding = self.get_file(block.filename())?;
        let mut file = file_binding.lock().unwrap();
        // println!("Locked file {:?} {:?}", block, file);
        let seek_from =
            std::io::SeekFrom::Start((block.block_number() * usize::from(self.block_size)) as u64);
        file.seek(seek_from).map_err(|error| IoError {
            error,
            context: format!("write seek file block {:?}", block),
        })?;
        file.write_all(page.get_contents().as_slice())
            .map_err(|error| IoError {
                error,
                context: format!("write write_all page contents file block {:?}", block),
            })?;
        file.flush().map_err(|error| IoError {
            error,
            context: format!("write flush file block {:?}", block),
        })?;
        Ok(())
    }

    pub fn block_length(&self, filename: &DbFilename) -> Result<usize, IoError> {
        // let mut file = self.get_file(filename)?;
        let file_binding = self.get_file(filename).unwrap();
        let mut file = file_binding.lock().unwrap();
        self._block_length(&mut file).map_err(|error| IoError {
            error,
            context: format!("block length {}", filename),
        })
    }

    pub fn _block_length(&self, file: &mut File) -> Result<usize, std::io::Error> {
        let eof_offset = file.seek(std::io::SeekFrom::End(0))?;
        Ok(eof_offset as usize / self.block_size)
    }

    pub fn append(&self, filename: &DbFilename) -> Result<BlockId, IoError> {
        let file_binding = self.get_file(filename).unwrap();
        let mut file = file_binding.lock().unwrap();
        let block = BlockId::new(
            filename.clone(),
            self._block_length(&mut file).map_err(|error| IoError {
                error,
                context: format!("append block length filename {}", filename),
            })?,
        );
        let seek_from =
            std::io::SeekFrom::Start((block.block_number() * usize::from(self.block_size)) as u64);
        file.seek(seek_from).map_err(|error| IoError {
            error,
            context: format!("block length {}", filename),
        })?;
        Ok(block)
    }
}

#[cfg(test)]
mod tests {
    use crate::datatypes::varcount::Varcount;
    use crate::datatypes::varpair::Varpair;
    use crate::file_management::block_id::{BlockId, DbFilename};
    use crate::file_management::file_manager::FileManagerBuilder;
    use crate::file_management::page::Page;
    use std::num::NonZeroUsize;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::thread;
    use std::thread::JoinHandle;

    const TEST_FILES_MAX: usize = 2000;
    const TEST_FILES_CACHE: NonZeroUsize = NonZeroUsize::new(500).unwrap();
    const TEST_FILES_BLOCKS: usize = 10;
    const TEST_FILES_BLOCKSIZE: NonZeroUsize = NonZeroUsize::new(4096).unwrap();
    #[test]
    fn test_file_manager_writing_and_then_reading() {
        let file_manager = Arc::new(
            FileManagerBuilder::unittest("file_manager_writing_and_then_reading")
                .block_size(TEST_FILES_BLOCKSIZE)
                .max_open_files(TEST_FILES_CACHE)
                .build()
                .unwrap(),
        );
        let mut parallel_write_threads: Vec<JoinHandle<()>> = Vec::new();
        for file_nr in 0..TEST_FILES_MAX {
            for block_nr in 0..TEST_FILES_BLOCKS {
                let fname = DbFilename::from(format!("testfile_{}", file_nr));
                let fm = file_manager.clone();
                parallel_write_threads.push(thread::spawn(move || {
                    let block = BlockId::new(fname, block_nr);
                    let page = Page::new(TEST_FILES_BLOCKSIZE);
                    page.set(
                        0,
                        &Varpair::from((Varcount::from(file_nr), Varcount::from(block_nr))),
                    );
                    // page.set_i32(0, file_nr as i32);
                    // page.set_i32(4, block_nr as i32);
                    fm.write(&block, &page).unwrap();
                }))
            }
        }
        let mut parallel_read_threads: Vec<JoinHandle<()>> = Vec::new();
        for file_nr in 0..TEST_FILES_MAX {
            for block_nr in 0..TEST_FILES_BLOCKS {
                let fname = DbFilename::from(format!("testfile_{}", file_nr));
                let fm = file_manager.clone();
                parallel_read_threads.push(thread::spawn(move || {
                    let block = BlockId::new(fname, block_nr);
                    let mut page = Page::new(TEST_FILES_BLOCKSIZE);
                    loop {
                        fm.read(&block, &mut page).unwrap();
                        let (&file_nr_got, &block_nr_got) =
                            page.get::<Varpair<Varcount, Varcount>>(0).as_tuple();
                        // let file_nr_got = page.get_i32(0);
                        // let block_nr_got = page.get_i32(4);
                        if usize::from(&file_nr_got) == file_nr
                            && usize::from(&block_nr_got) == block_nr
                        {
                            break;
                        } else {
                            thread::sleep(std::time::Duration::from_millis(20));
                        }
                    }
                }));
            }
        }
        for p in parallel_write_threads {
            p.join().unwrap()
        }
        for p in parallel_read_threads {
            p.join().unwrap()
        }
        assert_eq!(
            usize::from(TEST_FILES_CACHE),
            file_manager.open_files_count()
        );
    }

    const TEST_FILES_SOME: usize = 100;
    const PARALLEL_READS_THREADS: usize = 100;
    #[test]
    fn test_file_manager_not_blocking_writes() {
        let file_manager = Arc::new(
            FileManagerBuilder::unittest("file_manager_not_blocking_writes")
                .build()
                .unwrap(),
        );

        let testing_finished = Arc::new(AtomicBool::new(false));

        let mut parallel_read_threads_some_files: Vec<JoinHandle<()>> = Vec::new();
        for thread_nr in 0..PARALLEL_READS_THREADS {
            let fm = file_manager.clone();
            let testing_finished = testing_finished.clone();
            parallel_read_threads_some_files.push(thread::spawn(move || {
                println!(
                    "parallel read thread {} started with eternal loop",
                    thread_nr
                );
                loop {
                    let mut page = Page::new(TEST_FILES_BLOCKSIZE);
                    for file_nr in 0..TEST_FILES_SOME {
                        let fname = DbFilename::from(format!("testfile_{}", file_nr));
                        let block = BlockId::new(fname, 0);
                        fm.read(&block, &mut page).unwrap();
                    }
                    if testing_finished.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }
            }))
        }

        let fm = file_manager.clone();
        thread::spawn(move || {
            for file_nr in 0..TEST_FILES_MAX {
                let mut page = Page::new(TEST_FILES_BLOCKSIZE);
                page.set(0, &Varcount::from(file_nr));
                let fname = DbFilename::from(format!("testfile_write_{}", file_nr));
                let block = BlockId::new(fname, 0);
                println!("write to file_nr: {}", file_nr);
                fm.write(&block, &mut page).unwrap();
            }
        })
        .join()
        .unwrap();

        let fm = file_manager.clone();
        for file_nr in 0..TEST_FILES_MAX {
            let mut page = Page::new(TEST_FILES_BLOCKSIZE);
            let fname = DbFilename::from(format!("testfile_write_{}", file_nr));
            let block = BlockId::new(fname, 0);
            fm.read(&block, &mut page).unwrap();
            let file_nr_got = page.get::<Varcount>(0);
            assert_eq!(usize::from(&file_nr_got), file_nr);
        }

        testing_finished.store(true, std::sync::atomic::Ordering::Relaxed);

        let mut thread_nr = 0;
        for t in parallel_read_threads_some_files {
            println!("Stop read thread {thread_nr:?}");
            t.join().unwrap();
            thread_nr += 1;
        }
    }
}
