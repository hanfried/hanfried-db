use crate::file_management::block_id::BlockId;
use crate::file_management::page::Page;
use crate::utils::sync_resource_cache::SyncResourceCache;
use log::info;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct FileManager<'a> {
    db_directory: &'a str,
    pub block_size: NonZeroUsize,
    file_cache: Arc<SyncResourceCache<String, Arc<Mutex<File>>>>,
}

pub struct FileManagerBuilder<'a> {
    db_directory: &'a str,
    block_size: NonZeroUsize,
    max_open_files: NonZeroUsize,
}

impl<'a> FileManagerBuilder<'a> {
    pub fn new(db_directory: &str) -> FileManagerBuilder {
        FileManagerBuilder {
            db_directory,
            block_size: NonZeroUsize::new(4096).unwrap(),
            max_open_files: NonZeroUsize::new(512).unwrap(),
        }
    }

    pub fn unittest() -> Self {
        Self::new("/data/hanfried-db-unittest")
    }

    pub fn block_size(mut self, block_size: NonZeroUsize) -> Self {
        self.block_size = block_size;
        self
    }

    pub fn max_open_files(mut self, max_open_files: NonZeroUsize) -> Self {
        self.max_open_files = max_open_files;
        self
    }

    pub fn build(self) -> Result<FileManager<'a>, std::io::Error> {
        FileManager::new(self.db_directory, self.block_size, self.max_open_files)
    }
}

impl<'a> FileManager<'a> {
    pub fn new(
        db_directory: &'a str,
        block_size: NonZeroUsize,
        max_size: NonZeroUsize,
    ) -> Result<FileManager<'a>, std::io::Error> {
        let db_root: &Path = Path::new(db_directory);
        if !db_root.exists() {
            info!("Create db root: {:?}", db_root);
            fs::create_dir(db_root)?;
        }

        let temp_files: Vec<PathBuf> = fs::read_dir(db_root)?
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
                fs::remove_file(t)?;
            }
        }

        Ok(FileManager {
            db_directory,
            block_size,
            file_cache: Arc::new(SyncResourceCache::new(usize::from(max_size))),
        })
    }

    fn get_file(&self, filename: &str) -> Result<Arc<Mutex<File>>, std::io::Error> {
        self.file_cache.get_or_create(filename.to_string(), || {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(Path::new(self.db_directory).join(filename))?;
            Ok(Arc::new(Mutex::new(f)))
        })
    }

    pub fn open_files_count(&self) -> usize {
        self.file_cache.len_open()
    }

    pub fn read(&self, block: &BlockId, page: &mut Page) -> Result<(), std::io::Error> {
        let file_binding = self.get_file(block.filename)?;
        let mut file = file_binding.lock().unwrap();
        let block_size = self.block_size;
        let seek_from =
            std::io::SeekFrom::Start((block.block_number * usize::from(block_size)) as u64);
        file.seek(seek_from)?;
        let mut buf: Vec<u8> = vec![0; usize::from(block_size)];
        let _bytes_read = file.read(&mut buf);
        page.set_contents(buf.as_slice());
        Ok(())
    }

    pub fn write(&self, block: &BlockId, page: &Page) -> Result<(), std::io::Error> {
        let file_binding = self.get_file(block.filename)?;
        let mut file = file_binding.lock().unwrap();
        // println!("Locked file {:?} {:?}", block, file);
        let seek_from =
            std::io::SeekFrom::Start((block.block_number * usize::from(self.block_size)) as u64);
        file.seek(seek_from)?;
        file.write_all(page.get_contents())?;
        file.flush()?;
        Ok(())
    }

    pub fn block_length(&self, filename: &str) -> Result<usize, std::io::Error> {
        // let mut file = self.get_file(filename)?;
        let file_binding = self.get_file(filename).unwrap();
        let mut file = file_binding.lock().unwrap();
        self._block_length(&mut file)
    }

    pub fn _block_length(&self, file: &mut File) -> Result<usize, std::io::Error> {
        let eof_offset = file.seek(std::io::SeekFrom::End(0))?;
        Ok(eof_offset as usize / self.block_size)
    }

    pub fn append(&self, filename: &'a str) -> Result<BlockId<'a>, std::io::Error> {
        let file_binding = self.get_file(filename).unwrap();
        let mut file = file_binding.lock().unwrap();
        let block = BlockId::new(filename, self._block_length(&mut file)?);
        let seek_from =
            std::io::SeekFrom::Start((block.block_number * usize::from(self.block_size)) as u64);
        file.seek(seek_from)?;
        Ok(block)
    }
}

#[cfg(test)]
mod tests {
    use crate::file_management::block_id::BlockId;
    use crate::file_management::file_manager::{FileManager, FileManagerBuilder};
    use crate::file_management::page::Page;
    use std::num::NonZeroUsize;
    use std::thread;
    use std::thread::JoinHandle;

    const TEST_FILES_MAX: usize = 2000;
    const TEST_FILES_CACHE: NonZeroUsize = NonZeroUsize::new(500).unwrap();
    const TEST_FILES_BLOCKS: usize = 10;
    const TEST_FILES_BLOCKSIZE: NonZeroUsize = NonZeroUsize::new(4096).unwrap();
    #[test]
    fn test_file_manager() {
        let file_manager: FileManager = FileManagerBuilder::unittest()
            .block_size(TEST_FILES_BLOCKSIZE)
            .max_open_files(TEST_FILES_CACHE)
            .build()
            .unwrap();
        let mut parallel_write_threads: Vec<JoinHandle<()>> = Vec::new();
        for file_nr in 0..TEST_FILES_MAX {
            for block_nr in 0..TEST_FILES_BLOCKS {
                let fname = format!("testfile_{}", file_nr);
                let fm = file_manager.clone();
                parallel_write_threads.push(thread::spawn(move || {
                    let block = BlockId::new(fname.as_str(), block_nr);
                    let mut page = Page::new(TEST_FILES_BLOCKSIZE);
                    page.set_i32(0, file_nr as i32);
                    page.set_i32(4, block_nr as i32);
                    fm.write(&block, &page).unwrap();
                }))
            }
        }
        let mut parallel_read_threads: Vec<JoinHandle<()>> = Vec::new();
        for file_nr in 0..TEST_FILES_MAX {
            for block_nr in 0..TEST_FILES_BLOCKS {
                let fname = format!("testfile_{}", file_nr);
                let fm = file_manager.clone();
                parallel_read_threads.push(thread::spawn(move || {
                    let block = BlockId::new(fname.as_str(), block_nr);
                    let mut page = Page::new(TEST_FILES_BLOCKSIZE);
                    loop {
                        fm.read(&block, &mut page).unwrap();
                        let file_nr_got = page.get_i32(0);
                        let block_nr_got = page.get_i32(4);
                        if file_nr_got == file_nr as i32 && block_nr_got == block_nr as i32 {
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
}
