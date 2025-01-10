use crate::file_management::block_id::BlockId;
use crate::file_management::page::Page;
use log::info;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct FileManager {
    db_directory: String,
    block_size: usize,
    open_files: HashMap<String, File>,
}

impl FileManager {
    pub fn new(db_directory: String, block_size: usize) -> Self {
        let db_root: &Path = Path::new(db_directory.as_str());
        if !db_root.exists() {
            info!("Create db root: {:?}", db_root);
            fs::create_dir(db_root).unwrap();
        }

        let temp_files: Vec<PathBuf> = fs::read_dir(db_root)
            .unwrap()
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap().path())
            .filter(|p| p.starts_with("temp"))
            .collect();
        if !temp_files.is_empty() {
            info!("Delete temp files: {:?}", temp_files);
            temp_files.iter().for_each(|p| fs::remove_file(p).unwrap());
        }

        FileManager {
            db_directory,
            block_size,
            open_files: HashMap::new(),
        }
    }

    pub fn get_file(&mut self, filename: &str) -> &File {
        if !self.open_files.contains_key(filename) {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(Path::new(self.db_directory.as_str()).join(filename))
                .unwrap();
            self.open_files.insert(filename.to_string(), f);
        }
        self.open_files.get(filename).unwrap()
    }

    // TODO: Synchronize
    pub fn read(&mut self, block: &BlockId, page: &mut Page) {
        let block_size = self.block_size;
        let seek_from = std::io::SeekFrom::Start((block.block_number * block_size) as u64);
        let mut file = self.get_file(block.filename);
        file.seek(seek_from).unwrap();
        let mut buf: Vec<u8> = vec![0; block_size];
        file.read_exact(buf.as_mut_slice()).unwrap();
        page.set_contents(buf.as_slice());
    }

    // TODO: Synchronize
    pub fn write(&mut self, block: &BlockId, page: Page) {
        let seek_from = std::io::SeekFrom::Start((block.block_number * self.block_size) as u64);
        let mut file = self.get_file(block.filename);
        file.seek(seek_from).unwrap();
        file.write_all(page.get_contents()).unwrap();
    }

    fn block_length(&mut self, filename: &str) -> usize {
        let mut file = self.get_file(filename);
        let eof_offset = file.seek(std::io::SeekFrom::End(0)).unwrap();
        eof_offset as usize / self.block_size
    }

    // TODO: Synchronize
    pub fn append<'a>(&mut self, filename: &'a str) -> BlockId<'a> {
        let block = BlockId {
            filename,
            block_number: self.block_length(filename),
        };
        let seek_from = std::io::SeekFrom::Start((block.block_number * self.block_size) as u64);
        let mut file = self.get_file(filename);
        file.seek(seek_from).unwrap();
        block
    }
}
