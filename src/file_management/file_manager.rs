use crate::file_management::block_id::BlockId;
use crate::file_management::page::Page;
use log::info;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[derive(Debug)]
pub struct FileManager {
    db_directory: String,
    pub block_size: usize,
    open_files: HashMap<String, File>,
}

impl FileManager {
    #[allow(dead_code)]
    pub fn new(db_directory: String, block_size: usize) -> Result<Self, std::io::Error> {
        let db_root: &Path = Path::new(db_directory.as_str());
        if !db_root.exists() {
            info!("Create db root: {:?}", db_root);
            fs::create_dir(db_root)?;
        }

        let temp_files: Vec<PathBuf> = fs::read_dir(db_root)?
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap().path())
            .filter(|p| p.starts_with("temp"))
            .collect();
        if !temp_files.is_empty() {
            info!("Delete temp files: {:?}", temp_files);
            for t in temp_files {
                fs::remove_file(t)?;
            }
        }

        Ok(FileManager {
            db_directory,
            block_size,
            open_files: HashMap::new(),
        })
    }

    #[allow(dead_code)]
    pub fn get_file(&mut self, filename: &str) -> Result<&File, std::io::Error> {
        if !self.open_files.contains_key(filename) {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(Path::new(self.db_directory.as_str()).join(filename))?;
            self.open_files.insert(filename.to_string(), f);
        }
        Ok(self.open_files.get(filename).unwrap())
    }

    // TODO: Synchronize
    #[allow(dead_code)]
    pub fn read(&mut self, block: &BlockId, page: &mut Page) -> Result<(), std::io::Error> {
        let block_size = self.block_size;
        let seek_from = std::io::SeekFrom::Start((block.block_number * block_size) as u64);
        let mut file = self.get_file(block.filename)?;
        file.seek(seek_from)?;
        let mut buf: Vec<u8> = vec![0; block_size];
        file.read_exact(buf.as_mut_slice())?;
        page.set_contents(buf.as_slice());
        Ok(())
    }

    // TODO: Synchronize
    #[allow(dead_code)]
    pub fn write(&mut self, block: &BlockId, page: &Page) -> Result<(), std::io::Error> {
        let seek_from = std::io::SeekFrom::Start((block.block_number * self.block_size) as u64);
        let mut file = self.get_file(block.filename)?;
        file.seek(seek_from)?;
        file.write_all(page.get_contents())?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn block_length(&mut self, filename: &str) -> Result<usize, std::io::Error> {
        let mut file = self.get_file(filename)?;
        let eof_offset = file.seek(std::io::SeekFrom::End(0))?;
        Ok(eof_offset as usize / self.block_size)
    }

    // TODO: Synchronize
    #[allow(dead_code)]
    pub fn append<'a>(&mut self, filename: &'a str) -> Result<BlockId<'a>, std::io::Error> {
        let block = BlockId::new(filename, self.block_length(filename)?);
        let seek_from = std::io::SeekFrom::Start((block.block_number * self.block_size) as u64);
        let mut file = self.get_file(filename)?;
        file.seek(seek_from)?;
        Ok(block)
    }
}
