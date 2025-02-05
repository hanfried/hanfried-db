use crate::file_management::block_id::BlockId;
use crate::file_management::page::Page;
use log::info;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct FileManager<'a> {
    db_directory: &'a str,
    pub block_size: usize,
    open_files: HashMap<String, File>,
}

impl<'a> FileManager<'a> {
    pub fn new(
        db_directory: &'a str,
        block_size: usize,
    ) -> Result<FileManager<'a>, std::io::Error> {
        let db_root: &Path = Path::new(db_directory);
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

    pub fn get_file(&mut self, filename: &str) -> Result<&File, std::io::Error> {
        if !self.open_files.contains_key(filename) {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(Path::new(self.db_directory).join(filename))?;
            self.open_files.insert(filename.to_string(), f);
        }
        Ok(self.open_files.get(filename).unwrap())
    }

    // TODO: Synchronize
    pub fn read(&mut self, block: &BlockId, page: &mut Page) -> Result<(), std::io::Error> {
        let block_size = self.block_size;
        let seek_from = std::io::SeekFrom::Start((block.block_number * block_size) as u64);
        let mut file = self.get_file(block.filename)?;
        file.seek(seek_from)?;
        let mut buf: Vec<u8> = vec![0; block_size];
        let _bytes_read = file.read(&mut buf);
        page.set_contents(buf.as_slice());
        Ok(())
    }

    // TODO: Synchronize
    pub fn write(&mut self, block: &BlockId, page: &Page) -> Result<(), std::io::Error> {
        let seek_from = std::io::SeekFrom::Start((block.block_number * self.block_size) as u64);
        let mut file = self.get_file(block.filename)?;
        file.seek(seek_from)?;
        file.write_all(page.get_contents())?;
        Ok(())
    }

    pub fn block_length(&mut self, filename: &str) -> Result<usize, std::io::Error> {
        let mut file = self.get_file(filename)?;
        let eof_offset = file.seek(std::io::SeekFrom::End(0))?;
        Ok(eof_offset as usize / self.block_size)
    }

    // TODO: Synchronize
    pub fn append(&mut self, filename: &'a str) -> Result<BlockId<'a>, std::io::Error> {
        let block = BlockId::new(filename, self.block_length(filename)?);
        let seek_from = std::io::SeekFrom::Start((block.block_number * self.block_size) as u64);
        let mut file = self.get_file(filename)?;
        file.seek(seek_from)?;
        Ok(block)
    }
}
