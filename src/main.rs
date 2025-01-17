mod file_management;
mod utils;

use crate::file_management::page::Page;
use hanfried_db::file_management::file_manager::FileManager;
use hanfried_db::memory_management::log_manager::LogManager;
use hanfried_db::utils::logging::init_logging;
use log::info;
use std::cell::RefCell;
use std::rc::Rc;

fn create_log_record(s: &str, n: i32) -> Vec<u8> {
    let n_pos = s.len() + 4;
    let mut p = Page::new(n_pos + 4);
    p.set_string(0, s);
    p.set_i32(n_pos, n);
    p.get_contents().to_vec()
}

fn create_records(log_manager: &mut LogManager, start: i32, end: i32) {
    info!("Creating records start: {}, end: {}", start, end);
    for i in start..end + 1 {
        let log_record = create_log_record(format!("record{}", i).as_str(), i + 100);
        let log_sequence_number = log_manager.append(log_record.as_slice()).unwrap();
        info!("log sequence number: {}", log_sequence_number);
    }
    info!("Finished creating records start: {}, end: {}", start, end);
}

fn print_log_records(log_manager: &LogManager, msg: &str) {
    info!("{}", msg);
    let log_iterator = log_manager.iter().unwrap();
    for record in log_iterator {
        let page = Page::from_vec(record.unwrap());
        let s = page.get_string(0);
        let val = page.get_i32(page.max_length(s));
        println!("[{s:?} {val:?}]");
    }
}

fn main() {
    init_logging();

    let block_size = 400;
    let file_manager = Rc::new(RefCell::new(
        FileManager::new("/tmp/test".to_string(), block_size).unwrap(),
    ));
    println!("{file_manager:?}");

    let mut log_manager = LogManager::new(file_manager.clone(), "hfdb.log").unwrap();
    println!("{log_manager:?}");

    create_records(&mut log_manager, 1, 35);
    println!("{log_manager:?}");

    print_log_records(&log_manager, "The log file now has these records: ");
    create_records(&mut log_manager, 36, 70);

    log_manager.flush(65).unwrap();
    print_log_records(
        &log_manager,
        "The log file has now these records after flushing to 65.",
    );

    // let block = BlockId {
    //     filename: "testfile",
    //     block_number: 2,
    // };
    // println!("{block:?}");
    //
    // let mut page1 = Page::new(block_size);
    // let pos_string: usize = 42;
    // let s = "abcdefhgh";
    // page1.set_string(pos_string, s);
    // let pos_int = pos_string + page1.max_length(s.len());
    // page1.set_i32(pos_int, 12345);
    //
    // let s = page1.get_string(pos_string);
    // let i = page1.get_i32(pos_int);
    // println!("{page1:?} {s:?} {i:?}");
    // file_manager.write(&block, &page1).unwrap();
    //
    // let mut page2 = Page::new(block_size);
    // file_manager.read(&block, &mut page2).unwrap();
    // let s = page2.get_string(pos_string);
    // let i = page2.get_i32(pos_int);
    // println!("{page2:?} {s:?} {i:?}");
    //
    // let appended_block: BlockId = file_manager.append("testfile").unwrap();
    // println!("appended_block: {appended_block:?}");
}
