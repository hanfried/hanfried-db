use hanfried_db::db_management_system::hfdb::HanfriedDb;
use hanfried_db::file_management::page::Page;
use hanfried_db::memory_management::log_manager::{LogManager, LogSequenceNumber};
use hanfried_db::utils::logging::init_logging;
use log::info;
use std::num::NonZeroUsize;
use std::ops::DerefMut;

fn create_log_record(s: &str, n: i32) -> Vec<u8> {
    let n_pos = s.len() + 4;
    let mut p = Page::new(NonZeroUsize::new(n_pos + 4).unwrap());
    p.set_string(0, s);
    p.set_i32(n_pos, n);
    p.get_contents().to_vec()
}

fn create_records(log_manager: &mut LogManager, start: i32, end: i32) {
    info!("Creating records start: {}, end: {}", start, end);
    for i in start..end + 1 {
        let log_record = create_log_record(format!("record{}", i).as_str(), i + 100);
        let log_sequence_number = log_manager.append(log_record.as_slice()).unwrap();
        info!("log sequence number: {}", log_sequence_number.latest);
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

    let db_directory = "/data/hanfried-db-test";
    let block_size = 400;
    let log_file = "hfdb.log";

    let hanfried_db = HanfriedDb::new(
        db_directory.to_string(),
        block_size,
        log_file.to_string(),
        8,
        100,
    )
    .unwrap();
    println!("{hanfried_db:?}");

    // let file_manager = hanfried_db.file_manager;
    // let mut lm_binding = hanfried_db.log_manager.borrow_mut();
    // let lm = lm_binding.deref_mut();
    let mut lm_binding = hanfried_db.log_manager.lock().unwrap();
    let lm = lm_binding.deref_mut();

    create_records(lm, 1, 35);
    println!("{lm:?}");

    print_log_records(lm, "The log file now has these records: ");
    create_records(lm, 36, 70);

    lm.flush(LogSequenceNumber::from(65)).unwrap();
    print_log_records(
        lm,
        "The log file has now these records after flushing to 65.",
    );
}
