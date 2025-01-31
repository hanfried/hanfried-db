use hanfried_db::db_management_system::hfdb::HanfriedDb;
use hanfried_db::file_management::block_id::BlockId;
use hanfried_db::memory_management::buffer::TransactionNumber;
use hanfried_db::memory_management::log_manager::LogSequenceNumber;
use hanfried_db::utils::logging::init_logging;
use log::{debug, info};
use std::ops::DerefMut;

fn main() {
    init_logging();

    let db_directory = "/data/hanfried-db-test";
    let block_size = 400;
    let pool_size = 3;
    let log_file = "hfdb.log";

    let hanfried_db = HanfriedDb::new(db_directory, block_size, log_file, pool_size).unwrap();
    info!("HanfriedDB {hanfried_db:?}");

    let buffer_manager = hanfried_db.buffer_manager;
    let mut bm_binding = buffer_manager.borrow_mut();
    let bm = bm_binding.deref_mut();
    debug!("buffer_manager {:?}", buffer_manager);
    // debug!("buffer_manager {:?}", bm);
    //
    {
        let buffer1_pin = bm.pin(BlockId::new("testfile", 1));
        let mut buffer1_binding = buffer1_pin.unwrap();
        let page = buffer1_binding.contents_mut();
        let n = page.get_i32(80);
        page.set_i32(80, n + 1);
        buffer1_binding.set_modified(TransactionNumber::from(1), Some(LogSequenceNumber::from(0)));
        info!("Changed value of page from {} to {}", n, n + 1);
        buffer1_binding.unpin();
    }

    {
        let _buffer2_pin = bm.pin(BlockId::new("testfile", 2));
    }
    {
        let _buffer3_pin = bm.pin(BlockId::new("testfile", 3));
    }
    {
        let _buffer4_pin = bm.pin(BlockId::new("testfile", 4));
    }

    // buffer2_pin.unwrap().unpin();
    // let buffer1_pin = bm.pin(BlockId::new("testfile", 1));
    // let mut buffer1_binding = buffer1_pin.unwrap();
    // let page = buffer1_binding.contents_mut();
    // info!("Rereading set integer {:?}", page.get_i32(80));
    // page.set_i32(80, 9999);
    // buffer1_binding.set_modified(TransactionNumber::from(1), Some(LogSequenceNumber::from(0)));
    // buffer1_binding.unpin();
    //
    // let file_manager = hanfried_db.file_manager;
    // let mut fm_binding = file_manager.borrow_mut();
    // let fm = fm_binding.deref_mut();
    //
    // let block1 = BlockId::new("testfile", 1);
    // let mut page = Page::new(block_size);
    // fm.read(&block1, &mut page);
    // info!("Rereading set integer from unpinned, not yet flushed page {:?}", page.get_i32(80));

    // let file_manager = hanfried_db.file_manager;
    // let mut lm_binding = hanfried_db.log_manager.borrow_mut();
    // let lm = lm_binding.deref_mut();
    //
    // create_records(lm, 1, 35);
    // println!("{lm:?}");
    //
    // print_log_records(lm, "The log file now has these records: ");
    // create_records(lm, 36, 70);
    //
    // lm.flush(65).unwrap();
    // print_log_records(
    //     lm,
    //     "The log file has now these records after flushing to 65.",
    // );
}
