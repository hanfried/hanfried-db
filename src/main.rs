use hanfried_db::db_management_system::hfdb::HanfriedDb;
use hanfried_db::file_management::block_id::BlockId;
use hanfried_db::file_management::page::Page;
use hanfried_db::memory_management::buffer::TransactionNumber;
use hanfried_db::memory_management::log_manager::LogSequenceNumber;
use hanfried_db::utils::logging::init_logging;
use log::{debug, info};
use std::ops::{Deref, DerefMut};

fn main() {
    init_logging();

    let db_directory = "/data/hanfried-db-test";
    let block_size = 400;
    let pool_size = 3;
    let log_file = "hfdb.log";

    let hanfried_db = HanfriedDb::new(db_directory, block_size, log_file, pool_size, 100).unwrap();
    info!("HanfriedDB {hanfried_db:?}");

    let buffer_manager = hanfried_db.buffer_manager;
    let mut bm_binding = buffer_manager.borrow_mut();
    let bm = bm_binding.deref_mut();
    debug!("buffer_manager {:?}", buffer_manager);
    let block1 = BlockId::new("testfile", 1);
    let buffer1_pin = bm.pin(block1);
    {
        let mut buffer1_binding = buffer1_pin.unwrap().write().unwrap();
        let page = buffer1_binding.contents_mut();
        let n = page.get_i32(80);
        page.set_i32(80, n + 1);
        buffer1_binding.set_modified(TransactionNumber::from(1), Some(LogSequenceNumber::from(0)));
        info!("Changed value of page from {} to {}", n, n + 1);
        buffer1_binding.unpin();
    }

    let _buffer2_pin = bm.pin(BlockId::new("testfile", 2));
    let _buffer3_pin = bm.pin(BlockId::new("testfile", 3));
    let _buffer4_pin = bm.pin(BlockId::new("testfile", 4));

    let file_manager = hanfried_db.file_manager;
    let fm_binding = file_manager.borrow();
    let fm = fm_binding.deref();
    let mut page = Page::new(fm.block_size);
    fm.read(&block1, &mut page).unwrap();
    info!(
        "Reading value from disk of block {:?} => {:?} (should have changed)",
        block1,
        page.get_i32(80)
    );

    // bm.unpin(buffer2_pin);
}
