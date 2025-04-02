use hanfried_db::db_management_system::hfdb::HanfriedDb;
use hanfried_db::utils::logging::init_logging;
use log::{debug, info};

fn main() {
    init_logging();

    let db_directory = "/data/hanfried-db-test";
    let block_size = 400;
    let pool_size = 3;
    let log_file = "hfdb.log";

    let hanfried_db = HanfriedDb::new(
        db_directory.to_string(),
        block_size,
        log_file.to_string(),
        pool_size,
        100,
    )
    .unwrap();
    info!("HanfriedDB {hanfried_db:?}");

    let bm = hanfried_db.buffer_manager;
    debug!("buffer_manager {:?}", bm);
    // let fname = DbFilename::from("testfile");
    // let block1 = BlockId::new(fname, 1);
    // // let buffer1_pin = bm.pin(block1.clone());
    // {
    //     let mut buffer1_binding = buffer1_pin.unwrap().write().unwrap();
    //     let page = buffer1_binding.contents_mut();
    //     let n = page.get_i32(80);
    //     page.set_i32(80, n + 1);
    //     buffer1_binding.set_modified(TransactionNumber::from(1), Some(LogSequenceNumber::from(0)));
    //     info!("Changed value of page from {} to {}", n, n + 1);
    //     buffer1_binding.unpin();
    // }
    //
    // let _buffer2_pin = bm.pin(block1.with_other_block_number(2));
    // let _buffer3_pin = bm.pin(block1.with_other_block_number(3));
    // let _buffer4_pin = bm.pin(block1.with_other_block_number(4));
    //
    // // let file_manager = hanfried_db.file_manager;
    // // let fm_binding = file_manager.borrow();
    // // let fm = fm_binding.deref();
    // let fm = hanfried_db.file_manager;
    // let mut page = Page::new(fm.block_size);
    // fm.read(&block1, &mut page).unwrap();
    // info!(
    //     "Reading value from disk of block {:?} => {:?} (should have changed)",
    //     block1,
    //     page.get_i32(80)
    // );

    // bm.unpin(buffer2_pin);
}
