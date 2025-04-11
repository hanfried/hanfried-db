use hanfried_db::datatypes::varchar::Varchar;
use hanfried_db::datatypes::varint::Varint;
use hanfried_db::datatypes::varpair::Varpair;
use hanfried_db::file_management::block_id::{BlockId, DbFilename};
use hanfried_db::file_management::file_manager::FileManager;
use hanfried_db::file_management::page::Page;
use hanfried_db::utils;
use std::num::NonZeroUsize;

fn main() {
    utils::logging::init_logging();

    let block_size = NonZeroUsize::new(4096).unwrap();
    let file_manager = FileManager::new(
        "/tmp/test".to_string(),
        block_size,
        NonZeroUsize::new(100).unwrap(),
    )
    .unwrap();
    println!("{file_manager:?}");

    let fname = DbFilename::from("testfile");

    let block = BlockId::new(fname.clone(), 2);
    println!("{block:?}");

    let page1 = Page::new(block_size);
    let pos_string: usize = 42;
    let s = "abcdefhgh";
    // page1.set_string(pos_string, s);
    // let pos_int = pos_string + page1.max_length(s);
    // page1.set_i32(pos_int, 12345);
    page1.set(
        pos_string,
        &Varpair::from((Varchar::from(s), Varint::from(12345))),
    );

    // let s = page1.get_string(pos_string);
    // let i = page1.get_i32(pos_int);
    let records = page1.get::<Varpair<Varchar, Varint>>(0);
    let (s, i) = records.as_tuple();
    println!("{page1:?} {s:?} {i:?}");
    file_manager.write(&block, &page1).unwrap();

    let mut page2 = Page::new(block_size);
    file_manager.read(&block, &mut page2).unwrap();
    // let s = page2.get_string(pos_string);
    // let i = page2.get_i32(pos_int);
    let record = page2.get::<Varpair<Varchar, Varint>>(pos_string);
    let (s, i) = record.as_tuple();
    println!("{page2:?} {s:?} {i:?}");

    let appended_block: BlockId = file_manager.append(&fname).unwrap();
    println!("appended_block: {appended_block:?}");
}
