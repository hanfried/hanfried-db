use hanfried_db::file_management::block_id::BlockId;
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

    let block = BlockId {
        filename: "testfile",
        block_number: 2,
    };
    println!("{block:?}");

    let mut page1 = Page::new(block_size);
    let pos_string: usize = 42;
    let s = "abcdefhgh";
    page1.set_string(pos_string, s);
    let pos_int = pos_string + page1.max_length(s);
    page1.set_i32(pos_int, 12345);

    let s = page1.get_string(pos_string);
    let i = page1.get_i32(pos_int);
    println!("{page1:?} {s:?} {i:?}");
    file_manager.write(&block, &page1).unwrap();

    let mut page2 = Page::new(block_size);
    file_manager.read(&block, &mut page2).unwrap();
    let s = page2.get_string(pos_string);
    let i = page2.get_i32(pos_int);
    println!("{page2:?} {s:?} {i:?}");

    let appended_block: BlockId = file_manager.append("testfile").unwrap();
    println!("appended_block: {appended_block:?}");
}
