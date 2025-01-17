mod file_management;

use crate::file_management::block_id::BlockId;
use crate::file_management::file_manager::FileManager;
use crate::file_management::page::Page;
use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::Config;

fn init_logging() {
    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{h({d(%Y-%m-%d %H:%M:%S)(utc)} - {l}: {m}{n})}",
        )))
        .build();

    // let requests = FileAppender::builder()
    //     .encoder(Box::new(PatternEncoder::new("{d} - {m}{n}")))
    //     .build("log/requests.log")
    //     .unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        // .appender(Appender::builder().build("requests", Box::new(requests)))
        // .logger(Logger::builder().build("app::backend::db", LevelFilter::Info))
        // .logger(Logger::builder()
        //     .appender("requests")
        //     .additive(false)
        //     .build("app::requests", LevelFilter::Info))
        .build(Root::builder().appender("stdout").build(LevelFilter::Debug))
        .unwrap();

    log4rs::init_config(config).unwrap();
}

fn main() {
    init_logging();

    // env_logger::init();

    let block_size = 4096;
    let mut file_manager = FileManager::new("/tmp/test".to_string(), block_size).unwrap();
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
    let pos_int = pos_string + page1.max_length(s.len());
    page1.set_i32(pos_int, 12345);

    let s = page1.get_string(pos_string);
    let i = page1.get_i32(pos_int);
    println!("{page1:?} {s:?} {i:?}");
    file_manager.write(&block, page1).unwrap();

    let mut page2 = Page::new(block_size);
    file_manager.read(&block, &mut page2).unwrap();
    let s = page2.get_string(pos_string);
    let i = page2.get_i32(pos_int);
    println!("{page2:?} {s:?} {i:?}");

    let appended_block: BlockId = file_manager.append("testfile").unwrap();
    println!("appended_block: {appended_block:?}");
}
