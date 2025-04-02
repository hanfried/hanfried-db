use log::{warn, LevelFilter};
use log4rs::append::console::ConsoleAppender;
use log4rs::config::runtime::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;
use std::env;
use std::str::FromStr;
use std::sync::Once;

static INIT: Once = Once::new();

pub fn init_logging() {
    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{h({d(%Y-%m-%d %H:%M:%S)(utc)} - {l}: {m}{n})}",
        )))
        .build();

    // let requests = FileAppender::builder()
    //     .encoder(Box::new(PatternEncoder::new("{d} - {m}{n}")))
    //     .build("log/requests.log")
    //     .unwrap();

    let log_level = match env::var("HFDB_LOG_LEVEL") {
        Ok(level) => LevelFilter::from_str(&level).unwrap(),
        Err(_) => LevelFilter::Debug,
    };

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        // .appender(Appender::builder().build("requests", Box::new(requests)))
        // .logger(Logger::builder().build("app::backend::db", LevelFilter::Info))
        // .logger(Logger::builder()
        //     .appender("requests")
        //     .additive(false)
        //     .build("app::requests", LevelFilter::Info))
        .build(Root::builder().appender("stdout").build(log_level))
        .unwrap();

    INIT.call_once(|| {
        if let Err(e) = log4rs::init_config(config) {
            warn!("Error initializing logging config: {}", e)
        }
    })
}
