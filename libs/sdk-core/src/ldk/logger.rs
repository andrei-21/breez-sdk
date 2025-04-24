use ldk_node::logger::{LogLevel as LdkLevel, LogRecord, LogWriter};
use log::{logger, Level, Record};

pub(crate) struct Logger;

impl LogWriter for Logger {
    fn log(&self, record: LogRecord<'_>) {
        let record = Record::builder()
            .args(record.args)
            .level(to_log_level(record.level))
            .target("ldk_node")
            .line(Some(record.line))
            .module_path(Some(record.module_path))
            .build();
        logger().log(&record)
    }
}

fn to_log_level(level: LdkLevel) -> Level {
    match level {
        LdkLevel::Gossip => Level::Trace,
        LdkLevel::Trace => Level::Trace,
        LdkLevel::Debug => Level::Debug,
        LdkLevel::Info => Level::Info,
        LdkLevel::Warn => Level::Warn,
        LdkLevel::Error => Level::Error,
    }
}
