use std::fs::File;
use std::io::Read;
use std::collections::{BTreeMap};

use log::{info, LevelFilter};
use log4rs::{
    append::{console::{ConsoleAppender, Target},
             file::FileAppender,
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
    filter::threshold::ThresholdFilter,
};

use clap::{Arg, Command};

pub fn init_logger(file_path: &str) {
    let level = log::LevelFilter::Info;
    let stderr = ConsoleAppender::builder().target(Target::Stderr).build();
    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{h({d(%m-%d-%Y %H:%M:%S)})}|{m}{n}")))
        .build(file_path)
        .unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .appender(Appender::builder()
                  .filter(Box::new(ThresholdFilter::new(level)))
                  .build("stderr", Box::new(stderr)),)
        .build(Root::builder()
               .appender("logfile")
               .appender("stderr")
               .build(LevelFilter::Trace),
        )
        .unwrap();

    let _handle = log4rs::init_config(config);
}

/// Utility method to parse custom 
/// cli args for cli tool
pub fn parse_args() -> clap::ArgMatches {
    info!("parse_args|starting");

    let cli_args = Command::new("boot")
        .args(&[
            Arg::new("conf")
                .long("config")
                .short('c')
                .takes_value(true)
                .required(true),
            Arg::new("action")
                .long("action")
                .short('a')
                .takes_value(true)
                .required(false),
            Arg::new("topic")
                .long("topic")
                .short('t')
                .takes_value(true)
                .required(false),
            Arg::new("tweet_id")
                .long("tweet_id")
                .short('w')
                .takes_value(true)
                .required(false),
            Arg::new("username")
                .long("username")
                .short('u')
                .takes_value(true)
                .required(false),
            Arg::new("user_id")
                .long("user_id")
                .short('d')
                .takes_value(true)
                .required(false),
            Arg::new("help")
                .long("help")
                .short('h'),])
        .get_matches();

    info!("parse_args|completed");
    cli_args
}

/// Utility method to parse custom 
/// cli args for nlp_topic_land 
pub fn parse_args1() -> clap::ArgMatches {
    info!("parse_args|starting");

    let cli_args = Command::new("nlp_topic_land")
        .args(&[
            Arg::new("conf")
                .long("config")
                .short('c')
                .takes_value(true)
                .required(true),
            Arg::new("topic")
                .long("topic_id")
                .short('t')
                .takes_value(true)
                .required(true),
            Arg::new("output")
                .long("output_dir")
                .short('o')
                .takes_value(true)
                .required(true),
            Arg::new("job_step")
                .long("job_step_id")
                .short('j')
                .takes_value(true)
                .required(true),
            Arg::new("help")
                .long("help")
                .short('h'),])
        .get_matches();

    info!("parse_args|completed");
    cli_args
}


/// Utility fn to read and parse configuration.yaml
pub fn get_config(config_name: &str) -> BTreeMap<String, String> {
    let mut yaml_config = File::open(String::from(config_name)).expect(&format!("ERR: {} cannot be opened", config_name));

    let mut file_data = String::new();
    yaml_config.read_to_string(&mut file_data).expect(&format!("ERR: yaml_config cannot be read"));

    let conf: BTreeMap<String, String> = serde_yaml::from_str(&file_data).expect(&format!("ERR: serde_yaml parse failed. conf creation aborted..."));
    conf
}
