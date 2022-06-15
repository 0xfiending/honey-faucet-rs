use conf::{get_config, init_logger};
use serde_json::json;
use rust_blocking_queue::BlockingQueue;
use chrono::{Utc, DateTime, Datelike, Timelike};
use log::info;
use clap::{ArgMatches, Arg, Command};

use lightspeed_scheduler::job::Job;
use lightspeed_scheduler::{new_executor_with_utc_tz, JobExecutor};
use tokio::task;

use std::{
    sync::Arc,
    thread,
    time::{SystemTime, Duration}, 
    collections::BTreeMap,
};

use diesel::{
    query_dsl::{QueryDsl, RunQueryDsl},
    ExpressionMethods,
    NullableExpressionMethods,
    JoinOnDsl,
};

use base_diesel::{
    get_conn,
    schema::{
        job::{
            dsl::*,
            id as _job_id,
            job_name as _job_name,
            flow_id as _job_flow_id,
            status as _job_status,
            start_dt as _job_start_dt,
            updated_dt as _job_updated_dt,
        },
        flow_step::{
            dsl::flow_step,
            id as _flow_step_id,
            flow_id as _fs_flow_id,
            sequence_id as _fs_sequence_id,
            input_dir as _fs_input_dir,
            output_dir as _fs_output_dir,
            script_path as _fs_script_path,
            script_parameters as _fs_script_parameters,
        },
        job_step::{
            dsl::*,
            id as _job_step_id,
            flow_step_id as _js_flow_step_id,
            job_id as _js_job_id,
            command as _command,
            status as _job_step_status,
            updated_dt as _js_updated_dt,
        },
        flow::{
            dsl::*,
            topic_id as _topic_id,
            id as _flow_id,
        },
    },
    models::{
        Job as JobModel,
        JobStep,
        JobStepForm,
    },
};

#[allow(dead_code)]
fn usage() {
    println!("Usage: cargo run --bin job_controller -- --config <config>");
}

struct WorkOrder {
    job_step_id: i32,
    status_cd: String,
    subject_id: Option<i32>,
    job_start: Option<SystemTime>,
    script_name: String,
    script_params: Option<String>,
    in_path: Option<String>,
    out_path: Option<String>,
}

impl WorkOrder {
    pub fn new(
        job_step_id: i32,
        status_cd: String,
        subject_id: Option<i32>,
        job_start: Option<SystemTime>,
        script_name: String,
        script_params: Option<String>,
        in_path: Option<String>,
        out_path: Option<String>,
    ) -> Self {
        Self {
            job_step_id: job_step_id,
            status_cd: status_cd,
            subject_id: subject_id,
            job_start: job_start,
            script_name: script_name,
            script_params: script_params,
            in_path: in_path,
            out_path: out_path,
        }
    }
}

fn parse_args() -> clap::ArgMatches {
    let cli_args = Command::new("boot")
        .args(&[
            Arg::new("conf")
                .long("config")
                .short('c')
                .takes_value(true)
                .required(true),
            Arg::new("help")
                .long("help")
                .short('h'),
        ])
        .get_matches();

    cli_args
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli_args: ArgMatches = parse_args();
    let config_name = String::from(cli_args.value_of("conf").expect("ERR: cli [configuration] is invalid"));
    let config: BTreeMap<String, String> = get_config(&config_name);

    let dt = Utc::now().to_rfc3339();
    let log_dir = String::from(config.get("log_dir").expect("ERR: log_dir is invalid"));
    let log_path = format!("{}/{}_job_controller.log", &log_dir, &dt[0..19]);

    init_logger(&log_path);
    info!("job_controller|starting");
    info!("job_controller|conf={}", config_name);

    let conn = match get_conn(
        config.get("pg_db").expect("ERR: conf [pg_db] is invalid"),
        config.get("pg_user").expect("ERR: conf [pg_user] is invalid"),
        config.get("pg_secret").expect("ERR: conf [pg_secret] is invalid"),
        config.get("pg_host").expect("ERR: conf [pg_host] is invalid"),
        config.get("pg_port").expect("ERR: conf [pg_post] is invalid"),
    ) {
        Ok(connection) => {
            info!("job_controller|conn established");
            connection
        },
        Err(err) => {
            panic!("job_controller|ERR: failed to connect to db|err={}", err);
        },
    };

    ////\\\\//////\\\\\\//////\\\\\/////\\\\
    ////\\\\  worker orchestration  ////\\\\
    ////\\\\\ -------------------- /////\\\\

    let num_threads = 8;       // by len

    let mut shares: Vec<Arc<BlockingQueue<WorkOrder>>> = vec![];
    for _i in 0..num_threads {
        let share = Arc::new(BlockingQueue::<WorkOrder>::new());
        shares.push(share);
    }

    let mut workers: Vec<tokio::task::JoinHandle<_>> = vec![];
    for i in 0..num_threads {
        let t_config = String::from(cli_args.value_of("conf").unwrap());
        let t_share = Arc::clone(&shares[i]);

        let worker = task::spawn(async move {
            let worker_id = i;
            let executor = new_executor_with_utc_tz();
            info!("worker {}|spawning worker...", worker_id);

            let t_queue = t_share.clone();
            let t_conf: BTreeMap<String, String> = get_config(&t_config);
            let t_conn = get_conn(
                t_conf.get("pg_db").unwrap(), 
                t_conf.get("pg_user").unwrap(),
                t_conf.get("pg_secret").unwrap(),
                t_conf.get("pg_host").unwrap(),
                t_conf.get("pg_port").unwrap(),
            ).unwrap(); 

            loop {
                let next_job = t_queue.de_q();

                if !next_job.status_cd.eq("S") { continue; }
                if next_job.subject_id == None { 
                    info!("worker {}|next job - subject id is invalid", worker_id); 
                    continue; 
                }
                if next_job.script_name.eq("") { 
                    info!("worker {}|next job - script name is invalid", worker_id); 
                    continue; 
                }

                // POISON - OP: FAULT
                if next_job.job_step_id == -9999 {
                    break;
                }

                // SENTINEL - OP: FLUSH
                if next_job.job_step_id == -8888 {
                    // check if jobs are running
                    continue;
                }

                // SCHEDULER - OP: WORK
                match next_job.job_start {
                    Some(x) => {            // LIGHTSPEED 
                        info!("worker {}|scheduler start", worker_id);
                        let script_path = String::from(next_job.script_name);
                        let script_opts = next_job.script_params;
                        let conf_nm = t_config.clone();

                        let p = lightspeed_scheduler::job::Job::new(
                            format!("worker {}", worker_id),                 // group
                            format!("job step {}", next_job.job_step_id),    // name
                            None,                                            // retries
                            move || {
                                let bin_nm = script_path.clone();
                                let bin_opts = script_opts.clone();
                                let bin_conf = conf_nm.clone();

                                // let cmd_args

                                let script_params = match bin_opts {
                                    Some(y) => y.split(" ").map(|y| String::from(y)).collect(),
                                    None => vec![],
                                };

                                let mut cmd_args: Vec<String> = vec![];
                                cmd_args.push("run".into());
                                cmd_args.push("--bin".into());
                                cmd_args.push(bin_nm);
                                cmd_args.push("--".into());

                                // standard opts
                                for script_opt in script_params {
                                match script_opt.as_str() {
                                "--topic_id" => {
                                    match next_job.subject_id {
                                        Some(subject) => {
                                            let opt_val = String::from(format!("{}", subject));
                                            cmd_args.push("--topic_id".into());
                                            cmd_args.push(opt_val);
                                        },
                                        None => { 
                                            info!("invalid topic id found while parsing command opts, re-configure");
                                            continue
                                        }
                                    }
                                },
                                "--job_step_id" => {
                                    cmd_args.push("--job_step_id".into());
                                    cmd_args.push(String::from(format!("{}", next_job.job_step_id)));
                                },
                                "--config" => {
                                    cmd_args.push("--config".into());
                                    cmd_args.push(bin_conf.clone());
                                },
                                "--input_dir" => {
                                    match &next_job.in_path {
                                        Some(in_val) => {
                                            cmd_args.push("--input_dir".into());
                                            cmd_args.push(in_val.to_string());
                                        },
                                        None => {
                                            info!("invalid input path found while parsing command opts, re-configure");
                                            continue
                                        },
                                    }
                                },
                                "--output_dir" => {
                                    match &next_job.out_path {
                                        Some(out_val) => {
                                            cmd_args.push("--output_dir".into());
                                            cmd_args.push(out_val.to_string());
                                        },
                                        None => { 
                                            info!("invalid output path found while parsing command opts, re-configure");
                                            continue
                                        },
                                    }
                                },
                                _ => { info!("DEFAULT found while parsing command opts"); },
                                }}

                                Box::pin(async move {
                                    tokio::process::Command::new("cargo")
                                        .args(&cmd_args)
                                        .output()
                                        .await
                                        .expect("ERR: cargo command failed"); 

                                    Ok(())
                               })
                        });

                        let dt_start: DateTime<Utc> = x.clone().into();

                        // 1:S 2:M 3:H 4:d 5:m 6:DoW 7:Y
                        let formatted_start = String::from(
                            format!("{} {} {} {} {} * {}",
                                dt_start.second(),
                                dt_start.minute(),
                                dt_start.hour(),
                                dt_start.day(),
                                dt_start.month(),
                                dt_start.year(),)
                        );

                        let mut trigger: Vec<String> = vec![];
                        trigger.push(formatted_start);
                        executor.add_job(&trigger, p);

                        let now_dt = SystemTime::now();
                        match diesel::update(job_step)
                            .filter(_job_step_id.eq(next_job.job_step_id))
                            .set((
                                _job_step_status.eq("R"),
                                _command.eq(""),                  // TODO: update cmd_str
                                _js_updated_dt.eq(Some(now_dt)),
                            ))
                            .get_result::<JobStep>(&t_conn) 
                        {
                            Ok(result) => info!("worker {}|successfully scheduled job_step_id {}", worker_id, result.id),
                            Err(err) => info!("worker {}|ERR: failed to update job step, e={}", worker_id, err),
                        }

                        info!("worker {}|scheduler complete", worker_id);
                    },
                    None => {               // AD-HOC
                        info!("worker {}|ad-hoc execution start", worker_id);

                        // update db to running state
                         
                        let script_params = match next_job.script_params {
                            Some(x) => x.split(" ").map(|x| String::from(x)).collect(),
                            None => vec![],
                        };

                        let mut cmd_args: Vec<String> = vec![];
                        cmd_args.push("run".into());
                        cmd_args.push("--bin".into());
                        cmd_args.push(next_job.script_name);
                        cmd_args.push("--".into());

                        // standard opts
                        for script_opt in script_params {
                        match script_opt.as_str() {
                            "--topic_id" => {
                                match next_job.subject_id {
                                    Some(subject) => {
                                        let opt_val = String::from(format!("{}", subject));
                                        cmd_args.push("--topic_id".into());
                                        cmd_args.push(opt_val);
                                    },
                                    None => { 
                                        info!("invalid topic id found while parsing command opts, re-configure");
                                        continue
                                    }
                                }
                            },
                            "--job_step_id" => {
                                cmd_args.push("--job_step_id".into());
                                cmd_args.push(String::from(format!("{}", next_job.job_step_id)));
                            },
                            "--config" => {
                                let conf_opt = String::from(format!("{}", t_config));
                                cmd_args.push("--config".into());
                                cmd_args.push(conf_opt);
                            },
                            "--input_dir" => {
                                match &next_job.in_path {
                                    Some(in_val) => {
                                        cmd_args.push("--input_dir".into());
                                        cmd_args.push(in_val.to_string());
                                    },
                                    None => {
                                        info!("invalid input path found while parsing command opts, re-configure");
                                        continue
                                    },
                                }
                            },
                            "--output_dir" => {
                                match &next_job.out_path {
                                    Some(out_val) => {
                                        cmd_args.push("--output_dir".into());
                                        cmd_args.push(out_val.to_string());
                                    },
                                    None => { 
                                        info!("invalid output path found while parsing command opts, re-configure");
                                        continue
                                    },
                                }
                            },
                            _ => { info!("DEFAULT found while parsing command opts"); },
                        }}

                        tokio::process::Command::new("cargo")
                            .args(&cmd_args)
                            .output()
                            .await
                            .expect("ERR: cargo command failed");

                        info!("worker {}|ad-hoc execution complete", worker_id);
                    },
                }
            }
                
            info!("worker {}|worker shutting down..", worker_id);
            executor.stop(true).await.unwrap();
        });

        workers.push(worker);
    }

    /////////////////////////////////////////////
    // mission control ->> assembly line start //
    ////////////////////////////////////////////
    
    let num_threads = num_threads-1 as usize;   // by index
    let mut queue_counter = 0;                  // global queue index

    loop {
        let launch_steps: Vec<(i32, String, Option<SystemTime>, Option<i32>, String, Option<String>, String, String)> = job_step
            .inner_join(flow_step.on(_flow_step_id.eq(_js_flow_step_id)))
            .inner_join(job.on(_job_id.eq(_js_job_id)))
            .inner_join(flow.on(_flow_id.eq(_fs_flow_id)))
            .filter(_job_step_status.eq("S"))
            .filter(_job_status.eq("S"))
            .select((
                _job_step_id,
                _job_step_status,
                _job_start_dt,
                _topic_id.nullable(),
                _fs_script_path,
                _fs_script_parameters,
                _fs_input_dir,
                _fs_output_dir,
            ))
            .load(&conn)
            .unwrap_or(vec![]);

        let mut n = 0;     
        if launch_steps.len() == 0 {
            info!("main|no launch steps found|skipping...");
        } else {
            info!("main|processing {} launch steps", launch_steps.len());
            let now_dt = SystemTime::now();
            
            for launch_step in launch_steps {
                let (
                    js_id,
                    stat_cd,
                    start_time,
                    subject_id,
                    target_script,
                    target_opts,
                    in_path,
                    out_path,
                ) = launch_step;

                let work_order = WorkOrder::new(
                    js_id as i32,
                    stat_cd,
                    subject_id,
                    start_time,
                    target_script,
                    target_opts,
                    Some(in_path),
                    Some(out_path),
                );

                shares[queue_counter].en_q(work_order);
                if queue_counter == num_threads { queue_counter = 0; } 
                else { queue_counter += 1; }
            
                n += 1;
            }
        }

        let timeout = Duration::from_secs(600 as u64);
        info!("main|{} launch step(s) have been processed|sleeping for {:?}", n, timeout);
        thread::sleep(timeout);
    }
}
