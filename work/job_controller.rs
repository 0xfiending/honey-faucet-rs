use conf::{get_config, init_logger};
use serde_json::json;
use rust_blocking_queue::BlockingQueue;
use chrono::Utc;
use log::info;
use clap::{ArgMatches, Arg, Command};

use lightspeed_scheduler::job::Job;
use lightspeed_scheduler::JobExecutor;
use lightspeed_scheduler::new_executor_with_utc_tz;
use tokio::task;

use std::{
    sync::Arc,
    thread,
    time::SystemTime, 
    collections::BTreeMap,
};

use diesel::{
    query_dsl::{QueryDsl, RunQueryDsl},
    ExpressionMethods,
};

use base_diesel::{
    get_conn,
    schema::{
        job::{
            dsl::*,
            id as j_id,
            job_name,
            flow_id as job_fid,
            status as job_status,
            start_dt as job_start,
            updated_dt as ju_dt,
        },
        flow_step::{
            dsl::flow_step,
            id as fs_id,
            sequence_id as fs_sequence_id,
            input_dir as fs_input_dir,
            output_dir as fs_output_dir,
            script_path as fs_script_path,
            script_parameters as fs_script_parameters,
        },
        job_step::{
            dsl::*,
            id as js_id,
            command as js_cmd,
            status as js_status,
            updated_dt as js_dt,
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
    script_path: String,
    script_params: Option<String>,
    job_start: Option<SystemTime>,
}

impl WorkOrder {
    pub fn new(
        _job_step_id: i32,
        _script_path: String,
        _script_params: Option<String>,
        _job_start: Option<SystemTime>,
    ) -> Self {
        Self {
            job_step_id: _job_step_id,
            script_path: _script_path,
            script_params: _script_params,
            job_start: _job_start, 
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

// TODO: 
//  - A novel way to handle and populate args
//  - Propagate errors to db (missing a few pieces)
//  - Propagate errors back to master thread
//  - Add timeouts to threads and master
//  - explore tokio::task vs std::thread (async)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli_args: ArgMatches = parse_args();
    let config_name = cli_args.value_of("conf").expect("ERR: cli [configuration] is invalid");
    let config: BTreeMap<String, String> = get_config(config_name);

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

    /*
    let num_threads = 8;  // default, by len

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

                // POISON
                if next_job.job_step_id == -9999 {
                    break;
                }

                // SENTINEL
                // flush based on sentinel value 

                // format 
                let script_path = String::from(next_job.script_path);
                let script_params = match next_job.script_params 
                {
                    Some(x) => x.split(" ").map(|x| String::from(x)).collect(),
                    None => vec![],
                };

                let p = lightspeed_scheduler::job::Job::new(
                    format!("worker {}", worker_id),                 // group
                    format!("job step {}", next_job.job_step_id),    // name
                    None,                                            // retries
                    move || {
                        let job_path = script_path.clone();
                        let job_params = script_params.clone();

                        let mut cmd_args: Vec<String> = vec![];
                        cmd_args.push("run".into());
                        cmd_args.push("--bin".into());
                        cmd_args.push(job_path);
                        cmd_args.push("--".into());

                        if !job_params.is_empty() {
                            for job_param in job_params {
                                cmd_args.push(job_param);
                            }
                        }

                        Box::pin(async move {
                            tokio::process::Command::new("cargo")
                                .args(&cmd_args)
                                .output()
                                .await
                                .expect("ERR: cargo command failed"); 

                            Ok(())
                        })
                });

                let mut trigger: Vec<String> = vec![];
                trigger.push("0 30 20 1 6 * 2022".into());
                executor.add_job(&trigger, p);

                let now_dt = SystemTime::now();
                match diesel::update(job_step)
                    .filter(js_id.eq(next_job.job_step_id))
                    .set((
                        js_status.eq("S"),
                        js_cmd.eq(""),     // TODO: update cmd_str
                        js_dt.eq(now_dt),
                    ))
                    .get_result::<JobStep>(&t_conn) 
                {
                    Ok(result) => info!("worker {}|successfully scheduled job_step_id {}", worker_id, result.id),
                    Err(err) => info!("worker {}|ERR: failed to update job step, e={}", worker_id, err),
                }
            }
            info!("worker {}|worker shutting down..", worker_id);
            executor.stop(true).await.unwrap();
        });

        workers.push(worker);
    }*/

    /////////////////////////////////////////////
    // mission control ->> assembly line start //
    ////////////////////////////////////////////
    
    let num_threads = 7 as usize;   // default, by index

    loop {
        let jobs: Vec<(i32, String, i32, SystemTime)> = job_steps
            .filter(job_status.eq("S"))
            .filter()
            .select((j_id, job_name, job_fid, job_start))
            .load(&conn)
            .unwrap_or(vec![]);

        if jobs.len() > 0 {
            info!("loop_controller|processing {} jobs", jobs.len());
            let now_dt = SystemTime::now();
            let mut queue_counter = 0;

            for _job in jobs {
                let (_job_id, _job_name, _flow_id, _start_dt) = _job;
                info!("loop_controller|processing job_id {}", _job_id);

                /*
                let steps: Vec<(i32, i32, String, String, String, Option<String>)> = flow_step
                    .filter(fs_id.eq(_flow_id))
                    .select((fs_id, fs_sequence_id, fs_input_dir, fs_output_dir, fs_script_path, fs_script_parameters))
                    .load(&conn)
                    .unwrap_or(vec![]);
                
                if steps.is_empty() {
                    info!("loop_controller|ERR: no flow_steps found for flow_id={}", _flow_id);
                    continue;
                }*/

                /*
                for _step in steps {
                    let (_fs_id, _fs_sequence_id, _fs_input_dir, _fs_output_dir, _fs_script_path, _fs_script_parameters) = _step;         

                    let new_job_step = json!({
                        "job_id": Some(_job_id),
                        "flow_step_id": Some(_fs_id),
                        "sequence_id": Some(_fs_sequence_id),
                        "input_path": _fs_input_dir,
                        "output_path": _fs_output_dir,
                        "command": Some(""),
                        "status": "N",
                        "created_dt": now_dt,
                        "updated_dt": Some(now_dt),
                    });

                    let json_str = new_job_step.to_string();
                    let new_job_step_form = serde_json::from_str::<JobStepForm>(&json_str)?;

                    match diesel::insert_into(job_step)
                        .values(&new_job_step_form)
                        .get_result::<JobStep>(&conn)
                    {
                        Ok(result) => {
                            let work_order = WorkOrder::new(
                                result.id,
                                _fs_script_path,
                                _fs_script_parameters,
                                Some(now_dt),
                            );

                            shares[queue_counter].en_q(work_order);

                            if queue_counter == num_threads {
                                queue_counter = 0;
                            } else {
                                queue_counter += 1;
                            }
                        },
                        Err(err) => {
                            info!("loop_controller|ERR: unable to fill work order|err={}", err);
                            continue;
                        }, 
                    }
                }*/


                /*
                // update job status
                let result = diesel::update(job)
                    .filter(j_id.eq(_job_id))
                    .set((
                        job_status.eq("S"),
                        ju_dt.eq(now_dt),
                    ))
                    .get_result::<JobModel>(&conn);

                match result {
                    Ok(_) => info!("loop_controller|seeded job steps for job_id={}", _job_id),
                    Err(err) => info!("loop_controller|ERR: failed to update db for job_id={}|err={}", _job_id, err),
                }*/
            }
        }
    }
}
