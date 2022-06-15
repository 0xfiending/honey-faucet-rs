use conf::{get_config, init_logger};

use diesel::{
    query_dsl::{QueryDsl, RunQueryDsl},
    ExpressionMethods,
    NullableExpressionMethods,
};

use base_diesel::{
    get_conn,
    models::{
        Job,
        JobForm,
        JobStep,
        JobStepForm,
        Flow,
        FlowStep,
    },
    schema::{
        job::{
            dsl::job,
            id as _job_id,
            job_name as _job_name,
            flow_id as _job_flow_id,
            status as _job_status,
            created_dt as _job_created_dt,
            start_dt as _job_start_dt,
            updated_dt as _job_updated_dt,
        },
        flow::{
            dsl::flow,
            id as _flow_id,
            flow_name as _flow_name,
            frequency as _frequency,
            is_active as _is_active,
            run_flg as _run_flg,
            topic_id as _flow_topic_id,
            updated_dt as _flow_updated_dt,
        },
        flow_step::{
            dsl::flow_step,
            id as _flow_step_id,
            flow_id as _fsf_id,
            sequence_id as _fs_sequence_id,
            input_dir as _input_dir,
            output_dir as _output_dir,
            script_path as _script_path,
            script_parameters as _script_parameters,
        },
        job_step::dsl::{
            job_step,
            status as _job_step_status,
            sequence_id as _js_sequence_id,
            job_id as _js_job_id,
            updated_dt as _js_updated_dt,
        },
    },
};

use std::{
    thread,
    time::{
        Duration,
        SystemTime,
    },
    collections::BTreeMap,
    str::FromStr,
};

use serde_json::json;
use cron::Schedule;
use chrono::Utc;
use log::info;
use clap::{ArgMatches, Arg, Command};

#[allow(dead_code)]
fn usage() {
    println!("Usage: cargo run --bin flow_controller -- --config <config>");
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
    let config_name = cli_args.value_of("conf").expect("ERR: cli [configuration] is invalid");

    let config: BTreeMap<String, String> = get_config(config_name);

    let dt = Utc::now().to_rfc3339();
    let log_dir = String::from(config.get("log_dir").expect("ERR: log_dir is invalid"));
    let log_path = format!("{}/{}_flow_controller.log", &log_dir, &dt[0..19]);

    init_logger(&log_path);
    info!("flow_controller|starting");
    info!("flow_controller|conf={}", config_name);

    let conn = match get_conn(
        config.get("pg_db").expect("ERR: conf [pg_db] is invalid"),
        config.get("pg_user").expect("ERR: conf [pg_user] is invalid"),
        config.get("pg_secret").expect("ERR: conf [pg_secret] is invalid"),
        config.get("pg_host").expect("ERR: conf [pg_host] is invalid"),
        config.get("pg_port").expect("ERR: conf [pg_port] is invalid"),
    ) {
        Ok(connection) => {
            info!("main|conn established");
            connection
        },
        Err(err) => {
            panic!("main|ERR: failed to connect to db|err={}", err);
        },
    };

    loop {
        //let today = Utc::now();
        //let tom_date = NaiveDate::from_ymd(today.year(), today.month(), today.day()+1).and_hms(0, 0, 0);
        //let todays_date = NaiveDate::from_ymd(today.year(), today.month(), today.day()).and_hms(0, 0, 0);
            //.filter(_flow_updated_dt.gt(todays_date))
            //.filter(_flow_updated_dt.lt(tom_date))

        let _flows: Vec<(i32, String, String, Option<i32>)> = flow
            .filter(_is_active.eq(true))
            .filter(_run_flg.eq(false))
            .select((_flow_id, _flow_name, _frequency, _flow_topic_id.nullable()))
            .load(&conn)
            .unwrap_or(vec![]);

        if _flows.len() == 0 { 
            info!("main|no active flows found. skipping..."); 
        } else {
            info!("main|{} flow(s) found|flow processing start", _flows.len());

            for _flow in _flows {
                let (f_id, f_name, f_frequency, f_topic_id) = _flow;

                // process schedule
                let schedule = match Schedule::from_str(&f_frequency) {
                    Ok(s) => s,
                    Err(err) => {
                        info!("main|schedule cannot be parsed|frequency={}|err={}", f_frequency, err); 
                        continue
                    },
                };

                // get any scheduled times for the time
                let cur_date = Utc::today();
                let tasks = schedule
                    .upcoming(Utc)
                    .filter(|dt| dt.date() == cur_date);

                let mut n = 0;   // success count

                let now_dt = SystemTime::now();
                let now_secs = now_dt
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                for task in tasks {
                    let task_secs = task.timestamp() as u64;
                    let next_trigger = Duration::from_secs(task_secs - now_secs);
                    let scheduled_dt = match now_dt.checked_add(next_trigger) {
                        Some(next_time) => next_time,
                        _ => continue,
                    };
              
                    // job_name = {topic_id}_{flow_name}_{date}_{n}
                    let job_nm = match f_topic_id {
                        Some(x) => {
                            format!(
                                "{}_{}_{}_{}",
                                x,    // topic_id
                                f_name.replace("-", "_"),
                                cur_date.format("%Y_%m_%d"),
                                n,
                            )
                        },
                        None => {
                            info!("main|FLG: no topic id found for flow_id={}", f_id);
                            continue;
                        },
                    };

                    let new_job = json!({
                        "job_name": &job_nm,
                        "flow_id": Some(f_id),
                        "status": Some("N"),
                        "created_dt": now_dt,
                        "start_dt": scheduled_dt,
                        "updated_dt": Some(now_dt),
                    });

                    let json_str = new_job.to_string();
                    let new_job_form = serde_json::from_str::<JobForm>(&json_str)?;

                    match diesel::insert_into(job)
                        .values(&new_job_form)
                        .get_result::<Job>(&conn)
                    {
                        Ok(result) => info!("main|job name: {} created|flow_id={}|job_id={}", job_nm, f_id, result.id),
                        Err(err) => info!("main|job insert failed|err={}", err),
                    }
                    n += 1;
                }

                info!("main|seeded {} job(s) for flow_id={}, flow_name={}", n, f_id, f_name);
 
                // silent update
                match diesel::update(flow)  
                    .filter(_flow_id.eq(f_id))
                    .set((
                        _run_flg.eq(true),
                        _flow_updated_dt.eq(now_dt),
                    ))
                    .execute(&conn) 
                {
                    Ok(_) => info!("main|flow {} run flag toggle", f_id),
                    Err(err) => info!("main|ERR: flow update failed for flow id {}|err={}", f_id, err),
                    
                }
            }

        }

        let jobs: Vec<(i32, String, i32, Option<SystemTime>)> = job
            .filter(_job_status.eq("N"))
            .select((
                    _job_id, 
                    _job_name, 
                    _job_flow_id, 
                    _job_start_dt,))
            .load(&conn)
            .unwrap_or(vec![]);

        if jobs.len() == 0 {
            info!("main|no flow step(s) found|skipping...");
        } else {
            info!("main|processing {} flow step(s)", jobs.len());
            let now_dt = SystemTime::now();

            for _job in jobs {
                let (
                    j_id,
                    j_name,
                    j_flow_id,
                    j_start_dt,
                ) = _job;

                info!("main|processing job_id={}", j_id);

                let steps: Vec<(i32, i32, i32, String, String, String, Option<String>)> = flow_step
                    .filter(_fsf_id.eq(j_flow_id))
                    .select((
                            _flow_step_id,
                            _fsf_id, 
                            _fs_sequence_id, 
                            _input_dir, 
                            _output_dir, 
                            _script_path, 
                            _script_parameters,))
                    .load(&conn)
                    .unwrap_or(vec![]);

                let mut n = 0;
                let num_of_steps = steps.len();
                info!("main|{} flow step(s) found for flow_id={}|starting job step load for job_id={}",
                      num_of_steps,
                      j_flow_id,
                      j_id);

                for _step in steps {
                    let (step_id, upflow_id, seq_id, in_dir, out_dir, script_in, script_opts) = _step;

                    // do something w. script_in + script_opts
                    // format cmd

                    let new_job_step = json!({
                        "job_id": Some(j_id),
                        "flow_step_id": Some(step_id),
                        "sequence_id": Some(seq_id),
                        "input_path": in_dir,
                        "output_path": out_dir,
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
                            info!("main|job_step {} created for flow_step_id={}", result.id, step_id);
                            n += 1;
                        },
                        Err(err) => {
                            info!("main|failed to update db for job_id={}, flow_step_id={}, sequence_id={}",
                                  j_id,
                                  step_id,
                                  seq_id);
                            continue;
                        },
                    }
                }

                if num_of_steps != n {
                    match diesel::update(job)
                        .filter(_job_id.eq(j_id))
                        .set((
                            _job_status.eq("F"),
                            _job_updated_dt.eq(now_dt),
                        ))
                        .get_result::<Job>(&conn)
                    {
                        Ok(result) => info!("main|FLG: job_id {} failed.", j_id),
                        Err(err) => info!("main|ERR: failed to update db for job_id={}|err={}", j_id, err),
                    }
                } else {
                    match diesel::update(job)
                        .filter(_job_id.eq(j_id))
                        .set((
                            _job_status.eq("S"),
                            _job_updated_dt.eq(now_dt),
                        ))
                        .get_result::<Job>(&conn)
                    {
                        Ok(result) => info!("main|seeded {}/{} job step(s) for job_id={}", n, num_of_steps, j_id),
                        Err(err) => info!("main|ERR: failed to update db for job_id={}|err={}", j_id, err),
                    }
                }
            } 
        }    

        let update_time = SystemTime::now();
        let scheduled_jobs = job.filter(_job_status.eq("S")).select(_job_id).into_boxed();
        match diesel::update(job_step)
            .filter(_job_step_status.eq("N"))
            .filter(_js_sequence_id.eq(1))
            .filter(_js_job_id.eq_any(scheduled_jobs))
            .set((
                _job_step_status.eq("S"),
                _js_updated_dt.eq(update_time),
            ))
            .get_result::<JobStep>(&conn) 
        {
            Ok(_) => info!("main|job step launcher update successful"),
            Err(err) => info!("main|FLG: job step launcher failed|err={}", err),
        }

        
        let timeout = Duration::from_secs(600 as u64); 
        info!("main|all flows have been processed|sleeping for {:?}", timeout);
        thread::sleep(timeout);
    }
}
