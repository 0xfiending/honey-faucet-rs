#[macro_use]
extern crate diesel;
extern crate dotenv;

pub mod schema;
pub mod models;

use diesel::prelude::*;
use diesel::pg::PgConnection;
use std::env;

pub fn get_conn(
    db: &str,
    user: &str,
    pass: &str,
    host: &str,
    port: &str,
) -> Result<PgConnection, Box<dyn std::error::Error>> {
    if db.is_empty() { return Err("ERR: db_name is invalid".into()) }
    if user.is_empty() { return Err("ERR: db_user is invalid".into()) }
    if pass.is_empty() { return Err("ERR: db_password is invalid".into()) }
    if host.is_empty() { return Err("ERR: db_host is invalid".into()) }
    if port.is_empty() { return Err("ERR: db_port is invalid".into()) }

    // Construct database URL
    let db_url = format!("postgres://{}:{}@{}:{}/{}",
                             user,
                             pass,
                             host,
                             port,
                             db,
                         );

    let connection = PgConnection::establish(&db_url)
        .expect(&format!("ERR: cannot connect to {}", db_url));
    
    Ok(connection)
}
