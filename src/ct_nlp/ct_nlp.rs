use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use log::info;

use polars::prelude::NamedFrom;
use polars::series::Series;
use polars::frame::DataFrame;

pub type ResponseMap = HashMap<String, Vec<serde_json::Value>>;

#[derive(Debug, Deserialize, Serialize)]
struct Tweet {
    timestamp: u32,
    id: u32,
    user: String,
}

/*
/// Utility method to query the users_lookup (v2) endpoint
pub async fn users_lookup(bearer_token: &str, user_id: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    
}*/

/// Utility method to query the mentions timeline (v2) endpoint
pub async fn mentions_timeline(bearer_token: &str, user_id: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    info!("mentions_timeline|starting");

    if bearer_token == "" {
        return Err(format!("mentions_timeline|ERR: bearer token is not valid, bearer_token={}", bearer_token).into());
    }

    if user_id == "" {
        return Err(format!("mentions_timeline|ERR: user id is not valid, user_id={}", user_id).into());
    }

    let url = format!("https://api.twitter.com/2/users/{id}/mentions", id=user_id);
    info!("mentions_timeline|url={:?}", url);

    let tw_client = reqwest::Client::new();
    let mut response = tw_client.get(&url)
        .query(&[
            ("expansions", "author_id"),
            ("tweet.fields", "author_id,created_at,text"),
            ("max_results", "100"),
        ])
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()?;

    match response.status() {
        StatusCode::OK => info!("mentions_timeline|query success|parse starting..."),
        s => return Err(format!("mentions_timeline|status={}", s).into()),
    }

    let tmp: serde_json::Value = match response.text() {
        Ok(x) => serde_json::from_str(&x)?,
        Err(e) => return Err(format!("mentions_timeline|ERR: unable to parse response object, err={}", e).into()),
    };

    let data = match tmp["data"].as_array() {
        Some(x) => x,
        _ => return Err(format!("mentions_timeline|ERR: unable to parse data object").into()),
    };

    let mut author_vec: Vec<String> = vec![];
    //let mut username_vec: Vec<String> = vec![];
    let mut created_vec: Vec<String> = vec![];
    let mut id_vec: Vec<String> = vec![];
    let mut text_vec: Vec<String> = vec![];

    for tweet in data {
        let mut author_id = tweet["author_id"].to_string();
        let mut created_at = tweet["created_at"].to_string();
        let mut id = tweet["id"].to_string();
        let mut text = tweet["text"].to_string();

        if (author_id == "") || (created_at == "") || (id == "") || (text == "") { continue; }

        author_id = author_id[1..author_id.len()-1].to_string();
        created_at = created_at[1..created_at.len()-1].to_string();
        id = id[1..id.len()-1].to_string();
        text = text[1..text.len()-1].to_string();

        author_vec.push(author_id);
        created_vec.push(created_at);
        id_vec.push(id);
        text_vec.push(text);
    }

    let df = DataFrame::new(vec![
        Series::new("tweet_id", id_vec),
        Series::new("author_id", author_vec),
        Series::new("text", text_vec),
        Series::new("created_at", created_vec),
    ])?;

    println!("{:?}", df);
    info!("mentions_timeline|completed");
    Ok(df)
}

/// Utility method to query the user timeline (v2) endpoint
pub async fn user_timeline(bearer_token: &str, user_id: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    info!("user_timeline|starting");

    if bearer_token == "" {
        return Err(format!("user_timeline|ERR: bearer token is not valid, bearer_token={}", bearer_token).into());
    }

    if user_id == "" {
       return Err(format!("user_timeline|ERR: user id is not valid, user_id={}", user_id).into());
    }

    let url = format!("https://api.twitter.com/2/users/{id}/tweets", id=user_id);
    info!("user_timeline|url={:?}", url);

    let tw_client = reqwest::Client::new();
    let mut response = tw_client.get(&url)
        .query(&[
            ("expansions", "author_id"),
            ("tweet.fields", "author_id,created_at,text"),
            ("max_results", "100"),
        ])
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()?;

    match response.status() {
        StatusCode::OK => info!("user_timeline|query success|parse starting..."),
        s => return Err(format!("user_timeline|status={}", s).into()),
    }

    let tmp: serde_json::Value = match response.text() {
        Ok(x) => serde_json::from_str(&x)?,
        Err(e) => return Err(format!("user_timeline|ERR: unable to parse response object, err={}", e).into()),
    };

    let data = match tmp["data"].as_array() {
        Some(x) => x,
        _ => return Err(format!("user_timeline|ERR: unable to parse data object").into()),
    };

    let mut author_vec: Vec<String> = vec![];
    //let mut username_vec: Vec<String> = vec![];
    let mut created_vec: Vec<String> = vec![];
    let mut id_vec: Vec<String> = vec![];
    let mut text_vec: Vec<String> = vec![];

    for tweet in data {
        let mut author_id = tweet["author_id"].to_string();
        let mut created_at = tweet["created_at"].to_string();
        let mut id = tweet["id"].to_string();
        let mut text = tweet["text"].to_string();

        if (author_id == "") || (created_at == "") || (id == "") || (text == "") { continue; }

        author_id = author_id[1..author_id.len()-1].to_string();
        created_at = created_at[1..created_at.len()-1].to_string();
        id = id[1..id.len()-1].to_string();
        text = text[1..text.len()-1].to_string();

        author_vec.push(author_id);
        created_vec.push(created_at);
        id_vec.push(id);
        text_vec.push(text);
    }

    let df = DataFrame::new(vec![
        Series::new("tweet_id", id_vec),
        Series::new("author_id", author_vec),
        Series::new("text", text_vec),
        Series::new("created_at", created_vec),
    ])?;

    info!("user_timeline|completed");
    Ok(df)
}

/// Utility method to query tweet_lookup (v2) endpoint
/// The response object is returned if valid
pub async fn tweet_lookup(bearer_token: &str, tweet_id: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    info!("tweet_lookup|starting");

    if bearer_token == "" { 
        return Err(format!("tweet_lookup|ERR: bearer token is not valid, bearer_token={}", bearer_token).into());
    }
    
    if tweet_id == "" { 
        return Err(format!("tweet_lookup|ERR: tweet_id is not valid, tweet_id={}", tweet_id).into());
    }

    let url = format!("https://api.twitter.com/2/tweets/{}", tweet_id);
    info!("tweet_lookup|url={:?}", url);

    let tw_client = reqwest::Client::new();
    let mut response = tw_client.get(&url)
        .query(&[
            ("expansions", "author_id"),
            ("tweet.fields", "author_id,created_at,text"),
            ("user.fields", "name,username"),
        ])
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()?;

    match response.status() {
        StatusCode::OK => info!("tweet_lookup|query success|parse starting..."),
        s => return Err(format!("tweet_lookup|status={}", s).into()),
    }

    let result: serde_json::Value = match response.text() {
        Ok(x) => serde_json::from_str(&x)?,
        Err(e) => return Err(format!("tweet_lookup|ERR: unable to parse response object,  err={}", e).into()),
    };

    info!("tweet_lookup|completed");
    Ok(result)
}


/// Utility method to query recents (v2) endpoint
/// Additional parsing is done to seed a DataFrame
/// cols: author_id, created_at, tweet_id, text
pub async fn get_recent_tweets(bearer_token: &str, topic: &str, count: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    info!("get_recent_tweets|starting");
    info!("get_recent_tweets|topic: {}", topic);

    let mut author_vec: Vec<String> = vec![];
    //let mut username_vec: Vec<String> = vec![];
    let mut created_vec: Vec<String> = vec![];
    let mut id_vec: Vec<String> = vec![];
    let mut text_vec: Vec<String> = vec![];

    if bearer_token == "" { panic!("error: bearer_token is not valid");  }
        
    let url = String::from("https://api.twitter.com/2/tweets/search/recent");

    let tw_client = reqwest::Client::new();
    let mut response = tw_client.get(&url)
        .query(&[
            ("query", topic), 
            ("tweet.fields", "author_id,created_at,id,text"),
            ("user.fields", "name,username"),
            ("expansions", "author_id"),
            ("max_results", "100")
        ])
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()?;

    match response.status() {
        StatusCode::OK => info!("get_recent_tweets|query success|parse starting..."),
        s => println!("{}", s),
    }

    let tmp: serde_json::Value = match response.text() {
        Ok(x) => serde_json::from_str(&x)?,
        Err(e) => panic!("{:?}", e),
    };

    /*
    let meta = &tmp["meta"]; 
    let next_token = &tmp["next_token"]; 
    let oldest_id =  &tmp["oldest_id"];
    let result_count = &tmp["result_count"]; 

    let users = match tmp["includes"]["users"].as_array() {
        Some(x) => x,
        _ => panic!("error: unable to parse includes object from response"),
    };
    */

    let data = match tmp["data"].as_array() {
        Some(x) => x,
        _ => panic!("error: unable to parse data object from response"),
    };

    for tweet in data {
        let author_id = tweet["author_id"].to_string();
        let created_at = tweet["created_at"].to_string();
        let id = tweet["id"].to_string();
        let text = tweet["text"].to_string();

        if (author_id == "") || (created_at == "") || (id == "") || (text == "") { continue; }

        author_vec.push(author_id);
        created_vec.push(created_at);
        id_vec.push(id);
        text_vec.push(text);
    }

    let df = DataFrame::new(vec![
        Series::new("tweet_id", id_vec),
        Series::new("author_id", author_vec),
        Series::new("text", text_vec),
        Series::new("created_at", created_vec),
    ])?;

    info!("get_recent_tweets|completed");
    Ok(df)
}

/// Utility method to query counts (v2) endpoint
/// The response object is returned if valid
pub async fn get_tweet_counts(bearer_token: &str, topic: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    info!("get_tweet_counts|starting");
    info!("get_tweet_counts|topic={}", topic);

    if bearer_token == "" { 
        return Err(format!("get_tweet_counts|ERR: bearer_token is not valid, bearer_token={}", bearer_token).into()); 
    }

    if topic == "" {
        return Err(format!("get_tweet_counts|ERR: topic is not valid, topic={}", topic).into());
    }

    let url = String::from("https://api.twitter.com/2/tweets/counts/recent");

    let tw_client = reqwest::Client::new();
    let mut response = tw_client.get(&url)
        .query(&[
            ("query", topic), 
            ("granularity", "day"),
        ])
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()?;

    match response.status() {
        StatusCode::OK => info!("get_tweet_counts|query success|parse starting..."),
        s => return Err(format!("get_tweet_counts|status={}", s).into()),
    }

    let result: serde_json::Value = match response.text() {
        Ok(x) => serde_json::from_str(&x)?,
        Err(_) => return Err("get_tweet_counts|ERR: unable to parse response text".into()),
    };

    info!("get_tweet_counts|completed");
    Ok(result)
}
