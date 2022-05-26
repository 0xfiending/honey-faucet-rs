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

pub async fn get_response(
    bearer_token: &str, 
    url: &str, 
    params: Vec<(&str, &str)>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    info!("get_response|starting");

    let tw_client = reqwest::Client::new();
    let mut response = tw_client.get(url)
        .query(&params)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()?;

    match response.status() {
        StatusCode::OK => info!("get_response|query success"),
        s => return Err(format!("get_response|status={}", s).into()),
    }

    let result: serde_json::Value = match response.text() {
        Ok(x) => serde_json::from_str(&x)?,
        Err(e) => return Err(format!("get_response|ERR: unable to parse response object|e={}", e).into()),
    };

    info!("get_response|completed");
    Ok(result)
}


/// Utility method to query the users_lookup (v2) endpoint
pub async fn users_lookup(bearer_token: &str, username: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    info!("users_lookup|starting");

    if bearer_token == "" {
        return Err(format!("users_lookup|ERR: bearer token is not valid, bearer_token={}", bearer_token).into());
    }

    if username == "" {
        return Err(format!("users_lookup|ERR: username is not valid, username={}", username).into());
    }

    let url = String::from("https://api.twitter.com/2/users/by");
    info!("users_lookup|url={}", url);

    let params = vec![("usernames", username)];
    let result = get_response(bearer_token, &url, params).await?;

    info!("users_lookup|completed");
    Ok(result)
}


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

    let params = vec![
        ("expansions", "author_id"),
        ("tweet.fields", "author_id,created_at,text"),
        ("max_results", "100"),
    ];

    let result = get_response(&bearer_token, &url, params).await?;

    let data = match result["data"].as_array() {
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

    let params = vec![
        ("expansions", "author_id"),
        ("tweet.fields", "author_id,created_at,text"),
        ("max_results", "100"),
    ];

    let result = get_response(&bearer_token, &url, params).await?;

    let data = match result["data"].as_array() {
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

    let params = vec![
        ("expansions", "author_id"),
        ("tweet.fields", "author_id,created_at,text"),
        ("user.fields", "name,username"),
    ];

    let result = get_response(&bearer_token, &url, params).await?;

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

    let params = vec![
        ("query", topic),
        ("tweet.fields", "author_id,created_at,id,text"),
        ("user.fields", "name,username"),
        ("max_results", "100"),
    ];

    let result = get_response(&bearer_token, &url, params).await?;    

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

    let data = match result["data"].as_array() {
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


    let params = vec![
        ("query", topic),
        ("granularity", "day"),
    ];

    let result = get_response(&bearer_token, &url, params).await?;    

    info!("get_tweet_counts|completed");
    Ok(result)
}
