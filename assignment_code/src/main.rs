use std::{error::Error, collections::HashMap};

use futures::TryStreamExt;
use mongodb::{Client, options::ClientOptions};
use serde::{Serialize, Deserialize};

#[tokio::main]
async fn main() {
    if let Ok(client) = connect_db().await {
        println!("success!");
        let collection = client.database("twitter").collection::<tweet>("tweets");
        let mut tweets_cursor = collection
        .find(None, None)
        .await
        .ok()
        .expect("failed to fetch tweets");

        let mut hashtags: HashMap<String, u32> = HashMap::new();

        while let Ok(tweet_opt) = tweets_cursor.try_next().await {
            match tweet_opt {
                Some(tweet) => {
                    for hashtag in tweet.entities.hashtags {
                        *hashtags.entry(hashtag.text).or_insert(1) += 1;
                    }
                }
                _ => {}
            }
        };

        let mut hashtags_vec: Vec<(&String, &u32)> = hashtags.iter().collect();
        hashtags_vec.sort_by(|a, b| b.1.cmp(a.1));
        for (key, val) in hashtags_vec {
            println!("tag: {key} - count: {val}");
        }
    } else {
        println!("Connection failed!");
    }
}

async fn connect_db() -> Result<Client, Box<dyn Error>>{
    let mut client_options = ClientOptions::parse("mongodb://root:example@localhost:27017").await?;
    let client = Client::with_options(client_options)?;

    return Ok(client);
}

#[derive(Serialize, Deserialize)]
struct entities {
    hashtags: Vec<hashtag>
}

#[derive(Serialize, Deserialize)]
struct tweet {
    entities: entities
}

#[derive(Serialize, Deserialize)]
struct hashtag {
    text: String
}