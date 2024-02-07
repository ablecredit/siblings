use std::{collections::HashMap, env, fs::read_to_string};

use anyhow::Result;

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    load().await.unwrap();
}

async fn load() -> Result<()> {
    let isdev = env::var("X_ENV").map_or(false, |e| e == "dev");

    let db = db::Db::new(env::var("X_PROJECT")?.as_str()).await?;
    let data = serde_json::from_str::<HashMap<String, HashMap<String, String>>>(
        read_to_string( if isdev { "siblings-dev.json" } else { "siblings.json" })?.as_str(),
    )?;

    for (k, v) in data.iter() {
        let b = serde_json::to_vec(v)?;
        
        let key = if isdev {
            format!("dev-ep-{k}")
        } else {
            format!("ep-{k}")
        };

        db.set_cache(&key, &b[..], None)
            .await?;
    }
    Ok(())
}