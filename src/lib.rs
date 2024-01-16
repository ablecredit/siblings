#![feature(let_chains)]

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use db::Db;
use serde_derive::Deserialize;
use tokio::sync::RwLock;

#[macro_use]
extern crate log;

#[derive(Clone)]
pub struct Siblings {
    db: Arc<Db>,
    me: Option<String>, // define who is me - this has to be the template code
    endpoints: Arc<RwLock<Endpoints>>,
}

#[derive(Debug, Clone, Copy)]
pub enum Regions {
    IN,
    US,
}

impl From<&str> for Regions {
    fn from(value: &str) -> Self {
        match value {
            "IN" | "IND" => Self::IN,
            "US" | "USA" => Self::US,
            _ => panic!("Region {value} not supported"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Endpoints {
    august: Option<RegionEndpoint>,
    matrix: Option<RegionEndpoint>,
    pandora: Option<RegionEndpoint>,
    schematron: Option<RegionEndpoint>,
    sentry: Option<RegionEndpoint>,
    siblings: HashMap<String, RegionEndpoint>,
    thumbnailer: Option<RegionEndpoint>,
    xchange: Option<RegionEndpoint>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct RegionEndpoint {
    default: String,
    ind: Option<String>,
    usa: Option<String>,
}

impl RegionEndpoint {
    pub fn get(&self, region: Option<Regions>) -> Option<String> {
        if let Some(region) = region {
            match region {
                Regions::US => {
                    if let Some(us) = &self.usa {
                        return Some(us.clone());
                    }
                }
                Regions::IN => {
                    if let Some(ind) = &self.ind {
                        return Some(ind.clone());
                    }
                }
            }
        }

        Some(self.default.to_owned())
    }
}

impl Siblings {
    pub fn new(db: Arc<Db>, me: Option<&str>) -> Self {
        Self {
            me: me.map(|s| s.to_string()),
            db,
            endpoints: Arc::new(RwLock::new(Endpoints::default())),
        }
    }

    pub async fn august(&self, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(august) = &self.endpoints.read().await.august {
            return august.get(region);
        }

        if let Ok(c) = self.db.get_cache("ep-august").await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.august = Some(ep.clone());

            return ep.get(region);
        }

        warn!("august: endpoint not found and was not fetched!");
        None
    }

    pub async fn matrix(&self, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(matrix) = &self.endpoints.read().await.matrix {
            return matrix.get(region);
        }

        if let Ok(c) = self.db.get_cache("ep-matrix").await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.matrix = Some(ep.clone());

            return ep.get(region);
        }

        warn!("matrix: endpoint not found and was not fetched!");
        None
    }

    pub async fn pandora(&self, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(pandora) = &self.endpoints.read().await.pandora {
            return pandora.get(region);
        }

        if let Ok(c) = self.db.get_cache("ep-pandora").await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.pandora = Some(ep.clone());

            return ep.get(region);
        }

        warn!("pandora: endpoint not found and was not fetched!");
        None
    }

    pub async fn schematron(&self, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(schematron) = &self.endpoints.read().await.schematron {
            return schematron.get(region);
        }

        if let Ok(c) = self.db.get_cache("ep-schematron").await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.pandora = Some(ep.clone());

            return ep.get(region);
        }

        warn!("schematron: endpoint not found and was not fetched!");
        None
    }

    pub async fn sentry(&self, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(sentry) = &self.endpoints.read().await.sentry {
            return sentry.get(region);
        }

        if let Ok(c) = self.db.get_cache("ep-sentry").await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.sentry = Some(ep.clone());

            return ep.get(region);
        }

        warn!("sentry: endpoint not found and was not fetched!");
        None
    }

    pub async fn thumbnailer(&self, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(thumb) = &self.endpoints.read().await.thumbnailer {
            return thumb.get(region);
        }

        if let Ok(c) = self.db.get_cache("ep-thumbnailer").await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.thumbnailer = Some(ep.clone());

            return ep.get(region);
        }

        warn!("thumbnailer: endpoint not found and was not fetched!");
        None
    }

    pub async fn xchange(&self, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(x) = &self.endpoints.read().await.xchange {
            return x.get(region);
        }

        if let Ok(c) = self.db.get_cache("ep-xchange").await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.xchange = Some(ep.clone());

            return ep.get(region);
        }

        warn!("xchange: endpoint not found and was not fetched!");
        None
    }

    pub async fn siblings(&self, sibling: &str, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(siblingmap) = self.endpoints.read().await.siblings.get(sibling) {
            return siblingmap.get(region);
        }

        if let Ok(c) = self.db.get_cache(format!("ep-{sibling}").as_str()).await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.siblings.insert(sibling.to_owned(), ep.clone());

            return ep.get(region);
        }

        warn!("siblings: endpoint for sibling[{sibling}] not found and was not fetched!");
        None
    }

    pub async fn me(&self, region: Option<&str>) -> Option<String> {
        if let Some(me) = &self.me {
            self.siblings(me, region).await
        } else {
            None
        }
    }

    fn deserialize(data: Vec<u8>) -> Result<RegionEndpoint> {
        let ep: HashMap<String, String> = serde_json::from_slice(&data[..])?;

        Ok(RegionEndpoint {
            default: ep.get("default").unwrap().to_string(),
            ind: ep.get("in").map(|i| i.to_string()),
            usa: ep.get("us").map(|u| u.to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    // use crate::Siblings;

    use std::{collections::HashMap, env, fs::read_to_string};

    use crate::Siblings;

    #[tokio::test]
    async fn load() -> anyhow::Result<()> {
        pretty_env_logger::init();

        let db = crate::Db::new(env::var("X_PROJECT")?.as_str()).await?;
        let data = serde_json::from_str::<HashMap<String, HashMap<String, String>>>(
            read_to_string("siblings.json")?.as_str(),
        )?;

        for (k, v) in data.iter() {
            let b = serde_json::to_vec(v)?;
            db.set_cache(format!("ep-{k}").as_str(), &b[..], None)
                .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn check() -> anyhow::Result<()> {
        let db = std::sync::Arc::new(crate::Db::new(env::var("X_PROJECT")?.as_str()).await?);
        let sib = Siblings::new(db, None);

        let data = serde_json::from_str::<HashMap<String, HashMap<String, String>>>(
            read_to_string("siblings.json")?.as_str(),
        )?;

        assert_eq!(
            sib.august(Some("IN")).await.as_ref(),
            data.get("august").unwrap().get("in")
        );

        assert_eq!(
            sib.siblings("bank-statement", Some("IN")).await.as_ref(),
            data.get("bank-statement").unwrap().get("in")
        );
        assert_eq!(
            sib.siblings("bankstat", Some("IN")).await.as_ref(),
            data.get("bankstat").unwrap().get("in")
        );

        assert_eq!(
            sib.siblings("credit", Some("IN")).await.as_ref(),
            data.get("credit").unwrap().get("in")
        );

        assert_eq!(
            sib.siblings("finance-statement", Some("IN")).await.as_ref(),
            data.get("finance-statement").unwrap().get("in")
        );
        assert_eq!(
            sib.siblings("finsta", Some("IN")).await.as_ref(),
            data.get("finsta").unwrap().get("in")
        );

        assert_eq!(
            sib.siblings("gstr", Some("IN")).await.as_ref(),
            data.get("gstr").unwrap().get("in")
        );

        assert_eq!(
            sib.matrix(Some("IN")).await.as_ref(),
            data.get("matrix").unwrap().get("default")
        );

        assert_eq!(
            sib.pandora(Some("IN")).await.as_ref(),
            data.get("pandora").unwrap().get("default")
        );

        assert_eq!(
            sib.schematron(Some("IN")).await.as_ref(),
            data.get("schematron").unwrap().get("default")
        );

        assert_eq!(
            sib.sentry(Some("IN")).await.as_ref(),
            data.get("sentry").unwrap().get("default")
        );

        assert_eq!(
            sib.thumbnailer(Some("IN")).await.as_ref(),
            data.get("thumbnailer").unwrap().get("in")
        );

        assert_eq!(
            sib.xchange(Some("IN")).await.as_ref(),
            data.get("xchange").unwrap().get("default")
        );

        Ok(())
    }
}
