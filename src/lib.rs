#![feature(let_chains)]

use std::{collections::HashMap, env, sync::Arc};

use anyhow::Result;
use db::Db;
use serde_derive::Deserialize;
use tokio::sync::RwLock;

#[macro_use]
extern crate log;

#[derive(Clone)]
pub struct Siblings {
    db: Arc<db::RedisPool>,
    me: Option<String>, // define who is me - this has to be the template code
    env: Env,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Env {
    Prod,
    Dev,
}

impl Env {
    pub fn new_from_env() -> Self {
        env::var("X_ENV").map_or(Self::Dev, |env| {
            let env = env.to_lowercase();
            if &env == "prod" {
                Self::Prod
            } else if &env == "dev" {
                Self::Dev
            } else {
                panic!("Siblings: invalid env: {env}");
            }
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct Endpoints {
    august: Option<RegionEndpoint>,
    bankstatement: Option<RegionEndpoint>,
    k9: Option<RegionEndpoint>,
    matrix: Option<RegionEndpoint>,
    pandora: Option<RegionEndpoint>,
    retina: Option<RegionEndpoint>,
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
                    if self.usa.is_some() {
                        return self.usa.clone();
                    }
                }
                Regions::IN => {
                    if self.ind.is_some() {
                        return self.ind.clone();
                    }
                }
            }
        }

        Some(self.default.clone())
    }
}

impl Siblings {
    pub async fn new(db: Arc<db::RedisPool>, me: Option<&str>) -> Self {
        if env::var("X_LOCAL").map_or(false, |x| x == "TRUE") {
            return Self::for_local(db, me).await;
        }
        Self {
            me: me.map(|s| s.to_string()),
            db,
            env: Env::new_from_env(),
            endpoints: Arc::new(RwLock::new(Endpoints::default())),
        }
    }

    async fn for_local(db: Arc<db::RedisPool>, me: Option<&str>) -> Self {
        let slf = Self {
            me: me.map(|s| s.to_string()),
            db,
            env: Env::new_from_env(),
            endpoints: Arc::new(RwLock::new(Endpoints::default())),
        };

        let f_iter = if let Ok(i) = dotenvy::from_filename_iter("svc.env") {
            i
        } else {
            return slf;
        };

        for item in f_iter {
            let (key, val) = item.unwrap();
            let endpoint = RegionEndpoint {
                default: format!("http://localhost:{val}"),
                ..Default::default()
            };
            if &key == "bank-statement" {
                let mut w = slf.endpoints.write().await;
                w.bankstatement = Some(endpoint);
            } else if &key == "k9" {
                let mut w = slf.endpoints.write().await;
                w.k9 = Some(endpoint);
            } else if &key == "matrix" {
                let mut w = slf.endpoints.write().await;
                w.matrix = Some(endpoint);
            } else if &key == "pandora" {
                let mut w = slf.endpoints.write().await;
                w.pandora = Some(endpoint);
            } else if key == "retina" {
                let mut w = slf.endpoints.write().await;
                w.retina = Some(endpoint);
            } else if &key == "schematron" {
                let mut w = slf.endpoints.write().await;
                w.schematron = Some(endpoint);
            } else if &key == "sentry" {
                let mut w = slf.endpoints.write().await;
                w.sentry = Some(endpoint);
            } else if &key == "thumbnailer" {
                let mut w = slf.endpoints.write().await;
                w.thumbnailer = Some(endpoint);
            } else if &key == "xchange" {
                let mut w = slf.endpoints.write().await;
                w.xchange = Some(endpoint);
            } else {
                let mut w = slf.endpoints.write().await;
                let key = key.split('_').collect::<Vec<_>>().join("-");

                w.siblings.insert(key, endpoint);
            }
        }

        slf
    }

    async fn get_cache(&self, key: &str) -> Result<Vec<u8>> {
        // Appends `dev` if target environment is dev
        let key = if self.env == Env::Dev {
            format!("dev-{key}")
        } else {
            key.to_string()
        };
        info!("get_cache.key:  {key}");
        Db::get_cache_for_pool(self.db.clone(), &key).await
    }

    pub async fn august(&self, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(august) = &self.endpoints.read().await.august {
            return august.get(region);
        }

        if let Ok(c) = self.get_cache("ep-august").await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.august = Some(ep.clone());

            return ep.get(region);
        }

        warn!("august: endpoint not found and was not fetched!");
        None
    }

    pub async fn k9(&self, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(k9) = &self.endpoints.read().await.k9 {
            return k9.get(region);
        }

        if let Ok(c) = self.get_cache("ep-k9").await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.k9 = Some(ep.clone());

            return ep.get(region);
        }

        warn!("k9: endpoint not found and was not fetched!");
        None
    }

    pub async fn retina(&self, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(k9) = &self.endpoints.read().await.k9 {
            return k9.get(region);
        }

        if let Ok(c) = self.get_cache("ep-retina").await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.retina = Some(ep.clone());

            return ep.get(region);
        }

        warn!("retina: endpoint not found and was not fetched!");
        None
    }

    pub async fn bankstatement(&self, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(bs) = &self.endpoints.read().await.bankstatement {
            return bs.get(region);
        }

        if let Ok(c) = self.get_cache("ep-bank-statement").await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.bankstatement = Some(ep.clone());

            return ep.get(region);
        }

        warn!("retina: endpoint not found and was not fetched!");
        None
    }

    pub async fn matrix(&self, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(matrix) = &self.endpoints.read().await.matrix {
            return matrix.get(region);
        }

        if let Ok(c) = self.get_cache("ep-matrix").await
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

        if let Ok(c) = self.get_cache("ep-pandora").await
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

        if let Ok(c) = self.get_cache("ep-schematron").await
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

        if let Ok(c) = self.get_cache("ep-sentry").await
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

        if let Ok(c) = self.get_cache("ep-thumbnailer").await
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

        if let Ok(c) = self.get_cache("ep-xchange").await
            && let Ok(ep) = Self::deserialize(c)
        {
            let mut w = self.endpoints.write().await;
            w.xchange = Some(ep.clone());

            return ep.get(region);
        }

        warn!("xchange: endpoint not found and was not fetched!");
        None
    }

    pub async fn sibling(&self, sibling: &str, region: Option<&str>) -> Option<String> {
        let region = region.map(|r| r.into());
        if let Some(siblingmap) = self.endpoints.read().await.siblings.get(sibling) {
            return siblingmap.get(region);
        }

        if let Ok(c) = self.get_cache(format!("ep-{sibling}").as_str()).await
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
            self.sibling(me, region).await
        } else {
            None
        }
    }

    pub async fn flush(&self) {
        let mut ep = self.endpoints.write().await;
        *ep = Endpoints::default();
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

    use anyhow::Result;

    use crate::Siblings;

    #[tokio::test]
    async fn check_prod() -> Result<()> {
        let db = std::sync::Arc::new(crate::Db::connect_redis(false).await?);
        let sib = Siblings::new(db, None).await;

        let data = serde_json::from_str::<HashMap<String, HashMap<String, String>>>(
            read_to_string("siblings.json")?.as_str(),
        )?;

        assert_eq!(
            sib.august(Some("IN")).await.as_ref(),
            data.get("august").unwrap().get("in")
        );

        assert_eq!(
            sib.sibling("bank-statement", Some("IN")).await.as_ref(),
            data.get("bank-statement").unwrap().get("in")
        );
        assert_eq!(
            sib.sibling("bankstat", Some("IN")).await.as_ref(),
            data.get("bankstat").unwrap().get("in")
        );

        assert_eq!(
            sib.sibling("credit", Some("IN")).await.as_ref(),
            data.get("credit").unwrap().get("in")
        );

        assert_eq!(
            sib.sibling("finance-statement", Some("IN")).await.as_ref(),
            data.get("finance-statement").unwrap().get("in")
        );
        assert_eq!(
            sib.sibling("finsta", Some("IN")).await.as_ref(),
            data.get("finsta").unwrap().get("in")
        );

        assert_eq!(
            sib.sibling("gstr", Some("IN")).await.as_ref(),
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

    #[tokio::test]
    async fn check_dev() -> Result<()> {
        pretty_env_logger::init();

        let db = std::sync::Arc::new(crate::Db::connect_redis(true).await?);
        env::set_var("X_ENV", "dev");

        let sib = Siblings::new(db, None).await;

        let data = serde_json::from_str::<HashMap<String, HashMap<String, String>>>(
            read_to_string("siblings.json")?.as_str(),
        )?;

        assert_eq!(
            sib.august(Some("IN")).await.as_ref(),
            data.get("august").unwrap().get("in")
        );

        assert_eq!(
            sib.sibling("bank-statement", Some("IN")).await.as_ref(),
            data.get("bank-statement").unwrap().get("in")
        );
        assert_eq!(
            sib.sibling("bankstat", Some("IN")).await.as_ref(),
            data.get("bankstat").unwrap().get("in")
        );

        assert_eq!(
            sib.sibling("credit", Some("IN")).await.as_ref(),
            data.get("credit").unwrap().get("in")
        );

        assert_eq!(
            sib.sibling("finance-statement", Some("IN")).await.as_ref(),
            data.get("finance-statement").unwrap().get("in")
        );
        assert_eq!(
            sib.sibling("finsta", Some("IN")).await.as_ref(),
            data.get("finsta").unwrap().get("in")
        );

        assert_eq!(
            sib.sibling("gstr", Some("IN")).await.as_ref(),
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

    #[tokio::test]
    async fn check_local() -> Result<()> {
        let db = std::sync::Arc::new(crate::Db::connect_redis(false).await?);
        let sib = Siblings::new(db.clone(), None).await;

        let data = serde_json::from_str::<HashMap<String, HashMap<String, String>>>(
            read_to_string("siblings.json")?.as_str(),
        )?;

        assert_eq!(
            sib.august(Some("IN")).await.as_ref(),
            data.get("august").unwrap().get("in")
        );

        assert_eq!(
            sib.sibling("bank-statement", Some("IN")).await.as_ref(),
            data.get("bank-statement").unwrap().get("in")
        );
        assert_eq!(
            sib.sibling("bankstat", Some("IN")).await.as_ref(),
            data.get("bankstat").unwrap().get("in")
        );

        assert_eq!(
            sib.sibling("credit", Some("IN")).await.as_ref(),
            Some(&"http://localhost:8080".to_string())
        );

        let sib = Siblings::new(db, Some("credit")).await;

        assert_eq!(sib.me, Some("credit".to_string()));

        assert_eq!(
            sib.me(Some("IN")).await.as_ref(),
            Some(&"http://localhost:8080".to_string())
        );

        Ok(())
    }
}
