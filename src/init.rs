use config::{Config, File};
use eyre::{Context, Result};
use log::LevelFilter;
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub eth: EthCfg,
    pub log: Option<LogCfg>,
    pub tsdb: TsdbCfg,
    pub uniswap_v2: UniV2Cfg,
}

#[derive(Debug, Deserialize)]
pub struct EthCfg {
    pub http_url: String,
    pub ws_url: String,
    pub chain_id: u64,
    pub start_block: u64,
    pub end_block: u64,
    pub output_file: String,
}

#[derive(Debug, Deserialize)]
pub struct LogCfg {
    pub level: String,
}

#[derive(Debug, Deserialize)]
pub struct TsdbCfg {
    pub query_url: String,
    pub write_url: String,
    pub auth_token: String,
}

#[derive(Debug, Deserialize)]
pub struct UniV2Cfg {
    pub router_address: String,
}

impl AppConfig {
    pub fn new() -> Result<Self> {
        let config_path = "data/etl.toml";
        let config = Config::builder()
            .add_source(File::with_name(config_path))
            .build()
            .context("Failed to build configuration")?;

        let app_config: AppConfig = config
            .try_deserialize()
            .context("Failed to deserialize configuration")?;

        Ok(app_config)
    }
    pub fn init_log(&self) -> Result<LevelFilter> {
        let log_level = match &self.log {
            Some(log_cfg) => match log_cfg.level.to_lowercase().as_str() {
                "error" => LevelFilter::Error,
                "warn" => LevelFilter::Warn,
                "info" => LevelFilter::Info,
                "debug" => LevelFilter::Debug,
                "trace" => LevelFilter::Trace,
                _ => LevelFilter::Info,
            },
            None => LevelFilter::Info,
        };
        env_logger::Builder::new().filter_level(log_level).init();
        Ok(log_level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::info;

    #[test]
    fn test_app_config() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config : {:#?}", app_config);
    }
}
