use alloy::primitives::Address;
use config::{Config, File};
use eyre::{Context, Result};
use log::LevelFilter;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub eth: EthCfg,
    pub log: Option<LogCfg>,
    pub tsdb: TsdbCfg,
    pub uniswap_v2: UniV2Cfg,
    pub csv: CsvCfg,
}

#[derive(Debug, Deserialize)]
pub struct EthCfg {
    pub http_url: String,
    pub ws_url: String,
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
    pub from_block: u64,
    pub to_block: u64,
    pub pair_address: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct CsvCfg {
    pub output_dir: String,
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
    pub fn from_file(file_path: &str) -> Result<Self> {
        let config = Config::builder()
            .add_source(File::with_name(file_path))
            .build()?;
        config.try_deserialize().context("Failed to read config file")
    }

    pub fn from_univ2_event_cli(args: &crate::Univ2EventArgs) -> Result<Self> {
        Ok(Self {
            eth: EthCfg {
                http_url: args.http_url.clone().unwrap_or_default(),
                ws_url: "".to_string(), 
            },
            uniswap_v2: UniV2Cfg {
                router_address: args.router_address.clone().unwrap_or_default(),
                from_block: args.from_block.unwrap_or(0),
                to_block: args.to_block.unwrap_or(0),
                pair_address: None,
            },
            csv: CsvCfg {
                output_dir:args.output_dir.clone().unwrap_or_else(|| "./".into()),
            },
            log: None,
            tsdb: TsdbCfg {
                query_url: String::new(),
                write_url: String::new(),
                auth_token: String::new(),
            },
        })
    }

    pub fn from_subscribe_cli(args: &crate::SubscribeUniv2EventArgs) -> Result<Self> {
        Ok(Self {
            eth: EthCfg {
                ws_url: args.ws_url.clone().unwrap(),
                http_url: String::new(),
            },
            uniswap_v2: UniV2Cfg {
                pair_address: Some(args.pair_address.clone()),
                router_address: String::new(),
                from_block: 0,
                to_block: 0,
            },
            csv: CsvCfg {
                output_dir: args.output_dir.clone().unwrap_or_else(|| "./data".to_string()),
            },
            log: None, 
            tsdb: TsdbCfg { 
                query_url: String::new(),
                write_url: String::new(),
                auth_token: String::new(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::info;

    #[test]
    fn test_app_config() {
        let app_config = AppConfig::new().unwrap();
        _ = app_config.init_log();
        info!("app_config : {:#?}", app_config);

        let app_cfg_from_file = AppConfig::from_file("data/etl.toml").unwrap();
        info!("app_cfg_from_file : {:#?}", app_cfg_from_file);
    }
}
