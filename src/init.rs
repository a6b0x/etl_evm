use config::{Config, File, builder};
use eyre::{Context, Result};
use log::{LevelFilter, info};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub eth: EthCfg,
    pub log: Option<LogCfg>, // 日志配置，可选
}

#[derive(Debug, Deserialize)]
pub struct EthCfg {
    pub rpc_url: String,
    pub chain_id: u64,
    pub start_block: u64,
    pub end_block: u64,
    pub output_file: String,
}

#[derive(Debug, Deserialize)]
pub struct LogCfg {
    pub level: String,
}

pub fn init_config(config_path: &str) -> Result<AppConfig> {
    // 初始化日志
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    println!("Initializing configuration from {}...", config_path);
    let config = Config::builder()
        .add_source(File::with_name(config_path))
        .build()
        .context("Failed to build configuration")?;
    info!("Loaded configuration from {}: {:?}", config_path, config);
    // 将配置反序列化为 AppConfig 结构体
    let app_config: AppConfig = config
        .try_deserialize()
        .context("Failed to deserialize configuration")?;

    // 返回解析好的配置
    Ok(app_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_init_config() {
        let config_path = "data/etl.toml";
        let config = init_config(config_path);
        info!("Loaded configuration: {:?}", config);
    }
}
