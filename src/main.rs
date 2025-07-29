use alloy::primitives::Address;
use clap::Parser;
use eyre::Result;
use log::{debug, info};
use std::path::Path;
use std::str::FromStr;
use futures::StreamExt;

mod extract_block;
mod extract_event;
mod init;
mod load_block;
mod load_event;
mod transform_block;
mod transform_event;

use crate::{
    extract_block::EvmBlock,
    extract_event::{
        UniswapV2, UniswapV2MultiPair, UniswapV2Tokens, BURN_EVENT_SIGNATURE,
        MINT_EVENT_SIGNATURE, SWAP_EVENT_SIGNATURE,
    },
    init::AppConfig,
    load_event::{PairsTableFile, PairsTableTsdb},
    transform_event::{
        transform_burn_event, transform_mint_event, transform_pair_created_event,
        transform_swap_event, BurnEvent, MintEvent, SwapEvent,
    },
};

#[derive(Parser, Debug)]
#[command(name = "etl_evm")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    #[command(name = "getUniSwapV2Event")]
    GetUniv2Event(Univ2EventArgs),
    #[command(name = "subscribe_uniswapv2_event")]
    SubscribeUniv2Event(SubscribeUniv2EventArgs),
}

#[derive(Parser, Debug)]
struct Univ2EventArgs {
    #[arg(long)]
    http_url: Option<String>,
    #[arg(long)]
    from_block: Option<u64>,
    #[arg(long)]
    to_block: Option<u64>,
    #[arg(long)]
    router_address: Option<String>,
    #[arg(long)]
    output_dir: Option<String>,
}

#[derive(Parser, Debug)]
struct SubscribeUniv2EventArgs {
    #[arg(long)]
    ws_url: Option<String>,
    #[arg(long)]
    output_dir: Option<String>,
    #[arg(long)]
    pair_address: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let app_config = AppConfig::new()?;
    let _ = app_config.init_log()?;
    debug!("Parsed CLI arguments: {:#?}", cli);

    match cli.command {
        Commands::GetUniv2Event(args) => {
            let args_is_full = args.http_url.is_some()
                && args.router_address.is_some()
                && args.from_block.is_some()
                && args.to_block.is_some();

            let app_config = if args_is_full {
                info!("Using CLI arguments");
                AppConfig::from_univ2_event_cli(&args)?
            } else {
                info!("args is not full, Using default config from data/etl.toml");
                AppConfig::from_file("data/etl.toml")?
            };
            info!("app_config: {:#?}", app_config);
            get_univ2_event(&app_config).await?;
        }
        Commands::SubscribeUniv2Event(args) => {
            let is_pure_cli_mode = args.ws_url.is_some() && !args.pair_address.is_empty();

            let app_config = if is_pure_cli_mode {
                info!("Running in pure CLI mode (config file will be ignored).");
                AppConfig::from_subscribe_cli(&args)?
            } else {
                info!("args is not full, Using default config from data/etl.toml");
                AppConfig::from_file("data/etl.toml")?
            };

            info!("app_config: {:#?}", app_config);
            subscribe_univ2_event(&app_config).await?;
        }
    }

    Ok(())
}

async fn get_univ2_event(config: &AppConfig) -> Result<()> {
    let evm_block = EvmBlock::new(&config.eth.http_url).await?;
    let router_address = Address::from_str(&config.uniswap_v2.router_address)?;
    let uniswap_v2 = UniswapV2::new(evm_block.provider.clone(), router_address).await;

    let pair_created_logs = uniswap_v2
        .get_pair_created(config.uniswap_v2.from_block, config.uniswap_v2.to_block)
        .await?;
    let pair_created_events = transform_pair_created_event(&pair_created_logs)?;
    let output_dir = Path::new(&config.csv.output_dir);
    let output_file = output_dir.join("univ2_create_event.csv");

    let mut csv_file0 =
        PairsTableFile::new(output_file.to_str().unwrap())?;
    csv_file0.write_pair_created_event(&pair_created_events)?;
    info!("Wrote {} Pair Created events to {:?}.", pair_created_events.len(), output_file);

    let mut all_mint_events: Vec<MintEvent> = Vec::new();
    let mut all_burn_events: Vec<BurnEvent> = Vec::new();
    let mut all_swap_events: Vec<SwapEvent> = Vec::new();

    for event in pair_created_events {
        let pair_address = event.pair_address;
        let uniswap_v2_tokens =
            UniswapV2Tokens::new(pair_address, evm_block.provider.clone()).await?;

        let log3 = uniswap_v2_tokens
            .get_all_event(config.uniswap_v2.from_block, config.uniswap_v2.to_block)
            .await?;

        if let Some(mint_event_log) = log3.get("Mint") {
            let mint_events = transform_mint_event(mint_event_log)?;
            all_mint_events.extend(mint_events);
        }
        if let Some(burn_event_log) = log3.get("Burn") {
            let burn_events = transform_burn_event(burn_event_log)?;
            all_burn_events.extend(burn_events);
        }
        if let Some(swap_event_log) = log3.get("Swap") {
            let swap_events = transform_swap_event(
                swap_event_log,
                uniswap_v2_tokens.token0_decimals,
                uniswap_v2_tokens.token1_decimals,
            )?;
            all_swap_events.extend(swap_events);
        }
    }

    let file_mint = output_dir.join("univ2_mint_event.csv");
    let mut csv_file1 =
        PairsTableFile::new(file_mint.to_str().unwrap())?;
    csv_file1.write_mint_event(&all_mint_events)?;
    info!("Wrote {} Mint events to {:?}.", all_mint_events.len(), file_mint);
    let file_burn = output_dir.join("univ2_burn_event.csv");
    let mut csv_file2 =
        PairsTableFile::new(file_burn.to_str().unwrap())?;
    csv_file2.write_burn_event(&all_burn_events)?;
    info!("Wrote {} Burn events to {:?}.", all_burn_events.len(), file_burn);
    let file_swap = output_dir.join("univ2_swap_event.csv");
    let mut csv_file3 =
        PairsTableFile::new(file_swap.to_str().unwrap())?;
    csv_file3.write_swap_event(&all_swap_events)?;
    info!("Wrote {} Swap events to {:?}.", all_swap_events.len(), file_swap);

    Ok(())
}


async fn subscribe_univ2_event(config: &AppConfig) -> Result<()> {
    let provider = EvmBlock::new(&config.eth.ws_url).await?.provider;
    let pair_addresses = config.uniswap_v2.pair_address
        .as_ref()
        .ok_or_else(|| eyre::eyre!("Missing pair addresses in config"))?
        .iter()
        .map(|s| Address::from_str(s))
        .collect::<Result<Vec<_>, _>>()?;

    let multi_pair = UniswapV2MultiPair::new(
        provider,
        pair_addresses
    ).await?;


    let mut stream = multi_pair.subscribe_all_events().await?;

    let output_dir = Path::new(&config.csv.output_dir);
    std::fs::create_dir_all(output_dir)?;
    let file_mint = output_dir.join("subscribed_mint_events.csv");
    let mut csv_writer_mint = PairsTableFile::new(file_mint.to_str().unwrap())?;
    let file_burn = output_dir.join("subscribed_burn_events.csv");
    let mut csv_writer_burn = PairsTableFile::new(file_burn.to_str().unwrap())?;
    let file_swap = output_dir.join("subscribed_swap_events.csv");
    let mut csv_writer_swap = PairsTableFile::new(file_swap.to_str().unwrap())?;

    info!("Listening for Mint, Burn, and Swap events...");
    while let Some(log) = stream.next().await {
        let event_signature = if log.topics().is_empty() { continue; } else { log.topics()[0] };
        let pair_address = log.address();   

        match event_signature {
            
            sig if sig == MINT_EVENT_SIGNATURE => {
                let mint_events = transform_mint_event(&[log])?;
                csv_writer_mint.write_mint_event(&mint_events)?;
                info!("Stored 1 Mint event from pair {}", pair_address);
            }
            sig if sig == BURN_EVENT_SIGNATURE => {
                let burn_events = transform_burn_event(&[log])?;
                csv_writer_burn.write_burn_event(&burn_events)?;
                info!("Stored 1 Burn event from pair {}", pair_address);
            }
            sig if sig == SWAP_EVENT_SIGNATURE => {
                if let Some(pair_info) = multi_pair.pairs.get(&pair_address) {
                    let swap_events = transform_swap_event(&[log.clone()], pair_info.token0.decimals, pair_info.token1.decimals)?;
                    csv_writer_swap.write_swap_event(&swap_events)?;
                    info!("Stored 1 Swap event from pair {}", pair_address);
                }
            }
            _ => {
                debug!("Ignoring unknown event with signature: {}", event_signature);
            }
        }
    }

    Ok(())
}
