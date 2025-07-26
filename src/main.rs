use alloy::primitives::Address;
use clap::Parser;
use eyre::Result;
use log::info;
use std::str::FromStr;

mod extract_block;
mod extract_event;
mod init;
mod load_block;
mod load_event;
mod transform_block;
mod transform_event;

use crate::{
    extract_block::EvmBlock,
    extract_event::{UniswapV2, UniswapV2Tokens},
    init::AppConfig,
    load_event::PairsTableFile,
    transform_event::{
        transform_burn_event, transform_mint_event, transform_pair_created_event,
        transform_swap_event, BurnEvent, MintEvent, SwapEvent,
    },
};

#[derive(Parser, Debug)]
#[command(name = "etl_evm", version = "0.1.0", author = "Gemini")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// ETL for Uniswap V2 events
    Univ2Event(Univ2EventArgs),
}

#[derive(Parser, Debug)]
struct Univ2EventArgs {
    /// The HTTP RPC URL of the Ethereum node.
    #[arg(long, short = 'u')]
    rpc_url: Option<String>,

    /// The contract address of the Uniswap V2 Router.
    #[arg(long, short = 'r')]
    router: Option<Address>,

    /// The starting block number.
    #[arg(long, short = 'f')]
    from_block: Option<u64>,

    /// The ending block number.
    #[arg(long, short = 't')]
    to_block: Option<u64>,

    /// The directory to output CSV files.
    #[arg(long, short = 'o')]
    output_dir: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut app_config = AppConfig::new()?;
    let _log_level = app_config.init_log()?;

    match cli.command {
        Commands::Univ2Event(args) => {
            // Merge CLI arguments into AppConfig
            if let Some(rpc_url) = args.rpc_url {
                app_config.eth.http_url = rpc_url;
            }
            if let Some(router) = args.router {
                app_config.uniswap_v2.router_address = router.to_string();
            }
            if let Some(from_block) = args.from_block {
                app_config.eth.start_block = from_block;
            }
            if let Some(to_block) = args.to_block {
                app_config.eth.end_block = to_block;
            }
            if let Some(output_dir) = args.output_dir {
                app_config.eth.output_file = output_dir; // Assuming output_dir maps to a field in AppConfig
            }

            info!("Starting Uniswap V2 event ETL with config: {:#?}", app_config);
            run_univ2_event_etl(&app_config).await?;
        }
    }

    Ok(())
}

async fn run_univ2_event_etl(config: &AppConfig) -> Result<()> {
    let evm_block = EvmBlock::new(&config.eth.http_url).await?;
    let router_address = Address::from_str(&config.uniswap_v2.router_address)?;
    let uniswap_v2 = UniswapV2::new(evm_block.provider.clone(), router_address).await;

    let pair_created_logs = uniswap_v2
        .get_pair_created(config.eth.start_block, config.eth.end_block)
        .await?;
    let pair_created_events = transform_pair_created_event(&pair_created_logs)?;
    info!("Found {} PairCreated events.", pair_created_events.len());

    let mut csv_file0 = PairsTableFile::new("data/univ2_create_event.csv")?;
    csv_file0.write_pair_created_event(&pair_created_events)?;

    let mut all_mint_events: Vec<MintEvent> = Vec::new();
    let mut all_burn_events: Vec<BurnEvent> = Vec::new();
    let mut all_swap_events: Vec<SwapEvent> = Vec::new();

    for event in pair_created_events {
        let pair_address = event.pair_address;
        let uniswap_v2_tokens =
            UniswapV2Tokens::new(pair_address, evm_block.provider.clone()).await?;

        let log3 = uniswap_v2_tokens
            .get_all_event(config.eth.start_block, config.eth.end_block)
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

    let mut csv_file1 = PairsTableFile::new("data/univ2_mint_event.csv")?;
    csv_file1.write_mint_event(&all_mint_events)?;

    let mut csv_file2 = PairsTableFile::new("data/univ2_burn_event.csv")?;
    csv_file2.write_burn_event(&all_burn_events)?;

    let mut csv_file3 = PairsTableFile::new("data/univ2_swap_event.csv")?;
    csv_file3.write_swap_event(&all_swap_events)?;


    Ok(())
}