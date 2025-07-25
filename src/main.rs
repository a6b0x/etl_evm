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

    let pair_created_event = uniswap_v2
        .get_pair_created(config.eth.start_block, config.eth.end_block)
        .await?;
    let pair_created_event1 = transform_pair_created_event(&pair_created_event)?;
    info!("Found {} PairCreated events.", pair_created_event1.len());

    let mut output_dir = config.eth.output_file.clone();
    if !output_dir.ends_with('/') {
        output_dir.push('/');
    }

    let mut pair_created_event_csv_path = output_dir.clone();
    pair_created_event_csv_path.push_str("univ2_create_event.csv");
    let mut csv_file0 = PairsTableFile::new(&pair_created_event_csv_path)?;
    csv_file0.write_pair_created_event(&pair_created_event1)?;
    info!("Wrote PairCreated events to {}", pair_created_event_csv_path);

    let mut mint_event_temp: Vec<MintEvent> = Vec::new();
    let mut burn_event_temp: Vec<BurnEvent> = Vec::new();
    let mut swap_event_temp: Vec<SwapEvent> = Vec::new();

    for event in pair_created_event1 {
        let pair_address = event.pair_address;
        let uniswap_v2_tokens =
            UniswapV2Tokens::new(pair_address, evm_block.provider.clone()).await?;

        let log3 = uniswap_v2_tokens
            .get_all_event(config.eth.start_block, config.eth.end_block)
            .await?;

        if let Some(mint_event_log) = log3.get("Mint") {
            let mint_event = transform_mint_event(mint_event_log)?;
            mint_event_temp.extend(mint_event);
        }
        if let Some(burn_event_log) = log3.get("Burn") {
            let burn_event = transform_burn_event(burn_event_log)?;
            burn_event_temp.extend(burn_event);
        }
        if let Some(swap_event_log) = log3.get("Swap") {
            let swap_event = transform_swap_event(
                swap_event_log,
                uniswap_v2_tokens.token0_decimals,
                uniswap_v2_tokens.token1_decimals,
            )?;
            swap_event_temp.extend(swap_event);
        }
    }
    info!("Found {} Mint events.", mint_event_temp.len());
    info!("Found {} Burn events.", burn_event_temp.len());
    info!("Found {} Swap events.", swap_event_temp.len());

    let mut mint_event_csv_path = output_dir.clone();
    mint_event_csv_path.push_str("univ2_mint_event.csv");
    let mut csv_file1 = PairsTableFile::new(&mint_event_csv_path)?;
    csv_file1.write_mint_event(&mint_event_temp)?;
    info!("Wrote Mint events to {}", mint_event_csv_path);

    let mut burn_event_csv_path = output_dir.clone();
    burn_event_csv_path.push_str("univ2_burn_event.csv");
    let mut csv_file2 = PairsTableFile::new(&burn_event_csv_path)?;
    csv_file2.write_burn_event(&burn_event_temp)?;
    info!("Wrote Burn events to {}", burn_event_csv_path);

    let mut swap_event_csv_path = output_dir.clone();
    swap_event_csv_path.push_str("univ2_swap_event.csv");
    let mut csv_file3 = PairsTableFile::new(&swap_event_csv_path)?;
    csv_file3.write_swap_event(&swap_event_temp)?;
    info!("Wrote Swap events to {}", swap_event_csv_path);

    Ok(())
}