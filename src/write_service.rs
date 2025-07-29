use crate::load_event::PairsTableFile;
use crate::transform_event::{MintEvent, BurnEvent, SwapEvent};
use std::path::Path;
use eyre::Result;
use log::info;

pub trait WriteService {
    fn append_mint_events(&mut self, events: Vec<MintEvent>) -> Result<()>;
    fn append_burn_events(&mut self, events: Vec<BurnEvent>) -> Result<()>;
    fn append_swap_events(&mut self, events: Vec<SwapEvent>) -> Result<()>;
    fn flush_all(&mut self) -> Result<()>;
}

pub struct CsvWriteService {
    mint_writer: PairsTableFile,
    burn_writer: PairsTableFile,
    swap_writer: PairsTableFile,
    mint_buffer: Vec<MintEvent>,
    burn_buffer: Vec<BurnEvent>,
    swap_buffer: Vec<SwapEvent>,
    flush_threshold: usize,
}

impl CsvWriteService {
    pub fn new(
        output_dir: &Path,
        mint_path: &str,
        burn_path: &str,
        swap_path: &str,
        flush_threshold: usize,
    ) -> Result<Self> {
        std::fs::create_dir_all(output_dir)?;

        // A threshold of 0 would mean never flushing, so we default to 1 in that case.
        let threshold = if flush_threshold == 0 { 1 } else { flush_threshold };

        Ok(Self {
            mint_writer: PairsTableFile::new(&output_dir.join(mint_path).to_str().unwrap())?,
            burn_writer: PairsTableFile::new(&output_dir.join(burn_path).to_str().unwrap())?,
            swap_writer: PairsTableFile::new(&output_dir.join(swap_path).to_str().unwrap())?,
            mint_buffer: Vec::with_capacity(100),
            burn_buffer: Vec::with_capacity(100),
            swap_buffer: Vec::with_capacity(100),
            flush_threshold: threshold,
        })
    }


    fn check_flush(&mut self) -> Result<()> {
        if self.mint_buffer.len() >= self.flush_threshold {
            info!("Writing {} Mint events to file", self.mint_buffer.len());
            self.mint_writer.write_mint_event(&self.mint_buffer)?;
            self.mint_buffer.clear();
        }
        if self.burn_buffer.len() >= self.flush_threshold {
            info!("Writing {} Burn events to file", self.burn_buffer.len());
            self.burn_writer.write_burn_event(&self.burn_buffer)?;
            self.burn_buffer.clear();
        }
        if self.swap_buffer.len() >= self.flush_threshold {
            info!("Writing {} Swap events to file", self.swap_buffer.len());
            self.swap_writer.write_swap_event(&self.swap_buffer)?;
            self.swap_buffer.clear();
        }
        Ok(())
    }
}

impl WriteService for CsvWriteService {
    fn append_mint_events(&mut self, events: Vec<MintEvent>) -> Result<()> {
        self.mint_buffer.extend(events);
        self.check_flush()
    }

    fn append_burn_events(&mut self, events: Vec<BurnEvent>) -> Result<()> {
        self.burn_buffer.extend(events);
        self.check_flush()
    }

    fn append_swap_events(&mut self, events: Vec<SwapEvent>) -> Result<()> {
        self.swap_buffer.extend(events);
        self.check_flush()
    }

    fn flush_all(&mut self) -> Result<()> {
        if !self.mint_buffer.is_empty() {
            self.mint_writer.write_mint_event(&self.mint_buffer)?;
            self.mint_writer.flush()?;
            self.mint_buffer.clear();
        }
        if !self.burn_buffer.is_empty() {
            self.burn_writer.write_burn_event(&self.burn_buffer)?;
            self.burn_writer.flush()?;
            self.burn_buffer.clear();
        }
        if !self.swap_buffer.is_empty() {
            self.swap_writer.write_swap_event(&self.swap_buffer)?;
            self.swap_writer.flush()?;
            self.swap_buffer.clear();
        }
        Ok(())
    }
}

// Implementing Drop ensures that any remaining buffered data is written to disk
// when the CsvWriteService goes out of scope, for example, on graceful shutdown.
impl Drop for CsvWriteService {
    fn drop(&mut self) {
        info!("CsvWriteService is being dropped, flushing all remaining buffers.");
        // `flush_all` will write and flush any data remaining in the buffers.
        if let Err(e) = self.flush_all() {
            log::error!("Failed to flush buffers on drop: {}", e);
        }
    }
}
