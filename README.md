# ETL EVM
用于以太坊生态链上数据提取、转换和加载（ETL）。

# 从历史数据到CSV

```bash
# 不带参数，默认从配置文件读取，`./data/etl.toml`。
cargo run -- get_uniswapv2_event_csv

# 最小参数集，默认存储在当前文件夹
cargo run -- get_uniswapv2_event_csv \
--http-url "https://reth-ethereum.ithaca.xyz/rpc" \
--router-address "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D" \
--from-block 22828657 \
--to-block 22828691 \

# 最全参数集合
cargo run -- get_uniswapv2_event_csv \
--http-url "https://reth-ethereum.ithaca.xyz/rpc" \
--router-address "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D" \
--from-block 22828657 \
--to-block 22828691 \
--output-dir "./data"

```
# 从实时数据到CSV

```bash
# 不带参数，默认从配置文件读取，`./data/etl.toml`。
cargo run -- subscribe_uniswapv2_event_csv

# 最小参数集，默认存储在当前文件夹
cargo run -- subscribe_uniswapv2_event_csv \
    --ws-url "wss://reth-ethereum.ithaca.xyz/ws" \
    --pair-address 0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc \
    --pair-address 0xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11

# 最全参数集合
cargo run -- subscribe_uniswapv2_event_csv \
    --ws-url "wss://reth-ethereum.ithaca.xyz/ws" \
    --pair-address 0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc \
    --pair-address 0xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11 \
    --output-dir "./data"
```

提示`tls handshake eof`就等会再试

# 从实时数据到Tsdb
```bash
cargo run -- subscribe_uniswapv2_event_db 

cargo run -- subscribe_uniswapv2_event_db \
    --ws-url "wss://reth-ethereum.ithaca.xyz/ws" \
    --pair-address 0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc \
    --pair-address 0xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11 \
    --write-url "http://tsdb:8181/api/v3/write_lp?db=evm_uniswap_v2" \
    --auth-token "apiv3_di3lJBckgHFT2cJc5VLkKwsWsVEwI3XZsefjifwwLNR8kruGfhazhZ3tGBvIPZIquaFlbnqHJgTDdaLUFgIzrw"
```


# 数据存储
## Infuxdb（可选）
```bash
docker volume create influxdb3_data

docker run -it \
  -v influxdb3_data:/var/lib/influxdb3 \
  influxdb:3-core serve \
  --node-id host01 \
  --object-store file \
  --data-dir /var/lib/influxdb3
```