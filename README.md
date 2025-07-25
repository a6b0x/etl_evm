# ETL EVM
用于以太坊生态链上数据提取、转换和加载（ETL）。

# 依赖环境
## Infuxdb
```bash
docker volume create influxdb3_data

docker run -it \
  -v influxdb3_data:/var/lib/influxdb3 \
  influxdb:3-core serve \
  --node-id host01 \
  --object-store file \
  --data-dir /var/lib/influxdb3
```
## 
```bash
cd etl_evm && ./target/debug/etl_evm univ2-event \
--rpc-url "https://reth-ethereum.ithaca.xyz/rpc" \
--router "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D" \
--from-block 22828657 \
--to-block 22828691
```
