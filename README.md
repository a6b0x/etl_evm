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
## 测试驱动 增量开发
- 搭建开发框架 250625
- 解析配置文件并打印日志 250626
- 重构初始化模块 250626
- 获取以太最新区块号 250626
- 抽象以太RPC连接器 250626
- 获取最新区块的交易数据 250628
- 存储最新区块数据为CSV格式 250629
- 通过ABI方式读取UniswapV2交易对数量 250630
- 封装以太RPC连接器 250701
- 获取给定区块范围内UniswapV2的合约创建事件 250702
- 获取代币对的流动性事件 250703
- 封装UniswapV2执行器 250704
- 转换UniswapV2事件日志 250705
- 存储UniswapV2事件 250706
- 拆分UniswapV2数据表结构 250707
