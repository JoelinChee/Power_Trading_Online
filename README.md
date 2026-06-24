# Power Trading Online

一个基于 Python 的电力交易系统基础骨架，采用三段式 boot 架构，并通过 Kafka + Protobuf 进行异步消息编排。

- `data_boot`：负责数据采集、天气消息发布。
- `forecast_boot`：负责消费天气消息并生成预测消息。
- `execution_boot`：负责消费预测消息并执行风控与交易决策。

项目同时支持两种运行模式：

1. 开发模式（源码 + Python 解释器）
2. 发布模式（二进制可执行包，无 `.py` 源文件）

---

## 1. 项目目标

本项目用于构建一个可本地联调、可打包发布、可验证链路的电力交易系统雏形，重点覆盖：

- 业务服务拆分与服务边界清晰化
- Kafka 异步消息链路
- Protobuf 跨服务消息协议
- 本地一键启动/停止
- 二进制发布流程（包含 Kafka 运行时）

---

## 2. 整体架构

### 2.1 三个 Boot 服务

1. `data_boot`
- 提供数据接口
- 产出天气消息到 Kafka

2. `forecast_boot`
- 消费天气消息
- 产出预测消息到 Kafka

3. `execution_boot`
- 消费预测消息
- 执行风控判断与交易单生成

### 2.2 消息链路

当前链路如下：

1. `data_boot` 发布 `power_trading.weather.events`（`weather.proto`）
2. `forecast_boot` 订阅 `power_trading.weather.events`
3. `forecast_boot` 发布 `power_trading.forecast.events`（`trading_messages.proto`）
4. `execution_boot` 订阅 `power_trading.forecast.events`

---

## 3. 技术栈

- Python 3.8+
- FastAPI + Uvicorn
- Pydantic v2
- confluent-kafka
- protobuf
- grpcio-tools
- APScheduler
- PyInstaller（用于发布模式二进制构建）

---

## 4. 目录说明

```text
.
|-- boots/
|   |-- common/
|   |-- data_boot/
|   |-- forecast_boot/
|   `-- execution_boot/
|-- config/
|   |-- common/
|   |-- data_boot/
|   |-- forecast_boot/
|   `-- execution_boot/
|-- environment/
|   `-- requirements.txt
|-- generated/
|   |-- kafka-local/
|   |-- logs/
|   |-- pids/
|   |-- pycache/
|   |-- trading_messages_pb2.py
|   `-- weather_pb2.py
|-- infrastructure/
|   |-- kafka/
|   |-- loaders/
|   |-- logging/
|   `-- scheduler/
|-- pub_interfaces/
|   |-- trading_messages.proto
|   `-- weather.proto
|-- scripts/
|   |-- compile_protos.sh
|   |-- install_environment.sh
|   |-- start_all.sh
|   |-- stop_all.sh
|   |-- start_local_kafka.sh
|   |-- stop_local_kafka.sh
|   |-- release_version.sh
|   |-- package_release.sh
|   `-- test_release_package.sh
|-- VERSION
|-- .env.example
`-- README.md
```

说明：

- 运行产物统一写入 `generated/`。
- 发布产物统一写入 `generated/releases/`。
- 发布包内同样包含 `generated/kafka-local/kafka_2.13-3.7.1`，保证离线启动。

---

## 5. 配置体系

### 5.1 配置文件位置

- 公共配置：`config/common/`
- boot 专属配置：`config/data_boot/`、`config/forecast_boot/`、`config/execution_boot/`

### 5.2 运行时配置根目录

为了兼容二进制发布，配置加载器支持 `POWER_TRADING_HOME`。

- 开发模式：默认按源码目录解析。
- 发布模式：`scripts/start_all.sh` 会导出 `POWER_TRADING_HOME=<解压目录>`，从包内 `config/` 读取配置。

---

## 6. 开发模式快速开始

### 6.1 首次安装环境

```bash
./scripts/install_environment.sh
```

该脚本会自动：

1. 初始化 `.env`
2. 清理旧缓存/旧崩溃日志
3. 安装 `environment/requirements.txt`
4. 编译 proto 到 `generated/`
5. 预热本地 Kafka 运行时

### 6.2 手动安装（可选）

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r environment/requirements.txt
```

### 6.3 编译 Proto

```bash
./scripts/compile_protos.sh
```

### 6.4 启动全栈

```bash
./scripts/start_all.sh
```

默认端口：

- data_boot: `8001`
- forecast_boot: `8002`
- execution_boot: `8003`

### 6.5 停止全栈

```bash
./scripts/stop_all.sh
```

### 6.6 健康检查

```bash
curl -s http://127.0.0.1:8001/health
curl -s http://127.0.0.1:8002/health
curl -s http://127.0.0.1:8003/health
```

---

## 7. 接口概览

### 7.1 Data Boot

- `GET /health`
- `GET /api/v1/data/current`
- `POST /api/v1/data/ingest`
- `GET /api/v1/data/pipeline-status`

### 7.2 Forecast Boot

- `GET /health`
- `GET /api/v1/forecast/pipeline-status`

### 7.3 Execution Boot

- `GET /health`
- `POST /api/v1/execution/risk-check`
- `POST /api/v1/execution/trade-orders`
- `GET /api/v1/execution/pipeline-status`

---

## 8. 联调与冒烟测试

## 9. Kafka 数据录制与回灌

为了支持问题复现与离线回放，提供一组对标 `rostopic` / `rosbag` 的 Kafka 工具脚本：

- `scripts/kafka/kafka_topic_list.sh`：列出 Kafka topics。
- `scripts/kafka/kafka_topic_echo.sh`：实时打印单个或多个 topic。
- `scripts/kafka/kafka_record.py`：录制单个、多个或全部 topic 到二进制 `.bin` 文件。
- `scripts/kafka/kafka_play.py`：按录制间隔、全速或限速回放到 Kafka。
- `scripts/kafka/kafka_info.sh`：查看录制文件大小、消息数、topic 分布和预览。

建议先设置 broker 环境变量：

```bash
export BROKERS=127.0.0.1:9092
```

列出 topic：

```bash
scripts/kafka/kafka_topic_list.sh
```

实时打印 topic：

```bash
scripts/kafka/kafka_topic_echo.sh power_trading.weather.events
```

默认会加载 `generated/*_pb2.py`，按 topic 将 Protobuf payload 解析成缩进 JSON；连续消息之间用 `---` 分隔，风格接近 `rostopic echo`。确实需要原始 payload 时可显式开启：

```bash
scripts/kafka/kafka_topic_echo.sh --raw power_trading.weather.events
```

如果需要单行 JSON，使用：

```bash
scripts/kafka/kafka_topic_echo.sh --compact power_trading.weather.events
```

录制示例（录制 200 条天气事件）：

```bash
scripts/kafka/kafka_record.sh \
	power_trading.weather.events \
	--output generated/recordings/weather.bin \
	--max-messages 200 \
	--from-beginning
```

录制示例（录制多个 topic，从历史开头读到当前末尾后退出）：

```bash
scripts/kafka/kafka_record.sh \
	power_trading.weather.events,power_trading.forecast.events \
	--output generated/recordings/power_trading.bin \
	--from-beginning \
	--end-on-eof
```

录制示例（录制所有非内部 topic）：

```bash
scripts/kafka/kafka_record.sh \
	--all-topics \
	--output generated/recordings/all_topics.bin \
	--from-beginning \
	--end-on-eof
```

查看录制文件信息：

```bash
scripts/kafka/kafka_info.sh generated/recordings/all_topics.bin
```

按录制时的消息间隔回放到原 topic（默认，类似 `rosbag play`）：

```bash
scripts/kafka/kafka_play.sh \
	--input generated/recordings/all_topics.bin
```

全速回放到原 topic：

```bash
scripts/kafka/kafka_play.sh \
	--input generated/recordings/all_topics.bin \
	--full-speed
```

限速回放到隔离 topic：

```bash
scripts/kafka/kafka_play.sh \
	--input generated/recordings/weather.bin \
	--target-topic power_trading.weather.events.replay \
	--rate 20
```

说明：

- 录制文件使用长度分帧二进制格式，保留原始 `key/value` 字节与 `headers`、`timestamp`、`offset`。
- 录制支持单 topic、多个 topic 和全 topic（`--all-topics`）。
- 回放默认按原始消息时间戳间隔发送；`--full-speed` 可全速回放，`--rate` 可固定速率回放。
- 回放时会在 stderr 打印类似 `rosbag play` 的进度，例如 `12.3s / 61.1s (5/26, 19.2%)`。
- 建议优先回灌到隔离 topic，再切换消费者验证链路。

本地启动后可执行动态联调：

```bash
python3 scripts/kafka/kafka_pipeline_smoke_test.py
```

该脚本会触发 `data_boot -> forecast_boot -> execution_boot` 的主链路。

---

## 9. 版本管理与发布流程（二进制）

> 目标：发布包顶层仅允许 `bin/`、`lib/`、`scripts/`、`config/`；禁止任何 `.py` 文件。

### 9.1 版本号管理

```bash
./scripts/release_version.sh --show
./scripts/release_version.sh --bump patch
# 或指定版本
./scripts/release_version.sh --set 1.0.0
```

版本号来自仓库根目录 `VERSION`。

### 9.2 二进制打包

```bash
PYTHON_BIN=/home/joelin/miniconda3/envs/test_RL/bin/python ./scripts/package_release.sh
```

该脚本会执行：

1. 校验版本号
2. 安装/检查 PyInstaller
3. 编译 proto
4. 预热 Kafka 运行时目录（若不存在）
5. 通过 PyInstaller 构建可执行文件：`bin/power_trading_boot`
6. 组装发布目录（顶层白名单：`bin/lib/scripts/config`）
7. 强校验发布目录中不存在 `.py`
8. 生成归档和 buildinfo

产物位置：

- `generated/releases/power_trading_online-<version>.tar.gz`
- `generated/releases/power_trading_online-<version>.buildinfo`

`buildinfo` 包含：

- 版本号
- 构建时间
- git 短 SHA
- 解释器路径
- 打包模式（binary-only）

### 9.3 发布包验证

```bash
# 默认验证最新包
./scripts/test_release_package.sh

# 或指定包
./scripts/test_release_package.sh generated/releases/power_trading_online-1.0.0.tar.gz
```

验证内容：

1. 解包到 `generated/releases/_test_work/`
2. 校验包顶层目录白名单与无 `.py`
3. 校验 `bin/power_trading_boot` 可执行
4. 启动包内 `scripts/start_all.sh`
5. 检查三个 `/health`
6. 执行 `scripts/stop_all.sh` 清理

---

## 10. /tmp 发布验证参考流程

如果你需要在隔离目录（如 `/tmp`）验证发布包：

```bash
PKG=$(ls -t generated/releases/power_trading_online-*.tar.gz | head -n 1)
mkdir -p /tmp/power_trading_release_test
cp "$PKG" /tmp/power_trading_release_test/
cd /tmp/power_trading_release_test

tar -xzf "$(basename "$PKG")"
cd power_trading_online-*/

# 顶层必须仅有 bin/lib/scripts/config
find . -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort

# 必须为 0（禁止 .py）
find . -type f -name '*.py' | wc -l

bash scripts/start_all.sh
curl -s http://127.0.0.1:8001/health
curl -s http://127.0.0.1:8002/health
curl -s http://127.0.0.1:8003/health
bash scripts/stop_all.sh
```

---

## 11. 常见问题排查

### 11.1 启动失败：配置文件找不到

现象：日志提示 `Boot config not configured` 或 `Config file not found`。

处理：

1. 确认使用包内 `scripts/start_all.sh` 启动。
2. 确认脚本已导出 `POWER_TRADING_HOME`（当前脚本已内置）。
3. 确认包内 `config/common/boots_config.yaml` 存在。

### 11.2 发布包运行时尝试下载 Kafka

现象：日志出现“正在尝试下载 Kafka”。

处理：

1. 确认包内存在 `generated/kafka-local/kafka_2.13-3.7.1`。
2. 重新执行 `scripts/package_release.sh`，确保打包前已预热 Kafka 目录。

### 11.3 发布包中出现 `.py`

说明打包流程被破坏，应立即视为发布失败。

处理：

1. 执行 `./scripts/test_release_package.sh` 定位。
2. 检查 `scripts/package_release.sh` 的 staging 文件清单。

### 11.4 发布包目录不符合要求

现象：包内出现 `bin/lib/scripts/config` 以外的顶层目录或文件。

处理：

1. 执行 `./scripts/package_release.sh`（脚本内置白名单校验，不符合会直接失败）。
2. 执行 `./scripts/test_release_package.sh` 二次确认。

---

## 12. 后续建议

1. 增加 CI 发布校验（强制 no-`.py` + 可启动 + 可关闭）。
2. 为二进制包补充校验和（SHA256）与签名。
3. 增加 API 文档导出（OpenAPI 静态文件）。
4. 扩展端到端自动化测试数据集与回放脚本。
