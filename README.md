# Power Trading Online

一个基于 Python 的电力交易系统基础骨架，按三个 boot 服务拆分，并通过 Kafka + protobuf 进行异步消息编排：

- `data_boot`：数据获取服务，负责天气、企业负荷、实时电价、新能源出力信息的采集接口。
- `forecast_boot`：预测模型服务，负责天气、企业 96 点负荷与 96 点电价预测接口。
- `execution_boot`：执行端服务，负责风控校验与交易单生成接口。

系统使用 FastAPI 搭建每个 boot 的控制层、服务层与启动入口，使用 `confluent-kafka` 作为 Kafka 消息中间件客户端，并使用 `proto` 统一定义跨服务消息格式。

## 目录结构

```text
.
|-- boots
|   |-- data_boot
|   |   |-- controllers
|   |   |-- services
|   |   `-- main.py
|   |-- forecast_boot
|   |   |-- controllers
|   |   |-- services
|   |   `-- main.py
|   `-- execution_boot
|       |-- controllers
|       |-- services
|       `-- main.py
|-- common
|   |-- config.py
|   |-- kafka.py
|   |-- paths.py
|   `-- proto_loader.py
|   `-- schemas.py
|-- environment
|   `-- requirements.txt
|-- pub_interfaces
|   `-- trading_messages.proto
|-- generated/
|   |-- kafka-local/
|   |-- logs/
|   |-- pids/
|   |-- pycache/
|   `-- trading_messages_pb2.py
|-- scripts
|   |-- compile_protos.sh
|   |-- install_environment.sh
|   `-- kafka_pipeline_smoke_test.py
|   |-- start_all.sh
|   |-- start_local_kafka.sh
|   |-- stop_all.sh
|   `-- stop_local_kafka.sh
`-- .env.example
```

运行产物、日志、PID、Kafka 数据目录和 protobuf 生成文件全部放在仓库内的 `generated/` 目录中，并通过 `.gitignore` 统一忽略。

## 技术选型

- Python 3.8+
- FastAPI + Uvicorn
- Pydantic v2
- confluent-kafka
- protobuf
- grpcio-tools

说明：需求里写的是 `sprint boot`。在 Python 技术栈中没有对应框架，这里采用“类 Spring Boot 的分层 boot 结构”来实现，即每个服务都有独立启动入口、controller、service、config 和健康检查。

## 快速开始

### 1. 首次安装环境

```bash
./scripts/install_environment.sh
```

该脚本会自动完成以下动作：

1. 若根目录没有 `.env`，则自动从 `.env.example` 复制一份。
2. 清理仓库内旧的 `__pycache__`、`.pyc` 和 JVM 回放文件。
3. 从 `environment/requirements.txt` 安装 Python 依赖。
4. 编译 protobuf 到仓库内的 `generated/` 目录。
5. 下载并验证本地 Kafka，确认首次启动所需资源已经就绪。

### 2. 手动创建虚拟环境并安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r environment/requirements.txt
```

### 3. 配置环境变量

```bash
cp .env.example .env
```

按实际环境修改 Kafka 地址、主题前缀、端口等配置。

### 4. 编译 protobuf

```bash
./scripts/compile_protos.sh
```

### 5. 启动全部服务

```bash
./scripts/start_all.sh
```

`start_all.sh` 会自动完成以下动作：

1. 若根目录没有 `.env`，则自动从 `.env.example` 复制一份。
2. 自动调用 `scripts/compile_protos.sh` 编译所有 proto。
3. 自动调用 `scripts/start_local_kafka.sh` 启动本地低内存 Kafka。
4. 自动等待 Kafka 就绪后再启动三个 boot 服务。

首次执行时，如果 `generated/kafka-local/` 下还没有 Kafka 归档包，脚本会先下载 Kafka，因此启动时间会比后续重复启动更长。

默认端口：

- `data_boot`: `8001`
- `forecast_boot`: `8002`
- `execution_boot`: `8003`

### 6. 停止全部服务

```bash
./scripts/stop_all.sh
```

`stop_all.sh` 会自动停止三个 boot 服务，并继续调用 `scripts/stop_local_kafka.sh` 关闭本地 Kafka。

如需单独控制本地 Kafka，也可使用：

```bash
./scripts/start_local_kafka.sh
./scripts/stop_local_kafka.sh
```

## 服务接口

### Data Boot

- `GET /health`
- `GET /api/v1/data/current`
- `POST /api/v1/data/ingest`
- `GET /api/v1/data/pipeline-status`

### Forecast Boot

- `GET /health`
- `GET /api/v1/forecast/pipeline-status`

### Execution Boot

- `GET /health`
- `POST /api/v1/execution/risk-check`
- `POST /api/v1/execution/trade-orders`
- `GET /api/v1/execution/pipeline-status`

## Kafka 链路

当前仅保留两条 Kafka 收发链路（对应 `weather.proto` 与 `trading_messages.proto`）：

1. `data_boot` 发布 `power_trading.weather.events`（`weather.proto`）
2. `forecast_boot` 订阅 `power_trading.weather.events`
3. `forecast_boot` 基于天气数据生成并发布 `power_trading.forecast.events`（`trading_messages.proto`）
4. `execution_boot` 订阅 `power_trading.forecast.events`

## 动态联调

准备本地 Kafka 后，可运行：

```bash
python3 scripts/kafka_pipeline_smoke_test.py
```

该脚本会触发一次 `data_boot` 天气发布，并轮询三个 boot 的 `/pipeline-status` 接口，确认 `weather.events -> forecast.events -> execution_boot` 链路收发正常。

Python 运行期间产生的 `__pycache__` 也会统一写入 `generated/pycache/`，不会分散写入源码目录。

通过仓库脚本、VS Code 调试配置以及新打开的 VS Code 终端启动 Python 时，会默认继承 `PYTHONPYCACHEPREFIX=generated/pycache`。如果是在仓库外部 shell 手工执行 `python`，需要先手工执行 `export PYTHONPYCACHEPREFIX="$PWD/generated/pycache"`。

在资源较紧张的机器上，建议使用仓库内置的 `start_local_kafka.sh`，它会以低内存参数启动单节点 KRaft Kafka，并将 `__consumer_offsets` 等内部主题分区数降到 1，避免默认配置带来的额外内存压力。

## Kafka 主题建议

- `power_trading.weather.events`
- `power_trading.forecast.events`

## 后续扩展建议

- 接入真实天气、电价、负荷、新能源数据源。
- 将预测逻辑替换为时序模型或机器学习模型。
- 为风控引擎增加额度、偏差、电量边界、价格边界等规则。
- 增加数据库持久化、任务调度、监控告警和自动化测试。