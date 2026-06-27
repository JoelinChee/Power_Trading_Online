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
|   |-- common.sh
|   |-- kafka/
|   |   |-- start_local_kafka.sh
|   |   |-- stop_local_kafka.sh
|   |   |-- kafka_play.sh
|   |   |-- kafka_record.sh
|   |   |-- kafka_topic_echo.sh
|   |   `-- kafka_topic_list.sh
|   |-- proto/
|   |   `-- compile_protos.sh
|   |-- release/
|   |   |-- release_version.sh
|   |   |-- package_release.sh
|   |   `-- test_release_package.sh
|   |-- runtime/
|   |   |-- start_all.sh
|   |   `-- stop_all.sh
|   |-- setup/
|   |   `-- install_environment.sh
|   |-- smoke/
|   |   `-- weather_browser_smoke_test.py
|   `-- web/
|       |-- start_web.sh
|       `-- stop_web.sh
|-- unit_test/
|   |-- run_all_unit_tests.py
|   |-- kafka/
|   `-- system/
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
./scripts/setup/install_environment.sh
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
./scripts/proto/compile_protos.sh
```

### 6.4 启动全栈

```bash
./scripts/runtime/start_all.sh
```

默认端口：

- data_boot: `8001`
- forecast_boot: `8002`
- execution_boot: `8003`

### 6.5 停止全栈

```bash
./scripts/runtime/stop_all.sh
```

### 6.6 健康检查

```bash
curl -s http://127.0.0.1:8001/health
curl -s http://127.0.0.1:8002/health
curl -s http://127.0.0.1:8003/health
```

---

## 7. 脚本说明

`scripts/` 按用途分组。除 Kafka 录制/回放这类工具脚本外，大多数 shell 脚本都会先解析自身所在目录，再通过 `scripts/common.sh` 计算仓库根目录和 `generated/` 运行目录。因此这些脚本可以从任意当前工作目录执行，不依赖调用者先 `cd` 到特定位置。

### 7.1 共享脚本库

- `scripts/common.sh`

公共 shell helper，不建议直接执行。它负责统一计算 `PTO_REPO_ROOT`、`PTO_ARTIFACTS_ROOT`、`PTO_PID_DIR`、`PTO_LOG_DIR`、`PTO_PYCACHE_DIR`，并提供日志输出、失败退出、命令检查、运行目录创建、`.env` 初始化、TCP 端口等待、PID 文件停止和 stale 进程清理等公共能力。

这个文件存在的目的，是让启动、停止、安装、打包等脚本共享同一套路径和进程处理规则，避免每个脚本各自实现一份略有差异的逻辑。

### 7.2 运行控制脚本

- `scripts/runtime/start_all.sh`

开发模式下的统一启动入口。默认不带参数时启动本地 Kafka、`data_boot`、`forecast_boot`、`execution_boot`。也支持按需启动一个或多个目标：`all`、`kafka`、`data`、`forecast`、`execution`，以及全名 `data_boot`、`forecast_boot`、`execution_boot`。

示例：

```bash
./scripts/runtime/start_all.sh
./scripts/runtime/start_all.sh kafka execution
./scripts/runtime/start_all.sh data forecast
```

脚本行为说明：

1. 启动 boot 服务前会调用 `scripts/proto/compile_protos.sh`，确保 `generated/*_pb2.py` 是最新的。
2. 如果本次启动包含 Kafka 和任意 boot 服务，会等待 `127.0.0.1:9092` 可连接后再启动服务。
3. boot 服务按固定顺序启动：`data_boot -> forecast_boot -> execution_boot`，不受参数顺序影响。
4. 每个服务的 PID 写入 `generated/pids/<service>.pid`，日志写入 `generated/logs/<service>.log`。
5. 如果 PID 文件指向的服务仍在运行，脚本会复用已有进程；如果发现匹配的孤儿进程，会先清理再启动。

- `scripts/runtime/stop_all.sh`

开发模式下的统一停止入口。默认不带参数时关闭全部 boot 服务和本地 Kafka，也支持传入一个或多个目标，参数名与 `start_all.sh` 一致。

示例：

```bash
./scripts/runtime/stop_all.sh
./scripts/runtime/stop_all.sh execution
./scripts/runtime/stop_all.sh data forecast kafka
```

脚本行为说明：

1. boot 服务按反向依赖顺序停止：`execution_boot -> forecast_boot -> data_boot`。
2. 关闭时优先读取 `generated/pids/*.pid`，先发送普通 `TERM`，等待一段时间后仍未退出才强制结束。
3. 停止单个 boot 时，只会清理该目标对应的 stale 进程，不会误杀其它 boot 服务。
4. `kafka` 目标会调用 `scripts/kafka/stop_local_kafka.sh`，保持 Kafka 生命周期逻辑集中在 Kafka 目录下。

### 7.3 Kafka 脚本

- `scripts/kafka/start_local_kafka.sh`

本地 Kafka 启动脚本。首次执行时会下载 Kafka `2.13-3.7.1` 到 `generated/kafka-local/`，生成低内存单节点 KRaft 配置，初始化 cluster id 和 metadata，然后后台启动 broker。

脚本会通过 Kafka 自带的 `kafka-topics.sh --list` 判断 broker 是否真正 ready，而不是只检查端口是否打开。Kafka PID 写入 `generated/kafka-local/kafka.pid`，broker 日志写入 `generated/kafka-local/server.out`。

- `scripts/kafka/stop_local_kafka.sh`

停止本地 Kafka。它读取 `generated/kafka-local/kafka.pid`，支持重复执行：PID 文件不存在、PID 已失效或 Kafka 已停止时都会视为已清理状态。

- `scripts/kafka/kafka_topic_list.sh`

使用 `kcat` 查看 Kafka broker 元数据和 topic 列表。默认连接 `127.0.0.1:9092`，可通过 `BROKERS` 环境变量或 `--brokers/-b` 参数覆盖。

```bash
scripts/kafka/kafka_topic_list.sh
scripts/kafka/kafka_topic_list.sh --brokers 127.0.0.1:9092
```

- `scripts/kafka/kafka_topic_echo.sh`

消费并打印一个或多个 topic。默认会尝试把已知 topic 的 protobuf payload 解码成 JSON；`--raw` 可输出原始 payload；`--compact` 可输出单行 JSON；`--from-beginning` 从最早 offset 开始消费；`--all-topics` 监听所有非内部 topic。

```bash
scripts/kafka/kafka_topic_echo.sh power_trading.weather.events
scripts/kafka/kafka_topic_echo.sh --compact power_trading.forecast.events
scripts/kafka/kafka_topic_echo.sh --all-topics --from-beginning
```

- `scripts/kafka/kafka_record.sh` / `scripts/kafka/kafka_record.py`

录制 Kafka 消息到二进制 bag 文件，保留 topic、partition、offset、timestamp、headers、key 和 value 原始字节。适合复现线上或联调中的消息链路问题。`.sh` 是薄包装，真正逻辑在 `.py` 中；可通过 `PYTHON_BIN` 指定解释器。

常用参数包括：`--output/-o` 指定录制文件、`--from-beginning` 从最早 offset 录制、`--end-on-eof` 到达当前末尾后退出、`--max-messages` 限制消息数、`--max-seconds` 限制录制时长、`--all-topics` 录制所有非内部 topic。

- `scripts/kafka/kafka_play.sh` / `scripts/kafka/kafka_play.py`

把 `kafka_record.py` 生成的 `.bin` 文件回放到 Kafka。默认按原始 timestamp 间隔回放；`--full-speed` 全速回放；`--rate` 固定速率回放；`--target-topic` 可把所有消息回放到隔离 topic。

回放进度会输出到 stderr，时间统一以秒显示，并在等待下一条消息期间按 `0.1s` 刷新，格式类似：`54.0s / 61.2s (26/26, 100.0%)`。

- `scripts/kafka/kafka_info.sh`

读取录制文件并打印文件大小、消息总数、topic 分布和前几条消息预览。它不会连接 Kafka，只解析本地 `.bin` 文件。

- `scripts/kafka/kafka_pipeline_smoke_test.py`

动态链路冒烟测试。它会访问 `data_boot` 触发数据发布，然后轮询 `data_boot`、`forecast_boot`、`execution_boot` 的 pipeline status，确认 `data_boot -> forecast_boot -> execution_boot` 的 Kafka protobuf 链路完整打通。

### 7.4 Proto 脚本

- `scripts/proto/compile_protos.sh`

编译 `pub_interfaces/*.proto` 到 `generated/`，生成 `weather_pb2.py` 和 `trading_messages_pb2.py` 等模块。执行前会删除 `generated/` 根目录下旧的 `*_pb2.py`，但不会删除 Kafka runtime、日志、PID、release 包等其它运行产物。

默认使用 `python3`，也可以通过 `PYTHON_BIN` 指定项目环境中的 Python：

```bash
PYTHON_BIN=/path/to/python ./scripts/proto/compile_protos.sh
```

### 7.5 安装与环境脚本

- `scripts/setup/install_environment.sh`

首次安装入口。它会创建或复用 conda 环境，安装 `environment/requirements.txt`，安装 Kafka CLI 依赖，编译 proto，并预热本地 Kafka runtime。预热 Kafka 时设置了清理 trap，如果安装中途失败，会尽量停止临时启动的 Kafka，避免留下后台进程。

安装脚本还会检查浏览器是否可用。它会先检测系统或 conda 环境中的常见浏览器命令，例如 `firefox`、`google-chrome`、`chromium`、`microsoft-edge`，以及 WSL/Windows 下的 `powershell.exe`、`cmd.exe`。如果都不存在，会通过 conda-forge 自动安装 Firefox：

```bash
conda install -n "$CONDA_ENV_NAME" -c conda-forge firefox -y
```

浏览器不是 Python pip 包，因此不会写入 `environment/requirements.txt`。Python requirements 只记录项目直接 import 的 Python 依赖；当前 Streamlit 前端直接使用 `pandas`，因此 `requirements.txt` 中包含 `pandas==2.3.3`。

可通过环境变量调整 conda 环境名和 Python 版本：

```bash
CONDA_ENV_NAME=power_trading_online CONDA_PYTHON_VERSION=3.12 ./scripts/setup/install_environment.sh
```

### 7.6 Web 脚本

- `scripts/web/start_web.sh`

启动 Streamlit 前端。默认使用 `/home/joelin/miniconda3/envs/test_RL/bin/python`，默认监听 `0.0.0.0:8088`，PID 写入 `generated/pids/web_frontend.pid`，日志写入 `generated/logs/web_frontend.log`。脚本会关闭 Streamlit 首次启动的交互式统计提示，并尝试自动打开浏览器。

自动打开浏览器时，脚本会优先直接调用 `firefox`、`google-chrome`、`chrome`、`chromium`、`chromium-browser`、`microsoft-edge`、`brave-browser`。如果这些命令不可用，再回退到 `xdg-open`、`gio`、`powershell.exe`、`cmd.exe`。浏览器打开失败不会导致 web 服务启动失败，脚本会提示手动访问 `WEB_URL`。

常用覆盖项：

```bash
PYTHON_BIN=/path/to/python WEB_PORT=8088 ./scripts/web/start_web.sh
```

- `scripts/web/stop_web.sh`

停止 Streamlit 前端。它先按 PID 文件停止，再清理匹配 `streamlit run web/app.py` 的孤儿进程。

### 7.7 发布脚本

- `scripts/release/release_version.sh`

读取、设置或递增根目录 `VERSION`。支持 `--show`、`--set <x.y.z>`、`--bump major|minor|patch`。脚本会校验版本号必须是三段数字语义版本。

- `scripts/release/package_release.sh`

构建二进制发布包。它会校验版本、确保 PyInstaller 可用、编译 proto、准备 Kafka runtime、生成 `power_trading_boot` 单文件可执行程序，然后组装 `bin/`、`lib/`、`scripts/`、`config/` 四个顶层目录并打包。

发布包内部的 `scripts/` 目录保持扁平结构，这是给最终部署用户使用的运行脚本结构，和源码仓库内按用途分组的 `scripts/` 目录不同。

- `scripts/release/test_release_package.sh`

验证发布包是否满足二进制发布要求。默认选择 `generated/releases/` 下最新的 `power_trading_online-*.tar.gz`，也可以传入指定包路径。验证内容包括解包、顶层目录白名单、禁止 `.py`、可执行文件存在、启动三个服务、检查 `/health`、最后停止服务。

### 7.8 Smoke 脚本

- `scripts/smoke/weather_browser_smoke_test.py`

较轻量的浏览器链路冒烟测试。它访问 `data_boot` 触发天气数据发布，然后检查 `data_boot` 和 `forecast_boot` 的 pipeline status，确认天气消息进入 Kafka 并被预测服务消费。

---

## 8. 接口概览

### 8.1 Data Boot

- `GET /health`
- `GET /api/v1/data/current`
- `POST /api/v1/data/ingest`
- `GET /api/v1/data/pipeline-status`

### 8.2 Forecast Boot

- `GET /health`
- `GET /api/v1/forecast/pipeline-status`

### 8.3 Execution Boot

- `GET /health`
- `POST /api/v1/execution/risk-check`
- `POST /api/v1/execution/trade-orders`
- `GET /api/v1/execution/pipeline-status`

---

## 9. 联调与冒烟测试

本地联调通常先启动 Kafka 和需要验证的 boot 服务，再执行对应 smoke 脚本。轻量验证天气数据链路时，使用 `scripts/smoke/weather_browser_smoke_test.py`；需要验证完整 Kafka protobuf 主链路时，使用 `scripts/kafka/kafka_pipeline_smoke_test.py`。

```bash
./scripts/runtime/start_all.sh
python3 scripts/smoke/weather_browser_smoke_test.py
python3 scripts/kafka/kafka_pipeline_smoke_test.py
./scripts/runtime/stop_all.sh
```

如果只验证部分服务，也可以利用 `start_all.sh` / `stop_all.sh` 的多目标参数。例如只验证预测到执行链路时，可按需启动 `kafka execution` 或补充 `forecast`。

## 10. Kafka 数据录制与回灌

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

项目的系统测试默认使用 `generated/recordings/unittest_all_topics.bin` 作为录制/回放基准文件。这个文件由 Kafka 录制测试生成，包含 `power_trading.weather.events` 和 `power_trading.forecast.events` 两类消息。可用以下命令确认录制文件是否有效：

```bash
scripts/kafka/kafka_info.sh generated/recordings/unittest_all_topics.bin
```

只要 `Topics:` 下对应 topic 的计数大于 `0`，说明录制文件中包含该 topic 的消息，例如：

```text
Topics:
	51  power_trading.forecast.events
	51  power_trading.weather.events
```

本地启动后可执行动态联调：

```bash
python3 scripts/kafka/kafka_pipeline_smoke_test.py
```

该脚本会触发 `data_boot -> forecast_boot -> execution_boot` 的主链路。

---

## 11. Unit Test 测试脚本

`unit_test/` 下维护了一组基于 Python 标准库 `unittest` 的系统级测试。这些测试不是纯函数级单测，而是面向本地运行脚本、Kafka 工具链、录制/回放文件和 boot 服务联动的动态验证。`unittest` 是 Python 标准库，不需要写入 `environment/requirements.txt`。

### 11.1 总控脚本

- `unit_test/run_all_unit_tests.py`

总控入口会先做静态检查，确认所有测试脚本和必要清理脚本存在；然后执行启动前清理；最后串行运行所有测试并打印每个测试的 PASS/FAIL、耗时、输出和总体结果。

运行方式：

```bash
python unit_test/run_all_unit_tests.py
```

运行一开始会先执行：

```bash
bash scripts/web/stop_web.sh
bash scripts/runtime/stop_all.sh all
```

随后会用 `pgrep -f` + `kill -9` 强制清理可能残留的本地进程，包括：

- `uvicorn boots.data_boot.main:app`
- `uvicorn boots.forecast_boot.main:app`
- `uvicorn boots.execution_boot.main:app`
- `streamlit run web/app.py`
- `scripts/kafka/kafka_record.py`
- `scripts/kafka/kafka_play.py`
- `scripts/kafka/kafka_topic_echo.py`
- `kafka-server-start.sh`
- `kafka.Kafka`

总控脚本当前按以下顺序执行测试：

1. `system_start_all_after_generated_delete`
2. `system_stop_all_when_not_running`
3. `kafka_record_all_topics`
4. `kafka_play_echo_recording`
5. `kafka_topic_list_and_info`
6. `execution_boot_receives_replayed_forecasts`

### 11.2 System 测试

- `unit_test/system/test_start_all_after_generated_delete.py`

该测试先停止所有服务，然后删除 `generated/`，再执行：

```bash
scripts/runtime/start_all.sh all
```

它验证 `start_all.sh` 能在运行产物目录缺失时重新创建必要目录、重新编译 proto、启动本地 Kafka，并启动三个 boot 服务。测试会断言输出包含：

- `Started local Kafka with PID <数字>`
- `Kafka is ready on 127.0.0.1:9092`
- `Started data_boot on port 8001 with PID <数字>`
- `Started forecast_boot on port 8002 with PID <数字>`
- `Started execution_boot on port 8003 with PID <数字>`

如果启动输出中出现 `error`，测试会失败。测试结束会执行 `stop_all.sh all` 清理进程。

- `unit_test/system/test_stop_all_when_not_running.py`

该测试先静默执行一次停止命令，确保服务已经停止；然后再次执行：

```bash
scripts/runtime/stop_all.sh all
```

它验证停止脚本在服务均未运行时仍保持幂等成功，并断言输出包含：

```text
execution_boot is not running
forecast_boot is not running
data_boot is not running
Local Kafka is not running
Requested shutdown complete
```

### 11.3 Kafka 测试

- `unit_test/kafka/test_kafka_record_all_topics.py`

该测试启动完整后端：

```bash
scripts/runtime/start_all.sh
```

然后启动 Streamlit web：

```bash
scripts/web/start_web.sh
```

web 页面会在初始化和每 10 秒自动触发一次 `data_boot`，从而推动 `data_boot -> forecast_boot -> execution_boot` 链路。测试等待 web 可访问、等待一次自动触发周期；同时为了兼容 GitHub Actions 这类没有真实浏览器会话的 headless 环境，测试会显式访问 `data_boot` 根路径触发样本数据发布。随后它会轮询 `forecast_boot` 的 pipeline status，确认 forecast 消息已经发布。之后执行 3 分钟全 topic 录制：

```bash
scripts/kafka/kafka_record.sh \
	--all-topics \
	--from-beginning \
	--max-seconds 180 \
	--output generated/recordings/unittest_all_topics.bin
```

录制完成后，测试会调用：

```bash
scripts/kafka/kafka_info.sh generated/recordings/unittest_all_topics.bin
```

并解析 `Topics:` 区域，要求 `power_trading.weather.events` 和 `power_trading.forecast.events` 的消息数都大于 `0`。

- `unit_test/kafka/test_kafka_play_echo_recording.py`

该测试只启动 local Kafka，不启动三个 boot 服务，也不执行 `start_all.sh`。它先启动：

```bash
scripts/kafka/start_local_kafka.sh
```

再用一个独立进程启动：

```bash
scripts/kafka/kafka_topic_echo.sh \
	power_trading.weather.events \
	power_trading.forecast.events \
	--max-messages 1 \
	--compact
```

随后使用录制文件回放一条消息：

```bash
scripts/kafka/kafka_play.sh \
	-i generated/recordings/unittest_all_topics.bin \
	--full-speed \
	--limit 1
```

如果 echo 进程收到消息并打印包含 `power_trading.` 和 `payload` 的内容，则测试通过；如果回放报错、echo 没有输出或超时，测试失败。

- `unit_test/kafka/test_kafka_topic_list_and_info.py`

该测试验证两个 Kafka 工具脚本：

```bash
scripts/kafka/kafka_topic_list.sh
scripts/kafka/kafka_info.sh generated/recordings/unittest_all_topics.bin
```

测试只启动 local Kafka，然后把 `unittest_all_topics.bin` 回放到原 topic，并额外回放几条到 `power_trading.unittest.replay.events`，确保 Kafka broker 上至少有 3 个非内部 topic。随后解析 `kafka_topic_list.sh` 的 `kcat -L` 输出，要求非 `__` 开头的 topic 数量不少于 `3`。最后验证 `kafka_info.sh` 能正常读取本地 bin 文件，输出 `Records:`、`Topics:`，且记录数大于 `0`。

- `unit_test/kafka/test_execution_boot_receives_replayed_forecasts.py`

该测试只通过 `start_all.sh` 启动 Kafka 和 `execution_boot`：

```bash
scripts/runtime/start_all.sh kafka execution
```

然后清空 `generated/logs/execution_boot.log`，使用录制文件全速回放：

```bash
scripts/kafka/kafka_play.sh \
	-i generated/recordings/unittest_all_topics.bin \
	--full-speed
```

测试会轮询 `execution_boot.log`，统计日志文本 `Kafka message received service=execution_boot` 出现次数。只要收到不少于 `5` 次 Kafka 消息，就认为 `execution_boot` 能正确消费回放的 forecast 数据；否则测试失败并打印 execution 日志尾部用于排查。

---

## 12. 版本管理与发布流程（二进制）

> 目标：发布包顶层仅允许 `bin/`、`lib/`、`scripts/`、`config/`；禁止任何 `.py` 文件。

### 12.1 版本号管理

```bash
./scripts/release/release_version.sh --show
./scripts/release/release_version.sh --bump patch
# 或指定版本
./scripts/release/release_version.sh --set 1.0.0
```

版本号来自仓库根目录 `VERSION`。

### 12.2 二进制打包

```bash
PYTHON_BIN=/home/joelin/miniconda3/envs/test_RL/bin/python ./scripts/release/package_release.sh
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

### 12.3 发布包验证

```bash
# 默认验证最新包
./scripts/release/test_release_package.sh

# 或指定包
./scripts/release/test_release_package.sh generated/releases/power_trading_online-1.0.0.tar.gz
```

验证内容：

1. 解包到 `generated/releases/_test_work/`
2. 校验包顶层目录白名单与无 `.py`
3. 校验 `bin/power_trading_boot` 可执行
4. 启动包内 `scripts/start_all.sh`
5. 检查三个 `/health`
6. 执行 `scripts/stop_all.sh` 清理

---

## 13. /tmp 发布验证参考流程

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

## 13. 常见问题排查

### 13.1 启动失败：配置文件找不到

现象：日志提示 `Boot config not configured` 或 `Config file not found`。

处理：

1. 确认使用包内 `scripts/start_all.sh` 启动。
2. 确认脚本已导出 `POWER_TRADING_HOME`（当前脚本已内置）。
3. 确认包内 `config/common/boots_config.yaml` 存在。

### 13.2 发布包运行时尝试下载 Kafka

现象：日志出现“正在尝试下载 Kafka”。

处理：

1. 确认包内存在 `generated/kafka-local/kafka_2.13-3.7.1`。
2. 重新执行 `scripts/release/package_release.sh`，确保打包前已预热 Kafka 目录。

### 13.3 发布包中出现 `.py`

说明打包流程被破坏，应立即视为发布失败。

处理：

1. 执行 `./scripts/release/test_release_package.sh` 定位。
2. 检查 `scripts/release/package_release.sh` 的 staging 文件清单。

### 13.4 发布包目录不符合要求

现象：包内出现 `bin/lib/scripts/config` 以外的顶层目录或文件。

处理：

1. 执行 `./scripts/release/package_release.sh`（脚本内置白名单校验，不符合会直接失败）。
2. 执行 `./scripts/release/test_release_package.sh` 二次确认。

---

## 14. GitHub 配置 Unit Test CI：测试全部通过才允许 PR 合并

完整流程分三步：1. 编写 GitHub Actions 测试流水线，自动跑单元测试；2. 配置分支保护，强制校验测试结果；3. 提交 PR 验证效果。测试失败时，GitHub 的合并按钮会被置灰，不能合入受保护分支。

### 14.1 创建 GitHub Actions 单元测试脚本

本仓库已新增 CI 文件：

```text
.github/workflows/unit-test.yml
```

触发条件：

- 所有 `pull_request` 都会执行，用于 PR 合并门禁。
- 只有推送到 `main` 或 `master` 时才会执行 `push`，避免 PR 分支每次 push 同时触发 `push` 和 `pull_request` 两套重复检查。
- 测试脚本返回非 `0` 退出码时，CI 标红失败。
- 只有 CI 全绿，才满足分支保护的合并门禁条件。

当前 workflow 内容按本项目实际运行方式配置：

```yaml
name: Unit Test

on:
  pull_request:
  push:
    branches:
      - main
      - master

jobs:
  test:
    runs-on: ubuntu-latest
    timeout-minutes: 30

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Java for local Kafka
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "17"

      - name: Set up conda
        uses: conda-incubator/setup-miniconda@v3
        with:
          auto-update-conda: true
          python-version: "3.12"
          activate-environment: power_trading_online

      - name: Install project environment
        shell: bash -el {0}
        run: |
          ./scripts/setup/install_environment.sh

      - name: Run unit test suite
        shell: bash -el {0}
        run: |
          conda activate power_trading_online
          python unit_test/run_all_unit_tests.py
```

说明：

- 本项目不是用 `pytest`，而是使用 Python 标准库 `unittest`，入口是 `unit_test/run_all_unit_tests.py`。
- CI 先安装 Java，因为本地 Kafka 运行时需要 JVM。
- CI 使用 conda 创建 `power_trading_online` 环境。
- `scripts/setup/install_environment.sh` 会安装 Python requirements、`kafkacat`、必要浏览器、编译 proto，并预热 Kafka runtime。
- `unit_test/run_all_unit_tests.py` 会依次执行 system 和 Kafka 相关测试；任意测试失败都会让 workflow 失败。

### 14.2 配置分支保护强制校验测试结果

GitHub Actions 文件只能定义 CI，真正“测试不通过禁止合并”需要在 GitHub 仓库页面配置分支保护。

配置路径：

```text
Repository -> Settings -> Branches -> Branch protection rules -> Add rule
```

推荐配置：

1. Branch name pattern 填写主分支，例如 `main` 或 `master`。
2. 勾选 `Require a pull request before merging`。
3. 勾选 `Require status checks to pass before merging`。
4. 在 required checks 中选择本 workflow 的检查项，通常显示为 `test` 或 `Unit Test / test`。
5. 建议勾选 `Require branches to be up to date before merging`，避免旧代码绕过最新测试。
6. 保存规则。

配置完成后，PR 合并前必须满足：

- GitHub Actions 的 `Unit Test` workflow 运行成功。
- `unit_test/run_all_unit_tests.py` 中所有测试通过。
- 如果任意测试失败，PR 页面会显示红色检查项，合并按钮不可用。

### 14.3 验证效果

验证方式：

1. 提交一笔正常代码，推送分支并创建 PR。
2. 在 PR 页面查看 `Checks`，确认 `Unit Test / test` 自动运行。
3. 测试通过时，检查项显示绿色，PR 可以合并。
4. 临时制造一个测试失败，例如让某个 unittest 断言失败，再推送到测试分支。
5. 确认 `Unit Test / test` 变红，并且合并按钮被置灰。

本地可以先运行同一套测试，提前发现 CI 问题：

```bash
python unit_test/run_all_unit_tests.py
```

---

## 15. 后续建议

1. 增加 CI 发布校验（强制 no-`.py` + 可启动 + 可关闭）。
2. 为二进制包补充校验和（SHA256）与签名。
3. 增加 API 文档导出（OpenAPI 静态文件）。
4. 扩展端到端自动化测试数据集与回放脚本。
