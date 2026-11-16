# 多中心空腹血糖研究协调系统

两院检验明细**留院**前提下的合并中位数研究平台。协调端只拿到「样本数」和
「不大于指定值的计数」，通过二分搜索计算两院合并中位数，全程不接触检验明细。

## 1. 架构

```
                    ┌──────────────────────────────┐
                    │  coordinator (FastAPI)       │
                    │  - 批次队列 FIFO（SQLite）     │
                    │  - 仅取 n 与 count(<=x)        │
                    │  - 二分求合并中位数            │
                    │  - 报告留存版号/样本数/中位数  │
                    └───────────┬──────┬────────────┘
                                │      │  HTTP（超时重试 / 语义化错误码）
              ┌─────────────────┘      └──────────────────┐
              ▼                                             ▼
   ┌────────────────────┐                        ┌────────────────────┐
   │ hospital A         │                        │ hospital B         │
   │ SQLite（留院）       │                        │ SQLite（留院）       │
   │ - 整版 CSV 版本      │                        │ - 整版 CSV 版本      │
   │ - 快照名额×1        │                        │ - 快照名额×1        │
   │ - 物化固定样本       │                        │ - 物化固定样本       │
   └────────────────────┘                        └────────────────────┘
```

- **院端** (`app/hospital/`)：整版 CSV 导入（带版本号，更正可再次导入）；
  研究批次在 `acquire` 时把当前版本、日期范围内数据**物化**为不可变快照，
  之后的新版导入不影响在研批次（多轮不得换样本）。
- **协调端** (`app/coordinator/`)：按日期范围发起批次，FIFO 调度；
  每院只有一个 active 快照名额（SQLite 部分唯一索引保证），忙时新批回队列等待；
  批次到终态或协调端退出时自动回收名额。
- 血糖值约定：CSV 中为 **1..1000 的整数，单位 0.1 mmol/L**（55 = 5.5 mmol/L）。

## 2. 关键协议与不变量

| 需求 | 实现方式 |
| --- | --- |
| 检验明细留院 | 院端仅暴露 `GET /snapshots/{batch}/counts?value=x`，返回样本数与 `<=x` 计数，无任何明细导出接口 |
| 整版导入、唯一检验号 | `POST /imports` 上传 CSV（`test_id,date,value`），版内 test_id 唯一，非法行 422；每次导入生成新版本 v1/v2/…，旧版本保留 |
| 两院各固定样本和版号，更正仍可提交 | acquire 物化快照到 `snapshot_observations`；更正导入只影响后续新批次 |
| 多轮不得换样本 | 所有计数请求带同一 `batch_id`，院端按快照回答；重复 acquire 幂等返回原快照 |
| 偶数取中间两值平均 | 1..1000 整数域二分第 k 小；偶数池求 k=n/2 与 k=n/2+1 后平均，结果可能为 `x.5` |
| 合并为空不出中位数 | 池大小 0 时批次 `SUCCEEDED` 但 `median_value=null`，报告注明原因 |
| 每院仅一个快照名额，忙时等待 | 部分唯一索引 `WHERE status='active'`；协调端收到 409 SLOT_BUSY 回队列重试 |
| 批次结束/协调端退出自动回收 | 终态后带 generation 调 release；优雅停机及重启恢复流程兜底回收 |
| 旧批迟到回收不能释放新批名额 | 名额带 `generation` 代次令牌，代次不符返回 `STALE_GENERATION` 且不释放 |
| 任一院明确失败/样本过期 => 整批失败 | HTTP 4xx/5xx 不重试立即失败；410 样本过期语义化；两院名额仍保证回收 |
| 迟到响应不得生成报告 | 批次进入终态后不再有任何写轮次/报告的代码路径；持久延迟用例验证轮次不增长 |
| 报告留存两院版号、样本数、中位数 | `reports` 表 + `GET /reports/{batch_id}` |

## 3. Docker Compose 启动

```bash
docker compose up --build
```

| 服务 | 容器内端口 | 本机访问 | Swagger UI |
| --- | --- | --- | --- |
| coordinator | 8000 | http://localhost:8000 | http://localhost:8000/docs |
| hospital-a | 8000 | http://localhost:8101 | http://localhost:8101/docs |
| hospital-b | 8000 | http://localhost:8102 | http://localhost:8102/docs |

SQLite 数据保存在命名卷 `coordinator-data / hospital-a-data / hospital-b-data`。

## 4. 无 Docker 的本地运行

```bash
pip install -r requirements.txt
python scripts/generate_sample_data.py           # 生成 sample_data/*.csv

HOSPITAL_ID=A HOSPITAL_DB=data/runtime/a.sqlite3 PORT=8101 python run.py
HOSPITAL_ID=B HOSPITAL_DB=data/runtime/b.sqlite3 PORT=8102 python run.py
COORDINATOR_DB=data/runtime/c.sqlite3 \
  HOSPITAL_A_URL=http://127.0.0.1:8101 HOSPITAL_B_URL=http://127.0.0.1:8102 \
  PORT=8000 python run.py
```

## 5. Swagger 操作流程

1. 两院分别 `POST /imports` 上传 `sample_data/hospital_A_v1.csv`、`hospital_B_v1.csv`。
2. 协调端 `POST /batches` 发起研究：
   ```json
   {"date_from": "2026-01-01", "date_to": "2026-03-31"}
   ```
3. `GET /batches/{batch_id}` 轮询直到 `SUCCEEDED`；`GET /reports/{batch_id}` 看报告。
4. 故障模拟（两种方式等价）：
   - 发起时内嵌：
    ```json
    {"date_from": "2026-01-01", "date_to": "2026-03-31",
     "simulation": {"hospital": "A", "lose_receipt_once": true}}
    ```
   - 或先建批次再 `PUT /sim/config`；也可直接在院端 `PUT /sim/rules` 操作。
   - 支持字段：`delay_seconds`（延迟）、`fail_status`（明确失败，如 500）、
     `expire_now`（样本过期 410）、`lose_receipt_once`（回执丢失）、
     `phase`（acquire/count/release/all）、`persistent`（持续生效，模拟迟到响应）。

## 6. 自测

```bash
python scripts/selftest.py
```

自动拉起三个 uvicorn 进程并执行 38 项断言，包括：CSV 校验（值域/重复检验号/日期/空文件）、
中位数与独立实现对账（奇/偶/空池）、版本更正隔离、名额排队、明确失败整批失败、410 过期、
回执丢失幂等重试、代次令牌防旧批释放、迟到响应不产生报告、协调端停机/重启回收名额等。

## 7. 目录

```
app/
  config.py                 # 环境变量配置
  models.py                 # Pydantic 请求/响应模型
  hospital/
    db.py                   # 院端 SQLite：版本/物化快照/名额代次/模拟规则
    importer.py             # CSV 校验与解析
    app.py                  # 院端 FastAPI
  coordinator/
    db.py                   # 协调端 SQLite：批次/轮次/报告/模拟配置
    hospital_client.py      # 院端 HTTP 客户端（重试、语义错误）
    worker.py               # 调度引擎 + 仅计数二分中位数
    app.py                  # 协调端 FastAPI
scripts/
  generate_sample_data.py   # 示例 CSV
  selftest.py               # 端到端自测（36 项）
docker-compose.yml / Dockerfile / run.py
```
