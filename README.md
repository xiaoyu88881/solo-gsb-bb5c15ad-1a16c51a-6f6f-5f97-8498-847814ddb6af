# 设备告警事件段聚合服务

基于 FastAPI + SQLite 的告警汇聚服务：客户端上报离散告警事件，服务按 **source + 可配置静默间隔** 将乱序到达的事件归入同一事件段（segment），并保证 **event_id 幂等去重**——重复提交不改变任何计数。每次段变更都会同事务写入**单调递增的变更流**，下游可通过 `GET /segment-changes` 做增量同步。

## 快速开始

一条命令完成依赖安装、数据库初始化与服务启动：

```bash
docker compose up --build
```

服务地址：**http://localhost:8000**（交互式 API 文档：http://localhost:8000/docs）

数据库文件在首次启动时自动建表初始化，并通过命名卷 `alerts-data` 持久化。

## 配置项

均通过环境变量配置（可在 `docker-compose.yml` 的 `environment` 中修改）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SILENCE_INTERVAL_SECONDS` | `300` | 静默间隔（秒）。同一 source 下，事件时间与事件段边界相距不超过该值即归入同一段；超过则开新段 |
| `DATABASE_PATH` | `/data/alerts.db`（容器）/ `data/alerts.db`（本地） | SQLite 数据库文件路径 |

## API

### 提交事件 `POST /events`

```bash
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "edge-gateway-01",
    "event_id": "evt-0001",
    "occurred_at": "2026-09-20T08:00:00Z",
    "severity": "warning"
  }'
```

- `severity` 取值：`debug` / `info` / `warning` / `error` / `critical`（大小写不敏感，`warn`、`fatal` 为别名）
- `occurred_at` 为 ISO 8601 时间；不带时区时按 UTC 处理
- 响应：`{"deduplicated": false, "segment": {...}}`；重复提交同一 `(source, event_id)` 时 `deduplicated: true`，计数不变

### 查询事件段详情 `GET /segments/{id}`

```bash
curl http://localhost:8000/segments/1
```

```json
{
  "id": 1,
  "source": "edge-gateway-01",
  "started_at": "2026-09-20T08:00:00+00:00",
  "ended_at": "2026-09-20T08:04:11+00:00",
  "max_severity": "critical",
  "event_count": 3,
  "revision": 2
}
```

### 列出事件段 `GET /segments`（可选 `?source=` 过滤）

```bash
curl 'http://localhost:8000/segments?source=edge-gateway-01'
```

### 静默间隔重分段预演 `GET /segments/resegmentation-preview`

用**候选静默间隔**对某来源的全部原始事件重新归段，预览"如果改了间隔会变成什么样"。全程**只读**：在同一只读事务（`query_only` 连接）内读取该来源全部事件并按 `occurred_at`、`event_id` 稳定排序后重算，**不会改写事件、现有段或变更流**；模拟段仅以本次响应内有效的临时编号 `temp_id` 标识。

```bash
curl 'http://localhost:8000/segments/resegmentation-preview?source=edge-gateway-01&silence_interval_seconds=120'
```

```json
{
  "source": "edge-gateway-01",
  "silence_interval_seconds": 120.0,
  "event_count": 4,
  "simulated_segments": [
    {
      "temp_id": 1,
      "source": "edge-gateway-01",
      "started_at": "2026-09-20T08:00:00+00:00",
      "ended_at": "2026-09-20T08:02:30+00:00",
      "max_severity": "critical",
      "event_count": 4
    }
  ],
  "mappings": [
    {"segment_id": 1, "relation": "merged", "simulated_temp_ids": [1], "event_count": 2},
    {"segment_id": 2, "relation": "merged", "simulated_temp_ids": [1], "event_count": 2}
  ],
  "summary": {
    "changed_existing_segment_count": 2,
    "split_existing_segment_count": 0,
    "merged_existing_segment_count": 2,
    "unchanged_existing_segment_count": 0,
    "existing_segment_count": 2,
    "simulated_segment_count": 1
  }
}
```

- **`simulated_segments`**：按候选间隔重算出的模拟段（不落库），`temp_id` 从 1 起按时间顺序编号。
- **`mappings`**：每个**现有段**到模拟段的映射，按现有段 `id` 升序，依据事件成员关系判定：
  - `unchanged`：现有段与唯一一个模拟段的事件集合完全相同（候选间隔与现状等价）；
  - `split`：该现有段的事件被拆散到**多个**模拟段（候选间隔**更小**，切分更细）；
  - `merged`：**多个**现有段的事件并入同一个模拟段（候选间隔**更大**，段被合并）。
- **`summary`**：受影响（`split` + `merged`）现有段数，以及 split/merged/unchanged 各自计数、现有段总数与模拟段总数。
- 切分规则：事件按 `(occurred_at, event_id)` 排序后，相邻事件间隔 **大于**候选间隔处切分（恰好等于间隔仍归同段，与归段规则一致）。
- 相同入参的重复请求结果**确定一致**；`silence_interval_seconds >= 0`（`0` 表示仅同一时刻的事件归同段），未知来源返回各列表为空、计数为 0 的结果。

### 增量同步变更流 `GET /segment-changes?after=&limit=`

下游系统可按 `change_id` 升序增量拉取事件段变更，实现增量同步：

```bash
curl 'http://localhost:8000/segment-changes?after=0&limit=100'
```

```json
{
  "changes": [
    {
      "change_id": 1,
      "type": "upsert",
      "segment_id": 1,
      "merged_into": null,
      "revision": 1,
      "segment": {
        "id": 1,
        "source": "edge-gateway-01",
        "started_at": "2026-09-20T08:00:00+00:00",
        "ended_at": "2026-09-20T08:00:00+00:00",
        "max_severity": "warning",
        "event_count": 1,
        "revision": 1
      }
    },
    {
      "change_id": 4,
      "type": "merged",
      "segment_id": 2,
      "merged_into": 1,
      "revision": null,
      "segment": null
    }
  ],
  "next_after": 4
}
```

- **何时产生记录**：每次创建段、边界或最高级别变化（`revision` 递增）、多段合并，都会在事件写入的**同一事务**内追加变更记录；`change_id` 全局单调递增。
- **`upsert`**：段的新状态，`segment` 为变更时刻的完整快照（含 `event_count`、`revision`）。
- **`merged`**：合并墓碑，表示 `segment_id` 段已被删除并并入 `merged_into` 指向的保留段；每个被删除段各产生一条。
- **何时不产生记录**：仅事件数增加（`revision` 不变）不写 `upsert`；重复提交 `(source, event_id)` 的幂等重放不写任何记录。
- **分页约定**：用上一页返回的 `next_after` 作为下一页的 `after` 继续拉取；`change_id` 单调递增保证分页期间新写入的变更只会出现在后续页——不漏读、不重复。返回空页时 `next_after` 与入参 `after` 相同，可直接继续轮询。
- `after` 默认 `0`（从头开始）；`limit` 默认 `100`，最大 `1000`。

### 健康检查 `GET /healthz`

## 归段与 revision 规则

- 新事件的 `occurred_at` 落在某段 `[start - silence, end + silence]` 窗口内即并入该段；同时命中多个段（迟到事件桥接）时自动合并为一段，事件数累加。
- 段的 `revision` 初始为 1；**仅当**新事件导致段的起止边界或最高级别发生变化时递增，仅事件数增加不递增。
- `(source, event_id)` 唯一约束保证幂等：重复提交直接返回原段，不改计数、不改 revision。
- 上述每次段创建、边界/最高级别变化与合并都会写入变更流（见 `GET /segment-changes`），供下游增量同步；历史段（变更流上线前已存在）不补录，订阅从部署后的新变更开始。
- 想评估调整 `SILENCE_INTERVAL_SECONDS` 的影响时，调用重分段预演接口（见 `GET /segments/resegmentation-preview`），可在不改动任何数据的前提下查看模拟段与现有段的 split/merged 映射。

## 本地开发与测试

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest tests/ -v                 # 全部测试（核心逻辑 + API）
uvicorn app.main:app --reload    # 本地起服务，默认 http://127.0.0.1:8000
```

核心归段逻辑（`app/service.py`）仅依赖标准库，无第三方依赖也可验证：

```bash
python3 -m unittest tests.test_service -v
```

## 项目结构

```
app/
  main.py      # FastAPI 路由与生命周期（启动时幂等建表）
  service.py   # 归段/合并/去重核心逻辑
  schemas.py   # 请求/响应模型与校验
  severity.py  # 告警级别定义与排序
  db.py        # SQLite 连接、schema、事务
  config.py    # 环境变量配置
tests/         # 单元测试与 API 测试
Dockerfile / docker-compose.yml
```
