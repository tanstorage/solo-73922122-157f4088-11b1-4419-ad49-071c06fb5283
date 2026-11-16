# 理赔明细分批提取服务（FastAPI + PostgreSQL）

保险精算离线测算场景：按地区开启**快照会话**，按理赔 id 升序分页提取明细。
会话期间保单/理赔两表固定为开启时刻的同一快照，源端的增改删照常提交、
互不影响；会话有数量上限与 TTL，可主动关闭，关闭/到期后未开始响应的取页
一律失败且不返回明细。

## 一、快速启动（Docker Compose）

```bash
docker compose up --build
```

- API：<http://localhost:8000>
- Swagger 交互文档：<http://localhost:8000/docs>
- OpenAPI 原文：<http://localhost:8000/openapi.json>
- PostgreSQL：localhost:55432（库 `claims`，用户/密码 `actuary/actuary`）

首次启动时 `db/init/` 下脚本按文件名顺序自动执行：

| 文件 | 作用 |
| --- | --- |
| `01_schema.sql` | 建表：`policies(id, region)`、`claims(id, policy_id, amount)`，金额为非负整数（单位：分） |
| `02_seed.sql` | 演示数据：3 个地区、260 条理赔（华北 120 / 华东 100 / 华南 40） |
| `03_sql_examples.sql` | SQL 补录示例：新增保单/理赔、改金额、删除、按地区汇总 |

## 二、核心语义与实现方式

| 需求 | 实现 |
| --- | --- |
| 两表同一快照、后页/重复页一致 | 每会话独占一条连接，开启 `START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`，连接保持 idle-in-transaction，快照事务全程不结束 |
| 源端增改删照常提交 | 写操作走连接池内其它自动提交连接，MVCC 下与快照只读事务互不阻塞 |
| 不预存结果全集 | 每次取页现场执行带 `LIMIT/OFFSET` 的 SQL，服务端不缓存明细 |
| claim_id 升序、每页至多 100 行 | `ORDER BY c.id ASC LIMIT n+1 OFFSET ...`，多取 1 行判断 `has_more`；`page_size` 服务端强制截断为 100 |
| 最多 2 个会话，满额拒绝 | 内存注册表 + 名额计数，超额返回 `429 capacity_reached`（可用 `MAX_SESSIONS` 调整） |
| 开启 60 秒到期 | 后台 sweeper 线程定时清理（`SESSION_TTL_SECONDS` 可调），到期取页返回 410/409 且无明细 |
| 主动关闭及时受理 | 关闭请求先把状态翻为 `closing`（新取页立即失败），等待在途取页结束后回滚事务、归还连接、释放名额，再回报 `closed` |
| 不影响另一会话/后续新会话 | 名额只统计 open/closing 会话；终态不占名额，连接归还池后立即可复用 |
| 连接丢失/服务重启后旧会话失效 | 会话状态只存进程内存；连接断裂的会话作废返回 503 且绝不换快照续取；重启后旧 id 查询 404 |

### 会话状态机

```
open ──主动关闭──▶ closing ──回滚事务/还连接/释放名额──▶ closed
  │                                                  ▲
  └──TTL 到期（sweeper）─────────────────────────── expired
```

终态记录保留 10 分钟供状态查询，之后从注册表回收；终态不占用会话名额。

## 三、API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/sessions` | 按地区开启快照会话，body：`{"region": "华北"}` |
| GET | `/sessions` | 列出会话 |
| GET | `/sessions/{id}` | 查询会话状态 |
| POST | `/sessions/{id}/close` | 主动关闭 |
| GET | `/sessions/{id}/claims?page=1&page_size=100` | 快照内分页取理赔明细 |
| POST | `/admin/policies` | 源端补录保单 |
| POST | `/admin/claims` | 源端补录理赔 |
| PATCH | `/admin/claims/{id}?amount=999` | 源端改金额 |
| DELETE | `/admin/claims/{id}` | 源端删除理赔 |
| GET | `/health`、`/meta` | 健康检查、服务参数 |

取页失败响应只含 `detail`/`code`，不含任何明细行：

```json
HTTP 409
{"detail": "会话 ... 关闭已受理或会话已结束，取页失败且不返回明细",
 "code": "session_not_open"}
```

## 四、慢取页演示

镜像默认 `ENABLE_DEBUG_HOOKS=true`，带 `X-Debug-Sleep-Seconds` 请求头
可让本次取页在执行 SQL 前先睡眠，用于演示"关闭及时受理"：

```bash
python3 demo/demo.py                 # 完整端到端演示（标准库即可运行）
BASE_URL=http://localhost:8000 python3 demo/demo.py
DEMO_TTL_WAIT=1 python3 demo/demo.py # 追加等待到期场景（约多花 1 分钟）
```

演示覆盖：分页与重复取页一致性、源端写入的快照隔离、名额满 429、
慢取页期间关闭（在途请求结束后失败、无明细）、到期失效。

## 五、本地自测（无 Docker 时）

仓库测试以真实 uvicorn + PostgreSQL 运行：

```bash
pip install -r requirements.txt
# 准备一个 PostgreSQL 15，并执行 db/init/01_schema.sql、02_seed.sql
DATABASE_URL=postgresql://actuary:actuary@127.0.0.1:5432/claims \
SESSION_TTL_SECONDS=2 ENABLE_DEBUG_HOOKS=true \
python -m unittest tests.test_api -v
```

15 个用例覆盖：分页/上限/参数校验、重复取页同快照、两表 JOIN 快照、
源端增改删不可见、名额与释放、关闭幂等、慢取页+关闭竞争、
到期清理、连接丢失作废不续取、服务重启旧会话 404。

## 六、配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://actuary:actuary@db:5432/claims` | 数据库连接串 |
| `MAX_SESSIONS` | `2` | 并发会话上限 |
| `SESSION_TTL_SECONDS` | `60` | 会话存活秒数 |
| `PAGE_SIZE_CAP` | `100` | 单页行数硬上限 |
| `ENABLE_DEBUG_HOOKS` | `false`（compose 中为 true） | 是否允许慢取页调试头 |
| `SWEEPER_INTERVAL_SECONDS` | `0.25` | 到期检查间隔 |

> 说明：服务固定单 uvicorn worker——会话注册表与名额是进程内状态，
> 多 worker 会各自持有独立状态，破坏会话语义。
