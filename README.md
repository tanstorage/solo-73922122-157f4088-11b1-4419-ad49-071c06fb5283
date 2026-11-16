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
- Swagger 交互文档 `/docs`：<http://localhost:8000/docs>
- ReDoc 文档 `/redoc`：<http://localhost:8000/redoc>
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
| 开启 60 秒到期 | 后台 sweeper 线程定时检查（`SESSION_TTL_SECONDS` 可调）；到期先翻 `closing` 并停止在途 SQL，取页返回 410 且无明细 |
| 主动关闭及时受理 | 关闭请求先把状态翻为 `closing`（新取页立即失败），同时向在途 SQL 发 cancel；在途 SQL 结束后回滚事务、归还连接、释放名额，关闭请求才回报 `closed` |
| SQL 执行中关闭/到期也及时 | 取消不生效时在 `CANCEL_GRACE_SECONDS`（默认 1s）后强制 shutdown 该连接自身的 socket，在途读取立刻报错结束；绝不退化为"等读取自然读完" |
| 未开始响应的取页必失败 | 状态复核与"SQL 开始"登记在同一把锁内；关闭/到期受理后，尚未开始执行 SQL 的取页在拿到锁时直接失败，响应体不含 `rows` |
| 终态在 SQL 结束、连接归还后才写 | `closed`/`expired` 只在在途 SQL 已结束且连接 `putconn` 之后写入；名额（只统计 open/closing）此时才真正归还 |
| 重复关闭/迟到取页不重复归还 | `released` 标志 + 每会话 `io_lock`：并发/重复关闭共享同一次收尾，连接只 `putconn` 一次；终态后迟到取页只读到 409 |
| 不误停另一会话/复用的新会话 | `cancel`/强制中断都要求 `not released and query_active` 且在同一把锁内检查；连接归还池后绝不再对其发取消 |
| 连接丢失/服务重启后旧会话失效 | 会话状态只存进程内存；连接断裂的会话就地作废返回 503 且绝不换快照续取；重启后旧 id 查询 404 |
| 状态查询与另一会话取数不受影响 | 收尾只占用本会话的锁与连接；`GET /sessions/{id}` 直接读注册表，B 会话与之后新开会话不受 A 关闭影响 |

### 会话状态机

```
open ──主动关闭──▶ closing ──停 SQL/回滚事务/还连接──▶ closed
  │                 ▲                                （SQL 结束并归还连接后才写入）
  └──TTL 到期───────┘──────────────────────────────▶ expired
       （sweeper 受理后同样先停 SQL、再归还连接）
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

## 四、演示：SQL 执行中关闭 / 到期

镜像默认 `ENABLE_DEBUG_HOOKS=true`，提供两个演示头（仅调试模式生效）：

| 请求头 | 含义 |
| --- | --- |
| `X-Debug-SQL-Sleep-Seconds: N` | 本页在**同一快照事务内**先真实执行 `SELECT pg_sleep(N)`，模拟 "SQL 正在执行" 的慢查询；关闭/到期会取消并立即打断它 |
| `X-Debug-Pre-Sleep-Seconds: N` | 请求先等待 N 秒才开始执行 SQL，模拟 "尚未开始响应" 的取页；关闭/到期受理后它不会执行 SQL，直接 409/410 且无明细 |
| `X-Debug-Sleep-Seconds: N` | 旧头名，等同 `X-Debug-Pre-Sleep-Seconds` |

```bash
python3 demo/demo.py                 # 完整端到端演示（标准库即可运行）
BASE_URL=http://localhost:8000 python3 demo/demo.py
DEMO_TTL_WAIT=1 python3 demo/demo.py # 追加"SQL 执行中到期"场景（约多花 1 分钟）
```

演示覆盖：

1. 分页与重复取页一致性、`page_size` 截断；
2. 源端增/改/删提交后会话内快照不变；
3. 名额 2、满额 429、关闭后归还且互不影响；
4. **未开始的取页 + 关闭**：关闭 0.0xs 完成，慢请求不执行 SQL，409 且无 `rows`；
5. **SQL 执行中关闭**：SQL 原需 20 秒，关闭 1.x 秒完成并停止该 SQL，
   取页 2 秒左右以 409 返回、无明细；随后新会话取页正常；
6. **SQL 执行中到期**（`DEMO_TTL_WAIT=1`）：到期清理停止在途 SQL，
   取页 410 且无明细，终态 `expired`，名额归还。

示例输出（片段）：

```
=== 场景 5：SQL 执行中关闭：及时停止该会话正在执行的 SQL，且不返回明细 ===
  ✅ SQL 执行中关闭在 1.04s 内完成（SQL 原需 20s）
  ✅ 执行中的取页在关闭后失败（409/410），未返回明细
  ✅ 在途 SQL 被及时停止，未等它自然跑完
```

## 五、本地自测（无 Docker 时）

仓库测试以真实 uvicorn + PostgreSQL 运行：

```bash
pip install -r requirements.txt
# 准备一个 PostgreSQL 15，并执行 db/init/01_schema.sql、02_seed.sql
DATABASE_URL=postgresql://actuary:actuary@127.0.0.1:5432/claims \
SESSION_TTL_SECONDS=2 ENABLE_DEBUG_HOOKS=true \
python -m unittest tests.test_api -v
```

21 个用例覆盖：分页/上限/参数校验、重复取页同快照、两表 JOIN 快照、
源端增改删不可见、名额与释放、关闭幂等、未开始取页+关闭、
**SQL 执行中关闭（及时停止 SQL、耗时断言）**、并发/重复关闭只归还一次连接、
关闭不误停另一会话或复用的新会话、**SQL 执行中到期被打断且 410**、
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
| `CANCEL_GRACE_SECONDS` | `1.0` | 关闭/到期后等待 SQL 响应 cancel 的宽限；超时则强制中断该连接 |
| `CLOSE_WAIT_TIMEOUT_SECONDS` | `10` | 收尾等待在途 SQL 的硬上限 |

> 说明：服务固定单 uvicorn worker——会话注册表与名额是进程内状态，
> 多 worker 会各自持有独立状态，破坏会话语义。
