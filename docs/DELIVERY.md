# 交付文档：快照会话的关闭 / 到期收尾

本文档说明本轮针对「取页 SQL 执行中关闭会话或会话到期」的修复：会话在关闭/到期
受理后，**正在执行的 SQL 会被及时停止**；尚未开始响应的取页**必定失败且不含明细**；
SQL 结束并归还快照连接之后才写终态、归还名额；重复关闭与迟到取页不会重复归还连接，
也不会误停另一个会话或随后新开的会话。状态查询、另一会话取数、同快照分页与源端写入
均保持可用。

## 1. 可实际启动的 Docker Compose

```bash
docker compose up --build        # 首次会构建镜像并初始化数据库
docker compose ps                # db/api 均应为 healthy
curl http://localhost:8000/health
```

- API：<http://localhost:8000>
- **Swagger 文档（/docs）**：<http://localhost:8000/docs>
- ReDoc（/redoc）：<http://localhost:8000/redoc>
- OpenAPI 原文：<http://localhost:8000/openapi.json>
- PostgreSQL：`localhost:55432`，库 `claims`，用户/密码 `actuary/actuary`

`db/init/` 下脚本在首次启动时按文件名顺序自动执行：

| 文件 | 作用 |
| --- | --- |
| `01_schema.sql` | `policies(id, region)`、`claims(id, policy_id, amount)` |
| `02_seed.sql` | 3 个地区、260 条理赔（华北 120 / 华东 100 / 华南 40） |
| `03_sql_examples.sql` | 源端补录/改金额/删除/汇总的 SQL 示例 |

`api` 容器内固定单 uvicorn worker（会话注册表与名额是进程内状态），并带
`/health` healthcheck。

## 2. 收尾语义（修复后的关键点）

### 2.1 状态机与保证

```
open ──关闭受理──▶ closing ──在途 SQL 结束 + 连接归还池──▶ closed
  │                 ▲
  └──TTL 到期───────┘──────────────────────────────────▶ expired
```

- **受理即拒绝**：关闭请求或 sweeper 到期检查在第一时间把状态翻成 `closing`。
  该动作与取页内「SQL 开始执行」的登记在同一把 `state_lock` 内完成，因此：
  - 若取页尚未拿到连接使用权，它随后复核状态会直接失败（409/410，无 `rows`）；
  - 若取页正在执行 SQL，关闭方看得到 `query_active` 标记并立即发 cancel。
- **及时停止 SQL**：对在途 SQL 发送 PostgreSQL cancel（等待期间小步重试，覆盖
  “取消先于语句真正发出”的窗口）。取消在 `CANCEL_GRACE_SECONDS`（默认 1s）内
  未生效时，shutdown 该会话连接自身的 socket，让阻塞读取立刻报错返回；绝不退化为
  “等读取自然结束”。硬上限 `CLOSE_WAIT_TIMEOUT_SECONDS`（默认 10s）。
- **终态在收尾之后**：`closed`/`expired` 只在在途 SQL 已结束、事务已回滚、
  连接已 `putconn` 之后写入。名额只统计 `open/closing`，因此名额此刻才真正归还。
- **只归还一次**：每会话 `io_lock` 串行化收尾；`released` 标志在锁内先占位再归还。
  并发/重复关闭、终态后的迟到取页都只读状态，不会第二次归还连接或重复释放名额。
- **不误停他人**：`cancel`/强制中断都要求在 `state_lock` 内确认
  `not released and query_active`。连接一旦归还池（可能已被别的会话借用），
  本会话绝不会再对其发取消/断开。

### 2.2 关键交错

| 交错 | 结果 |
| --- | --- |
| 取页 SQL 执行中 ← 关闭 | SQL 被取消/中断（约 1–2s 内结束），取页 409，无明细；关闭在 SQL 结束后返回 `closed` |
| 取页 SQL 执行中 ← TTL 到期 | 同上，sweeper 停止 SQL，取页 410，终态 `expired`，名额归还 |
| 取页尚未开始响应 ← 关闭/到期 | 直接 409/410，不执行 SQL，无明细 |
| SQL 已跑完、响应尚未组装 ← 关闭 | 响应前最后复核状态，本页不返回明细 |
| 并发多次关闭同一会话 | 都返回 `closed`，连接只归还一次 |
| 关闭 A 时 B 在取数 / 之后新开会话复用连接 | B 与新会话正常，不被误停 |
| 终态后迟到取页 | 409，不触发任何归还 |

## 3. 演示（含 SQL 执行中关闭与到期）

镜像 `ENABLE_DEBUG_HOOKS=true`，提供：

| 请求头 | 作用 |
| --- | --- |
| `X-Debug-SQL-Sleep-Seconds: N` | 在同一快照事务内先执行 `SELECT pg_sleep(N)`，模拟 SQL 正在执行 |
| `X-Debug-Pre-Sleep-Seconds: N` | 开始执行 SQL 前先等待 N 秒，模拟“尚未开始响应” |
| `X-Debug-Sleep-Seconds: N` | 旧头名，等同 `X-Debug-Pre-Sleep-Seconds` |

```bash
python3 demo/demo.py                    # 35 项检查：分页/快照/名额/未开始取页+关闭/SQL执行中关闭
DEMO_TTL_WAIT=1 python3 demo/demo.py    # 追加 SQL 执行中到期（约多 75 秒），共 41 项
```

一次真实运行的关键输出：

```
=== 场景 5：SQL 执行中关闭：及时停止该会话正在执行的 SQL，且不返回明细 ===
  ✅ SQL 执行中关闭在 1.04s 内完成（SQL 原需 20s）
  ✅ 执行中的取页在关闭后失败（409/410），未返回明细
  ✅ 失败响应不含任何理赔明细行
  ✅ 在途 SQL 被及时停止，未等它自然跑完
  ✅ 关闭后会话状态仍可查询，终态=closed
  ✅ 随后新开的会话取页正常（连接未被误停/误还）

=== 场景 6：到期：60 秒后会话自动到期，执行中的 SQL 被停止，取页 410 且无明细 ===
  ✅ 到期打断了执行中的 SQL，取页失败
  ✅ 到期失败响应不含理赔明细行
  ✅ 会话状态可查，终态=expired
  ✅ 到期释放名额后可开启新会话
```

## 4. 自测

```bash
pip install -r requirements.txt
# 准备 PostgreSQL 15，执行 db/init/01_schema.sql、02_seed.sql
DATABASE_URL=postgresql://actuary:actuary@127.0.0.1:5432/claims \
SESSION_TTL_SECONDS=2 ENABLE_DEBUG_HOOKS=true \
python -m unittest tests.test_api -v
```

21 个用例，新增/强化的关键交错：

- `test_close_while_sql_executing_stops_query_promptly`：SQL 执行中关闭，
  断言关闭耗时 < 10s、在途请求 < 10s 结束、409 无明细。
- `test_connection_returned_exactly_once`：并发关闭 + 迟到取页，
  监视 `putconn` 断言该连接只归还一次。
- `test_repeated_close_and_late_page_release_connection_once`：重复关闭、
  终态后再关闭、名额不重复归还。
- `test_close_does_not_cancel_other_session_or_reused_connection`：关闭 A 时
  B 的慢 SQL 完整跑完；关闭后新会话复用池连接照常取页。
- `test_close_after_sql_completed_but_before_response_prepared`：未开始响应的
  取页在关闭后失败且无明细。
- `test_expiry_while_sql_executing_stops_query`：SQL 执行中到期，410 无明细、
  终态 expired、名额归还。

配置项补充：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `CANCEL_GRACE_SECONDS` | `1.0` | 等 SQL 响应 cancel 的宽限，超时强制中断该连接 |
| `CLOSE_WAIT_TIMEOUT_SECONDS` | `10` | 收尾等待在途 SQL 的硬上限 |

## 5. 已知限制

- 会话与名额是**进程内状态**：仅支持单 worker；服务重启后旧会话不可续取（404）。
- 强制中断兜底依赖 `pgconn.socket` 可得（TCP/Unix socket 均满足）；
  连接已断开时走既有的 503「会话作废」路径。
- 取消请求可被数据库立即接受并不代表服务端一定已停止，因此保留 socket 强制中断
  与硬超时两层兜底。
- 演示用的 SQL 慢查询靠 `pg_sleep` 调试头触发，生产应关闭 `ENABLE_DEBUG_HOOKS`。
