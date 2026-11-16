"""会话注册表与快照生命周期管理。

设计要点
========
1. 每个会话独占一条 PostgreSQL 连接，并在其上开启
   ``START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY``。
   连接以 "idle in transaction" 状态保持，事务快照在整个会话期间不变；
   源端其它连接上的增/改/删照常提交，本会话各页（含重复取页）读到的
   两张表始终是开启那一刻的同一快照（MVCC，读写互不阻塞）。
2. 会话状态机：``open -> closing -> closed/expired``。关闭请求或到期清理
   受理的第一时间即把状态翻成 ``closing``，此后尚未开始响应的取页一律失败；
   同时对正在执行的 SQL 发送 PostgreSQL cancel 请求（并小步重试，覆盖
   "取消先于语句真正发出" 的窗口），让在途查询尽快结束。取消在
   ``cancel_grace_seconds`` 内仍未生效时，shutdown 本会话连接自身的 socket
   强制打断读取，绝不退化为"等读取自然结束"。
3. 终态（``closed``/``expired``）只在**在途 SQL 已结束且快照连接已归还
   连接池**之后才写入；名额只统计 open/closing 会话，因此名额也在那时才
   真正归还。重复关闭、迟到取页都只观察状态，不会二次归还同一条连接。
4. ``cancel``/强制中断只允许在连接仍归本会话所有时执行（``released`` 与
   ``query_active`` 标志和操作在同一把锁内检查），连接归还池后绝不再动它，
   避免误停复用了该连接的其它会话/新会话正在执行的 SQL。
5. 名额（默认 2）在内存注册表中控制，会话状态全部保存在本进程内；
   进程重启或与数据库的连接丢失后旧会话不可恢复，符合
   "不得换快照续取" 的要求。
"""
from __future__ import annotations

import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import Settings


class SessionError(Exception):
    """会话层面的可预期错误，路由层据此映射 HTTP 状态码。"""

    def __init__(self, code: str, message: str, http_status: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


class CapacityReached(SessionError):
    def __init__(self, max_sessions: int):
        super().__init__(
            "capacity_reached",
            f"会话名额已满（最多 {max_sessions} 个），请关闭其它会话后重试",
            http_status=429,
        )


class SessionNotFound(SessionError):
    def __init__(self, session_id: str):
        super().__init__(
            "session_not_found",
            f"会话 {session_id} 不存在或已失效（服务重启/连接丢失后旧会话不可续取）",
            http_status=404,
        )


class SessionUnavailable(SessionError):
    """会话处于 closing/closed/expired，取页请求被拒绝。"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Session:
    session_id: str
    region: str
    conn: psycopg.Connection
    opened_at: datetime
    expires_at: datetime
    # state_lock 保护 status / 连接归属 / 计数器 / 时间戳。
    # io_lock 串行化同一会话上的查询与收尾；顺序固定为 io_lock -> state_lock。
    state_lock: threading.RLock = field(default_factory=threading.RLock)
    io_lock: threading.Lock = field(default_factory=threading.Lock)
    status: str = "open"
    pages_served: int = 0
    last_page_at: Optional[datetime] = None
    in_flight: int = 0
    # query_active: 当前是否有一条取页 SQL 正占用连接执行（允许被 cancel）。
    query_active: bool = False
    # released: 快照连接是否已归还连接池；只有仍归本会话所有时才允许 cancel。
    released: bool = False
    cancel_requested: bool = False
    # 首个关闭/到期受理原因（"closed"/"expired"），决定终态与取页错误码。
    close_reason: Optional[str] = None
    snapshot_time: Optional[datetime] = None
    snapshot_id: Optional[str] = None

    def is_open(self) -> bool:
        with self.state_lock:
            return self.status == "open" and _utcnow() < self.expires_at


class SessionManager:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._registry: dict[str, Session] = {}
        self._registry_lock = threading.RLock()
        self._pool: Optional[ConnectionPool] = None
        self._stop_event = threading.Event()
        self._sweeper: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    @staticmethod
    def _reset_connection(conn: psycopg.Connection) -> None:
        """连接归还池时：回滚残留事务并恢复自动提交，供下一个借用者干净使用。"""
        try:
            if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                conn.rollback()
        except psycopg.Error:
            pass
        try:
            conn.autocommit = True
        except psycopg.Error:
            pass

    def start(self) -> None:
        # 池默认自动提交：admin 写语句执行即提交；
        # 会话连接借出后显式 START TRANSACTION 长期持有快照事务。
        if self._pool is None or self._pool.closed:
            self._pool = ConnectionPool(
                self._settings.database_url,
                min_size=self._settings.pool_min_size,
                max_size=self._settings.pool_max_size,
                kwargs={"row_factory": dict_row, "autocommit": True},
                # 连接归还池时恢复自动提交并回滚任何残留事务
                reset=self._reset_connection,
                open=True,
            )
        # 每次 start 都确保清理线程在运行（stop 后重新 start 时需要重建）
        if self._sweeper is None or not self._sweeper.is_alive():
            self._stop_event = threading.Event()
            self._sweeper = threading.Thread(
                target=self._sweep_loop, name="session-sweeper", daemon=True
            )
            self._sweeper.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._sweeper is not None:
            self._sweeper.join(timeout=5)
        self._sweeper = None
        with self._registry_lock:
            session_ids = list(self._registry.keys())
        for sid in session_ids:
            try:
                self.close(sid, reason="shutdown")
            except SessionError:
                pass
        if self._pool is not None:
            self._pool.close()
            self._pool = None
        # 进程重启语义：内存注册表全部丢弃，旧会话 id 不可再查、不可续取
        with self._registry_lock:
            self._registry.clear()

    # ------------------------------------------------------------------
    # 开启会话
    # ------------------------------------------------------------------
    def open_session(self, region: str) -> Session:
        with self._registry_lock:
            # 名额检查与注册表写入在同一把锁内完成，避免并发超额。
            active = [
                s for s in self._registry.values()
                if s.status in ("open", "closing")
            ]
            if len(active) >= self._settings.max_sessions:
                raise CapacityReached(self._settings.max_sessions)

            assert self._pool is not None
            conn = self._pool.getconn()
            now = _utcnow()
            session = Session(
                session_id=uuid.uuid4().hex,
                region=region,
                conn=conn,
                opened_at=now,
                expires_at=now
                + timedelta(seconds=self._settings.session_ttl_seconds),
            )
            try:
                # 保持 autocommit=True：直接发 START TRANSACTION 由服务端
                # 显式开启 RR 只读事务（若先关 autocommit，psycopg 会隐式
                # 发 BEGIN，再发 START TRANSACTION 会产生重复事务警告）。
                # 事务在首条语句冻结快照，连接随会话停留在
                # "idle in transaction" 状态，快照保持不变。
                conn.execute(
                    "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                # pg_export_snapshot 返回当前事务快照 ID，
                # 同时确认快照已在事务内建立（RR 下首条语句即冻结快照）。
                row = conn.execute(
                    "SELECT now() AS snapshot_time, "
                    "pg_export_snapshot() AS snapshot_id"
                ).fetchone()
                session.snapshot_time = row["snapshot_time"]
                session.snapshot_id = row["snapshot_id"]
            except Exception:
                conn.rollback()
                self._pool.putconn(conn)
                raise
            self._registry[session.session_id] = session
            return session

    # ------------------------------------------------------------------
    # 取页
    # ------------------------------------------------------------------
    def fetch_page(
        self,
        session_id: str,
        page: int,
        page_size: int,
        debug_sql_sleep: float = 0.0,
    ):
        """在会话快照上取第 ``page`` 页（从 1 开始），claim_id 升序。

        ``debug_sql_sleep`` 仅由调试钩子传入，用于让本页 SQL 在数据库内
        真实执行较久（``pg_sleep``），便于演示/验证"SQL 执行中关闭或到期"。
        """
        session = self._get(session_id)

        with session.state_lock:
            self._ensure_fetchable_locked(session)
            session.in_flight += 1

        try:
            # io_lock 保证关闭流程与查询不会同时操作同一条连接。
            with session.io_lock:
                # 等锁期间关闭/到期可能已受理：尚未开始响应的取页立即失败。
                with session.state_lock:
                    self._ensure_fetchable_locked(session)
                    # 与关闭流程在同一把锁内登记"SQL 正在执行"：
                    # 关闭方随后要么看到该标志并 cancel，要么先翻状态使
                    # 本次取页在此处直接失败，不存在漏取消的窗口。
                    session.query_active = True
                try:
                    rows, has_more = self._query_page(
                        session, page, page_size, debug_sql_sleep
                    )
                finally:
                    with session.state_lock:
                        session.query_active = False
                # 响应发出前最后复核：即便 SQL 已经跑完，只要关闭/到期已
                # 受理，本页也不得返回任何明细。
                with session.state_lock:
                    self._ensure_fetchable_locked(session)
                    session.pages_served += 1
                    session.last_page_at = _utcnow()
                return rows, has_more
        except SessionUnavailable:
            raise
        except psycopg.Error as exc:
            # 关闭/到期触发的 cancel 会让在途 SQL 以 QueryCanceled 结束：
            # 这是预期失败（409/410），不是连接损坏，绝不能在这里还连接。
            with session.state_lock:
                if session.status != "open" or session.cancel_requested:
                    raise self._unavailable_locked(session) from None
            # 真正的连接丢失等数据库错误：会话作废，绝不换连接/换快照续取。
            # 此处已退出 io_lock 区域，按正常收尾流程获取锁、回滚并归还。
            self._teardown(session, "connection_lost")
            raise SessionError(
                "session_broken",
                f"数据库连接或快照失效，会话 {session_id} 已作废，"
                f"不会换快照续取：{self._sqlstate_name(exc)}",
                http_status=503,
            )
        finally:
            with session.state_lock:
                session.in_flight -= 1

    def _query_page(
        self,
        session: Session,
        page: int,
        page_size: int,
        debug_sql_sleep: float = 0.0,
    ):
        offset = (page - 1) * page_size
        if debug_sql_sleep > 0:
            # 调试：先在同一连接、同一快照事务内执行一条真实长 SQL
            # （pg_sleep），再执行取页 SQL。关闭/到期时该语句会被 cancel
            # 打断——这正是"应停止该会话正在执行的 SQL"的演示路径。
            session.conn.execute("SELECT pg_sleep(%s)", (debug_sql_sleep,))
        # 多取一行判断 has_more；OFFSET 分页基于快照内固定结果集，
        # 重复取同一页得到完全相同的数据。
        sql = """
            SELECT c.id          AS claim_id,
                   c.policy_id   AS policy_id,
                   p.region      AS region,
                   c.amount      AS amount
              FROM claims c
              JOIN policies p ON p.id = c.policy_id
             WHERE p.region = %s
             ORDER BY c.id ASC
             LIMIT %s OFFSET %s
        """
        rows = session.conn.execute(
            sql, (session.region, page_size + 1, offset)
        ).fetchall()
        has_more = len(rows) > page_size
        if has_more:
            rows = rows[:page_size]
        return rows, has_more

    # ------------------------------------------------------------------
    # 关闭 / 到期
    # ------------------------------------------------------------------
    def close(self, session_id: str, reason: str = "closed") -> Session:
        """受理关闭：翻状态拒绝新取页并 cancel 在途 SQL，收尾完成后返回终态。

        返回时保证：该会话在途 SQL 已结束、快照连接已归还池、名额已释放。
        重复关闭（含并发关闭）只会等待同一个收尾结果，不会二次归还连接。
        """
        session = self._get(session_id)
        with session.state_lock:
            if session.status in ("closed", "expired"):
                return session
            self._accept_close_locked(session, reason)
        self._teardown(session, reason)
        return session

    def _accept_close_locked(self, session: Session, reason: str) -> None:
        """（须持 state_lock）受理关闭/到期：翻状态、记录原因、取消在途 SQL。"""
        if session.status in ("closed", "expired"):
            return
        if session.status == "open":
            session.status = "closing"
        if session.close_reason is None:
            session.close_reason = reason
        session.cancel_requested = True
        self._cancel_locked(session)

    def _cancel_locked(self, session: Session) -> None:
        """（须持 state_lock）向本会话**仍在执行**的 SQL 发送 cancel 请求。

        ``released`` 与 ``query_active`` 都在 state_lock 内检查：连接一旦归还
        连接池（可能已被其它会话/新会话借用），这里绝不会再向它发 cancel，
        因此不会误停别的会话。
        """
        if session.released or not session.query_active:
            return
        conn = session.conn
        if conn.closed:
            return
        try:
            # psycopg 的 cancel 向该连接自己的后端发取消请求，可从其它线程调用。
            conn.cancel()
        except Exception:
            pass

    def _abort_locked(self, session: Session) -> None:
        """（须持 state_lock）强制中断在途 SQL：shutdown 该连接自身的 socket。

        仅当连接仍归本会话所有（``not released``）且确有一条 SQL 在执行时
        才动手，因此不会碰到池中已复用给其它会话的连接。socket shutdown
        让阻塞中的读取立刻报错返回，比 cancel 更硬，作为取消不生效时的兜底。
        """
        if session.released or not session.query_active:
            return
        conn = session.conn
        if conn.closed:
            return
        try:
            fd = conn.pgconn.socket
            if fd >= 0:
                # 借用同一个 fd：只 shutdown 收发方向，随后 detach，避免
                # 临时 socket 对象析构时把连接仍在使用的 fd 关掉。
                sock = socket.socket(fileno=fd)
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                finally:
                    sock.detach()
        except OSError:
            pass
        except Exception:
            pass

    def _teardown(self, session: Session, reason: str) -> None:
        """等待在途 SQL 结束 -> 回滚快照事务 -> 归还连接 -> 写终态。

        等待期间持续小步重发 cancel：若首次取消恰好落在"标志已置、
        语句尚未发到服务端"的窗口，下一轮会补上。若取消在
        ``cancel_grace_seconds`` 内仍未生效（例如服务端/网络不支持取消），
        强制关闭本会话的 socket 让在途读取立即报错返回——关闭/到期不能
        退化为"等读取自然结束"。强制关闭只针对本会话尚未归还的连接。
        """
        started = time.monotonic()
        grace_deadline = started + self._settings.cancel_grace_seconds
        hard_deadline = started + self._settings.close_wait_timeout_seconds
        # 极端兜底：socket 强断后读取仍卡住时，最终退回阻塞等待，避免忙等。
        final_deadline = hard_deadline + 30
        forced = False
        while True:
            if session.io_lock.acquire(timeout=0.05):
                break
            now = time.monotonic()
            with session.state_lock:
                if not forced:
                    self._cancel_locked(session)
                if (
                    not forced
                    and now >= grace_deadline
                    and not session.released
                    and session.query_active
                ):
                    # 兜底：连接对取消无响应时强制关闭 socket，让在途读取
                    # 立刻报错返回，避免关闭请求无限期挂起。
                    forced = True
                    print(
                        f"[teardown] 会话 {session.session_id} 的 SQL 未响应取消，"
                        "强制中断该会话连接",
                        flush=True,
                    )
                    self._abort_locked(session)
            if now >= hard_deadline and not forced:
                forced = True
                print(
                    f"[teardown] 会话 {session.session_id} 在 "
                    f"{self._settings.close_wait_timeout_seconds}s 内未结束，"
                    "强制中断连接",
                    flush=True,
                )
                with session.state_lock:
                    self._abort_locked(session)
            if now >= final_deadline:
                # 兜底的最后一道：继续阻塞等待收尾线程放开 io_lock，
                # 保证「终态在 SQL 结束、连接归还之后」不被破坏。
                session.io_lock.acquire()
                break
        self._finish_teardown(session, reason)

    def _finish_teardown(self, session: Session, reason: str) -> None:
        """（须持 io_lock）归还连接、写终态，保证只归还一次。"""
        try:
            with session.state_lock:
                if session.status in ("closed", "expired"):
                    return
                if session.status == "open":
                    # 连接丢失等内部收尾：先进入 closing 再写终态。
                    session.status = "closing"
                if session.close_reason is None:
                    session.close_reason = reason
                terminal = (
                    "expired" if session.close_reason == "expired" else "closed"
                )
                if session.released:
                    # 另一个线程已完成归还，这里只补齐终态，绝不二次 putconn。
                    session.status = terminal
                    return
                # 先占住归还权：此后任何线程的 cancel 都会因 released 跳过。
                session.released = True
                session.query_active = False
                conn = session.conn
            try:
                # 结束该会话自身的快照事务（同时停掉可能残留的只读事务）。
                if not conn.closed and (
                    conn.info.transaction_status
                    != psycopg.pq.TransactionStatus.IDLE
                ):
                    conn.rollback()
            except psycopg.Error:
                pass
            if self._pool is not None:
                try:
                    # 释放快照连接，归还连接池供其它会话/新会话使用
                    # （连接已被强制关闭时，池会丢弃它而不是复用）。
                    self._pool.putconn(conn)
                except Exception:
                    pass
            with session.state_lock:
                # 终态只在连接归还之后写入；名额统计只看 open/closing，
                # 因此此刻才真正归还名额。
                session.status = terminal
        finally:
            session.io_lock.release()

    def _sweep_loop(self) -> None:
        while not self._stop_event.wait(self._settings.sweeper_interval_seconds):
            try:
                self._sweep_once()
            except Exception as exc:
                # 清理线程永不因单次异常（含数据库连接丢失）退出。
                print(f"[sweeper] 本轮清理失败，将继续重试: {exc!r}", flush=True)

    def _sweep_once(self) -> None:
        now = _utcnow()
        with self._registry_lock:
            due = [
                s for s in list(self._registry.values())
                if s.status == "open" and now >= s.expires_at
            ]
            # 终态记录保留 10 分钟供状态查询，之后回收注册表项。
            stale = [
                s for s in list(self._registry.values())
                if s.status in ("closed", "expired")
                and now - s.expires_at > timedelta(minutes=10)
            ]
        # 第一阶段：所有到期会话立即翻 closing 并 cancel，保证到期后任何
        # 尚未开始响应的取页立刻失败，而不是被逐个收尾拖住。
        for session in due:
            try:
                with session.state_lock:
                    if (
                        session.status == "open"
                        and _utcnow() >= session.expires_at
                    ):
                        self._accept_close_locked(session, "expired")
            except Exception as exc:
                print(f"[sweeper] 会话 {session.session_id} 到期受理失败: {exc!r}",
                      flush=True)
        # 第二阶段：等待在途 SQL 结束、回滚事务、归还连接，再写终态。
        for session in due:
            with session.state_lock:
                pending = session.status == "closing"
            if not pending:
                continue
            try:
                self._teardown(session, reason="expired")
            except Exception as exc:
                # 连接丢失等情况下，先在注册表内标记终态释放名额，
                # 失效连接留给连接池自身的健康检查回收。
                print(f"[sweeper] 会话 {session.session_id} 到期清理失败: {exc!r}",
                      flush=True)
                with session.state_lock:
                    if session.status not in ("closed", "expired"):
                        session.status = "expired"
                        session.released = True
        with self._registry_lock:
            for session in stale:
                self._registry.pop(session.session_id, None)

    # ------------------------------------------------------------------
    # 错误映射
    # ------------------------------------------------------------------
    def _ensure_fetchable_locked(self, session: Session) -> None:
        """（须持 state_lock）open 且未到期才放行，否则以 409/410 失败。"""
        if session.status == "open" and _utcnow() < session.expires_at:
            return
        raise self._unavailable_locked(session)

    def _unavailable_locked(self, session: Session) -> SessionUnavailable:
        """（须持 state_lock）构造不含明细的取页失败错误。"""
        # 终态/受理原因优先；仅在会话仍为 open（清理线程尚未接手）时
        # 才用墙钟判断是否已到期。
        expired = (
            session.close_reason == "expired"
            or session.status == "expired"
            or (
                session.status == "open"
                and _utcnow() >= session.expires_at
            )
        )
        if expired:
            return SessionUnavailable(
                "session_expired",
                f"会话 {session.session_id} 已到期，取页失败且不返回明细",
                http_status=410,
            )
        return SessionUnavailable(
            "session_not_open",
            f"会话 {session.session_id} 状态为 {session.status}，"
            "关闭已受理或会话已结束，取页失败且不返回明细",
            http_status=409,
        )

    @staticmethod
    def _sqlstate_name(exc: psycopg.Error) -> str:
        name = getattr(exc, "sqlstate_name", None)
        return name or str(exc)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def _get(self, session_id: str) -> Session:
        with self._registry_lock:
            session = self._registry.get(session_id)
        if session is None:
            raise SessionNotFound(session_id)
        return session

    def get_status(self, session_id: str) -> Session:
        return self._get(session_id)

    def list_sessions(self) -> list[Session]:
        with self._registry_lock:
            return list(self._registry.values())

    def pool(self) -> ConnectionPool:
        if self._pool is None:
            raise SessionError(
                "not_started", "服务尚未启动", http_status=503
            )
        return self._pool

    def ttl_seconds(self) -> int:
        return self._settings.session_ttl_seconds
