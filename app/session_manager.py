"""会话注册表与快照生命周期管理。

设计要点
========
1. 每个会话独占一条 PostgreSQL 连接，并在其上开启
   ``START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY``。
   连接以 "idle in transaction" 状态保持，事务快照在整个会话期间不变；
   源端其它连接上的增/改/删照常提交，本会话各页（含重复取页）读到的
   两张表始终是开启那一刻的同一快照（MVCC，读写互不阻塞）。
2. 会话状态机：``open -> closing -> closed/expired``。关闭请求受理的
   第一时间即把状态翻成 ``closing``，此后尚未开始响应的取页一律失败；
   已经在执行中的取页允许跑完，随后回滚事务、归还连接、释放名额。
3. 名额（默认 2）在内存注册表中控制，会话状态全部保存在本进程内；
   进程重启或与数据库的连接丢失后旧会话不可恢复，符合
   "不得换快照续取" 的要求。
"""
from __future__ import annotations

import threading
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
    # state_lock 保护 status / 计数器 / 时间戳；
    # io_lock 串行化同一会话上的查询，保证取页与关闭不会并发使用连接。
    state_lock: threading.RLock = field(default_factory=threading.RLock)
    io_lock: threading.Lock = field(default_factory=threading.Lock)
    status: str = "open"
    pages_served: int = 0
    last_page_at: Optional[datetime] = None
    in_flight: int = 0
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
    def fetch_page(self, session_id: str, page: int, page_size: int):
        """在会话快照上取第 ``page`` 页（从 1 开始），claim_id 升序。"""
        session = self._get(session_id)

        with session.state_lock:
            if session.status != "open":
                raise SessionUnavailable(
                    "session_not_open",
                    f"会话 {session_id} 状态为 {session.status}，"
                    "关闭已受理或会话已结束，取页失败且不返回明细",
                    http_status=409,
                )
            if _utcnow() >= session.expires_at:
                # 到期瞬间、清理线程尚未接手：直接拒绝，清理稍后完成。
                raise SessionUnavailable(
                    "session_expired",
                    f"会话 {session_id} 已到期，取页失败且不返回明细",
                    http_status=410,
                )
            session.in_flight += 1

        try:
            # io_lock 保证关闭流程与查询不会同时操作同一条连接。
            with session.io_lock:
                # 拿到连接使用权后再复核一次状态：关闭可能在等锁期间受理。
                with session.state_lock:
                    if session.status != "open":
                        raise SessionUnavailable(
                            "session_not_open",
                            f"会话 {session_id} 关闭已受理，"
                            "该取页尚未开始响应，已失败且不返回明细",
                            http_status=409,
                        )
                rows, has_more = self._query_page(session, page, page_size)
                with session.state_lock:
                    session.pages_served += 1
                    session.last_page_at = _utcnow()
                return rows, has_more
        except SessionUnavailable:
            raise
        except psycopg.Error as exc:
            # 连接丢失等数据库错误：会话作废，绝不换连接/换快照续取。
            self._teardown(session, reason="connection_lost")
            raise SessionError(
                "session_broken",
                f"数据库连接或快照失效，会话 {session_id} 已作废，"
                f"不会换快照续取：{exc.sqlstate_name if hasattr(exc, 'sqlstate_name') else exc}",
                http_status=503,
            )
        finally:
            with session.state_lock:
                session.in_flight -= 1

    def _query_page(self, session: Session, page: int, page_size: int):
        # 多取一行判断 has_more；OFFSET 分页基于快照内固定结果集，
        # 重复取同一页得到完全相同的数据。
        offset = (page - 1) * page_size
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
        """受理关闭：先翻状态拒绝新取页，再停事务、还连接、释放名额。"""
        session = self._get(session_id)
        with session.state_lock:
            if session.status in ("closed", "expired"):
                return session
            # 第一时间受理：此后任何取页请求都失败。
            session.status = "closing"
        # 等待在途取页结束（它们持有 io_lock），然后独占连接做清理。
        self._teardown(session, reason=reason)
        return session

    def _teardown(self, session: Session, reason: str) -> None:
        with session.io_lock:
            with session.state_lock:
                if session.status in ("closed", "expired"):
                    return
            try:
                # 停止该会话自身的 SQL：回滚即结束快照事务。
                session.conn.rollback()
            except psycopg.Error:
                pass
            if self._pool is not None:
                try:
                    # 释放快照连接，归还连接池供其它会话/新会话使用。
                    self._pool.putconn(session.conn)
                except Exception:
                    pass
            with session.state_lock:
                session.status = "expired" if reason == "expired" else "closed"
        # 终态记录保留在注册表中以便查询会话状态；名额只统计
        # open/closing 会话（见 open_session），故终态记录不占名额。

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
        for session in due:
            try:
                self.close(session.session_id, reason="expired")
            except SessionError:
                pass
            except Exception as exc:
                # 连接丢失等情况下，先在注册表内标记终态释放名额，
                # 失效连接留给连接池自身的健康检查回收。
                print(f"[sweeper] 会话 {session.session_id} 到期清理失败: {exc!r}",
                      flush=True)
                with session.state_lock:
                    if session.status == "open":
                        session.status = "expired"
        with self._registry_lock:
            for session in stale:
                self._registry.pop(session.session_id, None)

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
