"""FastAPI 入口：理赔明细分批提取服务。"""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse

from .config import settings
from .schemas import (
    ClaimIn,
    ClaimOut,
    ClaimPage,
    ClaimRow,
    Message,
    OpenSessionRequest,
    PolicyIn,
    PolicyOut,
    SessionInfo,
    SessionStatus,
)
from .session_manager import SessionError, SessionManager

manager = SessionManager(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    manager.start()
    yield
    manager.stop()


app = FastAPI(
    title="理赔明细分批提取 API",
    description=(
        "按地区开启快照会话，按理赔 id 升序分页提取。\n\n"
        "- 会话内两表（policies/claims）固定为开启时刻的同一快照\n"
        "- 源端增改删照常提交，不影响会话内读取\n"
        f"- 最多 {settings.max_sessions} 个并发会话，会话 {settings.session_ttl_seconds} 秒到期\n"
        "- 关闭受理后未开始响应的取页一律失败，不返回明细"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ----------------------------------------------------------------------
# 异常映射
# ----------------------------------------------------------------------
@app.exception_handler(SessionError)
async def session_error_handler(request: Request, exc: SessionError):
    return JSONResponse(
        status_code=exc.http_status,
        content={"detail": exc.message, "code": exc.code},
    )


def _session_info(s) -> SessionInfo:
    return SessionInfo(
        session_id=s.session_id,
        region=s.region,
        status=SessionStatus(s.status),
        opened_at=s.opened_at,
        expires_at=s.expires_at,
        last_page_at=s.last_page_at,
        pages_served=s.pages_served,
        ttl_seconds=settings.session_ttl_seconds,
    )


# ----------------------------------------------------------------------
# 健康检查与服务参数
# ----------------------------------------------------------------------
@app.get("/health", tags=["meta"], summary="健康检查")
def health():
    return {"status": "ok"}


@app.get("/meta", tags=["meta"], summary="服务参数（TTL、页上限、会话名额）")
def meta():
    return {
        "max_sessions": settings.max_sessions,
        "session_ttl_seconds": settings.session_ttl_seconds,
        "page_size_cap": settings.page_size_cap,
        "close_wait_timeout_seconds": settings.close_wait_timeout_seconds,
        "cancel_grace_seconds": settings.cancel_grace_seconds,
        "debug_hooks_enabled": settings.enable_debug_hooks,
    }


# ----------------------------------------------------------------------
# 会话管理
# ----------------------------------------------------------------------
@app.post(
    "/sessions",
    response_model=SessionInfo,
    status_code=201,
    tags=["sessions"],
    summary="按地区开启快照会话",
    responses={429: {"model": Message, "description": "会话名额已满"}},
)
async def open_session(body: OpenSessionRequest):
    # 同步的 psycopg 调用放到线程池，避免阻塞事件循环
    session = await asyncio.to_thread(manager.open_session, body.region)
    return _session_info(session)


@app.get(
    "/sessions",
    response_model=list[SessionInfo],
    tags=["sessions"],
    summary="列出当前会话（含终态记录）",
)
async def list_sessions():
    sessions = await asyncio.to_thread(manager.list_sessions)
    return [_session_info(s) for s in sessions]


@app.get(
    "/sessions/{session_id}",
    response_model=SessionInfo,
    tags=["sessions"],
    summary="查询会话状态",
    responses={404: {"model": Message}},
)
async def session_status(session_id: str):
    return _session_info(await asyncio.to_thread(manager.get_status, session_id))


@app.post(
    "/sessions/{session_id}/close",
    response_model=SessionInfo,
    tags=["sessions"],
    summary="主动关闭会话（受理后未开始的取页立即失败）",
    responses={404: {"model": Message}},
)
async def close_session(session_id: str):
    session = await asyncio.to_thread(manager.close, session_id)
    return _session_info(session)


# ----------------------------------------------------------------------
# 取页
# ----------------------------------------------------------------------
@app.get(
    "/sessions/{session_id}/claims",
    response_model=ClaimPage,
    tags=["claims"],
    summary="按理赔 id 升序取一页（每页至多 100 行，可重复取页）",
    responses={
        404: {"model": Message},
        409: {"model": Message, "description": "关闭已受理/会话不可用"},
        410: {"model": Message, "description": "会话已到期"},
        503: {"model": Message, "description": "快照连接失效，会话作废"},
    },
)
async def fetch_claims(
    session_id: str,
    request: Request,
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(
        100, ge=1, description="本页行数上限，服务端强制不超过 100"
    ),
):
    page_size = min(page_size, settings.page_size_cap)

    # 演示用慢取页钩子：仅当 ENABLE_DEBUG_HOOKS=true 时生效。
    # X-Debug-SQL-Sleep-Seconds：让本页 SQL 在数据库内真实执行 sleep，模拟
    #   “SQL 正在执行”的慢查询——关闭/到期会取消并立即打断该 SQL。
    # X-Debug-Pre-Sleep-Seconds：让请求在开始响应前先等待，模拟“尚未开始
    #   响应”的取页——关闭/到期受理后它不会执行 SQL，直接失败且无明细。
    # X-Debug-Sleep-Seconds：旧头名，等同 X-Debug-Pre-Sleep-Seconds。
    # 睡眠都放在线程池中，不阻塞事件循环与其它请求。
    sql_sleep = 0.0
    if settings.enable_debug_hooks:
        # 兼容旧演示头：X-Debug-Sleep-Seconds 语义为“开始响应前等待”。
        raw = request.headers.get(
            "x-debug-pre-sleep-seconds"
        ) or request.headers.get("x-debug-sleep-seconds")
        if raw:
            try:
                delay = max(0.0, min(120.0, float(raw)))
            except ValueError:
                raise HTTPException(status_code=400, detail="非法的调试延迟头")
            await asyncio.to_thread(time.sleep, delay)
        raw_sql = request.headers.get("x-debug-sql-sleep-seconds")
        if raw_sql:
            try:
                sql_sleep = max(0.0, min(120.0, float(raw_sql)))
            except ValueError:
                raise HTTPException(status_code=400, detail="非法的调试延迟头")

    session = await asyncio.to_thread(manager.get_status, session_id)
    rows, has_more = await asyncio.to_thread(
        manager.fetch_page, session_id, page, page_size, sql_sleep
    )
    return ClaimPage(
        session_id=session_id,
        region=session.region,
        page=page,
        page_size=page_size,
        has_more=has_more,
        snapshot_time=session.snapshot_time,
        rows=[ClaimRow(**row) for row in rows],
    )


# ----------------------------------------------------------------------
# 源端补录接口（对应 db/init/03_sql_examples.sql 的 SQL 示例）
# 池连接为自动提交模式：单条写语句执行即提交，不触碰任何会话快照。
# ----------------------------------------------------------------------
@app.post(
    "/admin/policies",
    response_model=PolicyOut,
    status_code=201,
    tags=["admin"],
    summary="补录保单（源端写操作，照常提交）",
)
async def create_policy(body: PolicyIn):
    def _insert():
        conn = manager.pool().getconn()
        try:
            return conn.execute(
                "INSERT INTO policies (region) VALUES (%s) RETURNING id, region",
                (body.region,),
            ).fetchone()
        finally:
            manager.pool().putconn(conn)

    row = await asyncio.to_thread(_insert)
    return PolicyOut(**row)


@app.post(
    "/admin/claims",
    response_model=ClaimOut,
    status_code=201,
    tags=["admin"],
    summary="补录理赔（源端写操作，照常提交）",
)
async def create_claim(body: ClaimIn):
    def _insert():
        conn = manager.pool().getconn()
        try:
            return conn.execute(
                "INSERT INTO claims (policy_id, amount) VALUES (%s, %s) "
                "RETURNING id, policy_id, amount",
                (body.policy_id, body.amount),
            ).fetchone()
        finally:
            manager.pool().putconn(conn)

    row = await asyncio.to_thread(_insert)
    return ClaimOut(**row)


@app.patch(
    "/admin/claims/{claim_id}",
    response_model=ClaimOut,
    tags=["admin"],
    summary="修改理赔金额（源端写操作，照常提交）",
)
async def update_claim(claim_id: int, amount: int = Query(..., ge=0)):
    def _update():
        conn = manager.pool().getconn()
        try:
            return conn.execute(
                "UPDATE claims SET amount = %s WHERE id = %s "
                "RETURNING id, policy_id, amount",
                (amount, claim_id),
            ).fetchone()
        finally:
            manager.pool().putconn(conn)

    row = await asyncio.to_thread(_update)
    if row is None:
        raise HTTPException(status_code=404, detail=f"理赔 {claim_id} 不存在")
    return ClaimOut(**row)


@app.delete(
    "/admin/claims/{claim_id}",
    status_code=204,
    tags=["admin"],
    summary="删除理赔（源端写操作，照常提交）",
)
async def delete_claim(claim_id: int):
    def _delete():
        conn = manager.pool().getconn()
        try:
            return conn.execute(
                "DELETE FROM claims WHERE id = %s RETURNING id", (claim_id,)
            ).fetchone()
        finally:
            manager.pool().putconn(conn)

    row = await asyncio.to_thread(_delete)
    if row is None:
        raise HTTPException(status_code=404, detail=f"理赔 {claim_id} 不存在")
    # 204 必须无响应体
    return Response(status_code=204)
