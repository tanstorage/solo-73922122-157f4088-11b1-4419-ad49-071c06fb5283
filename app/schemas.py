"""HTTP 请求/响应模型。"""
from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, PositiveInt


class SessionStatus(str, Enum):
    OPEN = "open"          # 可正常取页
    CLOSING = "closing"    # 关闭已受理，正在等待在途取页结束
    CLOSED = "closed"      # 事务已回滚、连接已归还、名额已释放
    EXPIRED = "expired"    # 到期后由清理线程转入的终态（语义同 closed）


class OpenSessionRequest(BaseModel):
    region: str = Field(..., min_length=1, max_length=64, description="保单地区")


class SessionInfo(BaseModel):
    session_id: str
    region: str
    status: SessionStatus
    opened_at: datetime
    expires_at: datetime
    last_page_at: datetime | None = None
    pages_served: int
    ttl_seconds: int


class ClaimRow(BaseModel):
    claim_id: int
    policy_id: int
    region: str
    amount: int


class ClaimPage(BaseModel):
    session_id: str
    region: str
    page: PositiveInt
    page_size: int
    has_more: bool
    snapshot_time: datetime
    rows: list[ClaimRow]


class PolicyIn(BaseModel):
    region: str = Field(..., min_length=1, max_length=64)


class PolicyOut(BaseModel):
    id: int
    region: str


class ClaimIn(BaseModel):
    policy_id: int
    amount: int = Field(..., ge=0, description="整数理赔金额，单位分")


class ClaimOut(BaseModel):
    id: int
    policy_id: int
    amount: int


class Message(BaseModel):
    detail: str
