"""运行期配置，全部来自环境变量，便于容器化与演示覆写。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} 必须为正整数，实际为 {raw!r}")
    return value


@dataclass(frozen=True)
class Settings:
    database_url: str
    max_sessions: int
    session_ttl_seconds: int
    page_size_cap: int
    sweeper_interval_seconds: float
    close_wait_timeout_seconds: float
    cancel_grace_seconds: float
    enable_debug_hooks: bool
    pool_min_size: int
    pool_max_size: int

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            database_url=os.environ.get(
                "DATABASE_URL",
                "postgresql://actuary:actuary@db:5432/claims",
            ),
            max_sessions=_int_env("MAX_SESSIONS", 2),
            session_ttl_seconds=_int_env("SESSION_TTL_SECONDS", 60),
            page_size_cap=_int_env("PAGE_SIZE_CAP", 100),
            sweeper_interval_seconds=float(
                os.environ.get("SWEEPER_INTERVAL_SECONDS", "0.25")
            ),
            close_wait_timeout_seconds=float(
                os.environ.get("CLOSE_WAIT_TIMEOUT_SECONDS", "10")
            ),
            cancel_grace_seconds=float(
                os.environ.get("CANCEL_GRACE_SECONDS", "1.0")
            ),
            enable_debug_hooks=os.environ.get(
                "ENABLE_DEBUG_HOOKS", "false"
            ).lower()
            in ("1", "true", "yes", "on"),
            pool_min_size=_int_env("POOL_MIN_SIZE", 1),
            pool_max_size=_int_env("POOL_MAX_SIZE", 10),
        )


settings = Settings.from_env()
