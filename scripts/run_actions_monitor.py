#!/usr/bin/env python3
"""Run the aiohttp monitor inside GitHub Actions until the configured end time.

The normal server.py entrypoint tries to open a browser, which is useful locally
but unnecessary in GitHub Actions. This runner imports the app, starts it on
127.0.0.1, lets the project's built-in periodic tasks run, and shuts down at
15:00 Asia/Shanghai by default.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
import signal
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from aiohttp import web

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


CST = ZoneInfo("Asia/Shanghai")


def _parse_hhmm(value: str) -> tuple[int, int]:
    value = (value or "15:00").strip()
    try:
        hour_text, minute_text = value.split(":", 1)
        hour, minute = int(hour_text), int(minute_text)
    except Exception as exc:
        raise ValueError(f"结束时间必须是 HH:MM，例如 15:00；当前值: {value!r}") from exc
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"结束时间越界: {value!r}")
    return hour, minute


def _configured_push_times() -> set[str]:
    raw = os.environ.get("WECHAT_PUSH_TIME", "")
    result: set[str] = set()
    for part in re.split(r"[,，;；\s]+", raw):
        if not part:
            continue
        try:
            hour, minute = _parse_hhmm(part)
        except ValueError:
            continue
        result.add(f"{hour:02d}:{minute:02d}")
    return result


def _push_shutdown_grace_seconds(end_time: str) -> int:
    """Keep Actions alive briefly when a push is scheduled at RUN_UNTIL.

    Without this guard, a workflow ending at 15:00 can cancel the server at the
    exact minute that a 15:00 WeChat alert should run.
    """
    try:
        hour, minute = _parse_hhmm(end_time)
    except ValueError:
        return 0
    if f"{hour:02d}:{minute:02d}" not in _configured_push_times():
        return 0
    return max(0, int(os.environ.get("ACTIONS_PUSH_GRACE_SECONDS", "90") or 90))


def _deadline() -> datetime:
    after_minutes = os.environ.get("ACTIONS_END_AFTER_MINUTES", "").strip()
    now = datetime.now(CST)
    if after_minutes:
        minutes = max(1, int(after_minutes))
        return now + timedelta(minutes=minutes)

    end_time = os.environ.get("ACTIONS_END_TIME") or os.environ.get("RUN_UNTIL") or "15:00"
    hour, minute = _parse_hhmm(end_time)
    deadline = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    grace_seconds = _push_shutdown_grace_seconds(end_time)
    if grace_seconds:
        deadline += timedelta(seconds=grace_seconds)
        print(f"[INFO] RUN_UNTIL 与微信推送时间重合，追加 {grace_seconds} 秒保护窗口。", flush=True)
    return deadline


async def _wait_until(deadline: datetime, stop_event: asyncio.Event) -> str:
    while True:
        now = datetime.now(CST)
        remaining = (deadline - now).total_seconds()
        if remaining <= 0:
            return "deadline"
        print(
            f"[INFO] 当前北京时间 {now:%Y-%m-%d %H:%M:%S}，"
            f"计划结束 {deadline:%Y-%m-%d %H:%M:%S}，"
            f"剩余约 {remaining / 60:.1f} 分钟。",
            flush=True,
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=min(300, remaining))
            return "signal"
        except asyncio.TimeoutError:
            continue


async def main() -> None:
    deadline = _deadline()
    now = datetime.now(CST)
    if now >= deadline:
        print(f"[OK] 当前北京时间 {now:%H:%M:%S} 已到/超过结束时间 {deadline:%H:%M}，无需启动。")
        return

    port = int(os.environ.get("FUND_PORT", "8080"))
    from server import create_app  # noqa: WPS433 - delay import so --help works without DB deps
    app = create_app()
    runner = web.AppRunner(app)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    await runner.setup()
    site = web.TCPSite(runner, host="127.0.0.1", port=port)
    await site.start()
    print(f"[OK] LOF 监控服务已在 GitHub Actions 启动: http://127.0.0.1:{port}")
    print("[INFO] GitHub 托管 runner 不开放公网入站端口；此地址只在 runner 内部可访问。")

    try:
        reason = await _wait_until(deadline, stop_event)
        print(f"[OK] 收到结束条件: {reason}，开始清理并退出。")
    finally:
        await runner.cleanup()
        print("[OK] 服务已停止。")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LOF monitor inside GitHub Actions until a Beijing HH:MM time.")
    parser.add_argument("--until", default="", help="北京时间结束时间，例如 15:00")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.until:
        os.environ["ACTIONS_END_TIME"] = args.until
    asyncio.run(main())
