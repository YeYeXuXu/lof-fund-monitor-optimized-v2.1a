"""AkShare release fund-data adapter for LOF Fund Monitor.

This module embeds the fund-information fetching approach used by
``akshare-release`` v1.18.64, but keeps the monitor lightweight by using
``aiohttp`` and plain dictionaries instead of importing pandas/requests.

Embedded AkShare methods:
- ``fund_etf_spot_em``: EastMoney ETF spot list.  The project explicitly uses
  f441 as ``IOPV实时估值`` and f402 as ``基金折价率``; the same f402 value is
  also treated as the fund premium/discount rate used by alerts.
- ``fund_lof_spot_em``: EastMoney LOF spot list.
- ``fund_value_estimation_em``: EastMoney fund valuation list.

When any of these AkShare-derived endpoints is unavailable, callers keep using
existing project-specific fallback methods.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable

import aiohttp

logger = logging.getLogger(__name__)

CST = timezone(timedelta(hours=8))
AKSHARE_CACHE_TTL_SECONDS = max(15, int(os.environ.get("AKSHARE_FUND_CACHE_TTL", "60") or 60))
AKSHARE_HTTP_TIMEOUT = max(3, int(os.environ.get("AKSHARE_FUND_HTTP_TIMEOUT", "8") or 8))
AKSHARE_PAGE_SIZE = max(100, int(os.environ.get("AKSHARE_FUND_PAGE_SIZE", "5000") or 5000))

HEADERS_QUOTE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/",
}

HEADERS_FUND = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Referer": "https://fund.eastmoney.com/",
}

_ETF_SPOT_URLS = (
    "https://push2delay.eastmoney.com/api/qt/clist/get",
    "https://88.push2.eastmoney.com/api/qt/clist/get",
)

_LOF_SPOT_URLS = (
    "https://88.push2.eastmoney.com/api/qt/clist/get",
    "https://2.push2.eastmoney.com/api/qt/clist/get",
)

_FUND_VALUE_ESTIMATION_URL = "https://api.fund.eastmoney.com/FundGuZhi/GetFundGZList"

_COMMON_CLIST_PARAMS = {
    "pn": "1",
    "pz": str(AKSHARE_PAGE_SIZE),
    "po": "1",
    "np": "1",
    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
    "fltt": "2",
    "invt": "2",
    "wbp2u": "|0|0|0|web",
}

ETF_SPOT_PARAMS = {
    **_COMMON_CLIST_PARAMS,
    "fid": "f12",
    # Same market scope as akshare.fund_etf_spot_em.
    "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024,b:MK0827",
    "fields": (
        "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,"
        "f12,f13,f14,f15,f16,f17,f18,f20,f21,"
        "f23,f24,f25,f22,f11,f30,f31,f32,f33,"
        "f34,f35,f38,f62,f63,f64,f65,f66,f69,"
        "f72,f75,f78,f81,f84,f87,f115,f124,f128,"
        "f136,f152,f184,f297,f402,f441"
    ),
}

LOF_SPOT_PARAMS = {
    **_COMMON_CLIST_PARAMS,
    "fid": "f3",
    # Same market scope as akshare.fund_lof_spot_em.
    "fs": "b:MK0404,b:MK0405,b:MK0406,b:MK0407",
    # f402/f441/f297/f124 are requested opportunistically.  If EastMoney does
    # not return them for LOF rows, the existing fallback calculation remains in use.
    "fields": (
        "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,"
        "f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,"
        "f62,f128,f136,f115,f152,f297,f402,f441,f124"
    ),
}

FUND_VALUE_SYMBOL_MAP = {
    "全部": 1,
    "股票型": 2,
    "混合型": 3,
    "债券型": 4,
    "指数型": 5,
    "QDII": 6,
    "ETF联接": 7,
    "LOF": 8,
    "场内交易基金": 9,
}

_snapshot_cache: dict[str, Any] = {"snapshot": None, "ts": 0.0}
_snapshot_lock = asyncio.Lock()


def _is_blank(value: Any) -> bool:
    return value is None or str(value).strip() in {"", "-", "--", "---", "None", "nan", "NaN"}


def _to_float(value: Any, default: float = 0.0) -> float:
    if _is_blank(value):
        return default
    try:
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_float_or_none(value: Any) -> float | None:
    if _is_blank(value):
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    if "." in text:
        prefix, suffix = text.split(".", 1)
        # EastMoney secid values are commonly like "0.161725" or "1.513100".
        # Keep ordinary decimal-looking fund codes out of the result by taking the
        # right side when the left side is a market id.
        if prefix.isdigit() and suffix:
            text = suffix
    digits = "".join(ch for ch in text if ch.isdigit())
    if digits and len(digits) <= 6:
        return digits.zfill(6)
    return text


def _format_data_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text or text in {"0", "-", "--"}:
        return ""
    if text.endswith(".0"):
        text = text[:-2]
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _format_timestamp_seconds(value: Any) -> str:
    timestamp = _to_float(value, 0.0)
    if timestamp <= 0:
        return ""
    try:
        return datetime.fromtimestamp(timestamp, CST).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _rows_from_diff(diff: Any) -> list[dict[str, Any]]:
    if isinstance(diff, list):
        return [row for row in diff if isinstance(row, dict)]
    if isinstance(diff, dict):
        return [row for row in diff.values() if isinstance(row, dict)]
    return []


async def _request_json(
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, Any],
    headers: dict[str, str],
    timeout: int = AKSHARE_HTTP_TIMEOUT,
) -> dict[str, Any]:
    async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
        text = await resp.text()
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            logger.debug("AkShare adapter non-JSON response: %s %s", resp.status, text[:160])
            return {}


async def _fetch_clist_rows(
    session: aiohttp.ClientSession,
    urls: Iterable[str],
    base_params: dict[str, Any],
    source_name: str,
) -> list[dict[str, Any]]:
    """Fetch EastMoney clist rows using AkShare-compatible parameters.

    AkShare's ``fetch_paginated_data`` requests many pages and sleeps randomly
    between pages.  For GitHub Actions speed this adapter asks for a large page
    first and only falls back to concurrent pagination when EastMoney reports
    more rows than returned.
    """
    last_error = ""
    for url in urls:
        try:
            params = {**base_params, "pn": "1", "pz": str(AKSHARE_PAGE_SIZE)}
            data_json = await _request_json(session, url, params, HEADERS_QUOTE)
            data = data_json.get("data") or {}
            rows = _rows_from_diff(data.get("diff"))
            if not rows:
                last_error = f"empty diff from {url}"
                continue

            total = int(_to_float(data.get("total"), len(rows)) or len(rows))
            if total <= len(rows):
                return rows

            per_page = max(1, len(rows))
            total_pages = max(1, math.ceil(total / per_page))
            semaphore = asyncio.Semaphore(4)

            async def fetch_page(page_no: int) -> list[dict[str, Any]]:
                page_params = {**params, "pn": str(page_no)}
                async with semaphore:
                    try:
                        page_json = await _request_json(session, url, page_params, HEADERS_QUOTE)
                        page_data = page_json.get("data") or {}
                        return _rows_from_diff(page_data.get("diff"))
                    except Exception as exc:
                        logger.debug("AkShare adapter page fetch failed: %s page=%s error=%s", source_name, page_no, exc)
                        return []

            page_results = await asyncio.gather(*(fetch_page(page_no) for page_no in range(2, total_pages + 1)))
            for page_rows in page_results:
                rows.extend(page_rows)
            return rows
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            logger.debug("AkShare adapter %s failed via %s: %s", source_name, url, last_error)
    if last_error:
        logger.warning("AkShare adapter %s unavailable: %s", source_name, last_error)
    return []


def _normalize_spot_rows(rows: list[dict[str, Any]], source_name: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = _normalize_code(row.get("f12"))
        if not code:
            continue
        iopv = _to_float(row.get("f441"), 0.0)
        discount_rate = _to_float_or_none(row.get("f402"))
        trade_price = _to_float(row.get("f2"), 0.0)
        change_amount = _to_float(row.get("f4"), 0.0)
        change_rate = _to_float(row.get("f3"), 0.0)
        spot = {
            "fund_code": code,
            "fund_name": str(row.get("f14") or "").strip(),
            "market_id": row.get("f13"),
            "trade_price": trade_price,
            "trade_price_change": change_amount,
            "trade_price_change_rate": change_rate,
            "trade_amount": _to_float(row.get("f6"), 0.0),
            "trade_volume": _to_float(row.get("f5"), 0.0),
            "open": _to_float(row.get("f17"), 0.0),
            "high": _to_float(row.get("f15"), 0.0),
            "low": _to_float(row.get("f16"), 0.0),
            "previous_close": _to_float(row.get("f18"), 0.0),
            "iopv_estimated_nav": iopv,
            # f402 is explicitly mapped to 基金折价率 by AkShare.  The monitor uses
            # the same signed value as the alert premium/discount rate.
            "fund_discount_rate": discount_rate,
            "premium_rate": discount_rate,
            "data_date": _format_data_date(row.get("f297")),
            "quote_time": _format_timestamp_seconds(row.get("f124")),
            "source": source_name,
        }
        result[code] = spot
    return result


async def fetch_akshare_etf_spot(session: aiohttp.ClientSession) -> dict[str, dict[str, Any]]:
    rows = await _fetch_clist_rows(session, _ETF_SPOT_URLS, ETF_SPOT_PARAMS, "fund_etf_spot_em")
    return _normalize_spot_rows(rows, "akshare.fund_etf_spot_em")


async def fetch_akshare_lof_spot(session: aiohttp.ClientSession) -> dict[str, dict[str, Any]]:
    rows = await _fetch_clist_rows(session, _LOF_SPOT_URLS, LOF_SPOT_PARAMS, "fund_lof_spot_em")
    return _normalize_spot_rows(rows, "akshare.fund_lof_spot_em")


def _normalize_estimation_item(item: Any, data_meta: dict[str, Any], source_symbol: str) -> dict[str, Any] | None:
    """Normalize one item from AkShare fund_value_estimation_em."""
    if isinstance(item, dict):
        code = _normalize_code(item.get("FCODE") or item.get("fundCode") or item.get("fundcode") or item.get("code"))
        if not code:
            return None
        return {
            "fund_code": code,
            "fund_name": str(item.get("SHORTNAME") or item.get("name") or item.get("fund_name") or "").strip(),
            "estimated_nav": _to_float(item.get("GSZ") or item.get("estimated_nav"), 0.0),
            "estimated_change_rate": _to_float(item.get("GSZZL") or item.get("estimated_change_rate"), 0.0),
            "nav": _to_float(item.get("DWJZ") or item.get("nav"), 0.0),
            "daily_change_rate": _to_float(item.get("JZZZL") or item.get("daily_change_rate"), 0.0),
            "estimate_time": str(item.get("GZTIME") or item.get("estimate_time") or data_meta.get("gzrq") or "").strip(),
            "nav_date": str(item.get("JZRQ") or item.get("nav_date") or data_meta.get("gxrq") or "").strip(),
            "source": f"akshare.fund_value_estimation_em:{source_symbol}",
        }

    if not isinstance(item, (list, tuple)) or len(item) < 27:
        return None

    code = _normalize_code(item[0])
    if not code:
        return None
    nav = _to_float(item[24], 0.0) or _to_float(item[23], 0.0)
    return {
        "fund_code": code,
        "fund_name": str(item[26] or "").strip(),
        "estimated_nav": _to_float(item[20], 0.0),
        "estimated_change_rate": _to_float(item[21], 0.0),
        "nav": nav,
        "daily_change_rate": _to_float(item[22], 0.0),
        "estimate_time": str(item[11] or data_meta.get("gzrq") or "").strip(),
        "nav_date": str(data_meta.get("gxrq") or data_meta.get("gzrq") or "").strip(),
        "estimate_deviation": str(item[19] or "").strip(),
        "source": f"akshare.fund_value_estimation_em:{source_symbol}",
    }


async def fetch_akshare_fund_value_estimation(
    session: aiohttp.ClientSession,
    symbol: str = "LOF",
) -> dict[str, dict[str, Any]]:
    type_id = FUND_VALUE_SYMBOL_MAP.get(symbol, FUND_VALUE_SYMBOL_MAP["LOF"])
    params = {
        "type": str(type_id),
        "sort": "3",
        "orderType": "desc",
        "canbuy": "0",
        "pageIndex": "1",
        "pageSize": "20000",
        "_": int(time.time() * 1000),
    }
    try:
        data_json = await _request_json(session, _FUND_VALUE_ESTIMATION_URL, params, HEADERS_FUND)
        data = data_json.get("Data") or {}
        items = data.get("list") or []
        result: dict[str, dict[str, Any]] = {}
        for item in items:
            normalized = _normalize_estimation_item(item, data, symbol)
            if normalized:
                result[normalized["fund_code"]] = normalized
        return result
    except Exception as exc:
        logger.warning("AkShare adapter fund_value_estimation_em(%s) unavailable: %s", symbol, exc)
        return {}


async def fetch_akshare_estimation_snapshot(
    session: aiohttp.ClientSession,
    symbols: Iterable[str] = ("LOF", "场内交易基金", "QDII"),
) -> dict[str, dict[str, Any]]:
    tasks = [fetch_akshare_fund_value_estimation(session, symbol) for symbol in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    merged: dict[str, dict[str, Any]] = {}
    for result in results:
        if isinstance(result, Exception):
            logger.debug("AkShare estimation task failed: %s", result)
            continue
        # Later symbols only fill gaps; LOF-specific rows should keep priority.
        for code, item in result.items():
            merged.setdefault(code, item)
    return merged


async def fetch_akshare_fund_snapshot(session: aiohttp.ClientSession) -> dict[str, Any]:
    """Fetch all AkShare-derived fund snapshots needed by the monitor."""
    started = time.time()
    etf_task = fetch_akshare_etf_spot(session)
    lof_task = fetch_akshare_lof_spot(session)
    estimation_task = fetch_akshare_estimation_snapshot(session)
    etf_spot, lof_spot, estimation = await asyncio.gather(etf_task, lof_task, estimation_task)

    # LOF rows provide broad LOF quote coverage; ETF rows override when f441/f402
    # official IOPV/discount fields are present.
    spot = {**lof_spot, **etf_spot}
    snapshot = {
        "spot": spot,
        "etf_spot": etf_spot,
        "lof_spot": lof_spot,
        "estimation": estimation,
        "fetched_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S"),
        "elapsed_seconds": round(time.time() - started, 3),
    }
    logger.info(
        "AkShare fund snapshot fetched: spot=%s (etf=%s lof=%s), estimation=%s, %.2fs",
        len(spot), len(etf_spot), len(lof_spot), len(estimation), snapshot["elapsed_seconds"],
    )
    return snapshot


async def get_akshare_fund_snapshot(
    session: aiohttp.ClientSession,
    max_age_seconds: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Return a cached AkShare fund snapshot, refreshing it at most once per TTL."""
    ttl = AKSHARE_CACHE_TTL_SECONDS if max_age_seconds is None else max(0, int(max_age_seconds))
    now = time.time()
    cached = _snapshot_cache.get("snapshot")
    if cached and not force and ttl > 0 and now - float(_snapshot_cache.get("ts") or 0) <= ttl:
        return cached

    async with _snapshot_lock:
        now = time.time()
        cached = _snapshot_cache.get("snapshot")
        if cached and not force and ttl > 0 and now - float(_snapshot_cache.get("ts") or 0) <= ttl:
            return cached
        try:
            snapshot = await fetch_akshare_fund_snapshot(session)
            _snapshot_cache["snapshot"] = snapshot
            _snapshot_cache["ts"] = time.time()
            return snapshot
        except Exception as exc:
            logger.warning("AkShare fund snapshot refresh failed, using stale data if available: %s", exc)
            return cached or {"spot": {}, "etf_spot": {}, "lof_spot": {}, "estimation": {}, "fetched_at": ""}


def get_fund_akshare_data(fund_code: str, snapshot: dict[str, Any] | None) -> dict[str, Any]:
    code = _normalize_code(fund_code)
    snapshot = snapshot or {}
    spot = (snapshot.get("spot") or {}).get(code) or {}
    estimation = (snapshot.get("estimation") or {}).get(code) or {}
    return {"spot": spot, "estimation": estimation}


def apply_akshare_fund_data_to_result(
    result: dict[str, Any],
    fund_code: str,
    snapshot: dict[str, Any] | None,
) -> bool:
    """Merge AkShare-derived data into a realtime result dict.

    Returns True when at least one AkShare source supplied data.  Callers should
    still use existing project methods for fields that remain empty.
    """
    data = get_fund_akshare_data(fund_code, snapshot)
    spot = data.get("spot") or {}
    estimation = data.get("estimation") or {}
    sources: list[str] = []

    if estimation:
        sources.append(estimation.get("source", "akshare.fund_value_estimation_em"))
        if estimation.get("fund_name") and not result.get("fund_name"):
            result["fund_name"] = estimation["fund_name"]
        if _to_float(estimation.get("nav"), 0) > 0:
            result["nav"] = _to_float(estimation.get("nav"), 0)
        if estimation.get("nav_date"):
            result["nav_date"] = estimation.get("nav_date", "")
        if _to_float(estimation.get("estimated_nav"), 0) > 0:
            result["estimated_nav"] = _to_float(estimation.get("estimated_nav"), 0)
            result["source_estimated_nav"] = result["estimated_nav"]
        if estimation.get("estimated_change_rate") is not None:
            result["estimated_change_rate"] = _to_float(estimation.get("estimated_change_rate"), 0)
            result["source_estimated_change_rate"] = result["estimated_change_rate"]
        if estimation.get("estimate_time"):
            result["source_estimate_time"] = estimation.get("estimate_time", "")

    if spot:
        sources.append(spot.get("source", "akshare.fund_spot_em"))
        if spot.get("fund_name") and not result.get("fund_name"):
            result["fund_name"] = spot["fund_name"]
        if _to_float(spot.get("trade_price"), 0) > 0:
            result["trade_price"] = round(_to_float(spot.get("trade_price"), 0), 4)
        if spot.get("trade_price_change") is not None:
            result["trade_price_change"] = round(_to_float(spot.get("trade_price_change"), 0), 4)
        if _to_float(spot.get("trade_amount"), 0) > 0:
            result["trade_amount"] = _to_float(spot.get("trade_amount"), 0)
        if spot.get("data_date") and not result.get("nav_date"):
            result["nav_date"] = spot.get("data_date", "")
        iopv = _to_float(spot.get("iopv_estimated_nav"), 0)
        if iopv > 0:
            result["estimated_nav"] = round(iopv, 4)
            result["source_estimated_nav"] = round(iopv, 4)
            result["iopv_estimated_nav"] = round(iopv, 4)
            result["source_estimate_time"] = spot.get("quote_time") or spot.get("data_date") or result.get("source_estimate_time", "")
        premium_rate = spot.get("premium_rate")
        if premium_rate is not None:
            result["premium_rate"] = round(_to_float(premium_rate, 0), 2)
            result["akshare_premium_rate"] = result["premium_rate"]
            result["fund_discount_rate"] = result["premium_rate"]

    if sources:
        result["akshare_source"] = ", ".join(dict.fromkeys(sources))
        return True
    return False


def overlay_akshare_realtime_for_funds(
    funds: list[dict[str, Any]],
    snapshot: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Overlay AkShare quote/filter fields for WeChat alert screening.

    This is intentionally in-memory and fast: scheduled push filtering does not
    wait for a full holdings/NAV refresh cycle.
    """
    enriched: list[dict[str, Any]] = []
    for fund in funds:
        item = dict(fund)
        applied = apply_akshare_fund_data_to_result(item, item.get("fund_code", ""), snapshot)
        if applied and item.get("akshare_premium_rate") is None:
            base_nav = _to_float(item.get("estimated_nav"), 0) or _to_float(item.get("nav"), 0)
            trade_price = _to_float(item.get("trade_price"), 0)
            if base_nav > 0 and trade_price > 0:
                item["premium_rate"] = round((trade_price - base_nav) / base_nav * 100, 2)
        enriched.append(item)
    return enriched
