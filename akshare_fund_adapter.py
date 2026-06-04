"""AkShare release fund-data adapter for LOF Fund Monitor.

This module embeds the fund-information fetching approach used by
``akshare-release`` v1.18.64, but keeps the monitor lightweight by using
``aiohttp`` and plain dictionaries instead of importing pandas/requests.

Embedded AkShare methods:
- ``fund_etf_spot_em``: EastMoney ETF spot list.  AkShare maps f441 to
  ``IOPV实时估值`` and f402 to ``基金折价率``.  f402 is a discount-rate field,
  so the monitor converts it to the signed alert value with
  ``折溢价率 = -基金折价率``: discount is negative, premium is positive.
- ``fund_lof_spot_em``: EastMoney LOF spot list.  AkShare v1.18.64 exposes
  quote/turnover fields here but not f402/f441, so LOF premium/discount should
  be locally calculated from price and the best available estimated NAV.
- ``fund_value_estimation_em``: EastMoney fund valuation list.
- ``fund_purchase_em``: EastMoney/Tiantian batch purchase/redemption status and
  latest official NAV.

When AkShare cannot provide direct f402 or all data needed for local
premium/discount calculation, callers keep using existing project-specific
fallback methods.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable

import aiohttp

logger = logging.getLogger(__name__)

CST = timezone(timedelta(hours=8))
AKSHARE_CACHE_TTL_SECONDS = max(15, int(os.environ.get("AKSHARE_FUND_CACHE_TTL", "60") or 60))
AKSHARE_HTTP_TIMEOUT = max(3, int(os.environ.get("AKSHARE_FUND_HTTP_TIMEOUT", "8") or 8))
AKSHARE_HTTP_RETRIES = max(1, int(os.environ.get("AKSHARE_FUND_HTTP_RETRIES", "3") or 3))
AKSHARE_RETRY_SLEEP_SECONDS = max(0.1, float(os.environ.get("AKSHARE_FUND_RETRY_SLEEP", "0.6") or 0.6))
AKSHARE_PAGE_SIZE = max(100, int(os.environ.get("AKSHARE_FUND_PAGE_SIZE", "5000") or 5000))
AKSHARE_ESTIMATION_TIMEOUT_SECONDS = max(2.0, float(os.environ.get("AKSHARE_FUND_ESTIMATION_TIMEOUT", "8") or 8))
AKSHARE_ESTIMATION_PAGE_SIZE = max(1000, int(os.environ.get("AKSHARE_FUND_ESTIMATION_PAGE_SIZE", "20000") or 20000))

HEADERS_QUOTE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://quote.eastmoney.com/",
    "Connection": "close",
}

HEADERS_FUND = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://fund.eastmoney.com/",
    "Connection": "close",
}

_ETF_SPOT_URLS = (
    "https://push2.eastmoney.com/api/qt/clist/get",
    "https://push2delay.eastmoney.com/api/qt/clist/get",
    "https://88.push2.eastmoney.com/api/qt/clist/get",
    "https://2.push2.eastmoney.com/api/qt/clist/get",
)

_LOF_SPOT_URLS = (
    "https://push2.eastmoney.com/api/qt/clist/get",
    "https://push2delay.eastmoney.com/api/qt/clist/get",
    "https://88.push2.eastmoney.com/api/qt/clist/get",
    "https://2.push2.eastmoney.com/api/qt/clist/get",
)

_FUND_VALUE_ESTIMATION_URL = "https://api.fund.eastmoney.com/FundGuZhi/GetFundGZList"
_FUND_PURCHASE_STATUS_URL = "https://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx"

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
    # Same market scope and field set as akshare.fund_lof_spot_em in v1.18.64.
    # That official AkShare method does not expose f402/f441 for LOF rows.
    "fs": "b:MK0404,b:MK0405,b:MK0406,b:MK0407",
    "fields": (
        "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,"
        "f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,"
        "f62,f128,f136,f115,f152"
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


def _fmt_exc(exc: BaseException) -> str:
    detail = str(exc).strip()
    return f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__


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


def _normalize_purchase_status(value: Any) -> str:
    """Normalize EastMoney/AkShare purchase status for display and alerts."""
    text = str(value or "").strip()
    if not text or text in {"-", "--", "---", "None", "nan", "NaN"}:
        return "未知"
    compact = re.sub(r"\s+", "", text)
    if any(keyword in compact for keyword in ("限大额", "限制大额", "大额限制", "暂停大额")):
        return "限大额"
    if any(keyword in compact for keyword in ("暂停申购", "停止申购", "不可申购", "封闭期", "认购期", "发行中")):
        return "暂停"
    if any(keyword in compact for keyword in ("开放申购", "申购开放", "可申购")):
        return "开放"
    if compact == "开放":
        return "开放"
    if any(keyword in compact for keyword in ("暂停", "停止", "不可", "封闭")) and "赎回" not in compact:
        return "暂停"
    return "未知"


def _normalize_redeem_status(value: Any) -> str:
    """Normalize EastMoney/AkShare redemption status for display and alerts."""
    text = str(value or "").strip()
    if not text or text in {"-", "--", "---", "None", "nan", "NaN"}:
        return "未知"
    compact = re.sub(r"\s+", "", text)
    if any(keyword in compact for keyword in ("暂停赎回", "停止赎回", "不可赎回", "封闭期", "认购期", "发行中")):
        return "暂停"
    if any(keyword in compact for keyword in ("开放赎回", "赎回开放", "可赎回")):
        return "开放"
    if compact == "开放":
        return "开放"
    if any(keyword in compact for keyword in ("暂停", "停止", "不可", "封闭")) and "申购" not in compact:
        return "暂停"
    return "未知"


def _status_is_known(value: Any) -> bool:
    return str(value or "").strip() not in {"", "未知", "-", "--", "---"}


def _extract_js_array_after_key(text: str, key: str) -> str:
    """Extract a JSON-like array assigned to a JS object key.

    EastMoney's Fund_JJJZ_Data.aspx response follows AkShare's
    ``fund_purchase_em`` source but is returned as JavaScript such as
    ``var reData={datas:[[...]],allRecords:...}`` rather than strict JSON.
    This small scanner avoids adding demjson/pandas just for one array.
    """
    marker = f"{key}:"
    start = text.find(marker)
    if start < 0:
        marker = f'"{key}":'
        start = text.find(marker)
    if start < 0:
        return ""
    pos = text.find("[", start + len(marker))
    if pos < 0:
        return ""

    depth = 0
    quote = ""
    escape = False
    for idx in range(pos, len(text)):
        char = text[idx]
        if quote:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == quote:
                quote = ""
            continue
        if char in {'"', "'"}:
            quote = char
            continue
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return text[pos:idx + 1]
    return ""


def _decode_fund_purchase_rows(text: str) -> list[Any]:
    """Decode the ``datas`` array from AkShare fund_purchase_em's endpoint."""
    raw = (text or "").strip()
    if raw.startswith("var reData="):
        raw = raw[len("var reData="):].strip()
    if raw.endswith(";"):
        raw = raw[:-1].strip()

    # Some deployments return proper JSON, while the public page usually returns
    # a JavaScript object with unquoted keys.  Try strict JSON first, then extract
    # the datas array from the JS object.
    try:
        parsed = json.loads(raw)
        rows = parsed.get("datas", []) if isinstance(parsed, dict) else []
        return rows if isinstance(rows, list) else []
    except Exception:
        pass

    array_text = _extract_js_array_after_key(raw, "datas")
    if not array_text:
        return []
    try:
        return json.loads(array_text)
    except json.JSONDecodeError:
        # The endpoint is normally double-quoted JSON inside the array.  This
        # fallback handles rare single-quoted strings without executing JS.
        try:
            import ast
            return ast.literal_eval(array_text)
        except Exception:
            return []


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
    last_error = ""
    for attempt in range(1, AKSHARE_HTTP_RETRIES + 1):
        try:
            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                text = await resp.text()
                if resp.status != 200:
                    last_error = f"HTTP {resp.status}: {text[:120]}"
                    raise aiohttp.ClientResponseError(
                        resp.request_info,
                        resp.history,
                        status=resp.status,
                        message=text[:120],
                        headers=resp.headers,
                    )
                if not text:
                    last_error = "empty response"
                    raise ValueError(last_error)
                try:
                    return json.loads(text)
                except json.JSONDecodeError as exc:
                    last_error = f"JSONDecodeError: {text[:160]}"
                    raise exc
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, ValueError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < AKSHARE_HTTP_RETRIES:
                await asyncio.sleep(AKSHARE_RETRY_SLEEP_SECONDS * attempt)
                continue
            logger.debug(
                "AkShare adapter request failed after %s attempts: url=%s error=%s",
                AKSHARE_HTTP_RETRIES,
                url,
                last_error,
            )
    raise RuntimeError(last_error or "empty response")


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
                logger.info("AkShare adapter %s fetched %s rows via %s", source_name, len(rows), url)
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
            logger.info("AkShare adapter %s fetched %s/%s rows via %s", source_name, len(rows), total, url)
            return rows
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            logger.debug("AkShare adapter %s failed via %s: %s", source_name, url, last_error)
    if last_error:
        logger.warning("AkShare adapter %s unavailable: %s", source_name, last_error)
    return []


def _akshare_discount_to_signed_premium(discount_rate: float | None) -> float | None:
    """Convert AkShare f402 ``基金折价率`` to monitor's signed rate.

    EastMoney/AkShare names f402 as a discount-rate field: a positive number
    means discount and a negative number means premium.  The monitor, UI and
    WeChat filters use the opposite signed convention: discount < 0, premium > 0.
    """
    if discount_rate is None:
        return None
    return -discount_rate


def _normalize_spot_rows(
    rows: list[dict[str, Any]],
    source_name: str,
    *,
    include_iopv_discount_fields: bool,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = _normalize_code(row.get("f12"))
        if not code:
            continue
        iopv = _to_float(row.get("f441"), 0.0) if include_iopv_discount_fields else 0.0
        discount_rate = _to_float_or_none(row.get("f402")) if include_iopv_discount_fields else None
        signed_premium_rate = _akshare_discount_to_signed_premium(discount_rate)
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
            # Raw AkShare f402: 基金折价率.  Keep it for diagnostics, but expose
            # premium_rate using monitor semantics: discount negative, premium positive.
            "fund_discount_rate": discount_rate,
            "premium_rate": signed_premium_rate,
            "data_date": _format_data_date(row.get("f297")) if include_iopv_discount_fields else "",
            "quote_time": _format_timestamp_seconds(row.get("f124")) if include_iopv_discount_fields else "",
            "source": source_name,
        }
        result[code] = spot
    return result


async def fetch_akshare_etf_spot(session: aiohttp.ClientSession) -> dict[str, dict[str, Any]]:
    rows = await _fetch_clist_rows(session, _ETF_SPOT_URLS, ETF_SPOT_PARAMS, "fund_etf_spot_em")
    return _normalize_spot_rows(rows, "akshare.fund_etf_spot_em", include_iopv_discount_fields=True)


async def fetch_akshare_lof_spot(session: aiohttp.ClientSession) -> dict[str, dict[str, Any]]:
    rows = await _fetch_clist_rows(session, _LOF_SPOT_URLS, LOF_SPOT_PARAMS, "fund_lof_spot_em")
    return _normalize_spot_rows(rows, "akshare.fund_lof_spot_em", include_iopv_discount_fields=False)


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
    """Fetch one AkShare fund_value_estimation_em category as an optional source.

    This endpoint is useful but often the slowest EastMoney fund endpoint.  A
    timeout should not be treated as a project failure: spot quotes, official NAV
    from fund_purchase_em, and existing fallback estimators can still refresh and
    WeChat pushes must not wait indefinitely for this optional category.
    """
    type_id = FUND_VALUE_SYMBOL_MAP.get(symbol, FUND_VALUE_SYMBOL_MAP["LOF"])
    params = {
        "type": str(type_id),
        "sort": "3",
        "orderType": "desc",
        "canbuy": "0",
        "pageIndex": "1",
        "pageSize": str(AKSHARE_ESTIMATION_PAGE_SIZE),
        "_": int(time.time() * 1000),
    }
    started = time.time()
    try:
        data_json = await asyncio.wait_for(
            _request_json(
                session,
                _FUND_VALUE_ESTIMATION_URL,
                params,
                HEADERS_FUND,
                timeout=AKSHARE_ESTIMATION_TIMEOUT_SECONDS,
            ),
            timeout=AKSHARE_ESTIMATION_TIMEOUT_SECONDS + 0.5,
        )
        data = data_json.get("Data") or {}
        items = data.get("list") or []
        result: dict[str, dict[str, Any]] = {}
        for item in items:
            normalized = _normalize_estimation_item(item, data, symbol)
            if normalized:
                result[normalized["fund_code"]] = normalized
        logger.info(
            "AkShare adapter fund_value_estimation_em(%s) fetched %s rows in %.2fs",
            symbol,
            len(result),
            time.time() - started,
        )
        return result
    except asyncio.TimeoutError:
        logger.info(
            "AkShare adapter fund_value_estimation_em(%s) timed out after %.1fs; optional valuation category skipped",
            symbol,
            AKSHARE_ESTIMATION_TIMEOUT_SECONDS,
        )
        return {}
    except Exception as exc:
        logger.warning("AkShare adapter fund_value_estimation_em(%s) unavailable: %s", symbol, _fmt_exc(exc))
        return {}

async def fetch_akshare_fund_purchase_status(
    session: aiohttp.ClientSession,
) -> dict[str, dict[str, Any]]:
    """Fetch batch purchase/redemption statuses using AkShare fund_purchase_em.

    Source method in AkShare v1.18.64:
    ``akshare.fund.fund_em.fund_purchase_em`` ->
    ``https://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=8``.
    """
    params = {
        "t": "8",
        "page": "1,50000",
        "js": "reData",
        "sort": "fcode,asc",
        "_": int(time.time() * 1000),
    }
    try:
        last_error = ""
        for attempt in range(1, AKSHARE_HTTP_RETRIES + 1):
            try:
                async with session.get(
                    _FUND_PURCHASE_STATUS_URL,
                    params=params,
                    headers=HEADERS_FUND,
                    timeout=aiohttp.ClientTimeout(total=AKSHARE_HTTP_TIMEOUT),
                ) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        last_error = f"HTTP {resp.status}: {text[:120]}"
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message=text[:120],
                            headers=resp.headers,
                        )
                    rows = _decode_fund_purchase_rows(text)
                    if not rows:
                        last_error = "empty datas"
                        raise ValueError(last_error)
                    result: dict[str, dict[str, Any]] = {}
                    for row in rows:
                        if isinstance(row, dict):
                            code = _normalize_code(row.get("基金代码") or row.get("FCODE") or row.get("fcode") or row.get("code"))
                            fund_name = str(row.get("基金简称") or row.get("SHORTNAME") or row.get("name") or "").strip()
                            raw_nav = row.get("最新净值/万份收益") or row.get("DWJZ") or row.get("nav")
                            raw_nav_date = row.get("最新净值/万份收益-报告时间") or row.get("JZRQ") or row.get("nav_date")
                            raw_purchase = row.get("申购状态") or row.get("purchase_status") or row.get("sgstat")
                            raw_redeem = row.get("赎回状态") or row.get("redeem_status") or row.get("shstat")
                        elif isinstance(row, (list, tuple)) and len(row) >= 7:
                            # AkShare adds the sequence number after loading the
                            # DataFrame; the raw endpoint starts with fund code.
                            code = _normalize_code(row[0])
                            fund_name = str(row[1] or "").strip()
                            raw_nav = row[3] if len(row) > 3 else None
                            raw_nav_date = row[4] if len(row) > 4 else ""
                            raw_purchase = row[5]
                            raw_redeem = row[6]
                        else:
                            continue
                        if not code:
                            continue
                        nav = _to_float(raw_nav, 0.0)
                        purchase_status = _normalize_purchase_status(raw_purchase)
                        redeem_status = _normalize_redeem_status(raw_redeem)
                        if not (_status_is_known(purchase_status) or _status_is_known(redeem_status) or nav > 0):
                            continue
                        result[code] = {
                            "fund_code": code,
                            "fund_name": fund_name,
                            "nav": nav,
                            "nav_date": str(raw_nav_date or "").strip(),
                            "purchase_status": purchase_status,
                            "redeem_status": redeem_status,
                            "raw_purchase_status": str(raw_purchase or "").strip(),
                            "raw_redeem_status": str(raw_redeem or "").strip(),
                            "source": "akshare.fund_purchase_em",
                        }
                    logger.info("AkShare adapter fund_purchase_em fetched %s status rows", len(result))
                    return result
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                last_error = _fmt_exc(exc)
                if attempt < AKSHARE_HTTP_RETRIES:
                    await asyncio.sleep(AKSHARE_RETRY_SLEEP_SECONDS * attempt)
                    continue
                raise RuntimeError(last_error or "empty response")
    except Exception as exc:
        logger.warning("AkShare adapter fund_purchase_em unavailable: %s", _fmt_exc(exc))
        return {}


async def fetch_akshare_estimation_snapshot(
    session: aiohttp.ClientSession,
    symbols: Iterable[str] = ("LOF", "场内交易基金", "QDII"),
) -> dict[str, dict[str, Any]]:
    tasks = [fetch_akshare_fund_value_estimation(session, symbol) for symbol in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    merged: dict[str, dict[str, Any]] = {}
    counts: dict[str, int] = {}
    for symbol, result in zip(symbols, results):
        if isinstance(result, Exception):
            logger.debug("AkShare estimation task failed for %s: %s", symbol, _fmt_exc(result))
            counts[str(symbol)] = 0
            continue
        counts[str(symbol)] = len(result)
        # Earlier symbols keep priority.  LOF-specific rows should beat broader
        # 场内交易基金/QDII rows if the same code appears in multiple categories.
        for code, item in result.items():
            merged.setdefault(code, item)
    logger.info(
        "AkShare fund_value_estimation_em snapshot: %s; merged=%s",
        ", ".join(f"{name}={count}" for name, count in counts.items()),
        len(merged),
    )
    return merged

async def fetch_akshare_fund_snapshot(session: aiohttp.ClientSession) -> dict[str, Any]:
    """Fetch all AkShare-derived fund snapshots needed by the monitor."""
    started = time.time()
    etf_task = fetch_akshare_etf_spot(session)
    lof_task = fetch_akshare_lof_spot(session)
    estimation_task = fetch_akshare_estimation_snapshot(session)
    purchase_status_task = fetch_akshare_fund_purchase_status(session)
    etf_spot, lof_spot, estimation, purchase_status = await asyncio.gather(
        etf_task, lof_task, estimation_task, purchase_status_task
    )

    # LOF rows provide broad LOF quote coverage; ETF rows override when f441/f402
    # official IOPV/discount fields are present.
    spot = {**lof_spot, **etf_spot}
    snapshot = {
        "spot": spot,
        "etf_spot": etf_spot,
        "lof_spot": lof_spot,
        "estimation": estimation,
        "purchase_status": purchase_status,
        "fetched_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S"),
        "elapsed_seconds": round(time.time() - started, 3),
    }
    logger.info(
        "AkShare fund snapshot fetched: spot=%s (etf=%s lof=%s), estimation=%s, purchase_status=%s, %.2fs",
        len(spot), len(etf_spot), len(lof_spot), len(estimation), len(purchase_status), snapshot["elapsed_seconds"],
    )
    return snapshot


def _empty_snapshot() -> dict[str, Any]:
    return {"spot": {}, "etf_spot": {}, "lof_spot": {}, "estimation": {}, "purchase_status": {}, "fetched_at": "", "elapsed_seconds": 0}


def get_cached_akshare_fund_snapshot(max_age_seconds: int | None = None) -> dict[str, Any]:
    """Return the current cached snapshot when available and fresh enough."""
    cached = _snapshot_cache.get("snapshot")
    if not cached:
        return _empty_snapshot()
    if max_age_seconds is None:
        return cached
    age = time.time() - float(_snapshot_cache.get("ts") or 0)
    return cached if age <= max(0, int(max_age_seconds)) else _empty_snapshot()


async def get_akshare_fund_snapshot(
    session: aiohttp.ClientSession,
    max_age_seconds: int | None = None,
    force: bool = False,
    stale_if_busy: bool = False,
) -> dict[str, Any]:
    """Return a cached AkShare fund snapshot, refreshing it at most once per TTL.

    When ``stale_if_busy`` is true, callers such as the WeChat scheduler will not
    wait behind a full data-refresh snapshot fetch.  They use the current cached
    snapshot plus stored realtime data instead, so scheduled pushes stay on time.
    """
    ttl = AKSHARE_CACHE_TTL_SECONDS if max_age_seconds is None else max(0, int(max_age_seconds))
    now = time.time()
    cached = _snapshot_cache.get("snapshot")
    if cached and not force and ttl > 0 and now - float(_snapshot_cache.get("ts") or 0) <= ttl:
        return cached

    if stale_if_busy and _snapshot_lock.locked():
        if cached:
            logger.info(
                "AkShare fund snapshot refresh already running; using cached snapshot fetched_at=%s for non-blocking caller",
                cached.get("fetched_at", ""),
            )
            return cached
        logger.info("AkShare fund snapshot refresh already running; no cached snapshot yet for non-blocking caller")
        return _empty_snapshot()

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
            logger.warning("AkShare fund snapshot refresh failed, using stale data if available: %s", _fmt_exc(exc))
            return cached or _empty_snapshot()

def get_fund_akshare_data(fund_code: str, snapshot: dict[str, Any] | None) -> dict[str, Any]:
    code = _normalize_code(fund_code)
    snapshot = snapshot or {}
    spot = (snapshot.get("spot") or {}).get(code) or {}
    estimation = (snapshot.get("estimation") or {}).get(code) or {}
    purchase_status = (snapshot.get("purchase_status") or {}).get(code) or {}
    return {"spot": spot, "estimation": estimation, "purchase_status": purchase_status}


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
    purchase_status = data.get("purchase_status") or {}
    sources: list[str] = []

    if purchase_status:
        source = purchase_status.get("source", "akshare.fund_purchase_em")
        sources.append(source)
        if purchase_status.get("fund_name") and not result.get("fund_name"):
            result["fund_name"] = purchase_status["fund_name"]
        nav = _to_float(purchase_status.get("nav"), 0)
        if nav > 0 and _to_float(result.get("nav"), 0) <= 0:
            result["nav"] = nav
            result["nav_source"] = f"{source}:最新净值"
            if purchase_status.get("nav_date"):
                result["nav_date"] = purchase_status.get("nav_date", "")
        if _status_is_known(purchase_status.get("purchase_status")):
            result["purchase_status"] = purchase_status["purchase_status"]
        if _status_is_known(purchase_status.get("redeem_status")):
            result["redeem_status"] = purchase_status["redeem_status"]
        result["status_source"] = source

    if estimation:
        source = estimation.get("source", "akshare.fund_value_estimation_em")
        sources.append(source)
        if estimation.get("fund_name") and not result.get("fund_name"):
            result["fund_name"] = estimation["fund_name"]
        if _to_float(estimation.get("nav"), 0) > 0:
            result["nav"] = _to_float(estimation.get("nav"), 0)
            result["nav_source"] = f"{source}:公布单位净值"
        if estimation.get("nav_date"):
            result["nav_date"] = estimation.get("nav_date", "")
        if _to_float(estimation.get("estimated_nav"), 0) > 0:
            result["estimated_nav"] = _to_float(estimation.get("estimated_nav"), 0)
            result["source_estimated_nav"] = result["estimated_nav"]
            result["estimate_source"] = source
        if estimation.get("estimated_change_rate") is not None:
            result["estimated_change_rate"] = _to_float(estimation.get("estimated_change_rate"), 0)
            result["source_estimated_change_rate"] = result["estimated_change_rate"]
        if estimation.get("estimate_time"):
            result["source_estimate_time"] = estimation.get("estimate_time", "")

    if spot:
        source = spot.get("source", "akshare.fund_spot_em")
        sources.append(source)
        if spot.get("fund_name") and not result.get("fund_name"):
            result["fund_name"] = spot["fund_name"]
        if _to_float(spot.get("trade_price"), 0) > 0:
            result["trade_price"] = round(_to_float(spot.get("trade_price"), 0), 4)
            result["price_source"] = source
        if spot.get("trade_price_change") is not None:
            result["trade_price_change"] = round(_to_float(spot.get("trade_price_change"), 0), 4)
        if _to_float(spot.get("trade_amount"), 0) > 0:
            result["trade_amount"] = _to_float(spot.get("trade_amount"), 0)
            result["trade_amount_source"] = source
        if spot.get("data_date") and not result.get("nav_date"):
            result["nav_date"] = spot.get("data_date", "")
        iopv = _to_float(spot.get("iopv_estimated_nav"), 0)
        if iopv > 0:
            result["estimated_nav"] = round(iopv, 4)
            result["source_estimated_nav"] = round(iopv, 4)
            result["iopv_estimated_nav"] = round(iopv, 4)
            result["estimate_source"] = f"{source}:f441_IOPV实时估值"
            result["source_estimate_time"] = spot.get("quote_time") or spot.get("data_date") or result.get("source_estimate_time", "")
        raw_discount_rate = spot.get("fund_discount_rate")
        if raw_discount_rate is not None:
            result["fund_discount_rate"] = round(_to_float(raw_discount_rate, 0), 2)
        premium_rate = spot.get("premium_rate")
        if premium_rate is not None:
            result["premium_rate"] = round(_to_float(premium_rate, 0), 2)
            result["akshare_premium_rate"] = result["premium_rate"]
            result["premium_source"] = f"{source}:f402_基金折价率取反为折溢价率"

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
            est_nav = _to_float(item.get("estimated_nav"), 0)
            nav = _to_float(item.get("nav"), 0)
            base_nav = est_nav or nav
            trade_price = _to_float(item.get("trade_price"), 0)
            if base_nav > 0 and trade_price > 0:
                item["premium_rate"] = round((trade_price - base_nav) / base_nav * 100, 2)
                item["premium_source"] = "calculated_from_estimated_nav_and_trade_price" if est_nav else "calculated_from_nav_and_trade_price"
                item["premium_base_nav"] = round(base_nav, 4)
                item["premium_base_source"] = item.get("estimate_source") if est_nav else item.get("nav_source", "")
        enriched.append(item)
    return enriched
