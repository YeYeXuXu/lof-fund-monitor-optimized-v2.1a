"""NAV estimation algorithms for LOF Fund Monitor.

The optimized model keeps the public function names used by the existing
project, but implements a unified portfolio-return engine underneath.

Core idea:
    NAV_t = latest_published_NAV * (1 + estimated_portfolio_return)

For holdings-based funds, the known holdings contribute by their disclosed
weight. The unknown residual is estimated with a configured/category proxy when
available, rather than blindly expanding the top-10 average return to 100% of
assets. This is closer to fund NAV estimation logic because cash, undisclosed
positions, stale quarterly holdings, foreign-exchange moves and proxy quality
all matter.
"""
from __future__ import annotations

import aiohttp
import asyncio
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Any

from fetcher import (
    fetch_fx_change_rate,
    fetch_index_info,
    fetch_stock_change_rate,
    fetch_us_index_info,
    fetch_us_stock_change_rate,
    build_best_effort_us_em_code,
)

logger = logging.getLogger(__name__)

MODEL_VERSION = "净值估值模型优化v1.9a"

# China Standard Time (UTC+8)
CST = timezone(timedelta(hours=8))

# Time period boundaries for overseas estimation (Beijing time)
# Period 1: 09:00 - 16:00 (A-share trading hours)
# Period 2: 16:00 - 21:00 (After A-share close, before US open)
# Period 3: 21:00 - 09:00 (US market open / overnight)
PERIOD1_START = 9 * 100    # 0900
PERIOD1_END = 16 * 100     # 1600
PERIOD2_END = 21 * 100     # 2100

# Conservative target portfolio exposure used when we do not have full asset
# allocation. The remaining part is implicitly cash/other assets with 0 intraday
# return. This avoids the old "top-10 average -> 100% fund" over-extrapolation.
DEFAULT_TARGET_EXPOSURE = {
    "domestic": 0.92,
    "hk": 0.95,
    "overseas": 0.95,
}

# Residual proxies used only for the undisclosed part of holdings-based funds
# when users have not configured a fund-specific proxy. They are broad market
# proxies and are intentionally not treated as exact tracking indices.
DEFAULT_RESIDUAL_PROXY = {
    "domestic": "1.000300",   # CSI 300 / 沪深300
    "hk": "100.HSCEI",       # Hang Seng China Enterprises Index proxy
}

FOREIGN_MARKET_FX = {
    "116": "HKDCNY",  # HK stocks
    "105": "USDCNY",  # US stocks / ETFs
    "106": "USDCNY",
    "107": "USDCNY",
    "101": "USDCNY",  # COMEX futures
    "102": "USDCNY",  # NYMEX futures
    "112": "USDCNY",  # Brent futures
}


def get_overseas_period() -> int:
    """Determine current time period for overseas estimation.

    Returns:
        1: 09:00-16:00 Beijing (A-share trading, US closed)
        2: 16:00-21:00 Beijing (A-share closed, US not yet open)
        3: 21:00-09:00 Beijing (US market open / overnight)
    """
    now = datetime.now(CST)
    current_time = now.hour * 100 + now.minute
    if PERIOD1_START <= current_time < PERIOD1_END:
        return 1
    if PERIOD1_END <= current_time < PERIOD2_END:
        return 2
    return 3


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_round(value: Any, digits: int = 2) -> float:
    return round(_as_float(value), digits)


def _market_prefix(em_code: str) -> str:
    return str(em_code or "").split(".", 1)[0] if em_code else ""


def _is_us_like_market(prefix: str) -> bool:
    return prefix in {"105", "106", "107"}


def _fx_pair_for_em_code(em_code: str) -> str:
    return FOREIGN_MARKET_FX.get(_market_prefix(em_code), "")


def _compound_return_pct(asset_change_pct: float, fx_change_pct: float = 0.0) -> float:
    """Combine local asset return and FX return into CNY return.

    Approximation: (1 + local_return) * (1 + fx_return) - 1.
    """
    return ((1 + asset_change_pct / 100.0) * (1 + fx_change_pct / 100.0) - 1) * 100.0


def _estimate_nav(nav: float, change_rate_pct: float) -> float:
    return round(nav * (1 + change_rate_pct / 100.0), 4)


def _parse_report_date(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    # Common formats: 2026-03-31, 2026/03/31, 2026年03月31日
    match = re.search(r"(20\d{2})[-/年.](\d{1,2})[-/月.](\d{1,2})", text)
    if not match:
        return None
    try:
        year, month, day = map(int, match.groups())
        return datetime(year, month, day, tzinfo=CST)
    except ValueError:
        return None


def _staleness_penalty(holdings: list[dict]) -> tuple[float, str]:
    """Return confidence penalty and a human note for stale holdings."""
    dates = [_parse_report_date(h.get("report_date", "")) for h in holdings]
    dates = [d for d in dates if d is not None]
    if not dates:
        return 0.05, "持仓报告日期缺失"
    latest = max(dates)
    age_days = (datetime.now(CST) - latest).days
    if age_days <= 140:
        return 0.0, f"持仓报告约{age_days}天前"
    if age_days <= 230:
        return 0.12, f"持仓较旧，约{age_days}天前"
    return 0.25, f"持仓明显滞后，约{age_days}天前"


async def asyncio_coro_return(val):
    """Helper to return a value as a coroutine."""
    return val


async def _fetch_asset_change_rate(session: aiohttp.ClientSession, holding: dict) -> dict:
    """Fetch one holding's CNY-adjusted change rate."""
    em_code = str(holding.get("em_code", "") or "").strip()
    stock_code = str(holding.get("stock_code", "") or "").strip()
    if not em_code and stock_code:
        if stock_code.isdigit() and len(stock_code) == 6:
            # A-share fallback when the F10 link does not expose the EastMoney secid.
            em_code = f"1.{stock_code}" if stock_code.startswith("6") else f"0.{stock_code}"
        elif not stock_code.isdigit():
            # Best-effort fallback for US tickers parsed without EastMoney market prefix.
            # Unsupported foreign/local exchange strings remain empty and are skipped.
            em_code = build_best_effort_us_em_code(stock_code)

    if not em_code:
        return {
            "asset_change_rate": 0.0,
            "fx_change_rate": 0.0,
            "change_rate": 0.0,
            "em_code": "",
            "fx_pair": "",
            "quote_ok": False,
        }

    prefix = _market_prefix(em_code)
    if _is_us_like_market(prefix):
        asset_change = await fetch_us_stock_change_rate(session, em_code)
    else:
        asset_change = await fetch_stock_change_rate(session, em_code)

    fx_pair = _fx_pair_for_em_code(em_code)
    fx_change = await fetch_fx_change_rate(session, fx_pair) if fx_pair else 0.0
    total_change = _compound_return_pct(asset_change, fx_change)
    return {
        "asset_change_rate": round(asset_change, 4),
        "fx_change_rate": round(fx_change, 4),
        "change_rate": round(total_change, 4),
        "em_code": em_code,
        "fx_pair": fx_pair,
        "quote_ok": True,
    }


async def _fetch_proxy_change_rate(session: aiohttp.ClientSession, proxy_code: str) -> dict:
    """Fetch a domestic/HK/US/commodity proxy return, adjusted for FX if needed."""
    proxy_code = str(proxy_code or "").strip()
    if not proxy_code:
        return {}

    prefix = _market_prefix(proxy_code)
    if prefix in {"100", "101", "102", "105", "106", "107", "112"}:
        info = await fetch_us_index_info(session, proxy_code)
    else:
        info = await fetch_index_info(session, proxy_code)

    if not info:
        return {}

    asset_change = _as_float(info.get("change_rate", 0))
    fx_pair = _fx_pair_for_em_code(proxy_code)
    # 100.HSCEI is a Hong Kong index proxy; EastMoney uses market prefix 100 for
    # several global indices, so infer HKD for HSCEI/HSI-like symbols.
    upper_code = proxy_code.upper()
    if not fx_pair and ("HSCEI" in upper_code or "HSI" in upper_code):
        fx_pair = "HKDCNY"
    elif not fx_pair and prefix == "100":
        # US/global indices such as NDX/SPX/DJIA are USD-denominated proxies.
        fx_pair = "USDCNY"

    fx_change = await fetch_fx_change_rate(session, fx_pair) if fx_pair else 0.0
    change = _compound_return_pct(asset_change, fx_change)
    return {
        "index_code": proxy_code,
        "index_name": info.get("index_name", ""),
        "index_value": info.get("index_value", 0),
        "asset_change_rate": round(asset_change, 4),
        "fx_pair": fx_pair,
        "fx_change_rate": round(fx_change, 4),
        "change_rate": round(change, 4),
    }


def _holding_result(
    nav: float,
    holdings: list[dict],
    quote_results: list[dict],
    proxy_info: dict | None,
    category: str,
    target_exposure: float | None = None,
    residual_beta: float = 0.85,
) -> dict:
    """Build holdings-based NAV estimate from already-fetched quote data."""
    if nav <= 0:
        return _empty_result(nav, "holdings_plus_proxy", "单位净值缺失")
    if not holdings:
        return _empty_result(nav, "holdings_plus_proxy", "无可用持仓")

    details = []
    coverage = 0.0
    known_contribution = 0.0
    quote_ok_count = 0

    for holding, quote in zip(holdings, quote_results):
        ratio = max(0.0, _as_float(holding.get("holding_ratio", 0)))
        weight = ratio / 100.0
        change_rate = _as_float(quote.get("change_rate", 0))
        contribution = weight * change_rate
        coverage += weight
        known_contribution += contribution
        if quote.get("quote_ok"):
            quote_ok_count += 1

        details.append({
            "source": "holding",
            "stock_code": holding.get("stock_code", ""),
            "stock_name": holding.get("stock_name", ""),
            "holding_ratio": round(ratio, 4),
            "em_code": quote.get("em_code", holding.get("em_code", "")),
            "asset_change_rate": round(_as_float(quote.get("asset_change_rate", change_rate)), 2),
            "fx_pair": quote.get("fx_pair", ""),
            "fx_change_rate": round(_as_float(quote.get("fx_change_rate", 0)), 2),
            "change_rate": round(change_rate, 2),
            "contribution_pct": round(contribution, 4),
            "report_date": holding.get("report_date", ""),
        })

    coverage = min(max(coverage, 0.0), 1.0)
    target = target_exposure
    if target is None:
        target = DEFAULT_TARGET_EXPOSURE.get(category, 0.92)
    target = min(max(target, coverage), 1.0)

    weighted_avg = known_contribution / coverage if coverage > 0 else 0.0
    residual_weight = max(0.0, target - coverage)
    residual_source = "cash_or_unestimated"
    residual_change = 0.0
    residual_weighted_change = 0.0

    if residual_weight > 0:
        if proxy_info and proxy_info.get("change_rate") is not None:
            residual_change = _as_float(proxy_info.get("change_rate", 0))
            residual_source = "proxy_index"
            residual_weighted_change = residual_weight * residual_change * residual_beta
            details.append({
                "source": "residual_proxy",
                "index_code": proxy_info.get("index_code", ""),
                "index_name": proxy_info.get("index_name", ""),
                "index_value": proxy_info.get("index_value", 0),
                "asset_change_rate": round(_as_float(proxy_info.get("asset_change_rate", residual_change)), 2),
                "fx_pair": proxy_info.get("fx_pair", ""),
                "fx_change_rate": round(_as_float(proxy_info.get("fx_change_rate", 0)), 2),
                "change_rate": round(residual_change, 2),
                "residual_ratio": round(residual_weight * 100, 2),
                "residual_beta": residual_beta,
                "contribution_pct": round(residual_weighted_change, 4),
                "note": "未知持仓部分使用代理指数估算",
            })
        elif coverage >= 0.20:
            # No proxy: use top holdings average for the residual, but discount it
            # because this assumes representativeness of a stale top-10 snapshot.
            residual_change = weighted_avg
            residual_source = "discounted_holdings_average"
            avg_beta = min(residual_beta, 0.70)
            residual_weighted_change = residual_weight * residual_change * avg_beta
            details.append({
                "source": "residual_holdings_average",
                "change_rate": round(residual_change, 2),
                "residual_ratio": round(residual_weight * 100, 2),
                "residual_beta": avg_beta,
                "contribution_pct": round(residual_weighted_change, 4),
                "note": "无代理指数，未知部分按持仓平均涨跌折减估算",
            })
        else:
            details.append({
                "source": "residual_cash",
                "change_rate": 0,
                "residual_ratio": round(residual_weight * 100, 2),
                "contribution_pct": 0,
                "note": "持仓覆盖率过低，未知部分暂按现金/其他资产处理",
            })

    estimated_change = known_contribution + residual_weighted_change
    estimated_nav = _estimate_nav(nav, estimated_change)

    penalty, stale_note = _staleness_penalty(holdings)
    quote_score = quote_ok_count / len(holdings) if holdings else 0.0
    coverage_score = coverage / target if target > 0 else 0.0
    proxy_bonus = 0.15 if proxy_info else 0.0
    confidence = max(0.05, min(0.95, 0.25 + 0.35 * coverage_score + 0.20 * quote_score + proxy_bonus - penalty))

    note = (
        f"持仓覆盖{coverage * 100:.1f}%，目标暴露{target * 100:.1f}%，"
        f"未知部分来源：{residual_source}；{stale_note}"
    )

    return {
        "estimated_nav": estimated_nav,
        "estimated_change_rate": round(estimated_change, 2),
        "details": details,
        "model_version": MODEL_VERSION,
        "valuation_method": "holdings_plus_proxy",
        "valuation_confidence": round(confidence, 2),
        "valuation_note": note,
        "coverage_ratio": round(coverage * 100, 2),
        "target_exposure": round(target * 100, 2),
        "residual_ratio": round(residual_weight * 100, 2),
    }


def _empty_result(nav: float, method: str, note: str) -> dict:
    return {
        "estimated_nav": round(nav, 4) if nav else nav,
        "estimated_change_rate": 0,
        "details": [],
        "model_version": MODEL_VERSION,
        "valuation_method": method,
        "valuation_confidence": 0.0,
        "valuation_note": note,
    }


async def estimate_nav_by_holdings(
    session: aiohttp.ClientSession,
    nav: float,
    holdings: list,
    proxy_index_code: str = "",
    category: str = "domestic",
) -> dict:
    """Estimate NAV from disclosed holdings plus an optional residual proxy.

    Optimized formula:
        fund_change = Σ(holding_weight × CNY_adjusted_holding_return)
                      + residual_weight × proxy_return × residual_beta

    This differs from the old method which divided top-10 contribution by top-10
    coverage and implicitly assumed the unknown 90/80/50% of assets moved exactly
    like the disclosed holdings. The new method keeps known weights as actual
    fund weights and only estimates the residual with a proxy or discounted
    holdings average.
    """
    nav = _as_float(nav)
    if not holdings or nav <= 0:
        return _empty_result(nav, "holdings_plus_proxy", "无可用持仓或单位净值缺失")

    tasks = [_fetch_asset_change_rate(session, h) for h in holdings]
    quote_results = await asyncio.gather(*tasks, return_exceptions=True)
    quote_results = [q if isinstance(q, dict) else {} for q in quote_results]

    proxy_info = await _fetch_proxy_change_rate(session, proxy_index_code) if proxy_index_code else {}
    return _holding_result(nav, holdings, quote_results, proxy_info, category)


async def estimate_nav_by_industry_index(
    session: aiohttp.ClientSession,
    nav: float,
    index_code: str,
) -> dict:
    """Estimate NAV based on a configured industry/index proxy.

    Formula:
        estimated_nav = nav × (1 + CNY_adjusted_index_change / 100)
    """
    nav = _as_float(nav)
    if not index_code or nav <= 0:
        return _empty_result(nav, "index_proxy", "指数代码或单位净值缺失")

    proxy_info = await _fetch_proxy_change_rate(session, index_code)
    if not proxy_info:
        return _empty_result(nav, "index_proxy", f"无法获取指数 {index_code} 行情")

    change_rate = _as_float(proxy_info.get("change_rate", 0))
    estimated_nav = _estimate_nav(nav, change_rate)
    confidence = 0.88
    if proxy_info.get("fx_pair"):
        confidence = 0.82  # FX quote may be approximate/fallback.

    return {
        "estimated_nav": estimated_nav,
        "estimated_change_rate": round(change_rate, 2),
        "index_name": proxy_info.get("index_name", ""),
        "index_value": proxy_info.get("index_value", 0),
        "details": [{
            "source": "index_proxy",
            "index_code": index_code,
            "index_name": proxy_info.get("index_name", ""),
            "index_value": proxy_info.get("index_value", 0),
            "asset_change_rate": round(_as_float(proxy_info.get("asset_change_rate", change_rate)), 2),
            "fx_pair": proxy_info.get("fx_pair", ""),
            "fx_change_rate": round(_as_float(proxy_info.get("fx_change_rate", 0)), 2),
            "change_rate": round(change_rate, 2),
        }],
        "model_version": MODEL_VERSION,
        "valuation_method": "index_proxy",
        "valuation_confidence": confidence,
        "valuation_note": "使用配置指数/行业指数涨跌幅估算整只基金净值",
    }


async def estimate_nav_by_overseas_holdings(
    session: aiohttp.ClientSession,
    nav: float,
    cn_change_rate: float,
    overseas_holdings: list,
    us_index_code: str,
) -> dict:
    """Estimate NAV for QDII/overseas funds.

    The legacy argument ``cn_change_rate`` is retained for compatibility but no
    longer treated as the undisclosed portion of the portfolio by default. For
    most QDII LOF funds, the undisclosed portion is more likely foreign assets,
    cash or derivatives, so the optimized model uses the configured overseas
    index/commodity/ETF proxy for the residual and applies USD/HKD-CNY FX where
    available.
    """
    nav = _as_float(nav)
    period = get_overseas_period()
    if nav <= 0:
        result = _empty_result(nav, "overseas_proxy", "单位净值缺失")
        result.update({"period": period, "cn_ratio": 0, "us_ratio": 0, "us_index_name": "", "us_change_rate": 0})
        return result

    proxy_info = await _fetch_proxy_change_rate(session, us_index_code) if us_index_code else {}

    if overseas_holdings:
        tasks = [_fetch_asset_change_rate(session, h) for h in overseas_holdings]
        quote_results = await asyncio.gather(*tasks, return_exceptions=True)
        quote_results = [q if isinstance(q, dict) else {} for q in quote_results]
        result = _holding_result(
            nav,
            overseas_holdings,
            quote_results,
            proxy_info,
            "overseas",
            target_exposure=DEFAULT_TARGET_EXPOSURE["overseas"],
            residual_beta=1.0,
        )
        result["valuation_method"] = "overseas_holdings_plus_proxy"
    elif proxy_info:
        change_rate = _as_float(proxy_info.get("change_rate", 0))
        result = {
            "estimated_nav": _estimate_nav(nav, change_rate),
            "estimated_change_rate": round(change_rate, 2),
            "details": [{
                "source": "overseas_proxy",
                "index_code": us_index_code,
                "index_name": proxy_info.get("index_name", ""),
                "index_value": proxy_info.get("index_value", 0),
                "asset_change_rate": round(_as_float(proxy_info.get("asset_change_rate", change_rate)), 2),
                "fx_pair": proxy_info.get("fx_pair", ""),
                "fx_change_rate": round(_as_float(proxy_info.get("fx_change_rate", 0)), 2),
                "change_rate": round(change_rate, 2),
                "period": f"时段{period}",
                "note": "无可用境外持仓，使用配置的境外指数/商品/ETF代理",
            }],
            "model_version": MODEL_VERSION,
            "valuation_method": "overseas_proxy",
            "valuation_confidence": 0.78,
            "valuation_note": "无可用境外持仓，使用配置代理并尝试叠加汇率变动",
        }
    else:
        # Last resort: keep the public fund estimate if it exists. This is better
        # than manufacturing a CN/US split without holdings or proxy evidence.
        fallback_change = _as_float(cn_change_rate)
        result = {
            "estimated_nav": _estimate_nav(nav, fallback_change),
            "estimated_change_rate": round(fallback_change, 2),
            "details": [{
                "source": "fund_api_fallback",
                "change_rate": round(fallback_change, 2),
                "note": "无境外持仓和代理指数，回退使用基金估算/历史涨跌幅",
            }],
            "model_version": MODEL_VERSION,
            "valuation_method": "fund_api_fallback",
            "valuation_confidence": 0.35 if fallback_change else 0.0,
            "valuation_note": "缺少境外持仓和代理指数，结果仅作保守兜底",
        }

    us_change = 0.0
    if proxy_info:
        us_change = _as_float(proxy_info.get("change_rate", 0))
    else:
        us_change = _as_float(result.get("estimated_change_rate", 0))

    result.update({
        "period": period,
        "cn_ratio": 0,
        "us_ratio": _safe_round(result.get("target_exposure", DEFAULT_TARGET_EXPOSURE["overseas"] * 100), 2),
        "cn_change_rate": 0,
        "us_change_rate": round(us_change, 2),
        "us_index_name": proxy_info.get("index_name", "") if proxy_info else "",
    })
    return result


def _external_estimate_candidate(nav: float, data: dict) -> dict:
    """Build a fallback candidate from fundgz/lsjz data already fetched."""
    source_nav = _as_float(data.get("source_estimated_nav", data.get("estimated_nav", 0)))
    source_change = _as_float(data.get("source_estimated_change_rate", data.get("estimated_change_rate", 0)))
    if nav <= 0 or source_nav <= 0:
        return _empty_result(nav, "fund_api_fallback", "外部基金估算缺失")
    # Prefer the returned estimated NAV if present; otherwise reconstruct it.
    estimated_nav = round(source_nav, 4) if source_nav > 0 else _estimate_nav(nav, source_change)
    if source_change == 0 and nav > 0 and estimated_nav > 0:
        source_change = (estimated_nav / nav - 1) * 100
    return {
        "estimated_nav": estimated_nav,
        "estimated_change_rate": round(source_change, 2),
        "details": [{
            "source": "fund_api_fallback",
            "estimated_nav": estimated_nav,
            "change_rate": round(source_change, 2),
            "estimate_time": data.get("source_estimate_time", data.get("estimate_time", "")),
        }],
        "model_version": MODEL_VERSION,
        "valuation_method": "fund_api_fallback",
        "valuation_confidence": 0.55 if source_change else 0.25,
        "valuation_note": "回退使用天天基金/历史净值接口返回的估算或涨跌幅",
    }


def _pick_primary_or_fallback(primary: dict, fallback: dict, nav: float) -> dict:
    """Return primary unless it clearly lacks signal."""
    if primary and _as_float(primary.get("estimated_nav", 0)) > 0:
        if _as_float(primary.get("valuation_confidence", 0)) > 0 or primary.get("details"):
            return primary
    if fallback and _as_float(fallback.get("estimated_nav", 0)) > 0:
        return fallback
    return _empty_result(nav, "unavailable", "无可用估值信号")


async def estimate_nav_unified(
    session: aiohttp.ClientSession,
    fund: dict,
    data: dict,
) -> dict:
    """Unified reusable NAV valuation model.

    Inputs are deliberately the same fund configuration and fetched data already
    available to the project. The function decides which valuation path to use
    and returns a normalized result that can be stored/displayed consistently.
    """
    nav = _as_float(data.get("nav", 0))
    algo_type = (fund.get("algo_type") or "holdings").strip()
    category = (fund.get("category") or "domestic").strip()
    industry_index_code = (fund.get("industry_index_code") or "").strip()
    us_index_code = (fund.get("us_index_code") or "").strip()
    fallback = _external_estimate_candidate(nav, data)

    if nav <= 0:
        return _pick_primary_or_fallback({}, fallback, nav)

    # 1) Explicit index/industry funds: use configured index for the whole fund.
    if algo_type == "industry" and industry_index_code:
        primary = await estimate_nav_by_industry_index(session, nav, industry_index_code)
        return _pick_primary_or_fallback(primary, fallback, nav)

    # 2) Overseas/QDII: use foreign holdings if available, otherwise configured
    # US/HK/commodity proxy. Do not invent an A-share residual split.
    if algo_type == "overseas" or category == "overseas":
        primary = await estimate_nav_by_overseas_holdings(
            session,
            nav,
            _as_float(data.get("source_estimated_change_rate", data.get("estimated_change_rate", 0))),
            data.get("overseas_holdings", []),
            us_index_code,
        )
        return _pick_primary_or_fallback(primary, fallback, nav)

    # 3) Holdings funds, including HK funds. Combine A/H/foreign holdings if the
    # crawler found both sets. Use fund-specific index first; otherwise category
    # proxy only for the undisclosed residual.
    holdings = list(data.get("holdings", []) or [])
    if category == "hk":
        holdings.extend(list(data.get("overseas_holdings", []) or []))
    proxy_code = industry_index_code or DEFAULT_RESIDUAL_PROXY.get(category, "")
    primary = await estimate_nav_by_holdings(session, nav, holdings, proxy_code, category)
    return _pick_primary_or_fallback(primary, fallback, nav)
