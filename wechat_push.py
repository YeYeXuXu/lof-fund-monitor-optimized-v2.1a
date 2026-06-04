"""WeChat push module using ServerChan for LOF Fund Monitor."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Iterable

import aiohttp

logger = logging.getLogger(__name__)

CST = timezone(timedelta(hours=8))
SERVERCHAN_URL = "https://sctapi.ftqq.com"
MODEL_VERSION_TEXT = "净值估值模型优化v2.2a"

STATUS_ICON = {
    "开放": "✅",
    "限大额": "⚠️",
    "暂停": "🚫",
}


def parse_send_keys(send_keys: str | Iterable[str]) -> list[str]:
    """Normalize one or more ServerChan SendKeys.

    Supported separators: comma, Chinese comma, semicolon, whitespace, newline.
    GitHub Actions users can therefore store either one key in WECHAT_SEND_KEY or
    multiple keys in WECHAT_SEND_KEY / SERVERCHAN_SENDKEYS.
    """
    if not send_keys:
        return []

    if isinstance(send_keys, str):
        candidates = re.split(r"[,，;；\s]+", send_keys)
    else:
        candidates = []
        for item in send_keys:
            candidates.extend(re.split(r"[,，;；\s]+", str(item or "")))

    result: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        key = item.strip()
        if not key or key in seen:
            continue
        result.append(key)
        seen.add(key)
    return result


def mask_send_key(send_key: str) -> str:
    """Mask a single SendKey for logs/UI without leaking secrets."""
    key = str(send_key or "").strip()
    if not key:
        return ""
    if len(key) > 10:
        return f"{key[:4]}****{key[-4:]}"
    if len(key) > 4:
        return f"{key[:2]}****{key[-2:]}"
    return "****"


def mask_send_keys(send_keys: str | Iterable[str]) -> str:
    """Mask multiple SendKeys for UI display."""
    keys = parse_send_keys(send_keys)
    return ", ".join(mask_send_key(k) for k in keys)


def _serverchan_success(status: int, response_text: str) -> bool:
    """Best-effort ServerChan response check.

    ServerChan Turbo commonly returns HTTP 200 plus JSON. Different generations
    of the service have used fields such as code/errno/message, so accept the
    documented success shapes and the legacy text containing "success".
    """
    if status != 200:
        return False
    text = (response_text or "").strip()
    if not text:
        return False
    try:
        data = json.loads(text)
        code = data.get("code", data.get("errno", data.get("errcode")))
        if code in (0, "0"):
            return True
        message = str(data.get("message", data.get("msg", ""))).lower()
        return "success" in message or message == "ok"
    except Exception:
        return "success" in text.lower() or '"ok"' in text.lower()


async def _send_one_wechat_message(session: aiohttp.ClientSession, send_key: str, title: str, content: str) -> dict:
    """Send a message to one ServerChan SendKey."""
    url = f"{SERVERCHAN_URL}/{send_key}.send"
    data = {"title": title, "desp": content, "channel": "9"}
    masked_key = mask_send_key(send_key)
    try:
        async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            response_text = await resp.text()
            if _serverchan_success(resp.status, response_text):
                logger.info("微信推送成功: %s key=%s", title, masked_key)
                return {"success": True, "msg": "推送成功", "response": response_text, "send_key": masked_key}
            logger.warning("微信推送失败: status=%s key=%s response=%s", resp.status, masked_key, response_text[:300])
            return {
                "success": False,
                "msg": f"推送失败({resp.status})",
                "response": response_text,
                "send_key": masked_key,
            }
    except Exception as e:
        logger.error("微信推送异常: key=%s error=%s: %s", masked_key, type(e).__name__, e)
        return {"success": False, "msg": f"{type(e).__name__}: {e}", "response": "", "send_key": masked_key}


async def send_wechat_message(send_key: str, title: str, content: str) -> dict:
    """Send a message via ServerChan.

    The ``send_key`` argument may contain one key or multiple keys separated by
    comma / semicolon / whitespace / newline. The message is sent once to each
    unique key. The return value is successful when at least one recipient is
    successful, and includes per-recipient details for troubleshooting.
    """
    keys = parse_send_keys(send_key)
    if not keys:
        return {"success": False, "msg": "SendKey 未配置", "response": "", "results": []}

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *(_send_one_wechat_message(session, key, title, content) for key in keys),
            return_exceptions=False,
        )

    success_count = sum(1 for item in results if item.get("success"))
    failed_count = len(results) - success_count
    if success_count == len(results):
        return {
            "success": True,
            "msg": f"推送成功：{success_count}/{len(results)} 个微信接收方",
            "response": "",
            "results": results,
        }
    if success_count > 0:
        return {
            "success": True,
            "msg": f"部分推送成功：成功 {success_count} 个，失败 {failed_count} 个",
            "response": "",
            "results": results,
        }
    first_msg = results[0].get("msg", "推送失败") if results else "推送失败"
    return {"success": False, "msg": first_msg, "response": "", "results": results}


def _actionable_status_priority(status: str) -> int:
    """Return 0 for known non-suspended statuses, 1 for suspended/unknown."""
    text = str(status or "").strip()
    if text and text != "未知" and "暂停" not in text:
        return 0
    return 1


def _status_sort_key(fund: dict) -> tuple:
    """Sort helper for WeChat alerts.

    Premium alerts use purchase status; discount alerts use redemption status.
    Within the relevant status group, sort by absolute premium/discount rate.
    """
    premium = fund.get("premium_rate", 0) or 0
    threshold_type = fund.get("threshold_type", "")
    if threshold_type == "discount_lower" or premium < 0:
        status = fund.get("redeem_status", "")
    else:
        status = fund.get("purchase_status", "")
    return (_actionable_status_priority(status), -abs(premium))


def _status_label(status: str) -> str:
    """Return icon + status text."""
    icon = STATUS_ICON.get(status, "❓")
    return f"{icon}{status}"


def _fmt_vol(amount: float) -> str:
    """Format trade_amount (yuan) to 万元."""
    if amount and amount > 0:
        return f"{amount / 10000:.0f}万"
    return "--"

SOURCE_LABEL_AKSHARE = "akshare"
SOURCE_LABEL_PROJECT = "项目原方法"
SOURCE_LABEL_MIXED = "akshare+项目原方法"


def _source_text(value: object) -> str:
    """Normalize source marker text for source-label classification."""
    return str(value or "").strip().lower()


def _source_contains_akshare(*values: object) -> bool:
    """Return True when any source marker clearly came from AkShare."""
    return any("akshare" in _source_text(value) for value in values)


def _source_contains_project_method(*values: object) -> bool:
    """Return True when source markers came from the project's original path."""
    markers = (
        "original",
        "valuation_model",
        "fund_api_fallback",
        "holdings_plus_proxy",
        "index_proxy",
        "overseas_proxy",
        "calculated_from",
        "fundgz",
        "eastmoney",
        "pingzhongdata",
    )
    for value in values:
        text = _source_text(value)
        if "akshare" in text:
            # AkShare adapters also call EastMoney-compatible endpoints; keep them
            # classified as AkShare unless another source marker shows project logic.
            text = text.replace("akshare", "")
        if any(marker in text for marker in markers):
            return True
    return False


def _method_label_from_sources(*values: object) -> str:
    uses_akshare = _source_contains_akshare(*values)
    uses_project = _source_contains_project_method(*values)
    if uses_akshare and uses_project:
        return SOURCE_LABEL_MIXED
    if uses_akshare:
        return SOURCE_LABEL_AKSHARE
    return SOURCE_LABEL_PROJECT


def _metric_source_label(fund: dict, metric: str) -> str:
    """Return the parenthesized source label for WeChat alert rows.

    For 折溢价率/交易价格/估算净值, show ``akshare+项目原方法`` only
    when that metric actually combines AkShare inputs with the project's original
    fallback/model/calculation path; otherwise show the method actually used.
    """
    if metric == "trade_price":
        return _method_label_from_sources(fund.get("price_source"))

    if metric == "trade_amount":
        source = fund.get("trade_amount_source") or fund.get("amount_source") or fund.get("price_source")
        return _method_label_from_sources(source)

    if metric == "estimated_nav":
        estimate_source = fund.get("estimate_source") or ""
        valuation_method = fund.get("valuation_method") or ""
        values = [estimate_source, valuation_method]
        if _source_contains_project_method(estimate_source, valuation_method):
            # Project valuation uses the latest official NAV as its base; include
            # nav_source so AkShare NAV + project model is labeled as mixed.
            values.append(fund.get("nav_source"))
        elif not estimate_source and not valuation_method:
            values.append(fund.get("nav_source"))
        return _method_label_from_sources(*values)

    if metric == "premium_rate":
        premium_source = fund.get("premium_source") or ""
        premium_source_text = _source_text(premium_source)
        if premium_source_text.startswith("calculated_from_"):
            return _method_label_from_sources(
                premium_source,
                fund.get("price_source"),
                fund.get("trade_amount_source"),
                fund.get("premium_base_source"),
                fund.get("estimate_source"),
                fund.get("nav_source"),
                fund.get("valuation_method"),
            )
        return _method_label_from_sources(premium_source)

    return SOURCE_LABEL_PROJECT


def _source_suffix(fund: dict, metric: str) -> str:
    """Format the parenthesized source suffix for a WeChat metric."""
    return f"（{_metric_source_label(fund, metric)}）"



def build_threshold_alert_message(alerts: list, premium_upper: float = 3.0,
                                   discount_lower: float = -5.0,
                                   min_turnover: float = 60,
                                   enabled_conditions: list | None = None) -> str:
    """Build alert message body with exact filter criteria (Chinese)."""
    now = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")

    # Sort by relevant purchase/redeem status first, then absolute discount/premium rate.
    alerts_sorted = sorted(alerts, key=_status_sort_key)

    lines = []
    lines.append(f"## ⚠️ 折溢价阈值告警\n")
    lines.append(f"**时间：** {now}  \n")
    lines.append(f"**版本：** {MODEL_VERSION_TEXT}  \n")

    # Conditions
    lines.append("**触发条件：**  ")
    conds = []
    if enabled_conditions is None:
        has_prem = any(a.get("threshold_type") == "premium_upper" for a in alerts_sorted)
        has_disc = any(a.get("threshold_type") == "discount_lower" for a in alerts_sorted)
    else:
        has_prem = "premium_upper" in enabled_conditions
        has_disc = "discount_lower" in enabled_conditions
    if has_prem:
        conds.append(f"溢价率 ≥ {premium_upper}%")
    if has_disc:
        conds.append(f"折价率 ≤ {discount_lower}%")
    conds.append(f"成交金额 ≥ {int(min_turnover)} 万元")
    lines.append("  \n".join(conds))
    lines.append(f"  \n**告警数量：** {len(alerts_sorted)} 只  \n")
    lines.append("")

    for a in alerts_sorted:
        premium = a.get("premium_rate", 0) or 0
        direction = "🔴 溢价" if premium > 0 else "🟢 折价"
        purchase_status_label = _status_label(a.get("purchase_status", "未知"))
        redeem_status_label = _status_label(a.get("redeem_status", "未知"))
        vol = _fmt_vol(a.get("trade_amount", 0))
        lines.append(f"### {direction} **{a.get('fund_code', '')}** {a.get('fund_name', '')}\n")
        lines.append(f"- 折溢价率：**{premium:+.2f}%**{_source_suffix(a, 'premium_rate')}  \n")
        lines.append(f"- 交易价格：{a.get('trade_price', '--')}{_source_suffix(a, 'trade_price')}  \n")
        lines.append(f"- 估算净值：{a.get('estimated_nav', '--')}{_source_suffix(a, 'estimated_nav')}  \n")
        lines.append(f"- 成交金额：{vol}{_source_suffix(a, 'trade_amount')}  \n")
        lines.append(f"- 申购状态：{purchase_status_label}  \n")
        lines.append(f"- 赎回状态：{redeem_status_label}  \n")
        lines.append("")

    lines.append("---")
    lines.append(f"*自动告警推送 · {MODEL_VERSION_TEXT} · 仅供参考*")

    return "\n".join(lines)
