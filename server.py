"""Main aiohttp server for LOF Fund Monitor."""
import asyncio
import aiohttp
from aiohttp import web
import json
import logging
import os
import sys
import re
import signal
import socket
import subprocess
import webbrowser
from datetime import datetime, timezone, timedelta

# Add project directory to path
sys.path.insert(0, os.path.dirname(__file__))

from db import (init_db, seed_default_funds, get_all_funds, get_fund, add_fund, remove_fund, update_fund_algo,
    save_holdings, get_holdings, save_realtime, get_all_realtime, get_realtime,
    get_algo_configs, save_overseas_holdings, get_overseas_holdings, batch_add_funds,
    update_holdings_timestamp, get_funds_needing_holdings_refresh,
    get_wechat_config, save_wechat_config, claim_wechat_push_slot, mark_wechat_push_slot)
from fetcher import fetch_all_fund_data, fetch_fund_info, fetch_overseas_holdings, fetch_fund_holdings, fetch_fund_nav_from_lsjz
from estimator import estimate_nav_unified
from wechat_push import send_wechat_message, build_threshold_alert_message, mask_send_keys, parse_send_keys
from akshare_fund_adapter import (
    get_akshare_fund_snapshot, apply_akshare_fund_data_to_result,
    overlay_akshare_realtime_for_funds,
)

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Suppress noisy third-party loggers (aiohttp, asyncio, etc.)
for noisy_logger in ['aiohttp.access', 'aiohttp.client', 'aiohttp.internal', 'aiohttp.server', 'aiohttp.web', 'aiohttp.handler', 'asyncio']:
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)

# China Standard Time
CST = timezone(timedelta(hours=8))


def _env_enabled(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _running_in_github_actions() -> bool:
    return _env_enabled("GITHUB_ACTIONS")


def _describe_fund_data_sources(data: dict) -> str:
    """Build a compact source summary for Actions/runtime logs."""
    akshare_source = data.get("akshare_source") or "未命中"
    nav_source = data.get("nav_source") or "未记录"
    estimate_source = data.get("estimate_source") or data.get("valuation_method") or "未记录"
    price_source = data.get("price_source") or "未记录"
    premium_source = data.get("premium_source") or "未记录"
    return (
        f"AkShare={akshare_source}; "
        f"NAV={nav_source}; "
        f"估值={estimate_source}; "
        f"价格={price_source}; "
        f"折溢价={premium_source}"
    )


# DuckDNS dynamic DNS configuration (defaults, can be overridden via API)
DUCKDNS_CONFIG_FILE = os.path.join(os.path.dirname(__file__), "duckdns_config.json")
DUCKDNS_UPDATE_INTERVAL = 180  # 3 minutes in seconds


def load_duckdns_config() -> dict:
    """Load DuckDNS config from JSON file. Returns dict with domain, token keys."""
    defaults = {"domain": os.environ.get("DUCKDNS_DOMAIN", ""), "token": os.environ.get("DUCKDNS_TOKEN", "")}
    try:
        if os.path.exists(DUCKDNS_CONFIG_FILE):
            with open(DUCKDNS_CONFIG_FILE, "r") as f:
                data = json.load(f)
                return {"domain": data.get("domain", defaults["domain"]), "token": data.get("token", defaults["token"])}
    except Exception as e:
        logger.warning(f"Failed to load DuckDNS config: {e}")
    return defaults


def save_duckdns_config(domain: str, token: str) -> None:
    """Save DuckDNS config to JSON file."""
    try:
        with open(DUCKDNS_CONFIG_FILE, "w") as f:
            json.dump({"domain": domain, "token": token}, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Failed to save DuckDNS config: {e}")

# Global state
update_task = None
is_trading = False
update_lock = asyncio.Lock()
shutdown_event = asyncio.Event()


def get_global_ipv6_addresses() -> list[str]:
    """Detect all global-scope IPv6 addresses on this machine.
    
    Returns a list of global IPv6 address strings (without prefix length).
    Excludes link-local (fe80::), loopback (::1), and ULA (fc00::/7) addresses.
    
    Supports both Linux (ip command) and Windows (netsh / socket.getaddrinfo).
    """
    ipv6_addrs = []
    
    # Method 1: Use Python socket.getaddrinfo to enumerate local addresses
    try:
        # getaddrinfo on the hostname returns all local addresses
        hostname = socket.gethostname()
        addrs = socket.getaddrinfo(hostname, None, socket.AF_INET6, socket.SOCK_STREAM)
        for addr_info in addrs:
            addr = addr_info[4][0]  # (family, type, proto, canonname, (addr, port))
            # Skip link-local, loopback, and ULA
            if addr == '::1':
                continue
            if addr.lower().startswith('fe80:'):
                continue
            if addr.lower().startswith('fc') or addr.lower().startswith('fd'):
                continue  # ULA fc00::/7
            # Normalize and add
            normalized = socket.inet_ntop(socket.AF_INET6, socket.inet_pton(socket.AF_INET6, addr))
            if normalized not in ipv6_addrs:
                ipv6_addrs.append(normalized)
    except Exception:
        pass
    
    # Method 2: Linux ip command (more reliable on Linux)
    if not ipv6_addrs and sys.platform != 'win32':
        try:
            result = subprocess.run(
                ["ip", "-6", "addr", "show", "scope", "global"],
                capture_output=True, text=True, timeout=5
            )
            for line in result.stdout.splitlines():
                line = line.strip()
                if line.startswith("inet6 "):
                    addr_part = line.split()[1]  # "2001:db8::1/64"
                    addr = addr_part.split("/")[0]  # "2001:db8::1"
                    ipv6_addrs.append(addr)
        except Exception as e:
            logger.warning(f"Failed to detect global IPv6 addresses via ip command: {e}")
    
    # Method 3: Windows ipconfig command fallback
    # ipconfig is the most reliable way on Windows to get all IPv6 addresses
    # Output format (Chinese/English):
    #   "IPv6 Address . . . . . : 2001:da8:8000:1::80"  (English)
    #   "IPv6 地址 . . . . . . . : 2001:da8:8000:1::80"  (Chinese)
    #   "Temporary IPv6 Address . . : 2001:..."            (English)
    #   "临时 IPv6 地址 . . . . . : 2001:..."              (Chinese)
    if not ipv6_addrs and sys.platform == 'win32':
        try:
            result = subprocess.run(
                ["ipconfig"],
                capture_output=True, text=True, timeout=5
            )
            # Match lines containing "IPv6" followed by an address
            # Handles both English and Chinese Windows output
            for match in re.finditer(r'IPv6[^:\n]*:\s*([0-9a-fA-F:]+)', result.stdout):
                addr = match.group(1).strip()
                if not addr or addr == '::1':
                    continue
                try:
                    normalized = socket.inet_ntop(socket.AF_INET6, socket.inet_pton(socket.AF_INET6, addr))
                    # Skip link-local, loopback, ULA
                    if normalized == '::1':
                        continue
                    if normalized.lower().startswith('fe80:'):
                        continue
                    if normalized.lower().startswith('fc') or normalized.lower().startswith('fd'):
                        continue
                    if normalized not in ipv6_addrs:
                        ipv6_addrs.append(normalized)
                except Exception:
                    continue
        except Exception as e:
            logger.warning(f"Failed to detect global IPv6 addresses via ipconfig: {e}")
    
    if not ipv6_addrs:
        logger.debug("No global IPv6 addresses detected on this machine")
    
    return ipv6_addrs


def get_lan_ipv4_addresses() -> list[str]:
    """Detect LAN (private) IPv4 addresses on this machine.
    
    Returns a list of private IPv4 addresses (10.x.x.x, 172.16-31.x.x, 192.168.x.x).
    Excludes public IPv4 and loopback (127.x.x.x).
    
    Supports both Linux (ip command) and Windows (socket.getaddrinfo / ipconfig).
    """
    lan_addrs = []
    
    def _is_private_ipv4(addr: str) -> bool:
        """Check if an IPv4 address is a private/LAN address."""
        if addr.startswith("10."):
            return True
        if addr.startswith("192.168."):
            return True
        if addr.startswith("172."):
            try:
                second_octet = int(addr.split(".")[1])
                return 16 <= second_octet <= 31
            except (ValueError, IndexError):
                return False
        return False
    
    # Method 1: Use Python socket.getaddrinfo (cross-platform)
    try:
        hostname = socket.gethostname()
        addrs = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
        for addr_info in addrs:
            addr = addr_info[4][0]  # (family, type, proto, canonname, (addr, port))
            if _is_private_ipv4(addr) and addr not in lan_addrs:
                lan_addrs.append(addr)
    except Exception:
        pass
    
    # Method 2: Linux ip command (more reliable on Linux)
    if not lan_addrs and sys.platform != 'win32':
        try:
            result = subprocess.run(
                ["ip", "-4", "addr", "show"],
                capture_output=True, text=True, timeout=5
            )
            for line in result.stdout.splitlines():
                line = line.strip()
                if line.startswith("inet "):
                    addr_part = line.split()[1]  # "192.168.1.100/24"
                    addr = addr_part.split("/")[0]  # "192.168.1.100"
                    if _is_private_ipv4(addr) and addr not in lan_addrs:
                        lan_addrs.append(addr)
        except Exception as e:
            logger.warning(f"Failed to detect LAN IPv4 addresses via ip command: {e}")
    
    # Method 3: Windows ipconfig fallback
    if not lan_addrs and sys.platform == 'win32':
        try:
            result = subprocess.run(
                ["ipconfig"],
                capture_output=True, text=True, timeout=5
            )
            # Match IPv4 addresses from ipconfig output
            # English: "IPv4 Address. . . . . : 192.168.1.1"
            # Chinese: "IPv4 地址 . . . . . . . . . . . . : 192.168.1.1"
            for match in re.finditer(r'IPv4[^:]*:\s*(\d+\.\d+\.\d+\.\d+)', result.stdout):
                addr = match.group(1)
                if _is_private_ipv4(addr) and addr not in lan_addrs:
                    lan_addrs.append(addr)
        except Exception as e:
            logger.warning(f"Failed to detect LAN IPv4 addresses via ipconfig: {e}")
    
    return lan_addrs


def is_trading_time() -> bool:
    """Check if current time is within A-share trading hours (9:30-11:30, 13:00-15:00 CST)."""
    now = datetime.now(CST)
    weekday = now.weekday()
    if weekday >= 5:  # Weekend
        return False
    current_time = now.hour * 100 + now.minute
    # Morning: 9:30 - 11:30, Afternoon: 13:00 - 15:00
    if (930 <= current_time <= 1130) or (1300 <= current_time <= 1500):
        return True
    return False


def is_us_trading_time() -> bool:
    """Check if current time is within US stock market trading hours (Beijing time).
    
    US market: 9:30 AM - 4:00 PM Eastern Time
    In Beijing time (UTC+8):
      - Winter (EST, UTC-5): 22:30 - 05:00+1
      - Summer (EDT, UTC-4): 21:30 - 04:00+1
    
    We use a broad range of 21:00 - 05:00 Beijing time to cover both.
    Weekday check accounts for the overnight rollover:
      - 21:00-23:59 Beijing: US weekday = Beijing weekday (Mon-Fri)
      - 00:00-05:00 Beijing: US weekday = Beijing weekday - 1 (i.e., Beijing Tue = US Mon)
    """
    now = datetime.now(CST)
    weekday = now.weekday()
    current_time = now.hour * 100 + now.minute

    if 2100 <= current_time <= 2359:
        # Beijing evening 21:00-23:59, check if today is Mon-Fri
        return weekday < 5
    elif 0 <= current_time <= 500:
        # Beijing early morning 00:00-05:00, US market is yesterday's session
        # Beijing Monday 00:00-05:00 = US Sunday → closed
        # Beijing Tuesday 00:00-05:00 = US Monday → open
        # Beijing Saturday 00:00-05:00 = US Friday → open
        us_weekday = weekday - 1  # Yesterday in US time
        if us_weekday < 0:
            us_weekday = 6  # Sunday wraps to 6
        return us_weekday < 5  # Mon-Fri
    return False


async def _apply_valuation_model(session: aiohttp.ClientSession, fund: dict, data: dict) -> None:
    """Apply the unified NAV valuation model and write normalized fields into data.

    AkShare official spot IOPV has priority when available: in AkShare
    ``fund_etf_spot_em`` f441 is ``IOPV实时估值`` and f402 is the signed
    ``基金折价率`` value.  The same f402 value is used as the monitor's
    premium/discount rate for display and WeChat filtering.
    """
    iopv = data.get("iopv_estimated_nav", 0) or 0
    try:
        iopv = float(iopv)
    except (TypeError, ValueError):
        iopv = 0

    if iopv > 0:
        data["estimated_nav"] = round(iopv, 4)
        data["source_estimated_nav"] = round(iopv, 4)
        data["estimate_source"] = data.get("akshare_source") or "akshare.fund_etf_spot_em:f441"
        if data.get("akshare_premium_rate") is not None:
            data["premium_rate"] = round(float(data.get("akshare_premium_rate") or 0), 2)
            data["premium_source"] = data.get("akshare_source") or "akshare.fund_etf_spot_em:f402"
        data["model_version"] = "净值估值模型优化v1.7a"
        data["valuation_method"] = "akshare_fund_etf_spot_em_iopv"
        data["valuation_confidence"] = 0.9
        data["valuation_note"] = "优先使用 AkShare fund_etf_spot_em：f441=IOPV实时估值；f402=基金折价率/溢价率同一值"
        return

    est = await estimate_nav_unified(session, fund, data)
    for key in (
        "estimated_nav", "estimated_change_rate", "index_name", "index_value",
        "overseas_period", "period", "cn_ratio", "us_ratio", "us_index_name",
        "model_version", "valuation_method", "valuation_confidence", "valuation_note",
        "coverage_ratio", "target_exposure", "residual_ratio",
    ):
        if key in est:
            if key == "period":
                data["overseas_period"] = est[key]
            else:
                data[key] = est[key]
    if data.get("valuation_method"):
        data["estimate_source"] = f"valuation_model:{data['valuation_method']}"


async def _persist_fetched_holdings(fund_code: str, data: dict) -> None:
    """Persist latest holdings snapshots only when the crawler returned rows.

    Empty lists often mean a temporary API/parser miss, so keeping the last good
    snapshot is safer for intraday valuation than deleting holdings immediately.
    """
    if data.get("holdings"):
        await save_holdings(fund_code, data["holdings"])
        await update_holdings_timestamp(fund_code, "domestic")
    if data.get("overseas_holdings"):
        await save_overseas_holdings(fund_code, data["overseas_holdings"])
        await update_holdings_timestamp(fund_code, "overseas")


async def update_single_fund(fund_code: str, market: str = "sz"):
    """Update a single fund's data (used when adding a new fund)."""
    try:
        market_code = "0" if market == "sz" else "1"
        fund = await get_fund(fund_code)
        if not fund:
            return

        category = fund.get("category", "domestic")

        async with aiohttp.ClientSession() as session:
            akshare_snapshot = await get_akshare_fund_snapshot(session, max_age_seconds=30)
            data = await _fetch_fund_data_with_session(
                session, fund_code, market_code, category,
                akshare_snapshot=akshare_snapshot, refresh_holdings=True,
            )

            algo_type = fund.get("algo_type", "holdings")
            await _apply_valuation_model(session, fund, data)
            await _persist_fetched_holdings(fund_code, data)

            if data.get("akshare_premium_rate") is None:
                base_nav = data["estimated_nav"] if data["estimated_nav"] > 0 else data["nav"]
                if base_nav > 0 and data["trade_price"] > 0:
                    data["premium_rate"] = round((data["trade_price"] - base_nav) / base_nav * 100, 2)
                    data["premium_source"] = "calculated_from_nav_and_trade_price"

            await save_realtime(fund_code, data)
            logger.info(
                "Updated single fund %s: NAV=%s, Price=%s, Premium=%s%%, Algo=%s, Source=[%s]",
                fund_code,
                data.get("nav"),
                data.get("trade_price"),
                data.get("premium_rate"),
                algo_type,
                _describe_fund_data_sources(data),
            )
    except Exception as e:
        logger.error(f"Error updating single fund {fund_code}: {e}")


async def update_all_funds():
    """Update all fund data from APIs."""
    if update_lock.locked():
        logger.info("Update already in progress, skipping")
        return

    async with update_lock:
        funds = await get_all_funds()
        if not funds:
            logger.info("No funds to update")
            return

        try:
            async with aiohttp.ClientSession() as session:
                akshare_snapshot = await get_akshare_fund_snapshot(session, max_age_seconds=60)
                for fund in funds:
                    if shutdown_event.is_set():
                        break
                    try:
                        fund_code = fund["fund_code"]
                        market = "0" if fund.get("market", "sz") == "sz" else "1"

                        category = fund.get("category", "domestic")
                        data = await _fetch_fund_data_with_session(
                            session, fund_code, market, category,
                            akshare_snapshot=akshare_snapshot, refresh_holdings=False,
                        )

                        algo_type = fund.get("algo_type", "holdings")
                        await _apply_valuation_model(session, fund, data)
                        await _persist_fetched_holdings(fund_code, data)

                        # Recalculate premium rate only when AkShare f402 did not supply it.
                        if data.get("akshare_premium_rate") is None:
                            base_nav = data["estimated_nav"] if data["estimated_nav"] > 0 else data["nav"]
                            if base_nav > 0 and data["trade_price"] > 0:
                                data["premium_rate"] = round((data["trade_price"] - base_nav) / base_nav * 100, 2)
                                data["premium_source"] = "calculated_from_nav_and_trade_price"

                        await save_realtime(fund_code, data)

                        logger.info(
                            "Updated fund %s: NAV=%s, Price=%s, Premium=%s%%, Source=[%s]",
                            fund_code,
                            data.get("nav"),
                            data.get("trade_price"),
                            data.get("premium_rate"),
                            _describe_fund_data_sources(data),
                        )

                    except Exception as e:
                        logger.error(f"Error updating fund {fund.get('fund_code')}: {e}")

                    await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Error in update_all_funds: {e}")


async def _fetch_fund_data_with_session(
    session: aiohttp.ClientSession,
    fund_code: str,
    market: str = "0",
    category: str = "domestic",
    akshare_snapshot: dict | None = None,
    refresh_holdings: bool = False,
) -> dict:
    """Fetch all data for a single fund using an existing session.

    AkShare-derived batch data is applied first.  Any missing field keeps the
    original project fallback path: fundgz/lsjz for NAV, push2 for price, and F10
    pages or cached DB rows for holdings/status.
    """
    from fetcher import (
        fetch_fund_estimate, fetch_stock_price, fetch_fund_holdings,
        fetch_fund_purchase_status, fetch_fund_share_change,
    )

    result = {
        "fund_code": fund_code, "nav": 0, "nav_date": "", "estimated_nav": 0,
        "estimated_change_rate": 0, "trade_price": 0, "trade_price_change": 0,
        "trade_amount": 0, "premium_rate": 0, "purchase_status": "未知", "redeem_status": "未知",
        "yesterday_purchase_shares": 0, "holdings": [], "overseas_holdings": [],
        "source_estimated_nav": 0, "source_estimated_change_rate": 0, "source_estimate_time": "",
        "model_version": "", "valuation_method": "", "valuation_confidence": 0, "valuation_note": "",
        "akshare_source": "", "akshare_premium_rate": None, "iopv_estimated_nav": 0,
        "nav_source": "", "estimate_source": "", "price_source": "", "premium_source": "",
    }

    # 1) Prefer AkShare release methods for quote/IOPV/valuation information.
    akshare_used = apply_akshare_fund_data_to_result(result, fund_code, akshare_snapshot)
    if akshare_used:
        akshare_source = result.get("akshare_source") or "akshare"
        if result.get("nav", 0) > 0:
            result["nav_source"] = akshare_source
        if result.get("source_estimated_nav", 0) > 0:
            result["estimate_source"] = akshare_source
        if result.get("trade_price", 0) > 0:
            result["price_source"] = akshare_source
        if result.get("akshare_premium_rate") is not None:
            result["premium_source"] = akshare_source

    # 2) Existing NAV/estimate fallback if AkShare did not provide enough data.
    if result.get("nav", 0) <= 0 or result.get("source_estimated_nav", 0) <= 0:
        est_data = await fetch_fund_estimate(session, fund_code)
        if est_data:
            estimate_source = est_data.get("estimate_source", "original.fundgz")
            if result.get("nav", 0) <= 0:
                result["nav"] = est_data.get("nav", 0)
                result["nav_date"] = est_data.get("nav_date", "")
                result["nav_source"] = estimate_source
            if result.get("source_estimated_nav", 0) <= 0:
                result["estimated_nav"] = est_data.get("estimated_nav", 0)
                result["estimated_change_rate"] = est_data.get("estimated_change_rate", 0)
                result["source_estimated_nav"] = est_data.get("estimated_nav", 0)
                result["source_estimated_change_rate"] = est_data.get("estimated_change_rate", 0)
                result["source_estimate_time"] = est_data.get("estimate_time", "")
                result["estimate_source"] = estimate_source
        elif result.get("nav", 0) <= 0:
            # Fallback for QDII/overseas funds: fundgz.1234567.com.cn doesn't cover
            # these funds, so we fetch NAV from eastmoney's F10 historical NAV page.
            nav_data = await fetch_fund_nav_from_lsjz(session, fund_code)
            if nav_data:
                nav_source = nav_data.get("nav_source", "original.eastmoney.f10_lsjz")
                result["nav"] = nav_data.get("nav", 0)
                result["nav_date"] = nav_data.get("nav_date", "")
                result["nav_source"] = nav_source
                if result.get("source_estimated_nav", 0) <= 0:
                    result["estimated_nav"] = nav_data.get("nav", 0)
                    result["estimated_change_rate"] = nav_data.get("daily_change_rate", 0)
                    result["source_estimated_nav"] = nav_data.get("nav", 0)
                    result["source_estimated_change_rate"] = nav_data.get("daily_change_rate", 0)
                    result["source_estimate_time"] = nav_data.get("nav_date", "")
                    result["estimate_source"] = nav_source

    # 3) Existing quote fallback if AkShare spot did not provide price/amount.
    if result.get("trade_price", 0) <= 0:
        price_data = await fetch_stock_price(session, fund_code, market)
        if price_data:
            result["trade_price"] = price_data.get("trade_price", 0)
            result["trade_price_change"] = price_data.get("trade_price_change", 0)
            result["trade_amount"] = price_data.get("amount", 0)
            result["price_source"] = "original.eastmoney.push2delay.stock.get"

    # 4) Holdings are expensive and change slowly.  Use cached rows first during
    # regular quote refreshes; the dedicated daily/manual holdings refresh still
    # keeps them fresh, and a new fund can request refresh_holdings=True.
    cached_holdings = await get_holdings(fund_code)
    holdings = cached_holdings or []
    need_holdings_fetch = refresh_holdings or not holdings
    # If AkShare already supplied a direct IOPV/estimate, do not block scheduled
    # GitHub Actions pushes on a slow holdings crawl unless explicitly requested.
    if need_holdings_fetch and (refresh_holdings or result.get("source_estimated_nav", 0) <= 0):
        fetched_holdings = await fetch_fund_holdings(session, fund_code)
        if fetched_holdings:
            holdings = fetched_holdings
        elif cached_holdings:
            logger.info(f"Using cached domestic holdings for {fund_code}")
    elif cached_holdings:
        logger.debug(f"Using cached domestic holdings for {fund_code}")
    result["holdings"] = holdings

    # For HK/overseas funds, prefer cached overseas holdings during fast quote
    # cycles unless no AkShare/fund estimate is available.
    if category in ("hk", "overseas"):
        cached_overseas = await get_overseas_holdings(fund_code)
        overseas_hk = cached_overseas or []
        need_overseas_fetch = refresh_holdings or not overseas_hk
        if need_overseas_fetch and (refresh_holdings or result.get("source_estimated_nav", 0) <= 0):
            fetched_overseas = await fetch_overseas_holdings(session, fund_code)
            if fetched_overseas:
                overseas_hk = fetched_overseas
            elif cached_overseas:
                logger.info(f"Using cached HK/overseas holdings for {fund_code}")
        if overseas_hk:
            result["overseas_holdings"] = overseas_hk

    # 5) Purchase/redeem status and share changes are kept on the original path.
    # On GitHub Actions, skip this non-critical per-fund F10 request by default so
    # the process can reach scheduled WeChat push times reliably. Set
    # ACTIONS_FETCH_PURCHASE_STATUS=1 to keep the original status crawl in Actions.
    fetch_status = (
        refresh_holdings
        or not _running_in_github_actions()
        or _env_enabled("ACTIONS_FETCH_PURCHASE_STATUS")
    )
    if fetch_status:
        try:
            status = await fetch_fund_purchase_status(session, fund_code)
            result["purchase_status"] = status.get("purchase_status", "未知")
            result["redeem_status"] = status.get("redeem_status", "未知")
            result["yesterday_purchase_shares"] = status.get("yesterday_purchase_shares", 0)
        except Exception as exc:
            logger.debug("Purchase/redeem status fallback failed for %s: %s", fund_code, exc)

    if refresh_holdings:
        try:
            share_data = await fetch_fund_share_change(session, fund_code)
            if share_data and share_data.get("yesterday_purchase_shares", 0) > 0:
                result["yesterday_purchase_shares"] = share_data["yesterday_purchase_shares"]
        except Exception as exc:
            logger.debug("Share change fallback failed for %s: %s", fund_code, exc)

    # 6) Calculate premium only when AkShare did not provide f402.
    if result.get("akshare_premium_rate") is None:
        base_nav = result["estimated_nav"] if result["estimated_nav"] > 0 else result["nav"]
        if base_nav > 0 and result["trade_price"] > 0:
            result["premium_rate"] = round((result["trade_price"] - base_nav) / base_nav * 100, 2)
            result["premium_source"] = "calculated_from_nav_and_trade_price"

    return result


async def _estimate_overseas_fund(session, fund_code, fund, data):
    """Legacy helper kept for compatibility; delegates to the unified model."""
    if not data.get("overseas_holdings"):
        data["overseas_holdings"] = await get_overseas_holdings(fund_code)
    await _apply_valuation_model(session, fund, data)
    if data.get("us_index_name"):
        data["index_name"] = data["us_index_name"]


async def periodic_update():
    """Periodic update task with three refresh modes:
    
    1. A-share trading hours (9:30-11:30, 13:00-15:00): every 5 minutes
    2. US market trading hours (21:00-05:00 Beijing): every 5 minutes
    3. All other times: every 30 minutes
    
    During trading hours, estimated NAV is recalculated based on real-time
    holdings/index change rates, and the premium/discount rate is recomputed.
    """
    last_trading_update = 0
    last_non_trading_update = 0
    while not shutdown_event.is_set():
        try:
            now = asyncio.get_event_loop().time()
            cn_trading = is_trading_time()
            us_trading = is_us_trading_time()

            if cn_trading:
                # A-share trading hours: update every 5 minutes
                if now - last_trading_update >= 300:
                    logger.info("A-share trading time - updating fund data (5min cycle)...")
                    await update_all_funds()
                    last_trading_update = now
                else:
                    logger.debug("A-share trading - waiting for next 5min cycle")
            elif us_trading:
                # US market trading hours: update every 5 minutes
                if now - last_trading_update >= 300:
                    logger.info("US market trading time - updating fund data (5min cycle)...")
                    await update_all_funds()
                    last_trading_update = now
                else:
                    logger.debug("US trading - waiting for next 5min cycle")
            else:
                # Non-trading: update every 30 minutes (1800 seconds)
                if now - last_non_trading_update >= 1800:
                    logger.info("Non-trading time - performing periodic update (30min cycle)...")
                    await update_all_funds()
                    last_non_trading_update = now
                else:
                    logger.debug("Not trading time - skipping update")
        except Exception as e:
            logger.error(f"Error in periodic update: {e}")

        # Check every 30 seconds for shutdown or next cycle
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=30)
        except asyncio.TimeoutError:
            pass


async def get_public_ip(session: aiohttp.ClientSession = None) -> dict:
    """Detect this machine's public IP addresses (IPv4 and/or IPv6) via external APIs.
    
    Returns dict: {"ipv4": "...", "ipv6": "..."}  (values may be empty string)
    """
    result = {"ipv4": "", "ipv6": ""}
    
    # Get public IPv4 via external API
    own_session = False
    if session is None:
        session = aiohttp.ClientSession()
        own_session = True
    try:
        async with session.get("https://api.ipify.org", timeout=aiohttp.ClientTimeout(total=10)) as resp:
            text = await resp.text()
            if text and re.match(r'^\d+\.\d+\.\d+\.\d+$', text.strip()):
                result["ipv4"] = text.strip()
    except Exception:
        pass
    
    # Get public IPv6 via external API (only if no local global IPv6 detected)
    try:
        async with session.get("https://api6.ipify.org", timeout=aiohttp.ClientTimeout(total=10)) as resp:
            text = await resp.text()
            if text and ':' in text.strip():
                result["ipv6"] = text.strip()
    except Exception:
        pass
    
    if own_session:
        await session.close()
    
    return result


async def duckdns_do_update(session: aiohttp.ClientSession = None) -> dict:
    """Perform a single DuckDNS update with the best available IP addresses.
    
    Strategy:
    1. Detect local global IPv6 addresses (from ipconfig/ip)
    2. Detect public IPv4 via external API (ipify.org)
    3. Update DuckDNS with both ipv6= and ip= parameters
    
    Returns dict: {"success": bool, "msg": str, "ipv4": str, "ipv6": str, "detail": str}
    """
    config = load_duckdns_config()
    domain = config["domain"]
    token = config["token"]
    if not domain or not token:
        return {"success": False, "msg": "域名或Token未配置", "ipv4": "", "ipv6": "", "detail": ""}
    
    # 1. Try local global IPv6 detection
    local_ipv6 = ""
    ipv6_addrs = get_global_ipv6_addresses()
    if ipv6_addrs:
        local_ipv6 = ipv6_addrs[0]
    
    # 2. Get public IPs via external API
    own_session = False
    if session is None:
        session = aiohttp.ClientSession()
        own_session = True
    
    public_ip = await get_public_ip(session)
    
    # Prefer local IPv6 over API-detected IPv6 (local is more accurate for the machine)
    ipv6 = local_ipv6 or public_ip.get("ipv6", "")
    ipv4 = public_ip.get("ipv4", "")
    
    if not ipv4 and not ipv6:
        if own_session:
            await session.close()
        return {"success": False, "msg": "未检测到任何公网IP地址（IPv4/IPv6）", "ipv4": "", "ipv6": "", "detail": ""}
    
    # 3. Build DuckDNS update URL
    url = f"https://www.duckdns.org/update?domains={domain}&token={token}&verbose=true"
    if ipv4:
        url += f"&ip={ipv4}"
    if ipv6:
        url += f"&ipv6={ipv6}"
    
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            text = await resp.text()
    except Exception as e:
        if own_session:
            await session.close()
        return {"success": False, "msg": f"请求DuckDNS失败: {e}", "ipv4": ipv4, "ipv6": ipv6, "detail": ""}
    
    if own_session:
        await session.close()
    
    if text.startswith("OK"):
        parts = []
        if ipv4:
            parts.append(f"IPv4={ipv4}")
        if ipv6:
            parts.append(f"IPv6=[{ipv6}]")
        addr_info = ", ".join(parts)
        return {"success": True, "msg": f"{domain}.duckdns.org → {addr_info}", "ipv4": ipv4, "ipv6": ipv6, "detail": text.strip()}
    else:
        return {"success": False, "msg": f"DuckDNS返回: {text.strip()}", "ipv4": ipv4, "ipv6": ipv6, "detail": text.strip()}


async def duckdns_update_task():
    """Background task: update DuckDNS dynamic DNS record every 3 minutes.
    
    Detects the machine's IP addresses and updates DuckDNS so that the
    dynamic domain always points to this machine.
    Supports both IPv6 (local global or API-detected) and public IPv4 (via ipify.org).
    """
    while not shutdown_event.is_set():
        try:
            config = load_duckdns_config()
            if not config["domain"] or not config["token"]:
                logger.debug("DuckDNS: domain or token not configured, skipping update")
            else:
                async with aiohttp.ClientSession() as session:
                    result = await duckdns_do_update(session)
                if result["success"]:
                    logger.info(f"DuckDNS updated: {result['msg']} ({result['detail']})")
                else:
                    logger.warning(f"DuckDNS update failed: {result['msg']}")
        except Exception as e:
            logger.error(f"DuckDNS update error: {e}")

        # Wait 3 minutes, checking shutdown_event every 30 seconds
        for _ in range(6):
            if shutdown_event.is_set():
                return
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass


async def periodic_holdings_refresh():
    """Periodic holdings refresh task - refreshes overseas and domestic holdings every 24 hours.
    
    This ensures that fund holdings data stays current even though it changes infrequently.
    - Overseas (US stock) holdings are refreshed for all overseas-category funds
    - Domestic holdings are refreshed for all domestic/HK funds
    
    Runs on startup and then every 24 hours.
    """
    while not shutdown_event.is_set():
        try:
            logger.info("=== Starting periodic holdings refresh (24h cycle) ===")

            async with aiohttp.ClientSession() as session:
                # 1. Refresh overseas holdings for overseas AND hk funds
                overseas_funds = await get_funds_needing_holdings_refresh(hours=24, holdings_type="overseas")
                # Also include HK category funds that need overseas holdings refresh
                all_funds_list = await get_all_funds()
                hk_funds_for_overseas = [f for f in all_funds_list if f.get("category") == "hk"]
                # Merge: overseas funds from DB query + HK funds not already in the list
                overseas_codes = {f["fund_code"] for f in overseas_funds}
                for f in hk_funds_for_overseas:
                    if f["fund_code"] not in overseas_codes:
                        overseas_funds.append(f)
                        overseas_codes.add(f["fund_code"])

                if overseas_funds:
                    logger.info(f"Refreshing overseas/HK holdings for {len(overseas_funds)} funds...")
                    for fund in overseas_funds:
                        if shutdown_event.is_set():
                            break
                        try:
                            fund_code = fund["fund_code"]
                            api_holdings = await fetch_overseas_holdings(session, fund_code)
                            if api_holdings:
                                await save_overseas_holdings(fund_code, api_holdings)
                                await update_holdings_timestamp(fund_code, "overseas")
                                logger.info(f"Refreshed overseas holdings for {fund_code}: {len(api_holdings)} stocks")
                            else:
                                logger.warning(f"No overseas holdings data returned for {fund_code}")
                                # Still update timestamp to avoid retrying too frequently
                                await update_holdings_timestamp(fund_code, "overseas")
                        except Exception as e:
                            logger.error(f"Error refreshing overseas holdings for {fund.get('fund_code')}: {e}")
                        await asyncio.sleep(1)  # Rate limit
                else:
                    logger.info("All overseas holdings are up-to-date (within 24h)")

                # 2. Refresh domestic holdings for ALL funds (including overseas, which may have A-share/HK components)
                all_funds = await get_all_funds()
                if all_funds:
                    logger.info(f"Refreshing domestic holdings for {len(all_funds)} funds...")
                    for fund in all_funds:
                        if shutdown_event.is_set():
                            break
                        try:
                            fund_code = fund["fund_code"]
                            holdings = await fetch_fund_holdings(session, fund_code)
                            # Always save (even empty) to clear stale/overlapping data
                            await save_holdings(fund_code, holdings)
                            await update_holdings_timestamp(fund_code, "domestic")
                            logger.info(f"Refreshed domestic holdings for {fund_code}: {len(holdings)} stocks")
                        except Exception as e:
                            logger.error(f"Error refreshing domestic holdings for {fund.get('fund_code')}: {e}")
                        await asyncio.sleep(1)  # Rate limit
                else:
                    logger.info("No funds to refresh domestic holdings")

            logger.info("=== Periodic holdings refresh completed ===")

        except Exception as e:
            logger.error(f"Error in periodic holdings refresh: {e}")

        # Wait 24 hours before next refresh (but check shutdown_event every 60 seconds)
        for _ in range(1440):  # 1440 * 60s = 86400s = 24h
            if shutdown_event.is_set():
                return
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass



def _as_enabled(value) -> bool:
    """Return True for common truthy values from DB/API payloads."""
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _safe_float(value, default: float) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _parse_push_times(push_time: str) -> list[str]:
    """Parse comma/space separated HH:MM values into sorted unique canonical times."""
    if not push_time:
        return []
    result = []
    for part in re.split(r"[,，;；\s]+", str(push_time)):
        part = part.strip()
        if not part:
            continue
        match = re.fullmatch(r"(\d{1,2}):(\d{1,2})", part)
        if not match:
            continue
        hour = int(match.group(1))
        minute = int(match.group(2))
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            result.append(f"{hour:02d}:{minute:02d}")
    return sorted(set(result))


def _invalid_push_time_parts(push_time: str) -> list[str]:
    """Return non-empty push_time parts that are not valid HH:MM values."""
    if not push_time:
        return []
    invalid = []
    for part in re.split(r"[,，;；\s]+", str(push_time)):
        part = part.strip()
        if not part:
            continue
        match = re.fullmatch(r"(\d{1,2}):(\d{1,2})", part)
        if not match:
            invalid.append(part)
            continue
        hour = int(match.group(1))
        minute = int(match.group(2))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            invalid.append(part)
    return invalid


def _seconds_until_next_push_time(now: datetime, scheduled_times: list[str]) -> int:
    """Return a short sleep interval that wakes at the next configured minute.

    The task still caps sleep at 30 seconds so configuration changes are picked
    up quickly, but it never uses push_interval or any non-scheduled interval to
    send messages.
    """
    if not scheduled_times:
        return 30
    candidates = []
    for item in scheduled_times:
        try:
            hour, minute = [int(x) for x in item.split(":", 1)]
        except ValueError:
            continue
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= now:
            candidate += timedelta(days=1)
        candidates.append(candidate)
    if not candidates:
        return 30
    seconds = (min(candidates) - now).total_seconds()
    return max(1, min(30, int(seconds)))


def _build_wechat_push_key(day: str, scheduled_time: str) -> str:
    """Build the durable de-duplication key for a scheduled WeChat push slot."""
    return f"wechat_threshold_alert:{day}:{scheduled_time}"


def _wechat_push_grace_seconds() -> int:
    """Allow a short retry window so startup/data refresh cannot miss a push slot."""
    try:
        return max(60, int(os.environ.get("WECHAT_PUSH_GRACE_SECONDS", "90") or 90))
    except ValueError:
        return 90


def _match_due_push_slot(now: datetime, scheduled_times: list[str]) -> tuple[str, str]:
    """Return (scheduled_date, HH:MM) when now is inside a scheduled slot window."""
    grace_seconds = _wechat_push_grace_seconds()
    best: tuple[datetime, str] | None = None
    for item in scheduled_times:
        try:
            hour, minute = [int(x) for x in item.split(":", 1)]
        except ValueError:
            continue
        scheduled_at = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if scheduled_at > now:
            scheduled_at -= timedelta(days=1)
        elapsed = (now - scheduled_at).total_seconds()
        if 0 <= elapsed <= grace_seconds:
            if best is None or scheduled_at > best[0]:
                best = (scheduled_at, item)
    if best:
        return best[0].strftime("%Y-%m-%d"), best[1]
    return "", ""


def _normalize_wechat_config(config: dict) -> tuple[dict, str]:
    """Normalize and validate WeChat config from the API.

    Returns (normalized_config, error_message). error_message is empty on success.
    """
    normalized = dict(config or {})
    normalized["send_key"] = "\n".join(parse_send_keys(str(normalized.get("send_key", ""))))
    normalized["push_enabled"] = 1 if _as_enabled(normalized.get("push_enabled", 0)) else 0
    normalized["premium_alert_enabled"] = 1 if _as_enabled(normalized.get("premium_alert_enabled", 0)) else 0
    normalized["discount_alert_enabled"] = 1 if _as_enabled(normalized.get("discount_alert_enabled", 1)) else 0
    normalized["push_interval"] = max(1, _safe_int(normalized.get("push_interval", 60), 60))
    normalized["premium_upper"] = _safe_float(normalized.get("premium_upper", 3.0), 3.0)
    normalized["discount_lower"] = _safe_float(normalized.get("discount_lower", -5.0), -5.0)
    normalized["premium_lower"] = _safe_float(normalized.get("premium_lower", normalized["discount_lower"]), normalized["discount_lower"])
    normalized["min_turnover"] = max(0.0, _safe_float(normalized.get("min_turnover", 60), 60.0))

    raw_push_time = str(normalized.get("push_time", "")).strip()
    times = _parse_push_times(raw_push_time)
    invalid_times = _invalid_push_time_parts(raw_push_time)
    auto_enabled = bool(normalized["push_enabled"])
    if invalid_times or (raw_push_time and not times):
        invalid_desc = "、".join(invalid_times) if invalid_times else raw_push_time
        return normalized, f"推送时间格式无效：{invalid_desc}；请使用 HH:MM，例如 08:00,20:00"
    if auto_enabled and not times:
        return normalized, "启用微信定时告警推送时，必须设置至少一个有效推送时间（HH:MM）"
    if normalized["push_enabled"] and not (normalized["premium_alert_enabled"] or normalized["discount_alert_enabled"]):
        return normalized, "启用微信定时告警推送时，请至少开启溢价告警或折价告警"
    if normalized["premium_alert_enabled"] and normalized["premium_upper"] < 0:
        return normalized, "溢价阈值应大于或等于 0"
    if normalized["discount_alert_enabled"] and normalized["discount_lower"] > 0:
        return normalized, "折价阈值应小于或等于 0"
    normalized["push_time"] = ",".join(times)
    return normalized, ""


def _wechat_filter_values(config: dict) -> dict:
    """Extract typed filter values from WeChat config."""
    return {
        "premium_enabled": _as_enabled(config.get("premium_alert_enabled", 0)),
        "discount_enabled": _as_enabled(config.get("discount_alert_enabled", 1)),
        "premium_upper": _safe_float(config.get("premium_upper", 3.0), 3.0),
        "discount_lower": _safe_float(config.get("discount_lower", -5.0), -5.0),
        "min_turnover": max(0.0, _safe_float(config.get("min_turnover", 60), 60.0)),
    }


def _passes_turnover_filter(fund: dict, values: dict) -> bool:
    trade_amount = fund.get("trade_amount", 0) or 0
    return trade_amount >= values["min_turnover"] * 10000


def _passes_threshold_filter(fund: dict, values: dict) -> bool:
    premium = fund.get("premium_rate", 0) or 0
    threshold_enabled = values["premium_enabled"] or values["discount_enabled"]
    if not threshold_enabled:
        return True
    if values["premium_enabled"] and premium >= values["premium_upper"]:
        return True
    if values["discount_enabled"] and premium <= values["discount_lower"]:
        return True
    return False



def _collect_threshold_alerts(funds: list, config: dict) -> tuple[list, list]:
    """Return threshold alert rows and the threshold types that were triggered."""
    values = _wechat_filter_values(config)
    if not (values["premium_enabled"] or values["discount_enabled"]):
        return [], []
    alerts = []
    conditions = []
    for f in funds:
        if not _passes_turnover_filter(f, values):
            continue
        premium = f.get("premium_rate", 0) or 0
        if values["premium_enabled"] and premium >= values["premium_upper"]:
            alerts.append({**f, "threshold_type": "premium_upper"})
            if "premium_upper" not in conditions:
                conditions.append("premium_upper")
        if values["discount_enabled"] and premium <= values["discount_lower"]:
            alerts.append({**f, "threshold_type": "discount_lower"})
            if "discount_lower" not in conditions:
                conditions.append("discount_lower")
    return alerts, conditions


def _describe_wechat_filters(config: dict) -> str:
    """Build a compact Chinese description of the active push filters."""
    values = _wechat_filter_values(config)
    parts = [f"成交金额 ≥ {values['min_turnover']:g} 万元"]
    threshold_parts = []
    if values["premium_enabled"]:
        threshold_parts.append(f"溢价率 ≥ {values['premium_upper']:g}%")
    if values["discount_enabled"]:
        threshold_parts.append(f"折价率 ≤ {values['discount_lower']:g}%")
    if threshold_parts:
        parts.append(" 或 ".join(threshold_parts))
    else:
        parts.append("未启用折/溢价阈值")
    return "；".join(parts)


def _format_push_percent(value: float) -> str:
    """Format threshold values for the compact WeChat title."""
    return f"{value:g}%"


def _build_threshold_alert_title(values: dict, conditions: list) -> str:
    """Build the only scheduled WeChat push title for v1.7a.

    Example: LOF折溢价告警 溢价3% 成交60万
    """
    title_parts = ["LOF折溢价告警"]
    if "premium_upper" in conditions:
        title_parts.append(f"溢价{_format_push_percent(values['premium_upper'])}")
    if "discount_lower" in conditions:
        title_parts.append(f"折价{_format_push_percent(values['discount_lower'])}")
    if values["min_turnover"] > 0:
        title_parts.append(f"成交{values['min_turnover']:g}万")
    return " ".join(title_parts)


async def check_threshold_alerts(config: dict = None) -> dict:
    """Check funds for configured threshold breaches and send one compact alert push."""
    try:
        if config is None:
            config = await get_wechat_config()
        send_key = config.get("send_key", "")
        send_keys = parse_send_keys(send_key)
        values = _wechat_filter_values(config)
        if not send_keys or not (values["premium_enabled"] or values["discount_enabled"]):
            logger.info("WeChat alert skipped: send_keys=%s threshold_enabled=%s", len(send_keys), values["premium_enabled"] or values["discount_enabled"])
            return {"success": False, "sent": False, "msg": "告警未启用或 SendKey 未配置", "count": 0}

        funds = await get_all_realtime()
        if not funds:
            logger.warning("WeChat alert skipped: realtime fund data is empty")
            return {"success": False, "sent": False, "msg": "暂无基金数据", "count": 0}

        # Scheduled WeChat filtering uses AkShare release spot fields first.
        # This fast in-memory overlay avoids waiting for a full holdings refresh
        # cycle at the configured push minute.
        try:
            async with aiohttp.ClientSession() as session:
                akshare_snapshot = await get_akshare_fund_snapshot(session, max_age_seconds=60)
            logger.info(
                "WeChat alert source overlay: AkShare spot=%s estimation=%s fetched_at=%s; stored_realtime=%s funds",
                len(akshare_snapshot.get("spot") or {}),
                len(akshare_snapshot.get("estimation") or {}),
                akshare_snapshot.get("fetched_at", ""),
                len(funds),
            )
            funds = overlay_akshare_realtime_for_funds(funds, akshare_snapshot)
        except Exception as exc:
            logger.warning("AkShare overlay for WeChat filters failed, using stored realtime data: %s", exc)

        alerts, conditions = _collect_threshold_alerts(funds, config)
        if not alerts:
            logger.info(
                "Alert push skipped: no funds meet configured threshold filters (%s); checked=%s funds",
                _describe_wechat_filters(config),
                len(funds),
            )
            return {"success": True, "sent": False, "msg": "没有基金满足告警筛选条件", "count": 0}

        enabled_conditions = []
        if values["premium_enabled"]:
            enabled_conditions.append("premium_upper")
        if values["discount_enabled"]:
            enabled_conditions.append("discount_lower")

        # v1.7a: automatic WeChat push sends exactly one threshold-alert message
        # with a compact title such as "LOF折溢价告警 溢价3% 折价-5% 成交60万".
        title = _build_threshold_alert_title(values, enabled_conditions)
        content = build_threshold_alert_message(
            alerts,
            values["premium_upper"],
            values["discount_lower"],
            values["min_turnover"],
            enabled_conditions,
        )
        result = await send_wechat_message(send_key, title, content)
        if result["success"]:
            logger.info(
                "Alert pushed: %s funds (%s)",
                len(alerts),
                _describe_wechat_filters(config),
            )
            return {"success": True, "sent": True, "msg": "告警已推送", "count": len(alerts)}
        logger.warning(f"Alert push failed: {result['msg']}")
        return {"success": False, "sent": False, "msg": result["msg"], "count": len(alerts)}
    except Exception as e:
        logger.error(f"Error checking threshold alerts: {e}")
        return {"success": False, "sent": False, "msg": str(e), "count": 0}


async def periodic_wechat_push():
    """Automatic WeChat alert task for v1.7a.

    Strict rules:
    1. Only the configured push_time values are allowed to trigger a push.
    2. Each configured HH:MM slot can be executed at most once per CST date.
    3. The once-only guarantee is persisted in SQLite, so restarts, duplicate
       event-loop tasks, or duplicate server processes cannot push twice in the
       same scheduled minute.
    4. Data refresh remains independent and unchanged.
    """

    last_config_log = ""
    while not shutdown_event.is_set():
        sleep_seconds = 30
        try:
            config = await get_wechat_config()
            send_key = config.get("send_key", "")
            send_keys = parse_send_keys(send_key)
            scheduled_alert_enabled = _as_enabled(config.get("push_enabled", 0))
            alert_enabled = _as_enabled(config.get("premium_alert_enabled", 0)) or _as_enabled(config.get("discount_alert_enabled", 1))
            scheduled_times = _parse_push_times(config.get("push_time", ""))
            config_log = (
                f"enabled={scheduled_alert_enabled and alert_enabled and bool(send_keys)}; "
                f"recipients={len(send_keys)}; times={','.join(scheduled_times) or 'none'}; "
                f"filters={_describe_wechat_filters(config)}"
            )
            if config_log != last_config_log:
                logger.info("WeChat scheduler config: %s", config_log)
                last_config_log = config_log

            if not send_keys or not scheduled_alert_enabled or not alert_enabled:
                sleep_seconds = 30
            elif not scheduled_times:
                # Strict scheduling: no valid HH:MM means no automatic push.
                logger.warning("WeChat alert push skipped: no valid push_time configured")
                sleep_seconds = 30
            else:
                now = datetime.now(CST)
                scheduled_date, matched_time = _match_due_push_slot(now, scheduled_times)
                if matched_time:
                    push_key = _build_wechat_push_key(scheduled_date, matched_time)
                    logger.info(
                        "WeChat alert due: scheduled=%s %s, now=%s, recipients=%s, filters=%s",
                        scheduled_date,
                        matched_time,
                        now.strftime("%Y-%m-%d %H:%M:%S"),
                        len(send_keys),
                        _describe_wechat_filters(config),
                    )
                    if update_lock.locked():
                        logger.info(
                            "WeChat alert due but fund data update is still running; "
                            "retrying before claiming slot %s",
                            push_key,
                        )
                        sleep_seconds = 5
                    else:
                        claimed = await claim_wechat_push_slot(push_key, scheduled_date, matched_time)
                        if claimed:
                            result = await check_threshold_alerts(config=config)
                            if result.get("sent"):
                                status = "sent"
                            elif result.get("success"):
                                status = "skipped"
                            else:
                                status = "failed"
                            await mark_wechat_push_slot(
                                push_key,
                                status,
                                result.get("count", 0),
                                result.get("msg", ""),
                            )
                            logger.info(
                                "WeChat alert slot finished: %s status=%s count=%s msg=%s",
                                push_key,
                                status,
                                result.get("count", 0),
                                result.get("msg", ""),
                            )
                        else:
                            logger.info("WeChat alert push skipped: %s already executed", push_key)
                        sleep_seconds = _seconds_until_next_push_time(now, scheduled_times)

                else:
                    sleep_seconds = _seconds_until_next_push_time(now, scheduled_times)

        except Exception as e:
            logger.error(f"Error in periodic WeChat alert push: {e}")
            sleep_seconds = 30

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_seconds)
        except asyncio.TimeoutError:
            pass




# ============ API Routes ============

async def api_get_funds(request):
    try:
        category = request.query.get("category", "all")
        sort_by = request.query.get("sort", "")
        sort_order = request.query.get("order", "asc")
        realtime_data = await get_all_realtime(category=category, sort_by=sort_by, sort_order=sort_order)
        for item in realtime_data:
            item["nav"] = round(item.get("nav", 0), 4)
            item["estimated_nav"] = round(item.get("estimated_nav", 0), 4)
            item["trade_price"] = round(item.get("trade_price", 0), 3)
            item["premium_rate"] = round(item.get("premium_rate", 0), 2)
            item["estimated_change_rate"] = round(item.get("estimated_change_rate", 0), 2)
            item["trade_price_change"] = round(item.get("trade_price_change", 0), 3)
            item["trade_amount"] = round(item.get("trade_amount", 0), 0)
            item["cn_ratio"] = round(item.get("cn_ratio", 0), 2)
            item["us_ratio"] = round(item.get("us_ratio", 0), 2)
            item["valuation_confidence"] = round(item.get("valuation_confidence", 0), 2)
        return web.json_response({"code": 0, "data": realtime_data})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_get_fund_detail(request):
    fund_code = request.match_info.get("code", "")
    try:
        realtime = await get_realtime(fund_code)
        if not realtime:
            return web.json_response({"code": -1, "msg": "Fund not found"})
        holdings = await get_holdings(fund_code)
        overseas_holdings = await get_overseas_holdings(fund_code)
        realtime["holdings"] = holdings
        realtime["overseas_holdings"] = overseas_holdings
        realtime["nav"] = round(realtime.get("nav", 0), 4)
        realtime["estimated_nav"] = round(realtime.get("estimated_nav", 0), 4)
        realtime["trade_price"] = round(realtime.get("trade_price", 0), 3)
        realtime["premium_rate"] = round(realtime.get("premium_rate", 0), 2)
        realtime["valuation_confidence"] = round(realtime.get("valuation_confidence", 0), 2)
        return web.json_response({"code": 0, "data": realtime})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_add_fund(request):
    try:
        data = await request.json()
        fund_code = data.get("fund_code", "").strip()
        market = data.get("market", "sz")
        algo_type = data.get("algo_type", "holdings")
        category = data.get("category", "domestic")
        industry_index_code = data.get("industry_index_code", "")
        us_index_code = data.get("us_index_code", "")

        if not fund_code:
            return web.json_response({"code": -1, "msg": "基金代码不能为空"})

        async with aiohttp.ClientSession() as session:
            akshare_snapshot = await get_akshare_fund_snapshot(session, max_age_seconds=60)
            info = {"fund_code": fund_code, "fund_name": ""}
            apply_akshare_fund_data_to_result(info, fund_code, akshare_snapshot)
            if not info.get("fund_name"):
                info = await fetch_fund_info(session, fund_code)

        fund_name = info.get("fund_name", data.get("fund_name", ""))
        if not fund_name:
            return web.json_response({"code": -1, "msg": f"无法获取基金{fund_code}的信息，请检查代码是否正确"})

        await add_fund(fund_code, fund_name, market, algo_type, category, industry_index_code, us_index_code)
        # Trigger update for the newly added fund specifically
        asyncio.create_task(update_single_fund(fund_code, market))

        return web.json_response({"code": 0, "msg": f"成功添加基金 {fund_code} - {fund_name}，数据更新中..."})
    except Exception as e:
        logger.error(f"Error adding fund: {e}")
        return web.json_response({"code": -1, "msg": str(e)})


async def api_remove_fund(request):
    fund_code = request.match_info.get("code", "")
    try:
        await remove_fund(fund_code)
        return web.json_response({"code": 0, "msg": f"已删除基金 {fund_code}"})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_update_algo(request):
    fund_code = request.match_info.get("code", "")
    try:
        data = await request.json()
        algo_type = data.get("algo_type", "holdings")
        industry_index_code = data.get("industry_index_code", "")
        us_index_code = data.get("us_index_code", "")
        category = data.get("category", "")
        await update_fund_algo(fund_code, algo_type, industry_index_code, us_index_code, category)
        # Trigger update for this fund with new algo
        fund = await get_fund(fund_code)
        if fund:
            asyncio.create_task(update_single_fund(fund_code, fund.get('market', 'sz')))
        return web.json_response({"code": 0, "msg": "算法配置已更新"})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_get_algos(request):
    try:
        algos = await get_algo_configs()
        return web.json_response({"code": 0, "data": algos})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_manual_update(request):
    try:
        asyncio.create_task(update_all_funds())
        return web.json_response({"code": 0, "msg": "数据更新已触发"})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_get_holdings(request):
    fund_code = request.match_info.get("code", "")
    try:
        holdings = await get_holdings(fund_code)
        return web.json_response({"code": 0, "data": holdings})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_get_overseas_holdings(request):
    fund_code = request.match_info.get("code", "")
    try:
        holdings = await get_overseas_holdings(fund_code)
        return web.json_response({"code": 0, "data": holdings})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_save_overseas_holdings(request):
    fund_code = request.match_info.get("code", "")
    try:
        data = await request.json()
        holdings = data.get("holdings", [])
        if not holdings:
            return web.json_response({"code": -1, "msg": "持仓数据不能为空"})
        await save_overseas_holdings(fund_code, holdings)
        # Trigger update for this fund
        fund = await get_fund(fund_code)
        if fund:
            asyncio.create_task(update_single_fund(fund_code, fund.get('market', 'sz')))
        return web.json_response({"code": 0, "msg": f"已保存 {len(holdings)} 条境外持仓数据"})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_fetch_overseas_holdings(request):
    """Fetch overseas holdings from East Money API and save to DB."""
    fund_code = request.match_info.get("code", "")
    try:
        async with aiohttp.ClientSession() as session:
            holdings = await fetch_overseas_holdings(session, fund_code)
        if holdings:
            await save_overseas_holdings(fund_code, holdings)
            await update_holdings_timestamp(fund_code, "overseas")
            # Trigger update for this fund
            fund = await get_fund(fund_code)
            if fund:
                asyncio.create_task(update_single_fund(fund_code, fund.get('market', 'sz')))
            return web.json_response({"code": 0, "data": holdings, "msg": f"已获取 {len(holdings)} 条境外持仓"})
        else:
            return web.json_response({"code": -1, "msg": "未获取到境外持仓数据，请尝试手动输入"})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_batch_import(request):
    """Batch import funds from Excel data. Body: {"funds": [{"fund_code":..., "fund_name":..., "market":..., "algo_type":..., "category":..., ...}]}"""
    try:
        data = await request.json()
        funds = data.get("funds", [])
        if not funds:
            return web.json_response({"code": -1, "msg": "基金列表不能为空"})
        await batch_add_funds(funds)
        # Trigger full update after batch import
        asyncio.create_task(update_all_funds())
        return web.json_response({"code": 0, "msg": f"已导入 {len(funds)} 只基金，数据更新中..."})
    except Exception as e:
        logger.error(f"Error batch importing: {e}")
        return web.json_response({"code": -1, "msg": str(e)})


async def api_refresh_holdings(request):
    """Manually trigger a holdings refresh for all funds (both domestic and overseas)."""
    try:
        asyncio.create_task(_do_holdings_refresh())
        return web.json_response({"code": 0, "msg": "持仓数据刷新已触发，预计几分钟内完成"})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def _do_holdings_refresh():
    """Internal function to refresh all holdings."""
    async with aiohttp.ClientSession() as session:
        # Refresh overseas holdings (hours=0 means all overseas funds)
        overseas_funds = await get_funds_needing_holdings_refresh(hours=0, holdings_type="overseas")
        # Also include HK category funds that need overseas holdings refresh
        all_funds_list = await get_all_funds()
        hk_funds_for_overseas = [f for f in all_funds_list if f.get("category") == "hk"]
        overseas_codes = {f["fund_code"] for f in overseas_funds}
        for f in hk_funds_for_overseas:
            if f["fund_code"] not in overseas_codes:
                overseas_funds.append(f)
                overseas_codes.add(f["fund_code"])

        for fund in overseas_funds:
            try:
                fund_code = fund["fund_code"]
                api_holdings = await fetch_overseas_holdings(session, fund_code)
                if api_holdings:
                    await save_overseas_holdings(fund_code, api_holdings)
                    await update_holdings_timestamp(fund_code, "overseas")
                    logger.info(f"Manual refresh: overseas holdings for {fund_code}: {len(api_holdings)} stocks")
                else:
                    await update_holdings_timestamp(fund_code, "overseas")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error in manual holdings refresh for {fund.get('fund_code')}: {e}")

        # Refresh domestic holdings for ALL funds (including overseas, which may have A-share/HK components)
        all_funds = await get_all_funds()

        for fund in all_funds:
            try:
                fund_code = fund["fund_code"]
                holdings = await fetch_fund_holdings(session, fund_code)
                # Always save (even empty) to clear stale/overlapping data
                await save_holdings(fund_code, holdings)
                await update_holdings_timestamp(fund_code, "domestic")
                logger.info(f"Manual refresh: domestic holdings for {fund_code}: {len(holdings)} stocks")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error in manual holdings refresh for {fund.get('fund_code')}: {e}")

    # After refreshing holdings, also trigger a full fund data update
    await update_all_funds()


async def api_trading_status(request):
    cn_trading = is_trading_time()
    us_trading = is_us_trading_time()
    return web.json_response({
        "code": 0,
        "data": {
            "is_trading": cn_trading,
            "is_us_trading": us_trading,
            "is_any_trading": cn_trading or us_trading,
            "refresh_interval": 300 if (cn_trading or us_trading) else 1800,
            "current_time": datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S"),
        }
    })


async def api_get_duckdns_config(request):
    """Get current DuckDNS configuration."""
    config = load_duckdns_config()
    ipv6_addrs = get_global_ipv6_addresses()
    # Also try to get public IPv4
    public_ipv4 = ""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.ipify.org", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                text = await resp.text()
                if text and re.match(r'^\d+\.\d+\.\d+\.\d+$', text.strip()):
                    public_ipv4 = text.strip()
    except Exception:
        pass
    return web.json_response({
        "code": 0,
        "data": {
            "domain": config["domain"],
            "token": config["token"],
            "full_domain": f"{config['domain']}.duckdns.org" if config["domain"] else "",
            "current_ipv6": ipv6_addrs[0] if ipv6_addrs else "",
            "current_ipv4": public_ipv4,
        }
    })


async def api_save_duckdns_config(request):
    """Save DuckDNS configuration (domain and token)."""
    try:
        data = await request.json()
        domain = data.get("domain", "").strip()
        token = data.get("token", "").strip()
        if not domain:
            return web.json_response({"code": -1, "msg": "域名不能为空"})
        if not token:
            return web.json_response({"code": -1, "msg": "Token不能为空"})
        save_duckdns_config(domain, token)
        logger.info(f"DuckDNS config updated: {domain}.duckdns.org")
        return web.json_response({"code": 0, "msg": f"动态域名配置已保存：{domain}.duckdns.org"})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_update_duckdns_now(request):
    """Trigger an immediate DuckDNS update and return the result."""
    try:
        result = await duckdns_do_update()
        if result["success"]:
            return web.json_response({
                "code": 0,
                "msg": f"更新成功：{result['msg']}",
                "data": {"ipv4": result["ipv4"], "ipv6": result["ipv6"], "detail": result["detail"]}
            })
        else:
            return web.json_response({
                "code": -1,
                "msg": f"更新失败：{result['msg']}",
                "data": {"ipv4": result["ipv4"], "ipv6": result["ipv6"], "detail": result.get("detail", "")}
            })
    except Exception as e:
        logger.error(f"DuckDNS manual update error: {e}")
        return web.json_response({"code": -1, "msg": f"更新出错：{str(e)}"})


async def api_get_wechat_config(request):
    """Get WeChat push configuration."""
    try:
        config = await get_wechat_config()
        send_key = config.get("send_key", "")
        config["masked_key"] = mask_send_keys(send_key)
        config["send_key_count"] = len(parse_send_keys(send_key))
        return web.json_response({"code": 0, "data": config})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_save_wechat_config(request):
    """Save WeChat push configuration after validation/normalization."""
    try:
        data = await request.json()
        normalized, error = _normalize_wechat_config(data)
        if error:
            return web.json_response({"code": -1, "msg": error})
        await save_wechat_config(normalized)
        logger.info("WeChat push config updated: push_time=%s", normalized.get("push_time", ""))
        return web.json_response({"code": 0, "msg": "微信推送配置已保存", "data": {"push_time": normalized.get("push_time", "")}})
    except Exception as e:
        return web.json_response({"code": -1, "msg": str(e)})


async def api_test_wechat_push(request):
    """v1.7a keeps this route as a no-op so no extra WeChat messages are sent."""
    return web.json_response({"code": -1, "msg": "v1.7a 已取消测试推送；微信只在设置时间发送 1 条 LOF折溢价告警"})


async def api_send_summary_now(request):
    """v1.7a removes summary pushes; keep this route as a safe no-op for compatibility."""
    return web.json_response({"code": -1, "msg": "v1.7a 已取消汇总推送；自动微信推送只在设置时间发送 1 条 LOF折溢价告警"})


# ============ Static File Serving ============

async def index(request):
    return web.FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))

async def admin(request):
    return web.FileResponse(os.path.join(os.path.dirname(__file__), "static", "admin.html"))


# ============ App Setup ============

async def on_startup(app):
    await init_db()
    await seed_default_funds()
    app['update_task'] = asyncio.create_task(periodic_update())
    if _running_in_github_actions() and not _env_enabled("ACTIONS_REFRESH_HOLDINGS"):
        logger.info(
            "GitHub Actions: skipped automatic full holdings refresh; "
            "AkShare batch snapshots are used for intraday quote/alert data. "
            "Set ACTIONS_REFRESH_HOLDINGS=1 to restore the full 24h crawl."
        )
    else:
        app['holdings_refresh_task'] = asyncio.create_task(periodic_holdings_refresh())
    app['duckdns_task'] = asyncio.create_task(duckdns_update_task())
    app['wechat_push_task'] = asyncio.create_task(periodic_wechat_push())
    asyncio.create_task(update_all_funds())


async def on_cleanup(app):
    shutdown_event.set()
    for task_key in ['update_task', 'holdings_refresh_task', 'duckdns_task', 'wechat_push_task']:
        if task_key in app:
            app[task_key].cancel()
            try:
                await app[task_key]
            except asyncio.CancelledError:
                pass


def create_app():
    app = web.Application()

    app.router.add_get("/api/funds", api_get_funds)
    app.router.add_get("/api/funds/{code}", api_get_fund_detail)
    app.router.add_post("/api/funds", api_add_fund)
    app.router.add_delete("/api/funds/{code}", api_remove_fund)
    app.router.add_put("/api/funds/{code}/algo", api_update_algo)
    app.router.add_get("/api/algos", api_get_algos)
    app.router.add_get("/api/funds/{code}/holdings", api_get_holdings)
    app.router.add_get("/api/funds/{code}/overseas-holdings", api_get_overseas_holdings)
    app.router.add_post("/api/funds/{code}/overseas-holdings", api_save_overseas_holdings)
    app.router.add_post("/api/funds/{code}/overseas-holdings/fetch", api_fetch_overseas_holdings)
    app.router.add_post("/api/update", api_manual_update)
    app.router.add_post("/api/refresh-holdings", api_refresh_holdings)
    app.router.add_get("/api/trading-status", api_trading_status)
    app.router.add_post("/api/funds/batch-import", api_batch_import)
    app.router.add_get("/api/duckdns", api_get_duckdns_config)
    app.router.add_post("/api/duckdns", api_save_duckdns_config)
    app.router.add_post("/api/duckdns/update", api_update_duckdns_now)
    app.router.add_get("/api/wechat", api_get_wechat_config)
    app.router.add_post("/api/wechat", api_save_wechat_config)
    app.router.add_post("/api/wechat/test", api_test_wechat_push)
    app.router.add_post("/api/wechat/summary", api_send_summary_now)

    app.router.add_get("/", index)
    app.router.add_get("/admin", admin)

    app.router.add_static("/static", os.path.join(os.path.dirname(__file__), "static"))

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    return app


async def _open_browser(app):
    """Open the browser after the server starts."""
    port = int(os.environ.get("FUND_PORT", 8080))
    url = f"http://localhost:{port}"
    logger.info(f"Opening browser: {url}")
    webbrowser.open(url)


async def _start_server(app, port):
    """Start the server on multiple interfaces:
    - [::] (all IPv6, with IPV6_V6ONLY to avoid IPv4 mapping)
    - 127.0.0.1 (IPv4 localhost)
    - Any detected LAN IPv4 addresses (192.168.x.x, 10.x.x.x, 172.16-31.x.x)
    
    Does NOT bind to public IPv4 addresses.
    
    Uses SockSite (pre-bound sockets) for all interfaces to avoid version-dependent
    TCPSite keyword arguments (e.g., 'family' not supported in older aiohttp).
    """
    runner = web.AppRunner(app)
    await runner.setup()

    sites = []

    # 1. Listen on all IPv6 interfaces (dual-stack disabled, IPv6 only)
    # Create the socket manually to set IPV6_V6ONLY before binding
    try:
        sock_v6 = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock_v6.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        sock_v6.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_v6.bind(("::", port))
        sock_v6.listen(128)
        sock_v6.setblocking(False)  # Required for asyncio
        site_v6 = web.SockSite(runner, sock_v6)
        sites.append(("IPv6 [::]", site_v6))
    except Exception as e:
        logger.warning(f"Failed to create IPv6 site: {e}")

    # 2. Listen on IPv4 localhost
    try:
        sock_lo = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_lo.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_lo.bind(("127.0.0.1", port))
        sock_lo.listen(128)
        sock_lo.setblocking(False)
        site_lo = web.SockSite(runner, sock_lo)
        sites.append(("IPv4 127.0.0.1", site_lo))
    except Exception as e:
        logger.warning(f"Failed to create localhost site: {e}")

    # 3. Listen on any LAN IPv4 addresses
    lan_addrs = get_lan_ipv4_addresses()
    for addr in lan_addrs:
        try:
            sock_lan = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_lan.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_lan.bind((addr, port))
            sock_lan.listen(128)
            sock_lan.setblocking(False)
            site_lan = web.SockSite(runner, sock_lan)
            sites.append((f"IPv4 LAN {addr}", site_lan))
        except Exception as e:
            logger.warning(f"Failed to create LAN site for {addr}: {e}")

    # Start all sites
    started = []
    for name, site in sites:
        try:
            await site.start()
            started.append(name)
            logger.info(f"Server listening on {name}:{port}")
        except Exception as e:
            logger.error(f"Failed to start site {name}: {e}")

    if not started:
        logger.error("No server sites started! Trying fallback on 0.0.0.0...")
        try:
            sock_fb = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_fb.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_fb.bind(("0.0.0.0", port))
            sock_fb.listen(128)
            sock_fb.setblocking(False)
            fallback = web.SockSite(runner, sock_fb)
            await fallback.start()
            started.append("fallback 0.0.0.0")
            logger.info(f"Server listening on fallback 0.0.0.0:{port}")
        except Exception as e2:
            logger.error(f"Fallback also failed: {e2}")

    logger.info(f"Server started on interfaces: {', '.join(started)}")


if __name__ == "__main__":
    app = create_app()
    port = int(os.environ.get("FUND_PORT", 8080))
    app.on_startup.append(_open_browser)

    async def main():
        await _start_server(app, port)
        # Wait forever (or until signal)
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass

    loop = asyncio.new_event_loop()

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(on_cleanup(app))
        loop.close()
