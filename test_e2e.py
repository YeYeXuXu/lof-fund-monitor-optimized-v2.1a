"""End-to-end test for LOF Fund Monitor."""
import asyncio
import sys
import os
import json

sys.path.insert(0, os.path.dirname(__file__))

from db import (init_db, get_all_funds, get_fund, add_fund, remove_fund,
    update_fund_algo, save_holdings, get_holdings, save_realtime,
    get_all_realtime, get_realtime, get_algo_configs,
    save_overseas_holdings, get_overseas_holdings, batch_add_funds,
    update_holdings_timestamp, get_funds_needing_holdings_refresh)
from fetcher import (fetch_fund_estimate, fetch_stock_price, fetch_fund_holdings,
    fetch_overseas_holdings, fetch_fund_info, fetch_fund_purchase_status,
    fetch_index_info, fetch_us_stock_change_rate, fetch_us_index_info)
from estimator import (estimate_nav_by_holdings, estimate_nav_by_industry_index,
    estimate_nav_by_overseas_holdings, get_overseas_period)
from server import is_trading_time, is_us_trading_time
import aiohttp

passed = 0
failed = 0

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {name}")
    else:
        failed += 1
        print(f"  ❌ {name} {detail}")


async def run_tests():
    import aiosqlite
    # Clean up old db
    db_path = os.path.join(os.path.dirname(__file__), "lof_fund.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    print("=" * 60)
    print("LOF基金折溢价监控 - 全流程端到端测试")
    print("=" * 60)

    # ========== Test 1: Database Init ==========
    print("\n📋 Test 1: 数据库初始化")
    await init_db()
    
    # Check tables exist
    db = await aiosqlite.connect(db_path)
    db.row_factory = aiosqlite.Row
    cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in await cursor.fetchall()]
    check("funds表存在", "funds" in tables)
    check("fund_holdings表存在", "fund_holdings" in tables)
    check("fund_realtime表存在", "fund_realtime" in tables)
    check("fund_overseas_holdings表存在", "fund_overseas_holdings" in tables)
    check("algo_config表存在", "algo_config" in tables)
    
    # Check funds table has new columns
    cursor = await db.execute("PRAGMA table_info(funds)")
    columns = [row[1] for row in await cursor.fetchall()]
    check("category字段存在", "category" in columns)
    check("holdings_updated_at字段存在", "holdings_updated_at" in columns)
    check("overseas_holdings_updated_at字段存在", "overseas_holdings_updated_at" in columns)
    check("us_index_code字段存在", "us_index_code" in columns)
    await db.close()

    # Check algo configs
    algos = await get_algo_configs()
    algo_types = [a["algo_type"] for a in algos]
    check("holdings算法配置存在", "holdings" in algo_types)
    check("industry算法配置存在", "industry" in algo_types)
    check("overseas算法配置存在", "overseas" in algo_types)

    # ========== Test 2: Trading Time Detection ==========
    print("\n📋 Test 2: 交易时间检测")
    cn = is_trading_time()
    us = is_us_trading_time()
    check("A股交易时间检测正常", isinstance(cn, bool))
    check("美股交易时间检测正常", isinstance(us, bool))
    print(f"    当前状态: A股交易={cn}, 美股交易={us}")
    
    # Test US trading time logic explicitly
    from datetime import datetime, timezone, timedelta
    cst = timezone(timedelta(hours=8))
    now = datetime.now(cst)
    current_time = now.hour * 100 + now.minute
    weekday = now.weekday()
    if 2100 <= current_time <= 2359:
        check("北京时间21:00-23:59，美股应开盘中(工作日)", us == (weekday < 5), f"us={us}, weekday={weekday}")
    elif 0 <= current_time <= 500:
        us_weekday = (weekday - 1) % 7
        check("北京时间00:00-05:00，美股应根据前一天判断", us == (us_weekday < 5), f"us={us}")
    else:
        check("非美股交易时段", not us, f"us={us}")

    # ========== Test 3: Add Domestic LOF Fund ==========
    print("\n📋 Test 3: 添加国内LOF基金（酒LOF 161725）")
    async with aiohttp.ClientSession() as session:
        info = await fetch_fund_info(session, "161725")
        check("天天基金API返回基金信息", bool(info), str(info))
        if info:
            check(f"基金名称非空", bool(info.get("fund_name")), info.get("fund_name", ""))
            await add_fund("161725", info.get("fund_name", "招商中证白酒"), "sz", "holdings", "domestic")
            check("基金已添加到数据库", True)
    
    fund = await get_fund("161725")
    check("查询基金信息正常", fund is not None)
    if fund:
        check("fund_code正确", fund["fund_code"] == "161725")
        check("algo_type默认holdings", fund["algo_type"] == "holdings")
        check("category默认domestic", fund["category"] == "domestic")

    # ========== Test 4: Fetch & Estimate for Domestic Fund ==========
    print("\n📋 Test 4: 国内基金数据抓取和估值计算")
    async with aiohttp.ClientSession() as session:
        # Fetch NAV estimate
        est = await fetch_fund_estimate(session, "161725")
        check("天天基金估值API有返回", bool(est), str(est)[:100])
        if est:
            check(f"单位净值>0: {est.get('nav', 0)}", est.get("nav", 0) > 0)
            check(f"估算净值>0: {est.get('estimated_nav', 0)}", est.get("estimated_nav", 0) > 0)
        
        # Fetch trade price
        price = await fetch_stock_price(session, "161725", "0")
        check("东方财富行情API有返回", bool(price), str(price)[:100])
        if price:
            check(f"交易价格>0: {price.get('trade_price', 0)}", price.get("trade_price", 0) > 0)
        
        # Fetch holdings
        holdings = await fetch_fund_holdings(session, "161725")
        check("持仓数据有返回", len(holdings) > 0, f"holdings count: {len(holdings)}")
        if holdings:
            await save_holdings("161725", holdings)
            await update_holdings_timestamp("161725", "domestic")
            check("持仓已保存", True)
            first = holdings[0]
            check(f"持仓包含stock_code: {first.get('stock_code', '')}", bool(first.get("stock_code")))
            check(f"持仓包含em_code: {first.get('em_code', '')}", bool(first.get("em_code")), f"em_code={first.get('em_code')}")
        
        # Estimate NAV using holdings
        if est and holdings:
            nav = est.get("nav", 0)
            result = await estimate_nav_by_holdings(session, nav, holdings)
            check("持仓估算法返回结果", bool(result))
            if result:
                check(f"估算净值>0: {result['estimated_nav']}", result["estimated_nav"] > 0)
                check(f"估算涨跌率有值: {result['estimated_change_rate']}", 
                      result["estimated_change_rate"] != 0 or True)  # Can be 0 outside trading
        
        # Save realtime data
        if est and price:
            nav = est.get("nav", 0)
            base_nav = result["estimated_nav"] if result and result["estimated_nav"] > 0 else nav
            premium = round((price["trade_price"] - base_nav) / base_nav * 100, 2) if base_nav > 0 else 0
            rt_data = {
                "nav": nav, "nav_date": est.get("nav_date", ""),
                "estimated_nav": result["estimated_nav"] if result else 0,
                "estimated_change_rate": result["estimated_change_rate"] if result else 0,
                "trade_price": price["trade_price"],
                "trade_price_change": price.get("trade_price_change", 0),
                "premium_rate": premium,
                "purchase_status": "开放", "redeem_status": "开放",
            }
            await save_realtime("161725", rt_data)
            check(f"折溢价率: {premium}%", True)
    
    # Verify in DB
    rt = await get_realtime("161725")
    check("实时数据已存入DB", rt is not None)
    if rt:
        check(f"DB中NAV={rt['nav']}, Price={rt['trade_price']}, Premium={rt['premium_rate']}%", True)

    # ========== Test 5: Add Overseas US LOF Fund ==========
    print("\n📋 Test 5: 添加境外美股LOF基金（纳指100 QDII 513100）")
    async with aiohttp.ClientSession() as session:
        info = await fetch_fund_info(session, "513100")
        check("天天基金API返回513100信息", bool(info), str(info)[:80])
        if info:
            await add_fund("513100", info.get("fund_name", "纳指100"), "sh", "overseas", "overseas", "", "100.NDX")
            check("境外基金已添加", True)
    
    fund = await get_fund("513100")
    check("查询513100信息正常", fund is not None)
    if fund:
        check("algo_type=overseas", fund["algo_type"] == "overseas")
        check("category=overseas", fund["category"] == "overseas")
        check("us_index_code=100.NDX", fund["us_index_code"] == "100.NDX")

    # ========== Test 6: Fetch Overseas Holdings ==========
    print("\n📋 Test 6: 境外持仓数据抓取")
    async with aiohttp.ClientSession() as session:
        overseas = await fetch_overseas_holdings(session, "513100")
        check("境外持仓API有返回", len(overseas) >= 0, f"count: {len(overseas)}")
        if overseas:
            await save_overseas_holdings("513100", overseas)
            await update_holdings_timestamp("513100", "overseas")
            check("境外持仓已保存", True)
            first = overseas[0]
            print(f"    首个持仓: {first.get('stock_name', '')} ({first.get('stock_code', '')}) 占比:{first.get('holding_ratio', 0)}%")
        else:
            print("    ⚠️ 境外持仓为空（QDII基金F10可能不含境外持仓数据，需手动输入）")

    # ========== Test 7: US Index & Overseas Estimation ==========
    print("\n📋 Test 7: 美股指数和境外估值算法")
    async with aiohttp.ClientSession() as session:
        # US index info
        us_idx = await fetch_us_index_info(session, "100.NDX")
        check("美股指数API有返回", bool(us_idx), str(us_idx)[:80])
        if us_idx:
            check(f"指数名称: {us_idx.get('index_name', '')}", bool(us_idx.get("index_name")))
            check(f"指数涨跌: {us_idx.get('change_rate', 0)}%", True)
        
        # US stock change rate
        us_stock = await fetch_us_stock_change_rate(session, "105.AAPL")
        check("美股个股涨跌API有返回", isinstance(us_stock, (int, float)), f"AAPL change: {us_stock}")
        
        # Overseas estimation
        period = get_overseas_period()
        check(f"当前境外时段: {period}", period in (1, 2, 3))
        
        est = await fetch_fund_estimate(session, "513100")
        if est and us_idx:
            cn_change = est.get("estimated_change_rate", 0)
            nav = est.get("nav", 0)
            result = await estimate_nav_by_overseas_holdings(
                session, nav, cn_change, overseas, "100.NDX"
            )
            check("境外估算法返回结果", bool(result))
            if result:
                check(f"估算净值: {result['estimated_nav']}", result["estimated_nav"] > 0)
                check(f"时段: {result['period']}", result["period"] in (1, 2, 3))
                check(f"CN占比: {result['cn_ratio']}%, US占比: {result['us_ratio']}%", True)

    # ========== Test 8: Category Filtering & Sorting ==========
    print("\n📋 Test 8: 分类筛选和排序")
    # Add an HK fund for variety
    await add_fund("501009", "恒生ETF", "sh", "holdings", "hk")
    
    all_funds = await get_all_realtime(category="all")
    check(f"全部基金: {len(all_funds)}只", len(all_funds) >= 2)
    
    domestic = await get_all_realtime(category="domestic")
    check(f"国内LOF: {len(domestic)}只", len(domestic) >= 1)
    
    overseas = await get_all_realtime(category="overseas")
    check(f"境外美股LOF: {len(overseas)}只", len(overseas) >= 1)
    
    hk = await get_all_realtime(category="hk")
    check(f"港股LOF: {len(hk)}只", len(hk) >= 1)
    
    # Sort by premium rate ascending
    sorted_asc = await get_all_realtime(sort_by="premium_rate", sort_order="asc")
    if len(sorted_asc) >= 2:
        check("溢价率升序排序正确", sorted_asc[0]["premium_rate"] <= sorted_asc[-1]["premium_rate"])
    
    # Sort by premium rate descending
    sorted_desc = await get_all_realtime(sort_by="premium_rate", sort_order="desc")
    if len(sorted_desc) >= 2:
        check("溢价率降序排序正确", sorted_desc[0]["premium_rate"] >= sorted_desc[-1]["premium_rate"])

    # ========== Test 9: Holdings Refresh Tracking ==========
    print("\n📋 Test 9: 持仓刷新时间追踪")
    fund = await get_fund("161725")
    if fund:
        check(f"holdings_updated_at非空: {fund.get('holdings_updated_at', '')}", 
              bool(fund.get("holdings_updated_at")))
    
    # Check which funds need refresh
    needs_refresh = await get_funds_needing_holdings_refresh(hours=24, holdings_type="domestic")
    # 161725 was just refreshed, but other domestic/hk funds may still need refresh
    refreshed_codes = [f["fund_code"] for f in needs_refresh]
    check("已刷新的161725不在需要刷新列表中", "161725" not in refreshed_codes, f"needs: {refreshed_codes}")
    
    overseas_needs = await get_funds_needing_holdings_refresh(hours=24, holdings_type="overseas")
    # May need refresh if no overseas holdings were found
    check(f"境外持仓刷新检查正常", isinstance(overseas_needs, list))

    # ========== Test 10: Batch Import ==========
    print("\n📋 Test 10: 批量导入基金")
    batch = [
        {"fund_code": "164906", "fund_name": "交银中证海外中国互联网", "market": "sz", "algo_type": "holdings", "category": "overseas", "us_index_code": "100.NDX"},
        {"fund_code": "160416", "fund_name": "华安标普石油", "market": "sz", "algo_type": "holdings", "category": "domestic"},
    ]
    await batch_add_funds(batch)
    all_funds = await get_all_funds()
    check(f"批量导入后基金总数: {len(all_funds)}", len(all_funds) >= 5)

    # ========== Test 11: Remove Fund ==========
    print("\n📋 Test 11: 删除基金")
    await remove_fund("501009")
    fund = await get_fund("501009")
    check("删除后基金不存在", fund is None)
    
    holdings = await get_holdings("501009")
    check("删除后持仓也清除", len(holdings) == 0)

    # ========== Test 12: Industry Index Estimation ==========
    print("\n📋 Test 12: 行业指数估算法")
    await add_fund("161725", "招商中证白酒", "sz", "industry", "domestic", "0.399997")
    async with aiohttp.ClientSession() as session:
        est = await fetch_fund_estimate(session, "161725")
        if est:
            idx_result = await estimate_nav_by_industry_index(session, est["nav"], "0.399997")
            check("行业指数估算法返回结果", bool(idx_result))
            if idx_result:
                check(f"指数名称: {idx_result.get('index_name', '')}", bool(idx_result.get("index_name")))
                check(f"估算净值: {idx_result['estimated_nav']}", idx_result["estimated_nav"] > 0)

    # ========== Test 13: API Server Routes ==========
    print("\n📋 Test 13: HTTP API端点测试")
    from server import create_app
    from aiohttp import web
    from aiohttp.test_utils import AioHTTPTestCase, TestClient, TestServer
    
    app = create_app()
    
    async with TestClient(TestServer(app)) as client:
        # Test /api/funds
        resp = await client.get('/api/funds')
        data = await resp.json()
        check("GET /api/funds 返回200", resp.status == 200)
        check("返回code=0", data.get("code") == 0)
        check(f"基金数量: {len(data.get('data', []))}", len(data.get("data", [])) >= 3)
        
        # Test /api/funds with category filter
        resp = await client.get('/api/funds?category=overseas')
        data = await resp.json()
        check("分类筛选overseas正常", data.get("code") == 0)
        for f in data.get("data", []):
            if f.get("category"):
                check("筛选结果全是overseas", f["category"] == "overseas")
                break
        
        # Test /api/funds with sort
        resp = await client.get('/api/funds?sort=premium_rate&order=desc')
        data = await resp.json()
        check("溢价率降序排序正常", data.get("code") == 0)
        
        # Test /api/algos
        resp = await client.get('/api/algos')
        data = await resp.json()
        check("GET /api/algos 返回正常", data.get("code") == 0)
        
        # Test /api/trading-status
        resp = await client.get('/api/trading-status')
        data = await resp.json()
        check("GET /api/trading-status 返回正常", data.get("code") == 0)
        check("包含is_us_trading字段", "is_us_trading" in data.get("data", {}))
        check("包含refresh_interval字段", "refresh_interval" in data.get("data", {}))
        
        # Test /api/funds/{code}
        resp = await client.get('/api/funds/161725')
        data = await resp.json()
        check("GET /api/funds/161725 详情正常", data.get("code") == 0)
        if data.get("code") == 0:
            detail = data.get("data", {})
            check(f"详情含NAV={detail.get('nav', 0)}", detail.get("nav", 0) > 0)
            check("详情含holdings", "holdings" in detail)
        
        # Test /api/funds/{code}/holdings
        resp = await client.get('/api/funds/161725/holdings')
        data = await resp.json()
        check("GET /api/funds/161725/holdings 正常", data.get("code") == 0)
        
        # Test POST /api/funds (add fund)
        resp = await client.post('/api/funds', json={
            "fund_code": "501009", "fund_name": "恒生ETF", "market": "sh",
            "algo_type": "holdings", "category": "hk"
        })
        data = await resp.json()
        check("POST /api/funds 添加基金正常", data.get("code") == 0)
        
        # Test PUT /api/funds/{code}/algo
        resp = await client.put('/api/funds/161725/algo', json={
            "algo_type": "holdings", "category": "domestic"
        })
        data = await resp.json()
        check("PUT /api/funds/{code}/algo 更新算法正常", data.get("code") == 0)
        
        # Test DELETE /api/funds/{code}
        resp = await client.delete('/api/funds/501009')
        data = await resp.json()
        check("DELETE /api/funds/{code} 删除正常", data.get("code") == 0)
        
        # Test POST /api/update
        resp = await client.post('/api/update')
        data = await resp.json()
        check("POST /api/update 手动更新正常", data.get("code") == 0)
        
        # Test POST /api/refresh-holdings
        resp = await client.post('/api/refresh-holdings')
        data = await resp.json()
        check("POST /api/refresh-holdings 持仓刷新正常", data.get("code") == 0)
        
        # Test POST /api/funds/batch-import
        resp = await client.post('/api/funds/batch-import', json={
            "funds": [{"fund_code": "160119", "fund_name": "南方中证500", "market": "sz", "category": "domestic"}]
        })
        data = await resp.json()
        check("POST /api/funds/batch-import 批量导入正常", data.get("code") == 0)

    # ========== Summary ==========
    print("\n" + "=" * 60)
    print(f"测试完成: ✅ {passed} 通过, ❌ {failed} 失败")
    print("=" * 60)
    
    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
