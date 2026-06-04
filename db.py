"""SQLite database module for LOF Fund Monitor."""
import aiosqlite
import json
import logging
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "lof_fund.db")


async def get_db():
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    return db


async def seed_default_funds():
    """Seed default LOF funds from default_funds.json.
    
    - If the database is empty, imports all default funds.
    - If the database already has funds, syncs algorithm configurations (algo_type,
      industry_index_code, us_index_code) from the defaults for any matching fund codes.
      This ensures users who upgrade get proper algorithm assignments without losing
      their manually added funds or holdings data.
    """
    _logger = logging.getLogger(__name__)

    # Load default funds from JSON
    json_path = os.path.join(os.path.dirname(__file__), "default_funds.json")
    if not os.path.exists(json_path):
        return

    with open(json_path, "r", encoding="utf-8") as f:
        default_funds = json.load(f)

    if not default_funds:
        return

    db = await get_db()
    try:
        cursor = await db.execute("SELECT COUNT(*) as cnt FROM funds")
        row = await cursor.fetchone()
        count = dict(row)["cnt"] if row else 0
    finally:
        await db.close()

    if count == 0:
        # Fresh database — seed all default funds
        _logger.info(f"Seeding {len(default_funds)} default funds from default_funds.json...")
        await batch_add_funds(default_funds)
        _logger.info(f"Successfully seeded {len(default_funds)} default funds")
    else:
        # Existing database — sync algorithm configs for funds present in defaults
        _logger.info(f"Database has {count} funds, syncing algorithm configurations from defaults...")
        synced = 0
        db = await get_db()
        try:
            for f in default_funds:
                code = f["fund_code"]
                # Only update funds that already exist in the database
                cursor = await db.execute("SELECT fund_code FROM funds WHERE fund_code = ?", (code,))
                if await cursor.fetchone():
                    await db.execute(
                        """UPDATE funds SET algo_type = ?, category = ?,
                            industry_index_code = ?, us_index_code = ?,
                            updated_at = CURRENT_TIMESTAMP
                            WHERE fund_code = ?""",
                        (f.get("algo_type", "holdings"), f.get("category", "domestic"),
                         f.get("industry_index_code", ""), f.get("us_index_code", ""), code)
                    )
                    synced += 1
            await db.commit()
        finally:
            await db.close()
        _logger.info(f"Synced algorithm configs for {synced} existing funds")

        # Also add any default funds that don't exist yet
        new_funds = []
        existing_codes = set()
        db = await get_db()
        try:
            cursor = await db.execute("SELECT fund_code FROM funds")
            rows = await cursor.fetchall()
            existing_codes = {dict(r)["fund_code"] for r in rows}
        finally:
            await db.close()

        for f in default_funds:
            if f["fund_code"] not in existing_codes:
                new_funds.append(f)

        if new_funds:
            _logger.info(f"Adding {len(new_funds)} new default funds...")
            await batch_add_funds(new_funds)
            _logger.info(f"Successfully added {len(new_funds)} new default funds")


async def init_db():
    """Initialize database tables."""
    db = await get_db()
    try:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS funds (
                fund_code TEXT PRIMARY KEY,
                fund_name TEXT NOT NULL,
                market TEXT DEFAULT 'sz',
                algo_type TEXT DEFAULT 'holdings',
                category TEXT DEFAULT 'domestic',
                industry_index_code TEXT DEFAULT '',
                us_index_code TEXT DEFAULT '',
                holdings_updated_at TEXT DEFAULT '',
                overseas_holdings_updated_at TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS fund_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                holding_ratio REAL DEFAULT 0,
                em_code TEXT DEFAULT '',
                shares REAL DEFAULT 0,
                market_value REAL DEFAULT 0,
                report_date TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fund_code) REFERENCES funds(fund_code)
            );

            CREATE TABLE IF NOT EXISTS fund_realtime (
                fund_code TEXT PRIMARY KEY,
                nav REAL DEFAULT 0,
                nav_date TEXT DEFAULT '',
                estimated_nav REAL DEFAULT 0,
                estimated_change_rate REAL DEFAULT 0,
                trade_price REAL DEFAULT 0,
                trade_price_change REAL DEFAULT 0,
                trade_amount REAL DEFAULT 0,
                premium_rate REAL DEFAULT 0,
                purchase_status TEXT DEFAULT '开放',
                redeem_status TEXT DEFAULT '开放',
                yesterday_purchase_shares REAL DEFAULT 0,
                index_name TEXT DEFAULT '',
                overseas_period INTEGER DEFAULT 0,
                cn_ratio REAL DEFAULT 0,
                us_ratio REAL DEFAULT 0,
                us_index_name TEXT DEFAULT '',
                model_version TEXT DEFAULT '',
                valuation_method TEXT DEFAULT '',
                valuation_confidence REAL DEFAULT 0,
                valuation_note TEXT DEFAULT '',
                coverage_ratio REAL DEFAULT 0,
                target_exposure REAL DEFAULT 0,
                residual_ratio REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS fund_overseas_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                holding_ratio REAL DEFAULT 0,
                em_code TEXT DEFAULT '',
                report_date TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fund_code) REFERENCES funds(fund_code)
            );

            CREATE TABLE IF NOT EXISTS algo_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                algo_type TEXT UNIQUE NOT NULL,
                algo_name TEXT NOT NULL,
                description TEXT DEFAULT '',
                params TEXT DEFAULT '{}'
            );

            INSERT OR IGNORE INTO algo_config (algo_type, algo_name, description, params) VALUES
                ('holdings', '持仓+残差代理估算法', '已披露持仓按真实权重贡献，未知持仓用代理指数或折减持仓均值估算，并考虑境外汇率', '{}'),
                ('industry', '指数/行业代理估算法', '绑定行业或跟踪指数，使用指数涨跌幅估算整只基金净值，境外代理尝试叠加汇率', '{"index_code": ""}'),
                ('overseas', '境外持仓+代理估算法', '境外持仓按真实权重贡献，未知部分使用配置的美股/商品/港股代理并尝试叠加汇率，不再强行假设A股残差', '{"us_index_code": "", "target_exposure": 0.95}');

        """)
        # Refresh algorithm descriptions on upgrade.
        await db.execute("UPDATE algo_config SET algo_name = ?, description = ? WHERE algo_type = ?",
                         ("持仓+残差代理估算法", "已披露持仓按真实权重贡献，未知持仓用代理指数或折减持仓均值估算，并考虑境外汇率", "holdings"))
        await db.execute("UPDATE algo_config SET algo_name = ?, description = ? WHERE algo_type = ?",
                         ("指数/行业代理估算法", "绑定行业或跟踪指数，使用指数涨跌幅估算整只基金净值，境外代理尝试叠加汇率", "industry"))
        await db.execute("UPDATE algo_config SET algo_name = ?, description = ?, params = ? WHERE algo_type = ?",
                         ("境外持仓+代理估算法", "境外持仓按真实权重贡献，未知部分使用配置的美股/商品/港股代理并尝试叠加汇率，不再强行假设A股残差", '{"us_index_code": "", "target_exposure": 0.95}', "overseas"))
        await db.commit()

        # Create wechat config table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS wechat_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                send_key TEXT DEFAULT '',
                push_enabled INTEGER DEFAULT 0,
                push_interval INTEGER DEFAULT 60,
                push_time TEXT DEFAULT '08:00,20:00',
                premium_alert_enabled INTEGER DEFAULT 0,
                premium_upper REAL DEFAULT 3.0,
                premium_lower REAL DEFAULT -3.0,
                discount_alert_enabled INTEGER DEFAULT 1,
                discount_lower REAL DEFAULT -5.0,
                min_turnover REAL DEFAULT 60,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # One durable record per scheduled WeChat push slot.  The unique
        # push_key prevents duplicate sends even if the process restarts in the
        # same configured minute or two server processes are running together.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS wechat_push_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                push_key TEXT UNIQUE NOT NULL,
                scheduled_date TEXT NOT NULL,
                scheduled_time TEXT NOT NULL,
                status TEXT DEFAULT 'claimed',
                alert_count INTEGER DEFAULT 0,
                message TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_wechat_push_log_date_time
            ON wechat_push_log (scheduled_date, scheduled_time)
        """)
        # Ensure default config row exists
        await db.execute("""
            INSERT OR IGNORE INTO wechat_config (id) VALUES (1)
        """)
        await db.commit()

        # Migrate: preserve EastMoney quote code for domestic holdings on upgrades
        try:
            await db.execute("ALTER TABLE fund_holdings ADD COLUMN em_code TEXT DEFAULT ''")
        except Exception:
            pass
        await db.commit()

        # Migrate: add valuation model columns for existing databases
        for col, default in [
            ("model_version", "TEXT DEFAULT ''"),
            ("valuation_method", "TEXT DEFAULT ''"),
            ("valuation_confidence", "REAL DEFAULT 0"),
            ("valuation_note", "TEXT DEFAULT ''"),
            ("coverage_ratio", "REAL DEFAULT 0"),
            ("target_exposure", "REAL DEFAULT 0"),
            ("residual_ratio", "REAL DEFAULT 0"),
        ]:
            try:
                await db.execute(f"ALTER TABLE fund_realtime ADD COLUMN {col} {default}")
            except Exception:
                pass  # Column already exists
        await db.commit()

        # Migrate: add WeChat config columns if they don't exist (for existing databases)
        for col, default in [
            ("send_key", "TEXT DEFAULT ''"),
            ("push_enabled", "INTEGER DEFAULT 0"),
            ("push_interval", "INTEGER DEFAULT 60"),
            ("push_time", "TEXT DEFAULT '08:00,20:00'"),
            ("premium_alert_enabled", "INTEGER DEFAULT 0"),
            ("premium_upper", "REAL DEFAULT 3.0"),
            ("premium_lower", "REAL DEFAULT -3.0"),
            ("discount_alert_enabled", "INTEGER DEFAULT 1"),
            ("discount_lower", "REAL DEFAULT -5.0"),
            ("min_turnover", "REAL DEFAULT 60"),
            ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ]:
            try:
                await db.execute(f"ALTER TABLE wechat_config ADD COLUMN {col} {default}")
            except Exception:
                pass  # Column already exists

        # v1.7a compatibility: older threshold alerts could be scheduled even
        # when push_enabled was 0. Preserve those existing alert schedules while
        # no longer allowing push_enabled to trigger any summary push.
        await db.execute("""
            UPDATE wechat_config
            SET push_enabled = 1
            WHERE COALESCE(push_enabled, 0) = 0
              AND (COALESCE(premium_alert_enabled, 0) = 1 OR COALESCE(discount_alert_enabled, 0) = 1)
        """)
        # Migrate: add new columns if they don't exist (for existing databases)
        try:
            await db.execute("ALTER TABLE funds ADD COLUMN holdings_updated_at TEXT DEFAULT ''")
        except Exception:
            pass  # Column already exists
        try:
            await db.execute("ALTER TABLE funds ADD COLUMN overseas_holdings_updated_at TEXT DEFAULT ''")
        except Exception:
            pass  # Column already exists
        # Migrate: add overseas estimation columns to fund_realtime
        for col, default in [
            ('trade_amount', 'REAL DEFAULT 0'),
            ('overseas_period', 'INTEGER DEFAULT 0'),
            ('cn_ratio', 'REAL DEFAULT 0'),
            ('us_ratio', 'REAL DEFAULT 0'),
            ('us_index_name', "TEXT DEFAULT ''"),
        ]:
            try:
                await db.execute(f"ALTER TABLE fund_realtime ADD COLUMN {col} {default}")
            except Exception:
                pass  # Column already exists
        await db.commit()
    finally:
        await db.close()


async def add_fund(fund_code: str, fund_name: str, market: str = "sz", algo_type: str = "holdings", category: str = "domestic", industry_index_code: str = "", us_index_code: str = ""):
    db = await get_db()
    try:
        # Use INSERT OR IGNORE + UPDATE to preserve holdings_updated_at and overseas_holdings_updated_at
        await db.execute(
            "INSERT OR IGNORE INTO funds (fund_code, fund_name, market, algo_type, category, industry_index_code, us_index_code, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
            (fund_code, fund_name, market, algo_type, category, industry_index_code, us_index_code)
        )
        # If the fund already existed, update the mutable fields but preserve timestamps
        await db.execute(
            """UPDATE funds SET fund_name = ?, market = ?, algo_type = ?, category = ?, 
                industry_index_code = ?, us_index_code = ?, updated_at = CURRENT_TIMESTAMP
                WHERE fund_code = ?""",
            (fund_name, market, algo_type, category, industry_index_code, us_index_code, fund_code)
        )
        await db.commit()
    finally:
        await db.close()


async def remove_fund(fund_code: str):
    db = await get_db()
    try:
        await db.execute("DELETE FROM funds WHERE fund_code = ?", (fund_code,))
        await db.execute("DELETE FROM fund_holdings WHERE fund_code = ?", (fund_code,))
        await db.execute("DELETE FROM fund_realtime WHERE fund_code = ?", (fund_code,))
        await db.execute("DELETE FROM fund_overseas_holdings WHERE fund_code = ?", (fund_code,))
        await db.commit()
    finally:
        await db.close()


async def update_fund_algo(fund_code: str, algo_type: str, industry_index_code: str = "", us_index_code: str = "", category: str = ""):
    db = await get_db()
    try:
        if category:
            await db.execute(
                "UPDATE funds SET algo_type = ?, category = ?, industry_index_code = ?, us_index_code = ?, updated_at = CURRENT_TIMESTAMP WHERE fund_code = ?",
                (algo_type, category, industry_index_code, us_index_code, fund_code)
            )
        else:
            await db.execute(
                "UPDATE funds SET algo_type = ?, industry_index_code = ?, us_index_code = ?, updated_at = CURRENT_TIMESTAMP WHERE fund_code = ?",
                (algo_type, industry_index_code, us_index_code, fund_code)
            )
        await db.commit()
    finally:
        await db.close()


async def get_all_funds():
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM funds ORDER BY fund_code")
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def get_fund(code: str):
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM funds WHERE fund_code = ?", (code,))
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


async def save_holdings(fund_code: str, holdings: list):
    db = await get_db()
    try:
        await db.execute("DELETE FROM fund_holdings WHERE fund_code = ?", (fund_code,))
        for h in holdings:
            await db.execute(
                "INSERT INTO fund_holdings (fund_code, stock_code, stock_name, holding_ratio, em_code, shares, market_value, report_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (fund_code, h["stock_code"], h["stock_name"], h["holding_ratio"], h.get("em_code", ""), h.get("shares", 0), h.get("market_value", 0), h.get("report_date", ""))
            )
        await db.commit()
    finally:
        await db.close()


async def get_holdings(fund_code: str):
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM fund_holdings WHERE fund_code = ? ORDER BY holding_ratio DESC", (fund_code,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def save_realtime(fund_code: str, data: dict):
    db = await get_db()
    try:
        await db.execute(
            """INSERT OR REPLACE INTO fund_realtime
            (fund_code, nav, nav_date, estimated_nav, estimated_change_rate, trade_price, trade_price_change, trade_amount, premium_rate, purchase_status, redeem_status, yesterday_purchase_shares, index_name, overseas_period, cn_ratio, us_ratio, us_index_name, model_version, valuation_method, valuation_confidence, valuation_note, coverage_ratio, target_exposure, residual_ratio, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
            (fund_code, data.get("nav", 0), data.get("nav_date", ""), data.get("estimated_nav", 0),
             data.get("estimated_change_rate", 0), data.get("trade_price", 0), data.get("trade_price_change", 0),
             data.get("trade_amount", 0),
             data.get("premium_rate", 0), data.get("purchase_status", "开放"), data.get("redeem_status", "开放"),
             data.get("yesterday_purchase_shares", 0), data.get("index_name", ""),
             data.get("overseas_period", 0), data.get("cn_ratio", 0), data.get("us_ratio", 0),
             data.get("us_index_name", ""), data.get("model_version", ""),
             data.get("valuation_method", ""), data.get("valuation_confidence", 0),
             data.get("valuation_note", ""), data.get("coverage_ratio", 0),
             data.get("target_exposure", 0), data.get("residual_ratio", 0))
        )
        await db.commit()
    finally:
        await db.close()


async def get_all_realtime(category: str = "", sort_by: str = "", sort_order: str = "asc"):
    db = await get_db()
    try:
        where_clause = ""
        params = []
        if category and category != "all":
            where_clause = "WHERE f.category = ?"
            params.append(category)

        order_clause = "ORDER BY f.fund_code"
        if sort_by == "premium_rate":
            if sort_order == "desc":
                order_clause = "ORDER BY COALESCE(r.premium_rate, 0) DESC"
            else:
                order_clause = "ORDER BY COALESCE(r.premium_rate, 0) ASC"
        elif sort_by == "fund_code":
            if sort_order == "desc":
                order_clause = "ORDER BY f.fund_code DESC"
            else:
                order_clause = "ORDER BY f.fund_code ASC"

        sql = f"""
            SELECT f.fund_code, f.fund_name, f.algo_type, f.category, f.industry_index_code, f.us_index_code,
                   r.nav, r.nav_date, r.estimated_nav, r.estimated_change_rate,
                   r.trade_price, r.trade_price_change, r.trade_amount, r.premium_rate,
                   r.purchase_status, r.redeem_status, r.yesterday_purchase_shares, r.index_name,
                   r.overseas_period, r.cn_ratio, r.us_ratio, r.us_index_name,
                   r.model_version, r.valuation_method, r.valuation_confidence, r.valuation_note,
                   r.coverage_ratio, r.target_exposure, r.residual_ratio,
                   r.updated_at
            FROM funds f
            LEFT JOIN fund_realtime r ON f.fund_code = r.fund_code
            {where_clause}
            {order_clause}
        """
        cursor = await db.execute(sql, params)
        rows = await cursor.fetchall()
        result = []
        for row in rows:
            d = dict(row)
            # Set defaults for missing realtime data
            defaults = {
                'nav': 0, 'nav_date': '', 'estimated_nav': 0, 'estimated_change_rate': 0,
                'trade_price': 0, 'trade_price_change': 0, 'trade_amount': 0, 'premium_rate': 0,
                'purchase_status': '未知', 'redeem_status': '未知',
                'yesterday_purchase_shares': 0, 'index_name': '', 'updated_at': '',
                'overseas_period': 0, 'cn_ratio': 0, 'us_ratio': 0, 'us_index_name': '',
                'model_version': '', 'valuation_method': '', 'valuation_confidence': 0, 'valuation_note': '',
                'coverage_ratio': 0, 'target_exposure': 0, 'residual_ratio': 0,
            }
            for k, v in defaults.items():
                if d.get(k) is None:
                    d[k] = v
            result.append(d)
        return result
    finally:
        await db.close()


async def get_realtime(code: str):
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT f.fund_code, f.fund_name, f.algo_type, f.category, f.industry_index_code, f.us_index_code,
                   r.nav, r.nav_date, r.estimated_nav, r.estimated_change_rate,
                   r.trade_price, r.trade_price_change, r.trade_amount, r.premium_rate,
                   r.purchase_status, r.redeem_status, r.yesterday_purchase_shares, r.index_name,
                   r.overseas_period, r.cn_ratio, r.us_ratio, r.us_index_name,
                   r.model_version, r.valuation_method, r.valuation_confidence, r.valuation_note,
                   r.coverage_ratio, r.target_exposure, r.residual_ratio,
                   r.updated_at
            FROM funds f
            LEFT JOIN fund_realtime r ON f.fund_code = r.fund_code
            WHERE f.fund_code = ?
        """, (code,))
        row = await cursor.fetchone()
        if not row:
            return None
        d = dict(row)
        # Set defaults for missing realtime data
        defaults = {
            'nav': 0, 'nav_date': '', 'estimated_nav': 0, 'estimated_change_rate': 0,
            'trade_price': 0, 'trade_price_change': 0, 'trade_amount': 0, 'premium_rate': 0,
            'purchase_status': '未知', 'redeem_status': '未知',
            'yesterday_purchase_shares': 0, 'index_name': '', 'updated_at': '',
            'overseas_period': 0, 'cn_ratio': 0, 'us_ratio': 0, 'us_index_name': '',
            'model_version': '', 'valuation_method': '', 'valuation_confidence': 0, 'valuation_note': '',
            'coverage_ratio': 0, 'target_exposure': 0, 'residual_ratio': 0,
        }
        for k, v in defaults.items():
            if d.get(k) is None:
                d[k] = v
        return d
    finally:
        await db.close()


async def get_algo_configs():
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM algo_config ORDER BY id")
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


# ============ Overseas Holdings CRUD ============

async def save_overseas_holdings(fund_code: str, holdings: list):
    """Save overseas (US) stock holdings for a fund, replacing existing ones."""
    db = await get_db()
    try:
        await db.execute("DELETE FROM fund_overseas_holdings WHERE fund_code = ?", (fund_code,))
        for h in holdings:
            await db.execute(
                "INSERT INTO fund_overseas_holdings (fund_code, stock_code, stock_name, holding_ratio, em_code, report_date) VALUES (?, ?, ?, ?, ?, ?)",
                (fund_code, h["stock_code"], h["stock_name"], h.get("holding_ratio", 0), h.get("em_code", ""), h.get("report_date", ""))
            )
        await db.commit()
    finally:
        await db.close()


async def get_overseas_holdings(fund_code: str):
    """Get overseas (US) stock holdings for a fund."""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM fund_overseas_holdings WHERE fund_code = ? ORDER BY holding_ratio DESC", (fund_code,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def batch_add_funds(funds: list):
    """Batch add funds to the database. Each fund dict should have: fund_code, fund_name, market, algo_type, category, industry_index_code, us_index_code.
    Uses INSERT OR IGNORE + UPDATE to preserve holdings timestamps.
    """
    db = await get_db()
    try:
        for f in funds:
            await db.execute(
                "INSERT OR IGNORE INTO funds (fund_code, fund_name, market, algo_type, category, industry_index_code, us_index_code, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
                (f["fund_code"], f["fund_name"], f.get("market", "sz"), f.get("algo_type", "holdings"), f.get("category", "domestic"), f.get("industry_index_code", ""), f.get("us_index_code", ""))
            )
            await db.execute(
                """UPDATE funds SET fund_name = ?, market = ?, algo_type = ?, category = ?,
                    industry_index_code = ?, us_index_code = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE fund_code = ?""",
                (f["fund_name"], f.get("market", "sz"), f.get("algo_type", "holdings"), f.get("category", "domestic"), f.get("industry_index_code", ""), f.get("us_index_code", ""), f["fund_code"])
            )
        await db.commit()
    finally:
        await db.close()


# ============ Holdings Refresh Tracking ============

async def update_holdings_timestamp(fund_code: str, holdings_type: str = "domestic"):
    """Update the timestamp when holdings were last refreshed.
    holdings_type: 'domestic' or 'overseas'
    """
    db = await get_db()
    try:
        from datetime import datetime, timezone, timedelta
        cst = timezone(timedelta(hours=8))
        now_str = datetime.now(cst).strftime("%Y-%m-%d %H:%M:%S")
        if holdings_type == "overseas":
            await db.execute(
                "UPDATE funds SET overseas_holdings_updated_at = ? WHERE fund_code = ?",
                (now_str, fund_code)
            )
        else:
            await db.execute(
                "UPDATE funds SET holdings_updated_at = ? WHERE fund_code = ?",
                (now_str, fund_code)
            )
        await db.commit()
    finally:
        await db.close()


async def get_funds_needing_holdings_refresh(hours: int = 24, holdings_type: str = "domestic") -> list:
    """Get funds whose holdings haven't been refreshed in the specified hours.
    holdings_type: 'domestic' or 'overseas'
    hours: 0 means return all funds of that type regardless of timestamp
    Returns list of fund dicts.
    """
    db = await get_db()
    try:
        from datetime import datetime, timezone, timedelta
        cst = timezone(timedelta(hours=8))
        
        if hours <= 0:
            # Return all funds of the specified type
            if holdings_type == "overseas":
                cursor = await db.execute(
                    "SELECT * FROM funds WHERE (category = 'overseas' OR algo_type = 'overseas') ORDER BY fund_code"
                )
            else:
                cursor = await db.execute(
                    "SELECT * FROM funds WHERE (category != 'overseas' OR category IS NULL) ORDER BY fund_code"
                )
        else:
            cutoff = (datetime.now(cst) - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
            
            if holdings_type == "overseas":
                # Get overseas funds whose overseas holdings are stale or empty
                cursor = await db.execute(
                    """SELECT * FROM funds 
                    WHERE (category = 'overseas' OR algo_type = 'overseas')
                    AND (overseas_holdings_updated_at = '' OR overseas_holdings_updated_at IS NULL OR overseas_holdings_updated_at < ?)
                    ORDER BY fund_code""",
                    (cutoff,)
                )
            else:
                # Get domestic/hk funds whose domestic holdings are stale or empty
                cursor = await db.execute(
                    """SELECT * FROM funds 
                    WHERE (category != 'overseas' OR category IS NULL)
                    AND (holdings_updated_at = '' OR holdings_updated_at IS NULL OR holdings_updated_at < ?)
                    ORDER BY fund_code""",
                    (cutoff,)
                )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


# ============ WeChat Push Config ============

async def get_wechat_config() -> dict:
    """Get the WeChat push configuration."""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM wechat_config WHERE id = 1")
        row = await cursor.fetchone()
        if row:
            return dict(row)
        return {
            "id": 1, "send_key": "", "push_enabled": 0, "push_interval": 60,
            "push_time": "08:00,20:00", "premium_alert_enabled": 0,
            "premium_upper": 3.0, "premium_lower": -3.0,
            "discount_alert_enabled": 1, "discount_lower": -5.0,
            "min_turnover": 60,
        }
    finally:
        await db.close()


async def save_wechat_config(config: dict) -> None:
    """Save the WeChat push configuration."""
    db = await get_db()
    try:
        await db.execute("""
            INSERT OR REPLACE INTO wechat_config
            (id, send_key, push_enabled, push_interval, push_time,
             premium_alert_enabled, premium_upper, premium_lower,
             discount_alert_enabled, discount_lower, min_turnover, updated_at)
            VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            config.get("send_key", ""),
            config.get("push_enabled", 0),
            config.get("push_interval", 60),
            config.get("push_time", "08:00,20:00"),
            config.get("premium_alert_enabled", 0),
            config.get("premium_upper", 3.0),
            config.get("premium_lower", -3.0),
            config.get("discount_alert_enabled", 1),
            config.get("discount_lower", -5.0),
            config.get("min_turnover", 60),
        ))
        await db.commit()
    finally:
        await db.close()


async def claim_wechat_push_slot(push_key: str, scheduled_date: str, scheduled_time: str) -> bool:
    """Atomically claim one scheduled WeChat push slot.

    Returns True only for the first caller that claims this date + HH:MM slot.
    The claim is stored in SQLite, so duplicate app tasks, duplicate processes,
    or a restart during the same minute cannot send the same scheduled push
    twice.
    """
    db = await get_db()
    try:
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute(
            """
            INSERT OR IGNORE INTO wechat_push_log
                (push_key, scheduled_date, scheduled_time, status, message, updated_at)
            VALUES (?, ?, ?, 'claimed', '定时槽位已占用，准备检查告警', CURRENT_TIMESTAMP)
            """,
            (push_key, scheduled_date, scheduled_time),
        )
        await db.commit()
        return cursor.rowcount == 1
    except Exception:
        await db.rollback()
        raise
    finally:
        await db.close()


async def mark_wechat_push_slot(push_key: str, status: str, alert_count: int = 0, message: str = "") -> None:
    """Update the result of a claimed WeChat push slot."""
    db = await get_db()
    try:
        await db.execute(
            """
            UPDATE wechat_push_log
            SET status = ?, alert_count = ?, message = ?, updated_at = CURRENT_TIMESTAMP
            WHERE push_key = ?
            """,
            (status, int(alert_count or 0), str(message or "")[:500], push_key),
        )
        await db.commit()
    finally:
        await db.close()
