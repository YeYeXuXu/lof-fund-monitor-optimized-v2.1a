# LOF 基金折溢价监控 - 净值估值模型优化v1.9a

实时监控 LOF（上市开放式基金）的折溢价率，支持自定义净值估算算法，帮助发现套利机会。

基于 Python aiohttp 异步后端 + 原生前端，提供折溢价率实时展示、十大持仓估值、行业指数估值、申购赎回状态监控等功能。


## 净值估值模型优化v1.9a 更新

- v1.9a 修复：AkShare `fund_value_estimation_em(场内交易基金)` 超时改为可选估值分类单项限时降级，避免一个慢分类拖慢整轮快照或微信到点推送。
- v1.9a 修复：AkShare `f402` 字段按项目内部“正数=溢价、负数=折价”约定归一化；当 IOPV 和价格可用时会用 `(最新价 - IOPV) / IOPV × 100%` 校验正负号。
- v1.9a 修复：微信阈值推送中过滤不可操作基金，溢价列表过滤申购状态为“暂停/停止/不可/封闭”的基金，折价列表过滤赎回状态为“暂停/停止/不可/封闭”的基金。
- v1.9a 增强：数据刷新调度会打印当前刷新模式、下次刷新倒计时、每轮开始/完成统计和 AkShare 快照命中情况；微信到点推送最多等待 `WECHAT_AKSHARE_OVERLAY_TIMEOUT` 秒，超时即使用已保存实时数据，避免被数据刷新影响。
- v1.9a 修复：微信阈值告警正文中的申购/赎回状态优先使用 AkShare `fund_purchase_em` 批量接口；接口缺失、超时或单只基金未命中时，自动回退原东方财富 F10/基金页状态解析。
- v1.9a 调整：微信定时推送到点后不再等待后台数据刷新任务释放锁；推送使用已保存实时数据叠加快速 AkShare 快照，确保数据刷新不影响微信定时推送。
- v1.9a 明确：开盘/交易时段每 5 分钟刷新基金数据，休市/非交易时段每 30 分钟刷新；该刷新节奏与微信推送时间相互独立。
- v1.9a 修复：AkShare/EastMoney 行情接口增加 `push2`、`push2delay`、`88.push2`、`2.push2` 多主机兜底，并对断连、超时、空响应进行重试，降低 `ServerDisconnectedError` 导致整批 LOF 快照不可用的概率。
- v1.9a 增强：GitHub Actions/服务日志会输出每只基金的 `Source=[...]`，明确 NAV、估值、价格和折溢价来自 AkShare 还是原有接口/计算兜底。
- 新增 AkShare release v1.18.64 基金信息适配层 `akshare_fund_adapter.py`，项目内异步复刻并优先使用 `fund_etf_spot_em`、`fund_lof_spot_em`、`fund_value_estimation_em` 获取基金行情、IOPV、折溢价率、成交金额和净值估算信息。
- 明确采用 AkShare `fund_etf_spot_em` 字段映射：东方财富 `f441` = `IOPV实时估值`，`f402` = `基金折价率`；项目会结合最新价和 IOPV 将其归一化为“正数=溢价、负数=折价”的折溢价率。
- GitHub Actions 运行时每轮批量获取 AkShare 快照并缓存，减少逐基金行情/估值请求；微信定时推送在筛选前进行快速 AkShare 内存叠加，不等待完整持仓刷新，也不等待后台刷新锁。
- 若 AkShare 快照缺少某只基金或某项字段，仍自动回退原有 fundgz/lsjz、push2、F10 持仓及申购赎回状态获取方法；申赎状态优先来自 AkShare `fund_purchase_em`。
- 新增统一、可复用的净值估值模型 `estimate_nav_unified`：所有国内、港股、QDII/海外基金统一返回估算净值、估算涨跌、模型版本、估值方法、置信度和估值说明。
- 十大持仓估值不再把前十大持仓平均涨跌幅无条件外推到 100% 基金资产；已披露持仓按真实持仓权重贡献，未知持仓部分使用配置指数/分类代理指数或折减后的持仓均值估算，并保留现金/其他资产暴露。
- 行业/指数型基金使用配置指数作为主估值信号；港股/QDII/商品类基金支持使用境外代理指数、ETF 或商品期货代码，并尽力抓取 USD/CNY、HKD/CNY 汇率涨跌做人民币口径调整。
- QDII/海外基金不再默认把“非美股持仓”强行视为 A 股组件；缺少持仓时优先使用配置的海外代理，缺少代理时才回退到公开基金估算/历史净值涨跌。
- 修正海外持仓中 `ENBCN`、`TTEFP`、`EQNRNO`、`BP.` 等 Bloomberg/交易所后缀代码被误当作 `105.<代码>` 美股 secid 导致的告警刷屏。
- 微信阈值告警正文已增加版本号：`净值估值模型优化v1.9a`。

## v1.9a 更新

- GitHub Actions 支持 `SERVERCHAN_SENDKEYS` / `WECHAT_SEND_KEY` 配置多个 Server酱 SendKey；可用逗号、分号、空格或换行分隔，系统会逐个推送。
- 修复同一设定时间内可能重复发送微信告警的问题。
- 新增 SQLite 定时槽位锁：同一天同一个 `HH:MM` 只允许执行 1 次微信告警检查/推送；服务重启、重复启动或重复任务也不会重复发送。
- 继续保持“只在配置时间所在分钟触发，配置时间之外不推送任何消息”；汇总推送、测试推送仍保持禁用。
- 其他功能保持不变，数据刷新任务与微信告警任务仍完全独立。


## 快速开始

### 一行命令安装（推荐）

**全自动**：自动检测平台、安装 Python 依赖、克隆仓库、启动服务、检测端口、打开浏览器。一行命令，开箱即用：

**Windows（PowerShell）：**
```powershell
irm https://raw.githubusercontent.com/ctz168/fund/main/install.ps1 | iex
```

**Linux / macOS / Termux：**
```bash
curl -fsSL https://raw.githubusercontent.com/ctz168/fund/main/install.sh | bash
```

安装完成后浏览器会自动打开，按 Ctrl+C 可停止服务。

> 支持平台：Windows 10/11、Termux、Ubuntu/Debian、Fedora/CentOS、macOS、Alpine、Arch、openSUSE
>
> 默认地址：`http://localhost:8080`

**自定义安装目录：**
```bash
# Linux / macOS
FUND_INSTALL_DIR=~/my-fund curl -fsSL https://raw.githubusercontent.com/ctz168/fund/main/install.sh | bash
# Windows
$env:FUND_INSTALL_DIR="C:\my-fund"; irm https://raw.githubusercontent.com/ctz168/fund/main/install.ps1 | iex
```

**自定义端口：**
```bash
# Linux / macOS
FUND_PORT=9090 curl -fsSL https://raw.githubusercontent.com/ctz168/fund/main/install.sh | bash
# Windows
$env:FUND_PORT="9090"; irm https://raw.githubusercontent.com/ctz168/fund/main/install.ps1 | iex
```

### 手动安装

**Windows：**
```powershell
# 1. 安装 Python 3.9+（去 https://www.python.org/downloads/ 下载，安装时勾选 Add to PATH）
# 2. 打开 PowerShell
pip install aiohttp aiosqlite beautifulsoup4 lxml
git clone https://github.com/ctz168/fund.git
cd fund
python server.py
# 浏览器打开 http://localhost:8080
```

**Termux：**
```bash
pkg install python python-pip git
pip install aiohttp aiosqlite beautifulsoup4 lxml
git clone https://github.com/ctz168/fund.git && cd fund
python3 server.py
```

**Ubuntu / Debian / WSL：**
```bash
sudo apt install python3 python3-pip python3-venv git
pip3 install --break-system-packages aiohttp aiosqlite beautifulsoup4 lxml
git clone https://github.com/ctz168/fund.git && cd fund
python3 server.py
```

**macOS：**
```bash
brew install python git
pip3 install aiohttp aiosqlite beautifulsoup4 lxml
git clone https://github.com/ctz168/fund.git && cd fund
python3 server.py
```

**Fedora / CentOS：**
```bash
sudo dnf install python3 python3-pip git
pip3 install aiohttp aiosqlite beautifulsoup4 lxml
git clone https://github.com/ctz168/fund.git && cd fund
python3 server.py
```

**Alpine：**
```bash
sudo apk add python3 py3-pip git
pip3 install --break-system-packages aiohttp aiosqlite beautifulsoup4 lxml
git clone https://github.com/ctz168/fund.git && cd fund
python3 server.py
```

### Docker

```bash
docker run -d -p 8080:8080 --name lof-fund python:3.12-slim bash -c \
  "pip install aiohttp aiosqlite beautifulsoup4 lxml && git clone --depth 1 https://github.com/ctz168/fund.git /fund && cd /fund && python3 server.py"
```

启动后浏览器打开 `http://localhost:8080` 即可使用。

## 功能特性

### 前端监控页面

暗色主题折溢价率监控面板，支持基金列表实时展示：代码、名称、单位净值、估算净值、估算涨跌幅、二级市场交易价格。核心指标折溢价率以红色（溢价）/ 绿色（折价）高亮显示，一目了然。申购赎回状态实时更新，基金十大持仓一键展开查看。交易时段每 30 秒自动刷新数据。

### 后台管理页面

输入基金代码一键导入，自动获取基金名称。选择交易所（深交所/上交所），绑定不同净值估算算法。支持行业指数估算入口（可输入行业指数代码）。

### 净值估算算法

| 算法 | 说明 |
|------|------|
| **持仓+残差代理估算法**（默认） | 已披露持仓按真实权重贡献；未知持仓部分使用行业/分类代理指数或折减持仓均值估算，避免把前十大持仓无条件外推到全基金 |
| **指数/行业代理估算法** | 绑定行业或跟踪指数，直接用指数涨跌幅估算整只基金净值；境外代理会尝试叠加汇率变动 |
| **境外持仓+代理估算法** | QDII/海外基金优先使用境外持仓，未知部分使用美股/港股/商品代理；缺少持仓时使用配置代理兜底 |

### 数据自动更新

A 股交易时段（9:30-11:30, 13:00-15:00）和美股交易时段（北京时间 21:00-05:00）每 5 分钟自动抓取最新数据。非交易时段每 30 分钟更新一次保持数据新鲜。支持手动触发即时更新。

### 微信推送

- v1.9a 微信推送只用于折溢价阈值告警，不再发送定时汇总或测试消息；支持多个 Server酱 SendKey 同时推送。
- 到达“告警推送时间”配置的 `HH:MM` 后，系统只检查一次当前已刷新数据；满足条件时发送 1 条类似 `LOF折溢价告警 溢价3% 成交60万` 的微信消息。
- 同一天同一配置时间最多执行 1 次告警检查/推送，并写入数据库去重记录；没有基金满足溢价/折价阈值和成交金额条件时不推送。
- 微信推送时间只控制告警发送时间，不触发、不提前、不延后数据刷新；数据刷新仍保持交易时段每 5 分钟、非交易时段每 30 分钟。

## 折溢价率计算

```
折溢价率 = (交易价格 - 估算净值) / 估算净值 × 100%

- 正值 = 溢价（交易价格高于净值，可考虑申购套利）
- 负值 = 折价（交易价格低于净值，可考虑买入套利）
```

### 持仓+残差代理估值计算

```
1. 获取基金最新披露持仓及占比，并抓取持仓证券实时涨跌幅
2. 境外持仓尝试叠加 USD/CNY、HKD/CNY 汇率涨跌，转换为人民币口径收益
3. 已披露持仓贡献 = Σ(持仓占比/100 × 人民币口径涨跌幅)
4. 未披露/残差部分 = max(目标资产暴露 - 已披露覆盖率, 0)
5. 残差部分优先使用配置指数或分类代理指数估算；无代理时使用折减后的持仓平均涨跌幅；覆盖率过低时按现金/其他资产处理
6. 估算净值 = 最新披露单位净值 × (1 + 估算组合涨跌幅/100)
```

相比旧模型，新模型不会简单执行 `Σ加权贡献 / 覆盖率`，因此在前十大持仓覆盖率较低、持仓披露滞后、港股/QDII 汇率变动明显时更稳健。

## 项目结构

```
ctz168/fund/
├── server.py              # aiohttp 后端主服务（路由 + 定时任务）
├── fetcher.py             # 数据抓取模块（东方财富/天天基金 API）
├── estimator.py           # 净值估算算法模块
├── db.py                  # SQLite 数据库模块
├── requirements.txt       # Python 依赖
├── install.sh             # Linux/macOS 全自动安装（安装+启动+打开浏览器）
├── install.ps1            # Windows PowerShell 全自动安装（安装+启动+打开浏览器）
├── run.sh                 # Linux/macOS 一键启动脚本
├── run.bat                # Windows 一键启动脚本
├── start.sh               # 启动脚本（自动重启）
├── Dockerfile             # Docker 部署
├── .gitignore
├── README.md
└── static/
    ├── index.html          # 前端监控页面（暗色主题）
    └── admin.html          # 后台管理页面
```

## API 接口

服务端运行在 `http://localhost:8080`，所有 API 均返回 JSON。

### 基金管理

| 方法 | 接口 | 说明 |
|------|------|------|
| GET | `/api/funds` | 获取所有基金实时数据 |
| GET | `/api/funds/{code}` | 获取单只基金详情（含持仓） |
| POST | `/api/funds` | 添加基金 |
| DELETE | `/api/funds/{code}` | 删除基金 |
| PUT | `/api/funds/{code}/algo` | 修改估算算法 |

### 数据与算法

| 方法 | 接口 | 说明 |
|------|------|------|
| GET | `/api/algos` | 获取可用算法列表 |
| GET | `/api/funds/{code}/holdings` | 获取基金持仓 |
| POST | `/api/update` | 手动触发数据更新 |
| GET | `/api/trading-status` | 获取交易状态 |

### 添加基金示例

```bash
curl -X POST http://localhost:8080/api/funds \
  -H "Content-Type: application/json" \
  -d '{"fund_code":"161831","market":"sz","algo_type":"holdings"}'
```

## 数据源

全部来自公开 API，稳定可靠，无需申请密钥。

| 数据 | 来源 | 接口 |
|------|------|------|
| 基金名称 / 净值 / 估值 | 天天基金 | `fundgz.1234567.com.cn/js/{code}.js` |
| 二级市场交易价格 | 东方财富 | `push2delay.eastmoney.com` |
| 基金十大持仓 | 东方财富 | `fundf10.eastmoney.com` |
| 申购赎回状态 | 东方财富 | `fund.eastmoney.com` |
| 汇率涨跌 | 东方财富 | `push2delay.eastmoney.com`（USD/CNY、HKD/CNY，失败时回退 0） |

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `FUND_PORT` | `8080` | 服务监听端口 |
| `FUND_INSTALL_DIR` | `~/lof-fund` | 安装目录（仅安装脚本使用） |

## 测试基金

系统预置了两只 LOF 基金用于测试：

| 基金代码 | 基金名称 | 交易所 | 说明 |
|----------|---------|--------|------|
| 161831 | 银华恒生国企指数(QDII-LOF)A | 深交所 | 跟踪恒生国企指数，港股持仓 |
| 161124 | 易方达香港小型股指数A | 深交所 | 跟踪香港小型股指数 |

## 依赖

| 包 | 用途 |
|----|------|
| `aiohttp` | 异步 HTTP 服务 + 数据抓取 |
| `aiosqlite` | 异步 SQLite 操作 |
| `beautifulsoup4` | HTML 解析（持仓数据） |
| `lxml` | 高性能 HTML 解析引擎 |

> 无需数据库服务器，纯 SQLite 文件数据库，零配置。

## 环境要求

| 项目 | 最低要求 |
|------|----------|
| Python | 3.9+ |
| 核心依赖 | aiohttp >= 3.9, aiosqlite >= 0.20, beautifulsoup4 >= 4.12, lxml >= 5.0 |
| Git | 用于克隆仓库（安装脚本会自动安装） |
| 操作系统 | Windows 10/11, macOS, Linux (Termux/Ubuntu/Debian/Fedora/CentOS/Alpine/Arch) |
| 浏览器 | Chrome / Firefox / Safari / Edge（近两年版本） |

## Docker 部署

### 构建

```bash
docker build -t ctz168/fund .
```

### 运行

```bash
docker run -d -p 8080:8080 --name lof-fund ctz168/fund
```

### Docker Compose

```yaml
version: '3'
services:
  fund:
    image: ctz168/fund
    ports:
      - "8080:8080"
    restart: unless-stopped
```

## 更新

```bash
cd lof-fund
git pull
# 重启 server.py 即可
```

## 技术栈

- **后端**: Python aiohttp（异步 HTTP 服务 + 数据抓取）
- **前端**: 原生 HTML/CSS/JavaScript（无框架，暗色主题）
- **数据库**: SQLite（aiosqlite 异步操作）
- **数据源**: 天天基金 / 东方财富公开 API

## 免责声明

本工具仅供学习研究使用，数据来源于公开接口，可能存在延迟或误差。不构成任何投资建议，投资有风险，决策需谨慎。

## 许可证

MIT License
