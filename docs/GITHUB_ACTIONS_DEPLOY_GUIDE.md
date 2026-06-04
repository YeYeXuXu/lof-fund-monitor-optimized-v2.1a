# LOF 基金折溢价监控：GitHub Actions 09:30-15:00 部署指南

本目录已经为 GitHub Actions 增加了以下文件：

- `.github/workflows/fund-monitor.yml`：每天北京时间 09:30 启动工作流，默认 15:00 停止。
- `scripts/github_actions_init.py`：在 Actions 运行器上初始化 SQLite 数据库、导入 `default_funds.json`、从 GitHub Secrets/Variables 写入微信告警配置。
- `scripts/run_actions_monitor.py`：以无浏览器模式启动 aiohttp 服务，让项目内置定时任务运行，到 15:00 自动退出。
- `.env.github-actions.example`：需要配置的 Secrets/Variables 示例。

> 重要说明：GitHub 托管 runner 不适合当作长期公网 Web 服务器。这个方案适合“每天固定时段在云端跑监控和微信告警”。网页地址只在 runner 内部可访问，不能像 VPS 一样对公网打开。

> 注意：GitHub Actions 的 `schedule.cron` 固定按 UTC 解释，不支持 `timezone` 字段；本项目已改为 UTC 01:30，对应北京时间 09:30。

## 1. 创建 GitHub 仓库

1. 打开浏览器进入 GitHub，并登录你的账号。
2. 页面右上角点击 `+`。
3. 在下拉菜单里点击 `New repository`。
4. `Repository name` 填一个仓库名，例如 `lof-fund-monitor`。
5. 建议选择 `Public`：每天 09:30-15:00 约 330 分钟，私有仓库会消耗 GitHub Actions 分钟数配额。
6. 不要勾选 `Add a README file`、`.gitignore`、`license`，因为本项目已经自带文件。
7. 点击绿色按钮 `Create repository`。

## 2. 上传项目文件

### 方式 A：网页上传，适合不熟悉命令行

1. 解压 `lof-fund-monitor-optimized-v1.9a.zip`。
2. 进入解压后的文件夹，确认能看到 `server.py`、`requirements.txt`、`.github`、`scripts` 等文件。
3. 回到刚创建的 GitHub 仓库页面。
4. 如果页面显示 `Quick setup`，点击 `uploading an existing file`。
5. 如果没有看到这个链接，点击页面右侧或上方的 `Add file`，再点 `Upload files`。
6. 把解压后文件夹里的所有内容拖到上传区域。注意要拖“文件夹里面的内容”，不要只拖整个压缩包。
7. 页面下方 `Commit changes` 区域保持默认即可。
8. 点击绿色按钮 `Commit changes`。

### 方式 B：Git 命令上传，适合熟悉命令行

```bash
git init
git add .
git commit -m "Deploy LOF monitor with GitHub Actions"
git branch -M main
git remote add origin https://github.com/你的用户名/你的仓库名.git
git push -u origin main
```

## 3. 配置微信推送 Secret

1. 打开你的 GitHub 仓库主页。
2. 点击仓库上方的 `Settings`。
3. 左侧栏找到 `Security` 区域。
4. 点击 `Secrets and variables`。
5. 点击它下面的 `Actions`。
6. 默认会进入 `Secrets` 标签页；如果没有，点击上方 `Secrets`。
7. 点击 `New repository secret`。
8. 推荐 `Name` 填：`SERVERCHAN_SENDKEYS`。
9. `Secret` 填你的 Server酱 SendKey；如果要同时推送给两个以上微信，填多个 SendKey，用英文逗号、分号、空格或换行分隔，例如：`SCTxxxx1,SCTxxxx2`。
10. 点击 `Add secret`。

兼容旧变量名：如果你已经配置了 `WECHAT_SEND_KEY`，可以继续使用；v1.9a 也支持在 `WECHAT_SEND_KEY` 里填写多个 SendKey。若 `SERVERCHAN_SENDKEYS` 和 `WECHAT_SEND_KEY` 同时存在，系统会合并去重后逐个发送。

没有配置 `SERVERCHAN_SENDKEYS` 或 `WECHAT_SEND_KEY` 时，工作流仍会运行和刷新数据，但不会发送微信告警。

## 4. 配置可选 Variables

仍然在 `Settings -> Secrets and variables -> Actions` 页面：

1. 点击上方 `Variables` 标签页。
2. 点击 `New repository variable`。
3. 按需新增下面这些变量。

| 变量名 | 推荐值 | 说明 |
|---|---:|---|
| `WECHAT_PUSH_ENABLED` | `1` | 启用定时阈值告警 |
| `WECHAT_PUSH_TIME` | `09:35,10:00,10:30,11:00,11:25,13:05,13:30,14:00,14:30,14:55` | 告警检查时间点 |
| `PREMIUM_ALERT_ENABLED` | `1` | 启用溢价告警 |
| `DISCOUNT_ALERT_ENABLED` | `1` | 启用折价告警 |
| `PREMIUM_UPPER` | `3` | 溢价率大于等于 3% 告警 |
| `DISCOUNT_LOWER` | `-5` | 折价率小于等于 -5% 告警 |
| `MIN_TURNOVER` | `60` | 成交额至少 60 万元 |
| `WECHAT_PUSH_GRACE_SECONDS` | `90` | 用于允许启动/调度轻微滞后的推送时间窗口；v1.9a 起不再等待数据刷新锁 |
| `ACTIONS_PUSH_GRACE_SECONDS` | `90` | 当结束时间与微信推送时间重合时，额外等待的保护秒数 |
| `ACTIONS_REFRESH_HOLDINGS` | `0` | Actions 中默认不跑完整持仓刷新；设为 `1` 可恢复 |
| `ACTIONS_FETCH_PURCHASE_STATUS` | `0` | 默认优先用 AkShare `fund_purchase_em` 批量状态；失败自动回退原接口，设为 `1` 可额外强制逐只刷新 |
| `AKSHARE_FUND_CACHE_TTL` | `60` | AkShare 批量行情快照缓存秒数，减少重复请求 |
| `AKSHARE_FUND_HTTP_RETRIES` | `3` | AkShare/EastMoney 断连或超时后的重试次数 |
| `AKSHARE_FUND_HTTP_TIMEOUT` | `8` | 单次 AkShare/EastMoney 请求超时秒数 |
| `AKSHARE_FUND_ESTIMATION_TIMEOUT` | `5` | `fund_value_estimation_em` 每个估值分类的最长等待秒数，避免慢分类阻塞刷新 |
| `AKSHARE_FUND_ESTIMATION_SYMBOLS` | `LOF,场内交易基金,QDII` | AkShare 估值分类；如“场内交易基金”经常超时，可改为 `LOF,QDII` |
| `WECHAT_AKSHARE_OVERLAY_TIMEOUT` | `5` | 微信到点推送时 AkShare 快照叠加最长等待秒数；超时后直接使用已保存实时数据 |

如果不配置这些 Variables，脚本会使用上面的默认值。v1.9a 会优先使用 AkShare 批量快照做行情、IOPV、折溢价率、申购/赎回状态和微信阈值筛选；缺失字段再走原有接口。

## 5. 启用并手动测试 GitHub Actions

1. 打开仓库主页。
2. 点击上方 `Actions` 标签。
3. 如果看到提示 `Workflows aren’t being run on this forked repository` 或类似安全提示，点击 `I understand my workflows, go ahead and enable them`。
4. 左侧工作流列表中点击 `LOF Fund Monitor v1.9a 09:30-15:00`。
5. 右侧点击 `Run workflow`。
6. `Branch` 选择 `main`。
7. `run_until` 可以临时填一个离当前北京时间较近的结束时间，例如当前 14:10 就填 `14:20`；正式运行可填 `15:00` 或留默认。
8. 点击绿色按钮 `Run workflow`。
9. 等待几秒，页面出现一条新的运行记录后点击进去。
10. 点击 job 名称 `Run LOF monitor v1.9a until 15:00 CST` 查看日志。
11. 日志中看到 `[OK] LOF 监控服务已在 GitHub Actions 启动` 表示成功。

## 6. 确认自动定时

`.github/workflows/fund-monitor.yml` 当前配置：

```yaml
schedule:
  # GitHub Actions cron 使用 UTC；01:30 UTC = 北京时间 09:30
  - cron: '30 1 * * *'
```

含义是：每天北京时间 09:30 启动。

如果只想周一到周五运行，把它改成：

```yaml
schedule:
  # 工作日北京时间 09:30，对应 UTC 01:30
  - cron: '30 1 * * 1-5'
```

结束时间不靠 cron 触发，而是由 `scripts/run_actions_monitor.py` 在 runner 内部守到 15:00 后自动停止。

## 7. 查看运行结果和排错

- 查看运行记录：仓库主页 -> `Actions` -> `LOF Fund Monitor v1.9a 09:30-15:00` -> 点击当天运行记录。
- 查看具体日志：进入运行记录后，点击左侧或中间的 job 名称。
- 依赖安装失败：确认 `requirements.txt` 在仓库根目录。
- 没收到微信：确认 `SERVERCHAN_SENDKEYS` 或 `WECHAT_SEND_KEY` 是 Secret，不是 Variable；确认 Variables 中 `WECHAT_PUSH_ENABLED=1`；查看 Actions 初始化日志中的“接收方 N 个”。
- 09:30 未准点启动：GitHub schedule 可能会因为平台负载延迟，尤其整点附近更明显。9:30 已经避开整点，但仍不保证秒级准时。
- 私有仓库分钟数不足：改用 Public 仓库、自托管 runner，或改成每 5 分钟触发一次短任务而不是长时间占用 runner。

## 8. 不要提交这些文件

- `lof_fund.db`：本地 SQLite 数据库，可能包含微信 SendKey 或运行记录。
- `duckdns_config.json`：可能包含 DuckDNS token。
- `.env`：可能包含密钥。

本压缩包已故意不包含 `lof_fund.db`，避免把你的本地密钥提交到 GitHub。
