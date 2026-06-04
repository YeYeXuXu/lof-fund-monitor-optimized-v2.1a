# LOF 基金折溢价监控 — Windows 全自动安装脚本
# 用法: irm https://raw.githubusercontent.com/ctz168/fund/main/install.ps1 | iex
#
# 全自动：检测 Python → 安装依赖 → 克隆仓库 → 启动服务 → 打开浏览器

$ErrorActionPreference = "Stop"
$InstallDir = if ($env:FUND_INSTALL_DIR) { $env:FUND_INSTALL_DIR } else { "$env:USERPROFILE\lof-fund" }
$Port = if ($env:FUND_PORT) { $env:FUND_PORT } else { "8080" }
$RepoUrl = "https://github.com/ctz168/fund.git"

Write-Host ""
Write-Host "══════════════════════════════════════════════"
Write-Host "  LOF 基金折溢价监控 — 全自动安装 (Windows)"
Write-Host "══════════════════════════════════════════════"
Write-Host ""

# ---------- 检测 Python ----------
function Find-Python {
    $pyCommands = @("python", "python3", "py")
    foreach ($cmd in $pyCommands) {
        try {
            $result = & $cmd --version 2>&1
            if ($result -match "Python (\d+)\.(\d+)") {
                $major = [int]$Matches[1]
                $minor = [int]$Matches[2]
                if ($major -ge 3 -and $minor -ge 9) {
                    Write-Host "[ OK ]  找到 Python: $result" -ForegroundColor Green
                    return $cmd
                }
            }
        } catch {}
    }
    return $null
}

# ---------- 检测 Git ----------
function Find-Git {
    try {
        $result = & git --version 2>&1
        Write-Host "[ OK ]  找到 Git: $result" -ForegroundColor Green
        return $true
    } catch {
        return $false
    }
}

# ---------- 克隆仓库 ----------
function Clone-Repo {
    if (Test-Path "$InstallDir\.git") {
        Write-Host "[INFO]  仓库已存在，拉取最新代码..." -ForegroundColor Cyan
        Push-Location $InstallDir
        git pull --ff-only 2>$null
        Pop-Location
    } else {
        Write-Host "[INFO]  克隆仓库到 $InstallDir ..." -ForegroundColor Cyan
        git clone --depth 1 $RepoUrl $InstallDir
    }
    Write-Host "[ OK ]  代码就绪" -ForegroundColor Green
}

# ---------- 创建虚拟环境 & 安装依赖 ----------
function Setup-Venv {
    $venvPython = "$InstallDir\.venv\Scripts\python.exe"
    $venvPip = "$InstallDir\.venv\Scripts\pip.exe"

    if (Test-Path $venvPython) {
        Write-Host "[ OK ]  虚拟环境已存在" -ForegroundColor Green
        return @{ Python = $venvPython; Pip = $venvPip }
    }

    $py = Find-Python
    if (-not $py) {
        Write-Host "[ERR]  未找到 Python 3.9+，请先安装: https://www.python.org/downloads/" -ForegroundColor Red
        Write-Host "       安装时请勾选 'Add to PATH'" -ForegroundColor Red
        exit 1
    }

    Write-Host "[INFO]  创建虚拟环境..." -ForegroundColor Cyan
    & $py -m venv "$InstallDir\.venv"

    if (Test-Path $venvPip) {
        Write-Host "[INFO]  安装依赖..." -ForegroundColor Cyan
        & $venvPip install -q -r "$InstallDir\requirements.txt"
    } else {
        Write-Host "[INFO]  安装依赖..." -ForegroundColor Cyan
        & $py -m pip install -q -r "$InstallDir\requirements.txt"
    }

    Write-Host "[ OK ]  依赖安装完成" -ForegroundColor Green
    return @{ Python = $venvPython; Pip = $venvPip }
}

# ---------- 初始化数据库 ----------
function Init-Data {
    $venvPython = "$InstallDir\.venv\Scripts\python.exe"
    $py = if (Test-Path $venvPython) { $venvPython } else { (Find-Python) }

    Write-Host "[INFO]  初始化数据库并预置测试基金..." -ForegroundColor Cyan
    Push-Location $InstallDir
    & $py -c @"
import asyncio, sys, os
sys.path.insert(0, '.')
from db import init_db, add_fund, get_all_funds

async def main():
    await init_db()
    funds = await get_all_funds()
    if not funds:
        await add_fund('161831', '银华恒生国企指数(QDII-LOF)A', 'sz', 'holdings')
        await add_fund('161124', '易方达香港小型股指数A', 'sz', 'holdings')
        print('  已预置测试基金: 161831, 161124')
    else:
        print(f'  数据库已有 {len(funds)} 只基金')
asyncio.run(main())
"@
    Pop-Location
    Write-Host "[ OK ]  数据库就绪" -ForegroundColor Green
}

# ---------- 启动服务器 ----------
function Start-Server {
    $venvPython = "$InstallDir\.venv\Scripts\python.exe"
    $py = if (Test-Path $venvPython) { $venvPython } else { (Find-Python) }

    $url = "http://localhost:$Port"

    Write-Host ""
    Write-Host "  ╔══════════════════════════════════════════╗"
    Write-Host "  ║   LOF 基金折溢价监控  —  运行中         ║"
    Write-Host "  ╠══════════════════════════════════════════╣"
    Write-Host "  ║  监控页面: $url          ║"
    Write-Host "  ║  后台管理: ${url}/admin     ║"
    Write-Host "  ║  按 Ctrl+C 停止服务                      ║"
    Write-Host "  ╚══════════════════════════════════════════╝"
    Write-Host ""

    # 打开浏览器
    Start-Process $url

    # 设置端口并启动
    $env:FUND_PORT = $Port
    Push-Location $InstallDir
    & $py server.py
    Pop-Location
}

# ---------- 主流程 ----------
# 1. 检查 Python
$py = Find-Python
if (-not $py) {
    Write-Host "[ERR]  未找到 Python 3.9+，请先安装: https://www.python.org/downloads/" -ForegroundColor Red
    exit 1
}

# 2. 检查 Git
if (-not (Find-Git)) {
    Write-Host "[ERR]  未找到 Git，请先安装: https://git-scm.com/downloads" -ForegroundColor Red
    exit 1
}

# 3. 克隆仓库
Clone-Repo

# 4. 创建虚拟环境 & 安装依赖
Setup-Venv

# 5. 初始化数据库
Init-Data

# 6. 启动服务器
Start-Server
