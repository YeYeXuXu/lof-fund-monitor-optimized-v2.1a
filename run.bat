@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

echo.
echo ════════════════════════════════════════════
echo   LOF 基金折溢价监控 — 一键部署 (Windows)
echo ════════════════════════════════════════════
echo.

set "DIR=%~dp0"
set "VENV=%DIR%.venv"
set "PORT=8080"

REM 检测 Python
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERR] 未检测到 Python，请先安装 Python 3.9+
    echo       https://www.python.org/downloads/
    pause
    exit /b 1
)

python --version 2>&1 | findstr /R "3\.[1-9][0-9]" >nul
if %errorlevel% neq 0 (
    echo [ERR] 需要 Python 3.9+，请升级
    pause
    exit /b 1
)
echo [ OK ] Python 检测通过

REM 创建虚拟环境
if not exist "%VENV%\Scripts\python.exe" (
    echo [INFO] 创建虚拟环境...
    python -m venv "%VENV%"
    echo [ OK ] 虚拟环境创建完成
)

REM 安装依赖
echo [INFO] 安装依赖...
"%VENV%\Scripts\pip.exe" install -q -r "%DIR%requirements.txt"
echo [ OK ] 依赖安装完成

REM 初始化数据库
echo [INFO] 初始化数据库并预置测试基金...
cd /d "%DIR%"
"%VENV%\Scripts\python.exe" -c "import asyncio,sys;sys.path.insert(0,'.');from db import init_db,add_fund,get_all_funds;async def m():await init_db();f=await get_all_funds();await add_fund('161831','银华恒生国企指数(QDII-LOF)A','sz','holdings') if not f else None;await add_fund('161124','易方达香港小型股指数A','sz','holdings') if not f else None;print('OK');asyncio.run(m())" 2>nul
echo [ OK ] 数据库就绪

REM 打开浏览器
echo [INFO] 正在打开浏览器...
start http://localhost:%PORT%

REM 启动服务器
echo.
echo   ╔══════════════════════════════════════════╗
echo   ║   LOF 基金折溢价监控  —  运行中         ║
echo   ╠══════════════════════════════════════════╣
echo   ║  监控页面: http://localhost:%PORT%          ║
echo   ║  后台管理: http://localhost:%PORT%/admin     ║
echo   ║  按 Ctrl+C 停止服务                      ║
echo   ╚══════════════════════════════════════════╝
echo.

"%VENV%\Scripts\python.exe" server.py
