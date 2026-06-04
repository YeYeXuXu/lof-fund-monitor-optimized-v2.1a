#!/usr/bin/env bash
# ============================================================
#  LOF 基金折溢价监控 — 全自动安装脚本
#  用法: curl -fsSL https://raw.githubusercontent.com/ctz168/fund/main/install.sh | bash
#
#  全自动：检测平台 → 安装 Python 依赖 → 克隆仓库 → 启动服务 → 打开浏览器
# ============================================================
set -e

# ---------- 配置 ----------
INSTALL_DIR="${FUND_INSTALL_DIR:-$HOME/lof-fund}"
PORT="${FUND_PORT:-8080}"
REPO_URL="https://github.com/ctz168/fund.git"

# ---------- 颜色 ----------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { printf "${CYAN}[INFO]${NC}  %s\n" "$1"; }
ok()    { printf "${GREEN}[ OK ]${NC}  %s\n" "$1"; }
warn()  { printf "${YELLOW}[WARN]${NC}  %s\n" "$1"; }
err()   { printf "${RED}[ERR]${NC}  %s\n" "$1"; }

echo ""
echo "══════════════════════════════════════════════"
echo "  LOF 基金折溢价监控 — 全自动安装"
echo "══════════════════════════════════════════════"
echo ""

# ---------- 检测平台 ----------
detect_platform() {
    local uname_s="$(uname -s)"
    case "$uname_s" in
        Linux)
            if [ -f /etc/os-release ]; then
                . /etc/os-release
                echo "$ID"
            elif command -v apt-get &>/dev/null; then
                echo "debian"
            elif command -v dnf &>/dev/null; then
                echo "fedora"
            elif command -v apk &>/dev/null; then
                echo "alpine"
            else
                echo "linux"
            fi
            ;;
        Darwin)  echo "macos" ;;
        MINGW*|MSYS*|CYGWIN*) echo "windows" ;;
        *)       echo "unknown" ;;
    esac
}

# ---------- 检测 Termux ----------
is_termux() {
    [ -n "$TERMUX_VERSION" ] || [ -d "/data/data/com.termux" ]
}

# ---------- 安装 Python & Git ----------
install_deps() {
    local platform="$(detect_platform)"
    info "检测到平台: $platform"

    if is_termux; then
        info "检测到 Termux 环境"
        pkg install -y python python-pip git 2>/dev/null || true
        return
    fi

    case "$platform" in
        debian|ubuntu|linuxmint|pop)
            sudo apt-get update -qq
            sudo apt-get install -y -qq python3 python3-pip python3-venv git 2>/dev/null || true
            ;;
        fedora|rhel|centos|rocky|alma)
            sudo dnf install -y python3 python3-pip git 2>/dev/null || \
            sudo yum install -y python3 python3-pip git 2>/dev/null || true
            ;;
        alpine)
            sudo apk add python3 py3-pip git 2>/dev/null || true
            ;;
        arch|manjaro)
            sudo pacman -S --noconfirm python python-pip git 2>/dev/null || true
            ;;
        opensuse*)
            sudo zypper install -y python3 python3-pip git 2>/dev/null || true
            ;;
        macos)
            if ! command -v python3 &>/dev/null; then
                if command -v brew &>/dev/null; then
                    brew install python git 2>/dev/null || true
                else
                    warn "请先安装 Homebrew: https://brew.sh"
                fi
            fi
            ;;
        windows)
            warn "Windows 请使用 PowerShell 安装命令:"
            warn 'irm https://raw.githubusercontent.com/ctz168/fund/main/install.ps1 | iex'
            exit 0
            ;;
        *)
            warn "未识别的平台，尝试继续..."
            ;;
    esac
}

# ---------- 检测 Python ----------
detect_python() {
    for cmd in python3 python; do
        if command -v "$cmd" &>/dev/null; then
            ver=$($cmd -c 'import sys; print(".".join(map(str,sys.version_info[:3])))')
            major=$(echo "$ver" | cut -d. -f1)
            minor=$(echo "$ver" | cut -d. -2)
            if [ "$major" -ge 3 ] && [ "$minor" -ge 9 ]; then
                echo "$cmd"
                return 0
            fi
        fi
    done
    return 1
}

# ---------- 克隆仓库 ----------
clone_repo() {
    if [ -d "$INSTALL_DIR/.git" ]; then
        info "仓库已存在，拉取最新代码..."
        cd "$INSTALL_DIR"
        git pull --ff-only 2>/dev/null || warn "git pull 失败，使用现有代码"
    else
        info "克隆仓库到 $INSTALL_DIR ..."
        git clone --depth 1 "$REPO_URL" "$INSTALL_DIR"
        cd "$INSTALL_DIR"
    fi
    ok "代码就绪"
}

# ---------- 创建虚拟环境 & 安装依赖 ----------
setup_venv() {
    local VENV_DIR="$INSTALL_DIR/.venv"

    if [ -f "$VENV_DIR/bin/python" ]; then
        ok "虚拟环境已存在"
        return 0
    fi

    local PY
    PY=$(detect_python)
    if [ -z "$PY" ]; then
        err "需要 Python >= 3.9，请先安装"
        exit 1
    fi
    ok "使用 $PY ($($PY --version 2>&1))"

    info "创建虚拟环境..."
    "$PY" -m venv "$VENV_DIR" 2>/dev/null || {
        warn "venv 创建失败，尝试直接安装依赖"
    }

    if [ -f "$VENV_DIR/bin/pip" ]; then
        PIP="$VENV_DIR/bin/pip"
    else
        PIP="$PY -m pip"
    fi

    info "安装依赖..."
    if [ -f "$INSTALL_DIR/requirements.txt" ]; then
        $PIP install -q -r "$INSTALL_DIR/requirements.txt" 2>/dev/null || \
        $PIP install -q --break-system-packages -r "$INSTALL_DIR/requirements.txt" 2>/dev/null || {
            err "依赖安装失败，请手动运行: pip install -r requirements.txt"
            exit 1
        }
    fi
    ok "依赖安装完成"
}

# ---------- 初始化数据库 & 预置测试基金 ----------
init_data() {
    local VENV_DIR="$INSTALL_DIR/.venv"
    local PYTHON="$VENV_DIR/bin/python"
    [ ! -f "$PYTHON" ] && PYTHON=$(detect_python)

    info "初始化数据库并预置测试基金..."
    cd "$INSTALL_DIR"
    $PYTHON -c "
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
" 2>/dev/null && ok "数据库就绪" || warn "数据库初始化可能需要稍后重试"
}

# ---------- 打开浏览器 ----------
open_browser() {
    local url="http://localhost:${PORT}"
    info "正在打开浏览器: $url"
    sleep 2
    case "$(uname -s)" in
        Darwin)  open "$url" 2>/dev/null     ;;
        Linux)   xdg-open "$url" 2>/dev/null || sensible-browser "$url" 2>/dev/null ;;
        MINGW*|MSYS*|CYGWIN*) start "$url" 2>/dev/null ;;
        *)       ;;
    esac
    ok "浏览器已打开 (如未自动打开请手动访问 $url)"
}

# ---------- 启动服务器 ----------
start_server() {
    local VENV_DIR="$INSTALL_DIR/.venv"
    local PYTHON="$VENV_DIR/bin/python"
    [ ! -f "$PYTHON" ] && PYTHON=$(detect_python)

    cd "$INSTALL_DIR"
    export FUND_PORT="$PORT"

    echo ""
    echo "  ╔══════════════════════════════════════════╗"
    echo "  ║   LOF 基金折溢价监控  —  运行中         ║"
    echo "  ╠══════════════════════════════════════════╣"
    echo "  ║  监控页面: http://localhost:${PORT}          ║"
    echo "  ║  后台管理: http://localhost:${PORT}/admin     ║"
    echo "  ║  按 Ctrl+C 停止服务                      ║"
    echo "  ╚══════════════════════════════════════════╝"
    echo ""

    exec $PYTHON server.py
}

# ---------- 主流程 ----------
main() {
    # 1. 安装系统依赖
    install_deps

    # 2. 检查 Python
    PY=$(detect_python)
    if [ -z "$PY" ]; then
        err "未找到 Python 3.9+，请安装后重试"
        exit 1
    fi
    ok "Python 就绪: $($PY --version 2>&1)"

    # 3. 检查 Git
    if ! command -v git &>/dev/null; then
        err "未找到 Git，请安装后重试"
        exit 1
    fi
    ok "Git 就绪"

    # 4. 克隆仓库
    clone_repo

    # 5. 创建虚拟环境 & 安装依赖
    setup_venv

    # 6. 初始化数据库
    init_data

    # 7. 打开浏览器
    open_browser &

    # 8. 启动服务器
    start_server
}

main
