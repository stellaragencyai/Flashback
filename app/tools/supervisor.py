#!/usr/bin/env python3
# Flashback — Supervisor v2 (Root-aware)
# Keeps all core bots running. If any crash, it restarts them automatically.
# Loads .env from the project root so tokens/keys actually exist.
# Logs each bot’s output to app/logs/*.log and sends Telegram updates.

import subprocess
import time
import sys
import os
import signal
import contextlib
from pathlib import Path

import requests
from dotenv import load_dotenv

# ---------- PATHS & ENV ----------

# This file is expected at: project_root/app/tools/supervisor.py
THIS_FILE = Path(__file__).resolve()
TOOLS_DIR = THIS_FILE.parent          # .../app/tools
APP_DIR = TOOLS_DIR.parent            # .../app
ROOT_DIR = APP_DIR.parent             # project_root

# Ensure we always behave as if running from project_root
os.chdir(ROOT_DIR)

# Load .env from project root explicitly
ENV_PATH = ROOT_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

TG_TOKEN = os.getenv("TG_TOKEN_MAIN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_MAIN", "")

def send_tg(msg: str):
    """Send a Telegram message (safe, won’t crash if Telegram fails)."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg},
            timeout=6,
        )
    except Exception:
        pass

# ---------- BOT LIST ----------

# app/tools/supervisor.py → BOTS list
BOTS = [
    "app.bots.breaker_watch",
    "app.bots.tier_enforcer",
    "app.bots.risk_guardian",
    "app.bots.tp_sl_manager",
    "app.bots.trade_journal",
    "app.bots.volatility_scout",
    "app.bots.profit_sweeper",
    "app.bots.per_position_drip",
]

procs = {}

def start(mod: str) -> subprocess.Popen:
    """Start a bot and log its output."""
    log_dir = APP_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"{mod.replace('.', '_')}.log"

    print(f"▶ Starting {mod}")
    send_tg(f"✅ Bot started: {mod.split('.')[-1]} is now running.")

    # Use ROOT_DIR as the working directory so imports and paths are stable
    return subprocess.Popen(
        [sys.executable, "-m", mod],
        cwd=str(ROOT_DIR),
        stdout=open(log_path, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )

def stop_all():
    """Stop all bots when exiting."""
    print("\n🛑 Stopping all bots...")
    send_tg("🛑 All Flashback bots are stopping now.")
    for m, p in procs.items():
        with contextlib.suppress(Exception):
            print(f" - Stopping {m}")
            p.send_signal(signal.SIGTERM)
    time.sleep(2)
    for m, p in procs.items():
        with contextlib.suppress(Exception):
            if p.poll() is None:
                p.kill()
    send_tg("✅ All bots stopped successfully.")
    print("✅ All bots stopped successfully.")

def main():
    print(f"Project root: {ROOT_DIR}")
    print(f"Using .env:   {ENV_PATH} (exists={ENV_PATH.exists()})")
    print(f"TG configured: {'yes' if TG_TOKEN and TG_CHAT_ID else 'no'}")

    for m in BOTS:
        procs[m] = start(m)

    try:
        while True:
            for m, p in list(procs.items()):
                if p.poll() is not None:
                    bot_name = m.split('.')[-1]
                    msg = f"⚠ {bot_name} crashed. Restarting it now..."
                    print(msg)
                    send_tg(msg)
                    time.sleep(2)
                    procs[m] = start(m)
            time.sleep(2)
    except KeyboardInterrupt:
        stop_all()
    except Exception as e:
        send_tg(f"❌ Supervisor error: {e}")
        stop_all()

if __name__ == "__main__":
    main()
