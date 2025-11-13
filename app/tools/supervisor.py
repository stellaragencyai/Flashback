#!/usr/bin/env python3
# Flashback — Supervisor (Simplified)
# Keeps all core bots running. If any crash, it restarts them automatically.
# Sends clear Telegram-style messages: “bot stopped → restarted”.

import subprocess
import time
import sys
import os
import signal
import contextlib
import requests
from dotenv import load_dotenv

# Load Telegram credentials from .env
load_dotenv()
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

# Bots to supervise
BOTS = [
    "app.bots.breaker_watch",
    "app.bots.tier_enforcer",
    "app.bots.risk_guardian",
    "app.bots.tp_sl_manager",
    "app.bots.trade_journal",
    "app.bots.volatility_scout",
]

procs = {}

def start(mod: str) -> subprocess.Popen:
    """Start a bot and log its output."""
    log_dir = "app/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{mod.replace('.', '_')}.log")

    print(f"▶ Starting {mod}")
    send_tg(f"✅ Bot started: {mod.split('.')[-1]} is now running.")
    return subprocess.Popen(
        [sys.executable, "-m", mod],
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
