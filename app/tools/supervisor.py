#!/usr/bin/env python3
# Flashback — Supervisor v4.1 (Root-aware + Subaccount Status + Central Notifier)
#
# What this does:
# - Forces project root as working directory so imports and .env are consistent.
# - Loads .env from project root explicitly.
# - On startup:
#     • Checks Bybit connectivity for MAIN + flashback01..flashback10
#     • Sends a Telegram "boot report" with subaccount status + planned bots
# - Keeps all core bots running; auto-restarts on crash.
# - Logs each bot's stdout/stderr to app/logs/*.log
# - Sends Telegram alerts on bot start/crash + periodic heartbeat.
#
# Expected .env keys (with some fallbacks):
#
#   BYBIT_BASE=https://api.bybit.com   (optional; defaults to mainnet)
#
#   # Preferred:
#   BYBIT_MAIN_API_KEY=...
#   BYBIT_MAIN_API_SECRET=...
#
#   # Fallbacks for MAIN (if above not set):
#   BYBIT_MAIN_READ_KEY=...
#   BYBIT_MAIN_READ_SECRET=...
#
#   # Subaccounts (optional until you wire them):
#   BYBIT_FLASHBACK01_API_KEY=...
#   BYBIT_FLASHBACK01_API_SECRET=...
#   ...
#   BYBIT_FLASHBACK10_API_KEY=...
#   BYBIT_FLASHBACK10_API_SECRET=...
#
#   TG_TOKEN_MAIN=...
#   TG_CHAT_MAIN=...
#
#   # Optional:
#   SUPERVISOR_HEARTBEAT_SEC=300

import subprocess
import time
import sys
import os
import signal
import contextlib
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import hmac
import hashlib
import requests
import traceback
from dotenv import load_dotenv

from app.core.notifier_bot import get_notifier

SUPERVISOR_VERSION = "4.1"

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

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")

# Supervisor heartbeat interval (seconds), overridable via env
HEARTBEAT_INTERVAL = int(os.getenv("SUPERVISOR_HEARTBEAT_SEC", "300"))

# Central notifier
tg = get_notifier("main")

# ---------- TELEGRAM HELPERS ----------

def _tg_configured() -> bool:
    """Return True if central notifier has a usable token + chat."""
    return bool(getattr(tg, "token", None) and getattr(tg, "chat_id", None))

def send_tg(msg: str) -> None:
    """
    Send a Telegram message via the central notifier.
    Safe: will not crash supervisor if Telegram fails or is misconfigured.
    """
    if not _tg_configured():
        # Still print locally so you see *something* in logs
        print(f"[SUPERVISOR][TG disabled] {msg}")
        return
    try:
        tg.info(msg)
    except Exception:
        # Never let Telegram kill the supervisor
        print(f"[SUPERVISOR][TG error] {msg}")

# ---------- BYBIT AUTH & SUBACCOUNT CHECKS ----------

def _bybit_signed_get(
    path: str,
    query: Dict[str, str],
    api_key: str,
    api_secret: str,
    base_url: str,
    timeout: float = 7.0,
) -> requests.Response:
    """
    Minimal Bybit v5 signed GET helper.

    Signature rule (v5 GET):
      sign = HMAC_SHA256(secret, timestamp + api_key + recv_window + queryString)
      queryString is the URL-encoded query in key-sorted order: k1=v1&k2=v2...
    """
    ts = str(int(time.time() * 1000))
    recv_window = "5000"

    # Build sorted query string
    items = sorted(query.items())
    query_string = "&".join(f"{k}={v}" for k, v in items)

    pre_sign = f"{ts}{api_key}{recv_window}{query_string}"
    sign = hmac.new(
        api_secret.encode("utf-8"),
        pre_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": sign,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv_window,
    }

    url = f"{base_url}{path}"
    resp = requests.get(url, headers=headers, params=query, timeout=timeout)
    return resp


def _load_subaccount_creds(prefix: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Load key/secret from env using the given prefix.

    Example: prefix="BYBIT_FLASHBACK01" ->
         BYBIT_FLASHBACK01_API_KEY
         BYBIT_FLASHBACK01_API_SECRET

    For MAIN, also supports your existing naming pattern:
        BYBIT_MAIN_READ_KEY / BYBIT_MAIN_READ_SECRET
        BYBIT_MAIN_TRADE_KEY / BYBIT_MAIN_TRADE_SECRET
    """
    key = os.getenv(f"{prefix}_API_KEY")
    secret = os.getenv(f"{prefix}_API_SECRET")

    if prefix == "BYBIT_MAIN":
        # Fallbacks if the *_API_* variant is not set
        if not key:
            key = os.getenv("BYBIT_MAIN_READ_KEY") or os.getenv("BYBIT_MAIN_TRADE_KEY")
        if not secret:
            secret = os.getenv("BYBIT_MAIN_READ_SECRET") or os.getenv("BYBIT_MAIN_TRADE_SECRET")

    return key, secret


def check_subaccount(label: str, prefix: str) -> Dict[str, str]:
    """
    Check a single subaccount:
      - If creds missing: status = MISSING_CREDS
      - If Bybit wallet-balance call works: status = OK, equity string
      - Else: status = ERROR, detail with retCode/retMsg or exception
    """
    api_key, api_secret = _load_subaccount_creds(prefix)
    if not api_key or not api_secret:
        return {
            "label": label,
            "prefix": prefix,
            "status": "MISSING_CREDS",
            "equity": "",
            "detail": "missing API_KEY / API_SECRET in .env (or MAIN_READ/TRADE_* for MAIN)",
        }

    try:
        resp = _bybit_signed_get(
            "/v5/account/wallet-balance",
            {"accountType": "UNIFIED", "coin": "USDT"},
            api_key=api_key,
            api_secret=api_secret,
            base_url=BYBIT_BASE,
        )
    except Exception as e:
        return {
            "label": label,
            "prefix": prefix,
            "status": "ERROR",
            "equity": "",
            "detail": f"network err: {type(e).__name__}",
        }

    try:
        data = resp.json()
    except Exception:
        return {
            "label": label,
            "prefix": prefix,
            "status": "ERROR",
            "equity": "",
            "detail": f"http {resp.status_code}, invalid JSON",
        }

    ret_code = data.get("retCode")
    ret_msg = data.get("retMsg", "")

    if ret_code == 0:
        # Try to extract some equity info for the report
        equity_str = ""
        try:
            lst = data.get("result", {}).get("list", [])
            if lst:
                acct = lst[0]
                equity_str = acct.get("totalEquity") or acct.get("totalWalletBalance") or ""
        except Exception:
            equity_str = ""

        return {
            "label": label,
            "prefix": prefix,
            "status": "OK",
            "equity": equity_str,
            "detail": "",
        }

    return {
        "label": label,
        "prefix": prefix,
        "status": "ERROR",
        "equity": "",
        "detail": f"retCode={ret_code}, retMsg={ret_msg}",
    }


def check_all_subaccounts() -> List[Dict[str, str]]:
    """
    Check MAIN + flashback01..flashback10 and return a list of status dicts.
    """
    subconfigs = [
        {"label": "MAIN",         "prefix": "BYBIT_MAIN"},
        {"label": "flashback01",  "prefix": "BYBIT_FLASHBACK01"},
        {"label": "flashback02",  "prefix": "BYBIT_FLASHBACK02"},
        {"label": "flashback03",  "prefix": "BYBIT_FLASHBACK03"},
        {"label": "flashback04",  "prefix": "BYBIT_FLASHBACK04"},
        {"label": "flashback05",  "prefix": "BYBIT_FLASHBACK05"},
        {"label": "flashback06",  "prefix": "BYBIT_FLASHBACK06"},
        {"label": "flashback07",  "prefix": "BYBIT_FLASHBACK07"},
        {"label": "flashback08",  "prefix": "BYBIT_FLASHBACK08"},
        {"label": "flashback09",  "prefix": "BYBIT_FLASHBACK09"},
        {"label": "flashback10",  "prefix": "BYBIT_FLASHBACK10"},
    ]

    results: List[Dict[str, str]] = []
    for cfg in subconfigs:
        try:
            res = check_subaccount(cfg["label"], cfg["prefix"])
        except Exception as e:
            res = {
                "label": cfg["label"],
                "prefix": cfg["prefix"],
                "status": "ERROR",
                "equity": "",
                "detail": f"check-exc: {type(e).__name__}",
            }
        results.append(res)
    return results


def format_boot_report(subs: List[Dict[str, str]], bots: List[str]) -> str:
    """
    Build a human-readable boot report for Telegram.
    """
    lines: List[str] = []
    lines.append(f"🚀 Flashback Supervisor v{SUPERVISOR_VERSION} Booted")
    lines.append("")
    lines.append("Subaccounts status:")

    for s in subs:
        label = s["label"]
        status = s["status"]
        equity = s.get("equity") or ""
        detail = s.get("detail") or ""

        if status == "OK":
            icon = "✅"
            eq_str = f" | equity≈{equity}" if equity else ""
            lines.append(f"  {icon} {label}{eq_str}")
        elif status == "MISSING_CREDS":
            icon = "⛔"
            lines.append(f"  {icon} {label} (missing creds)")
        else:
            icon = "⚠️"
            detail_short = detail if len(detail) <= 60 else detail[:57] + "..."
            lines.append(f"  {icon} {label} (error: {detail_short})")

    lines.append("")
    lines.append("Bots to supervise:")
    for b in bots:
        short = b.split(".")[-1]
        lines.append(f"  • {short}")

    return "\n".join(lines)

# ---------- BOT LIST ----------

# app/tools/supervisor.py → BOTS list
BOTS: List[str] = [
    "app.bots.breaker_watch",
    "app.bots.tier_enforcer",
    "app.bots.risk_guardian",
    "app.bots.tp_sl_manager",
    "app.bots.trade_journal",
    "app.bots.volatility_scout",
    "app.bots.profit_sweeper",
    "app.bots.per_position_drip",
]

procs: Dict[str, subprocess.Popen] = {}
restart_counts: Dict[str, int] = {}

# ---------- BOT PROCESS MANAGEMENT ----------

def start(mod: str) -> subprocess.Popen:
    """Start a bot and log its output."""
    log_dir = APP_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"{mod.replace('.', '_')}.log"

    print(f"[START] {mod}")
    send_tg(f"✅ Bot started: {mod.split('.')[-1]} is now running.")

    # Use ROOT_DIR as the working directory so imports and paths are stable
    return subprocess.Popen(
        [sys.executable, "-m", mod],
        cwd=str(ROOT_DIR),
        stdout=open(log_path, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )


def stop_all() -> None:
    """Stop all bots when exiting."""
    print("\n[STOP] Stopping all bots...")
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
    print("[STOP] All bots stopped successfully.")

# ---------- MAIN LOOP ----------

def main() -> None:
    print(f"Flashback Supervisor v{SUPERVISOR_VERSION}")
    print(f"Project root: {ROOT_DIR}")
    print(f"Using .env:   {ENV_PATH} (exists={ENV_PATH.exists()})")
    print(f"TG configured: {'yes' if _tg_configured() else 'no'}")
    print(f"Bybit base:   {BYBIT_BASE}")
    print(f"Heartbeat:    {HEARTBEAT_INTERVAL} sec")

    # 1) Subaccount status check + boot report
    try:
        subs = check_all_subaccounts()
    except Exception as e:
        subs = []
        send_tg(f"⚠️ Subaccount check failed: {type(e).__name__}")
    if subs:
        boot_msg = format_boot_report(subs, BOTS)
        send_tg(boot_msg)

    # 2) Start all bots
    for m in BOTS:
        procs[m] = start(m)
        restart_counts[m] = 0

    start_ts = time.time()
    next_heartbeat = start_ts + HEARTBEAT_INTERVAL

    try:
        while True:
            # Check each bot
            for m, p in list(procs.items()):
                if p.poll() is not None:
                    bot_name = m.split(".")[-1]
                    restart_counts[m] = restart_counts.get(m, 0) + 1
                    msg = f"⚠️ {bot_name} crashed. Restarting it now... (restart #{restart_counts[m]})"
                    print(msg)
                    send_tg(msg)
                    time.sleep(2)
                    procs[m] = start(m)

            # Periodic heartbeat
            now = time.time()
            if now >= next_heartbeat:
                alive = sum(1 for p in procs.values() if p.poll() is None)
                total = len(BOTS)
                total_restarts = sum(restart_counts.values())
                uptime_min = int((now - start_ts) / 60)
                hb = (
                    f"🩺 Flashback Supervisor heartbeat (v{SUPERVISOR_VERSION})\n"
                    f"- Uptime: {uptime_min} min\n"
                    f"- Bots running: {alive}/{total}\n"
                    f"- Total restarts: {total_restarts}"
                )
                send_tg(hb)
                next_heartbeat = now + HEARTBEAT_INTERVAL

            time.sleep(2)
    except KeyboardInterrupt:
        stop_all()
    except Exception:
        tb = traceback.format_exc()
        msg = f"❌ Supervisor fatal error:\n{tb}"
        print(msg)
        if _tg_configured():
            try:
                tg.error(msg)
            except Exception:
                pass
        stop_all()


if __name__ == "__main__":
    main()
