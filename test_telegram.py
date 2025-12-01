#!/usr/bin/env python3
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"

print(f"Loading .env from: {ENV_PATH} (exists={ENV_PATH.exists()})")
load_dotenv(ENV_PATH)

token = os.getenv("TG_TOKEN_MAIN")
chat_id = os.getenv("TG_CHAT_MAIN")

print(f"TG_TOKEN_MAIN present: {bool(token)}")
print(f"TG_CHAT_MAIN: {chat_id!r}")

if not token or not chat_id:
    print("ERROR: Missing TG_TOKEN_MAIN or TG_CHAT_MAIN in .env")
    raise SystemExit(1)

msg = "Test message from test_telegram.py"

resp = requests.post(
    f"https://api.telegram.org/bot{token}/sendMessage",
    json={"chat_id": chat_id, "text": msg},
    timeout=10,
)

print("HTTP status:", resp.status_code)
print("Response body:", resp.text)
