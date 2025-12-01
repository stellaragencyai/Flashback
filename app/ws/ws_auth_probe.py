#!/usr/bin/env python3
import os
import time
import json
import hmac
import hashlib
import websocket  # websocket-client, not websockets

URL = os.getenv("BYBIT_WS_PRIVATE_URL", "wss://stream.bybit.com/v5/private")
API_KEY = os.getenv("BYBIT_MAIN_API_KEY")
API_SECRET = os.getenv("BYBIT_MAIN_API_SECRET")

assert API_KEY and API_SECRET, "Missing BYBIT_MAIN_API_KEY/SECRET in env"

expires = int((time.time() + 1) * 1000)
msg = f"GET/realtime{expires}".encode("utf-8")
sig = hmac.new(API_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()

payload = {
    "op": "auth",
    "args": [API_KEY, expires, sig],
}

print("URL:", URL)
print("Auth payload:", payload)

ws = websocket.WebSocketApp(
    URL,
    on_message=lambda ws, m: print("MSG:", m),
    on_error=lambda ws, e: print("ERR:", e),
    on_close=lambda ws, c, r: print("CLOSE:", c, r),
    on_open=lambda ws: ws.send(json.dumps(payload)),
)

ws.run_forever()
