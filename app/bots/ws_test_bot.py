#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Switchboard — Test Bot Client

Purpose
-------
- Connects to ws://127.0.0.1:5000/ws/bot
- Sends a HELLO / REGISTER message so the server can track this bot.
- Sends a heartbeat every 5 seconds.
- Prints anything the server sends back.

Use this to confirm:
- The WebSocket handshake works
- Messages flow both ways
"""

import asyncio
import json
import sys
import time

import websockets

SWITCHBOARD_URL = "ws://127.0.0.1:5000/ws/bot"

BOT_ID = "test-echo-bot"
ROLE = "tester"


async def run_bot():
    while True:
        try:
            print(f"[client] Connecting to {SWITCHBOARD_URL} ...")
            async with websockets.connect(SWITCHBOARD_URL) as ws:
                print("[client] Connected ✅")

                # 1) Send HELLO / REGISTER payload
                hello = {
                    "type": "hello",
                    "bot_id": BOT_ID,
                    "role": ROLE,
                    "channels": ["broadcast", "test"],
                    "ts": int(time.time() * 1000),
                }
                await ws.send(json.dumps(hello))
                print(f"[client] -> HELLO sent: {hello}")

                # 2) Start tasks: one to send heartbeats, one to read messages
                async def send_heartbeats():
                    while True:
                        msg = {
                            "type": "heartbeat",
                            "bot_id": BOT_ID,
                            "ts": int(time.time() * 1000),
                        }
                        await ws.send(json.dumps(msg))
                        print("[client] -> heartbeat sent")
                        await asyncio.sleep(5)

                async def recv_loop():
                    async for message in ws:
                        try:
                            data = json.loads(message)
                        except Exception:
                            data = message
                        print(f"[client] <- received: {data}")

                await asyncio.gather(send_heartbeats(), recv_loop())

        except KeyboardInterrupt:
            print("\n[client] Stopping by user request.")
            sys.exit(0)
        except Exception as e:
            print(f"[client] Connection error: {e!r}, retrying in 3 seconds...")
            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(run_bot())
