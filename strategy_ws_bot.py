#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Strategy WS Bot (Skeleton)

Purpose
-------
This is the first "real" WS-based bot that hangs off the Switchboard.

What it does right now:
- Uses SwitchboardClient to:
    • connect to ws://127.0.0.1:5000/ws/bot
    • send HELLO with bot_id / role
    • SUBSCRIBE to some example topics
    • send heartbeats
- Logs:
    • ACKs from the switchboard
    • EVENTS (topic + payload)
    • COMMAND RESULTS
    • ERRORS

What it does NOT yet:
- Talk to Bybit directly
- Apply TP/SL logic or strategy logic
- Place real orders

Later, you'll graft your real bot logic into this skeleton, instead of
every bot inventing its own WebSocket client.

Run:
    python strategy_ws_bot.py

Make sure:
- server.py is running
- ws_client_base.py is in the same folder (or importable)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from ws_client_base import SwitchboardClient  # same folder as this file

log = logging.getLogger("strategy_ws_bot")


class StrategyWSBot(SwitchboardClient):
    """
    Example strategy bot wired to the Switchboard.

    You will eventually:
    - map topics like "events:positions" and "events:orders"
      to your TP/SL Manager, executor, etc.
    """

    def __init__(self) -> None:
        # You can tweak topics later. For now, they're just placeholders.
        super().__init__(
            bot_id="strategy-main",
            role="strategy",
            topics=[
                "test",                 # generic test channel
                "events:positions",     # hypothetical position updates
                "events:orders",        # hypothetical order updates
            ],
        )

    # --------- Overridden handlers --------- #

    async def on_ack(self, message: Dict[str, Any]) -> None:
        """
        Handle ACK messages from the switchboard.
        """
        log.info(f"[strategy] ACK from switchboard: {message}")

    async def on_event(self, topic: str, data: Any, raw: Dict[str, Any]) -> None:
        """
        Handle events published to topics this bot subscribed to.
        """
        log.info(f"[strategy] EVENT topic={topic}, data={data}")

        # Example pattern for future:
        # if topic == "events:positions":
        #     await self._handle_position_event(data)
        # elif topic == "events:orders":
        #     await self._handle_order_event(data)

    async def on_command_result(self, message: Dict[str, Any]) -> None:
        """
        Handle responses to commands this bot sent (e.g. place_order).
        """
        log.info(f"[strategy] COMMAND RESULT: {message}")

    async def on_error(self, message: Dict[str, Any]) -> None:
        """
        Handle error messages from the switchboard.
        """
        log.warning(f"[strategy] ERROR from switchboard: {message}")

    async def on_unknown(self, message: Dict[str, Any]) -> None:
        """
        Handle unknown / untyped messages.
        """
        log.info(f"[strategy] UNKNOWN message: {message}")

    # --------- Future hooks (TP/SL, executor, etc.) --------- #

    # async def _handle_position_event(self, payload: Dict[str, Any]) -> None:
    #     """
    #     This is where TP/SL Manager or position-based logic would plug in.
    #     Example shape of payload could be:
    #         {
    #             "symbol": "PUMPFUNUSDT",
    #             "side": "Buy",
    #             "size": 100,
    #             "entry_price": 3.10,
    #             "liq_price": 2.50,
    #             "unrealizedPnl": 25.4,
    #             ...
    #         }
    #     """
    #     pass

    # async def _handle_order_event(self, payload: Dict[str, Any]) -> None:
    #     """
    #     This could track order fills, cancellations, etc.
    #     """
    #     pass


async def main() -> None:
    """
    Entrypoint for this bot.
    """
    # Optional: more detailed logging just for this module
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] [strategy_ws_bot] %(message)s",
    )
    bot = StrategyWSBot()
    await bot.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[strategy] Interrupted by user, exiting.")
