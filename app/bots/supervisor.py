#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Bot Supervisor (Multi-bot launcher & watchdog)

What this does
--------------
- Starts multiple bot scripts (7 by default).
- Keeps them running: if a bot crashes, it is restarted with a small backoff.
- Logs start/stop/crash events.
- Intended to be run on your MAIN machine or Pi as the "one thing" you start.

Important:
- This supervisor does NOT contain strategy logic or trading logic.
- It ONLY spawns and watches child processes (other Python scripts).

How to use
----------
From the project root (Flashback):

    python app/bots/supervisor.py

Env:
- SUPERVISOR_LOG = INFO / DEBUG / WARNING / ERROR
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


# ---------- Paths & logging ---------- #

# Resolve project ROOT from this file:
# <ROOT>/app/bots/supervisor.py  -> parents[2] == ROOT
SUPERVISOR_PATH = Path(__file__).resolve()
ROOT = SUPERVISOR_PATH.parents[2]

LOG_LEVEL = os.getenv("SUPERVISOR_LOG", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[%(asctime)s] [%(levelname)s] [supervisor] %(message)s",
)
log = logging.getLogger("supervisor")

log.info("Supervisor file: %s", SUPERVISOR_PATH)
log.info("Project root resolved to: %s", ROOT)


# ---------- Config: define your bots here ---------- #

@dataclass
class BotConfig:
    name: str
    module: Optional[str] = None      # unused for now
    script: Optional[str] = None      # path relative to ROOT
    extra_args: List[str] = field(default_factory=list)


# NOTE:
# - Comment out any bot you do NOT want the supervisor to run.
# - All script paths are interpreted relative to ROOT.
BOTS: List[BotConfig] = [
    BotConfig(name="tp_sl_manager",   script="app/bots/tp_sl_manager.py"),
    BotConfig(name="trade_journal",   script="app/bots/trade_journal.py"),
    BotConfig(name="portfolio_guard", script="app/bots/portfolio_guard.py"),
    BotConfig(name="risk_daemon",     script="app/bots/risk_daemon.py"),
    BotConfig(name="notifier_main",   script="app/bots/notifier_main.py"),
    BotConfig(name="executor_v2",     script="app/bots/executor_v2.py"),
    BotConfig(name="observer",        script="app/bots/observer.py"),
]

PYTHON = sys.executable  # use current interpreter (venv-safe)


@dataclass
class BotProcess:
    cfg: BotConfig
    process: Optional[asyncio.subprocess.Process] = None
    restart_delay: float = 3.0


# ---------- Supervisor core ---------- #

class Supervisor:
    def __init__(self, bots: List[BotConfig]) -> None:
        # Filter out bots whose script doesn't exist (and log it)
        self.bots: List[BotProcess] = []
        for b in bots:
            if b.script:
                script_path = ROOT / b.script
                if not script_path.is_file():
                    log.error(
                        "Bot %s script not found at %s; skipping this bot.",
                        b.name,
                        script_path,
                    )
                    continue
            self.bots.append(BotProcess(cfg=b))

        if not self.bots:
            log.error("No valid bots configured. Nothing to supervise.")
        else:
            log.info(
                "Configured bots: %s",
                ", ".join(bp.cfg.name for bp in self.bots),
            )

        self._stop = asyncio.Event()

    async def run(self) -> None:
        """
        Main loop:
        - ensure all bots are running
        - restart crashed bots
        - wait until stop() is called (SIGINT or SIGTERM)
        """
        log.info("Supervisor starting, managing %d bots", len(self.bots))

        if not self.bots:
            log.warning("No bots to manage. Exiting.")
            return

        # Start a watcher task per bot
        tasks = [
            asyncio.create_task(self._bot_watcher(bp))
            for bp in self.bots
        ]

        # Wait until global stop is set
        await self._stop.wait()
        log.info("Stop requested, terminating all bots...")

        # Cancel bot watchers
        for t in tasks:
            t.cancel()

        # Terminate child processes
        await self._terminate_all()

        log.info("Supervisor stopped cleanly.")

    def stop(self) -> None:
        self._stop.set()

    async def _terminate_all(self) -> None:
        for bp in self.bots:
            if bp.process and bp.process.returncode is None:
                log.info("Terminating bot %s (pid=%s)", bp.cfg.name, bp.process.pid)
                try:
                    bp.process.terminate()
                except ProcessLookupError:
                    pass
        # Give them a moment to die
        await asyncio.sleep(1.0)
        # Hard kill any stubborn ones
        for bp in self.bots:
            if bp.process and bp.process.returncode is None:
                log.warning("Killing stubborn bot %s (pid=%s)", bp.cfg.name, bp.process.pid)
                try:
                    bp.process.kill()
                except ProcessLookupError:
                    pass

    async def _bot_watcher(self, bp: BotProcess) -> None:
        """
        Keep one bot alive:
        - start it
        - if it exits unexpectedly, restart after a delay
        - exit only when supervisor is stopping
        """
        name = bp.cfg.name
        while not self._stop.is_set():
            log.info("Launching bot %s ...", name)
            bp.process = await self._spawn_process(bp.cfg)

            if bp.process is None:
                log.error(
                    "Failed to spawn bot %s, retrying in %.1fs",
                    name,
                    bp.restart_delay,
                )
                await asyncio.sleep(bp.restart_delay)
                continue

            # Wait until it exits
            returncode = await bp.process.wait()
            if self._stop.is_set():
                log.info(
                    "Bot %s stopped (supervisor shutting down), rc=%s",
                    name,
                    returncode,
                )
                break

            log.warning(
                "Bot %s exited with rc=%s, restarting in %.1fs",
                name,
                returncode,
                bp.restart_delay,
            )
            await asyncio.sleep(bp.restart_delay)

    async def _spawn_process(self, cfg: BotConfig) -> Optional[asyncio.subprocess.Process]:
        """
        Spawn a single bot process based on its config.
        We now use the script path form (`python app/bots/x.py`) and always start
        from ROOT to keep imports consistent.
        """
        if cfg.script:
            cmd = [PYTHON, str(ROOT / cfg.script)] + cfg.extra_args
        elif cfg.module:
            # Fallback if you ever want to go back to -m style
            cmd = [PYTHON, "-m", cfg.module] + cfg.extra_args
        else:
            log.error("Bot %s has neither module nor script defined", cfg.name)
            return None

        log.info("Starting %s with command: %s", cfg.name, " ".join(cmd))
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(ROOT),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            log.error("Failed to start %s: file/module not found", cfg.name)
            return None
        except Exception as e:
            log.error("Failed to start %s: %r", cfg.name, e)
            return None

        # Background readers for stdout/stderr
        asyncio.create_task(self._stream_logs(proc.stdout, cfg.name, "STDOUT"))
        asyncio.create_task(self._stream_logs(proc.stderr, cfg.name, "STDERR"))

        return proc

    async def _stream_logs(self, stream: asyncio.StreamReader, name: str, label: str) -> None:
        """
        Stream child process logs into supervisor logger.
        """
        if stream is None:
            return
        try:
            while not stream.at_eof():
                line = await stream.readline()
                if not line:
                    break
                text = line.decode(errors="ignore").rstrip()
                if text:
                    log.info("[%s][%s] %s", name, label, text)
        except Exception as e:
            log.warning("Log stream error for %s (%s): %r", name, label, e)


# ---------- Entrypoint ---------- #

async def _main_async() -> None:
    sup = Supervisor(BOTS)

    loop = asyncio.get_running_loop()

    def _handle_sig(*_args) -> None:
        log.info("Signal received, requesting shutdown...")
        sup.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_sig)
        except NotImplementedError:
            # Windows ProactorEventLoop might not support signals
            pass

    await sup.run()


def main() -> None:
    try:
        asyncio.run(_main_async())
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt, shutting down supervisor.")


if __name__ == "__main__":
    main()
