#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Signal Replay Tool

Reads signals from signals/observed.jsonl and replays them into the system,
either by:
  - printing them for inspection, or
  - writing them to a replay file that executor_v2 can consume instead of live feed.

Usage:
  python -m app.tools.signal_replay --mode=print
  python -m app.tools.signal_replay --mode=write --out=signals/replay.jsonl
"""

import os
import argparse
from typing import Dict, Any, Iterable

import orjson

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SIG_DIR = os.path.join(PROJECT_ROOT, "signals")

DEFAULT_SOURCE = os.path.join(SIG_DIR, "observed.jsonl")


def iter_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    if not os.path.exists(path):
        return
    with open(path, "rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield orjson.loads(line)
            except Exception:
                continue


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--mode", choices=["print", "write"], default="print")
    parser.add_argument("--out", default=os.path.join(SIG_DIR, "replay.jsonl"))
    args = parser.parse_args()

    if args.mode == "print":
        for row in iter_jsonl(args.source):
            print(orjson.dumps(row).decode())
    else:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, "wb") as f:
            for row in iter_jsonl(args.source):
                f.write(orjson.dumps(row) + b"\n")
        print(f"Wrote replay file to {args.out}")


if __name__ == "__main__":
    main()
