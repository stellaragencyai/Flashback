# app/core/subs.py
import os, json, time
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import requests

CONFIG_PATH = Path("app/config/subaccounts.json")
STATE_PATH  = Path("app/state/subs_state.json")

def _load_json(path: Path) -> dict:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _save_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def load_subs() -> List[Dict]:
    cfg = _load_json(CONFIG_PATH)
    subs = cfg.get("subs", [])
    return [s for s in subs if str(s.get("uid", "")).strip()]

def rr_index() -> int:
    st = _load_json(STATE_PATH)
    return int(st.get("rr_idx", 0))

def rr_advance(n: int) -> int:
    subs = load_subs()
    if not subs:
        return 0
    idx = (rr_index() + n) % len(subs)
    _save_json(STATE_PATH, {"rr_idx": idx})
    return idx

def rr_next() -> Optional[Dict]:
    subs = load_subs()
    if not subs:
        return None
    idx = rr_advance(1)  # move forward one each call
    return subs[idx]

def peek_current() -> Optional[Dict]:
    subs = load_subs()
    if not subs:
        return None
    return subs[rr_index() % len(subs)]

def send_tg_to_sub(sub: Dict, text: str) -> None:
    token = sub.get("tg_token", "")
    chat  = sub.get("tg_chat", "")
    if not token or not chat:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": text, "disable_web_page_preview": True},
            timeout=8
        )
    except Exception:
        pass
