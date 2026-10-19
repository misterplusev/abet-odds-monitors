"""Durable state for the MLB monitors.

WHY THIS EXISTS
---------------
The HuggingFace Space filesystem is EPHEMERAL. Free-tier Spaces have no
persistent volume (that is a paid add-on), so every deploy, every sleep/wake
and every crash-restart destroys /home/user/app/data in its entirety.

Two things broke because of that, both of them user-visible:

  1. DISCORD SPAM. Every monitor tracks what it has already announced in a
     plain in-memory dict (e.g. PitcherTracker.posted at
     mlb_pitcher_monitor_v003.py:355 — no load, no save). A restart empties it,
     so every game on the slate reads as "new" again and the whole slate is
     re-announced. This is the single largest source of duplicate posts.

  2. LOST EVIDENCE. The event log and preflight receipts live on the same
     disk, so deploying a fix destroys exactly the records needed to judge
     whether the fix worked.

HOW IT WORKS
------------
A private HuggingFace Dataset repo is used as the durable store. It is free,
versioned, and completely unaffected by container restarts. The Space already
carries an HF_TOKEN secret, so no new credential is required.

Deliberately implemented on `requests` alone, using HuggingFace's NDJSON commit
API, rather than pulling in `huggingface_hub`. Adding a dependency would force
an image rebuild on a runtime that is currently healthy; this module needs two
HTTP calls and is not worth that risk.

DESIGN RULES
------------
* Never raise. Every failure degrades to local-only behaviour, which is exactly
  today's behaviour, so this module can only improve reliability, never reduce
  it. A monitor must never die because a state push failed.
* Write locally first, always, and push remotely on a debounce. Each remote
  write is a git commit; saving per-game would produce ~15 commits per run and
  bloat the repo. Local writes stay instantaneous and authoritative within a
  process; the remote copy is what survives a restart.
* Prune on load. State is keyed by game_pk and only the recent window matters.
"""

import json
import os
import time
from pathlib import Path

try:
    import requests
except Exception:                                    # pragma: no cover
    requests = None

# ── configuration ────────────────────────────────────────────────────────────
REPO = os.getenv("ABET_STATE_REPO", "abetco/mlb-monitors-state")
HF_TOKEN = os.getenv("HF_TOKEN", "")
BASE_DIR = Path(os.environ.get("ABET_BASE_DIR", "/home/user/app"))
CACHE_DIR = BASE_DIR / "data" / "state"

# Minimum seconds between remote pushes of the SAME file. Bounds commit volume
# while keeping the durable copy fresh enough that a restart loses at most this
# much history.
FLUSH_INTERVAL_SEC = int(os.getenv("ABET_STATE_FLUSH_SEC", "120"))

_UA = "Mozilla/5.0 (compatible; ABETMonitor/1.0)"
_last_push: dict = {}      # name -> monotonic timestamp of last successful push
_pending: dict = {}        # name -> object not yet pushed

_ENABLED = bool(HF_TOKEN and requests)


def enabled() -> bool:
    """True when durable (restart-surviving) storage is actually available."""
    return _ENABLED


def _local_path(name: str) -> Path:
    return CACHE_DIR / name


def _local_read(name: str):
    try:
        p = _local_path(name)
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return None


def _local_write(name: str, obj) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _local_path(name).write_text(
            json.dumps(obj, default=str), encoding="utf-8")
    except Exception:
        pass


def _remote_read(name: str):
    if not _ENABLED:
        return None
    try:
        r = requests.get(
            f"https://huggingface.co/datasets/{REPO}/resolve/main/{name}",
            headers={"Authorization": f"Bearer {HF_TOKEN}", "User-Agent": _UA},
            timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code != 404:
            print(f"[STATE] read {name} -> HTTP {r.status_code}", flush=True)
    except Exception as e:
        print(f"[STATE] read {name} failed: {e}", flush=True)
    return None


def _remote_write(name: str, obj) -> bool:
    if not _ENABLED:
        return False
    import base64
    try:
        blob = json.dumps(obj, default=str).encode("utf-8")
        ndjson = (
            json.dumps({"key": "header",
                        "value": {"summary": f"state: {name}"}}) + "\n" +
            json.dumps({"key": "file",
                        "value": {"path": name,
                                  "content": base64.b64encode(blob).decode(),
                                  "encoding": "base64"}}) + "\n"
        ).encode("utf-8")
        r = requests.post(
            f"https://huggingface.co/api/datasets/{REPO}/commit/main",
            data=ndjson,
            headers={"Authorization": f"Bearer {HF_TOKEN}",
                     "Content-Type": "application/x-ndjson", "User-Agent": _UA},
            timeout=60)
        if r.status_code == 200:
            _last_push[name] = time.monotonic()
            _pending.pop(name, None)
            return True
        print(f"[STATE] write {name} -> HTTP {r.status_code} "
              f"{r.text[:160]}", flush=True)
    except Exception as e:
        print(f"[STATE] write {name} failed: {e}", flush=True)
    return False


# ── public API ───────────────────────────────────────────────────────────────
def load_json(name: str, default=None):
    """Load durable state. Remote wins; local cache is the fallback.

    Remote is preferred because the local copy may be a fresh empty container.
    Falls all the way back to `default`, which reproduces today's behaviour, so
    an outage degrades to "might repost" rather than "crashes".
    """
    obj = _remote_read(name)
    src = "remote"
    if obj is None:
        obj = _local_read(name)
        src = "local"
    if obj is None:
        print(f"[STATE] {name}: no durable state found — starting empty "
              f"(durable={'on' if _ENABLED else 'OFF'})", flush=True)
        return default if default is not None else {}
    _local_write(name, obj)
    print(f"[STATE] {name}: loaded from {src}", flush=True)
    return obj


def save_json(name: str, obj, force: bool = False) -> bool:
    """Persist state. Local write is immediate; remote push is debounced.

    Pass force=True at the end of a cycle to guarantee the durable copy is
    current before the process may be killed.
    """
    _local_write(name, obj)
    _pending[name] = obj
    if not _ENABLED:
        return False
    due = (time.monotonic() - _last_push.get(name, 0.0)) >= FLUSH_INTERVAL_SEC
    if force or due:
        return _remote_write(name, obj)
    return False


def flush(name: str = None) -> int:
    """Push any state that has been saved locally but not yet pushed."""
    names = [name] if name else list(_pending.keys())
    return sum(1 for n in names if n in _pending and _remote_write(n, _pending[n]))


def prune_by_date(mapping: dict, keep_keys: set) -> dict:
    """Drop entries whose key is not in keep_keys. Keeps state files small."""
    if not isinstance(mapping, dict):
        return {}
    return {k: v for k, v in mapping.items() if str(k) in keep_keys}


def int_keys(d):
    """Restore integer dict keys destroyed by the JSON round-trip.

    THIS IS LOAD-BEARING. JSON object keys are ALWAYS strings, but the monitors
    index these dicts with an integer game_pk:

        mlb_pitcher_monitor_v003.py:916   if g["pk"] not in tracker.posted
        mlb_pitcher_monitor_v003.py:381   if g["pk"] in self.game_details

    If the restored dict came back with string keys, every one of those lookups
    would miss, the monitor would conclude nothing had been posted, and it would
    re-announce the entire slate anyway. The persistence would look applied and
    fix nothing — the same failure mode as a graphic dict that silently drops a
    field. Keys that are not integers (e.g. date strings) are left alone.
    """
    if not isinstance(d, dict):
        return {}
    out = {}
    for k, v in d.items():
        try:
            out[int(k)] = v
        except (TypeError, ValueError):
            out[k] = v
    return out


def load_tracker(name: str) -> dict:
    """Load a monitor's tracker snapshot with integer game_pk keys restored."""
    snap = load_json(name, {}) or {}
    return {
        "posted": int_keys(snap.get("posted", {})),
        "game_details": int_keys(snap.get("game_details", {})),
        "daily_counts": snap.get("daily_counts", {}),   # keyed by date string
    }


def save_tracker(name: str, posted: dict, game_details: dict,
                 daily_counts: dict, force: bool = False) -> bool:
    """Persist a monitor's tracker snapshot.

    discovery_times is deliberately NOT persisted: it holds datetime objects
    that do not survive JSON, and it only affects sleep-interval tuning, never
    whether something gets re-announced.
    """
    return save_json(name, {
        "posted": posted,
        "game_details": game_details,
        "daily_counts": daily_counts,
    }, force=force)


# ── compressed blobs ─────────────────────────────────────────────────────────
# The odds monitor's history is far too large for the plain-JSON path above:
# measured 484 rows per cycle, so a 3-day window is ~35,000 rows / ~4 MB of raw
# JSON. Gzipped that is ~0.7 MB, which is fine to push. These helpers keep that
# traffic off the small-state path and out of the local cache as text.

def save_gz_json(name: str, obj, force: bool = True) -> bool:
    """Persist a large object as gzipped JSON. Never raises."""
    if not _ENABLED:
        return False
    import base64
    import gzip
    try:
        raw = json.dumps(obj, default=str).encode("utf-8")
        blob = gzip.compress(raw, compresslevel=6)
        ndjson = (
            json.dumps({"key": "header",
                        "value": {"summary": f"state: {name}"}}) + "\n" +
            json.dumps({"key": "file",
                        "value": {"path": name,
                                  "content": base64.b64encode(blob).decode(),
                                  "encoding": "base64"}}) + "\n"
        ).encode("utf-8")
        r = requests.post(
            f"https://huggingface.co/api/datasets/{REPO}/commit/main",
            data=ndjson,
            headers={"Authorization": f"Bearer {HF_TOKEN}",
                     "Content-Type": "application/x-ndjson", "User-Agent": _UA},
            timeout=180)
        if r.status_code == 200:
            print(f"[STATE] {name}: pushed {len(raw):,}B -> {len(blob):,}B gzipped",
                  flush=True)
            return True
        print(f"[STATE] {name} write -> HTTP {r.status_code} {r.text[:160]}",
              flush=True)
    except Exception as e:
        print(f"[STATE] {name} write failed: {e}", flush=True)
    return False


def load_gz_json(name: str, default=None):
    """Load a gzipped-JSON blob. Returns `default` on any failure. Never raises."""
    if not _ENABLED:
        return default
    import gzip
    try:
        r = requests.get(
            f"https://huggingface.co/datasets/{REPO}/resolve/main/{name}",
            headers={"Authorization": f"Bearer {HF_TOKEN}", "User-Agent": _UA},
            timeout=120)
        if r.status_code == 404:
            print(f"[STATE] {name}: none stored yet", flush=True)
            return default
        if r.status_code != 200:
            print(f"[STATE] {name} read -> HTTP {r.status_code}", flush=True)
            return default
        obj = json.loads(gzip.decompress(r.content).decode("utf-8"))
        print(f"[STATE] {name}: restored {len(r.content):,}B gzipped", flush=True)
        return obj
    except Exception as e:
        print(f"[STATE] {name} read failed: {e}", flush=True)
    return default


# ---------------------------------------------------------------------------
# Binary file storage (gzipped SQLite DBs, logs, charts)
# ---------------------------------------------------------------------------

def save_binary(name, data, force=False):
    if not enabled():
        return False
    try:
        import base64, json
        b64 = base64.b64encode(data).decode('ascii')
        ndjson = (
            json.dumps({'key': 'header', 'value': {'summary': 'binary: ' + name}}) + chr(10) +
            json.dumps({'key': 'file', 'value': {'path': name, 'content': b64, 'encoding': 'base64'}}) + chr(10)
        ).encode('utf-8')
        r = requests.post('https://huggingface.co/api/datasets/' + REPO + '/commit/main', data=ndjson, headers={'Authorization': 'Bearer ' + HF_TOKEN, 'Content-Type': 'application/x-ndjson', 'User-Agent': _UA}, timeout=120)
        if r.status_code == 200:
            return True
        print('[STATE] save_binary ' + name + ' -> HTTP ' + str(r.status_code), flush=True)
    except Exception as e:
        print('[STATE] save_binary ' + name + ' failed: ' + str(e), flush=True)
    return False

def load_binary(name):
    if not enabled():
        return None
    try:
        r = requests.get('https://huggingface.co/datasets/' + REPO + '/resolve/main/' + name, headers={'Authorization': 'Bearer ' + HF_TOKEN, 'User-Agent': _UA}, timeout=60)
        if r.status_code == 200:
            return r.content
        if r.status_code != 404:
            print('[STATE] load_binary ' + name + ' -> HTTP ' + str(r.status_code), flush=True)
    except Exception as e:
        print('[STATE] load_binary ' + name + ' failed: ' + str(e), flush=True)
    return None

def save_gzipped_db(db_path, remote_name):
    try:
        import gzip
        from pathlib import Path
        p = Path(db_path)
        if not p.exists():
            return False
        raw = p.read_bytes()
        gz = gzip.compress(raw, compresslevel=9)
        print('[STATE] pushing ' + p.name + ' (' + str(len(raw)) + ' -> ' + str(len(gz)) + ' bytes)', flush=True)
        return save_binary(remote_name, gz)
    except Exception as e:
        print('[STATE] save_gzipped_db failed: ' + str(e), flush=True)
        return False

def load_gzipped_db(remote_name, local_path):
    try:
        import gzip
        from pathlib import Path
        data = load_binary(remote_name)
        if not data:
            return False
        raw = gzip.decompress(data)
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        Path(local_path).write_bytes(raw)
        print('[STATE] restored ' + remote_name + ' -> ' + local_path + ' (' + str(len(raw)) + ' bytes)', flush=True)
        return True
    except Exception as e:
        print('[STATE] load_gzipped_db failed: ' + str(e), flush=True)
        return False

def push_log(name, log_text):
    try:
        return save_binary(name, log_text.encode('utf-8'), force=True)
    except Exception as e:
        print('[STATE] push_log failed: ' + str(e), flush=True)
        return False
