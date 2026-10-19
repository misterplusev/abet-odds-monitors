#!/usr/bin/env python3
"""
NFL Unified Odds Monitor v001
Complete odds monitoring with 9-key API rotation, chart generation, and Discord posting.
Replicates the MLB Unified Odds Monitor v007 pattern for the NFL.
Generates moneyline, spread, and totals movement charts with stored averages.
Single API call for all three market types — saves credits.

Features:
  - Self-running continuous loop (every 30 minutes)
  - Smart 9-key API rotation with credit tracking
  - Single API fetch for all markets (h2h, spreads, totals)
  - SQLite database with history tracking
  - Averages stored in database at fetch time
  - Moneyline movement charts with stored averages
  - Spread movement charts for all lines
  - Totals movement charts for all lines
  - Discord posting to 3 separate channels
  - Crash/shutdown Discord notifications
  - Signal handling for graceful shutdown
"""

SCRIPT_VERSION = "004"  # 2026-10-16 2250 MDT - v003 + format heal (strftime + T-replace) + stale-connection retry + output guards (OUTPUT GUARD / CHART DELIVERY SHORTFALL) + chart_quality_gate extension to assert charts_rendered>0. Mirrors MLB v009. Tested: real render from mixed-format DB + guard-path tests.
SCRIPT_NAME = "NFL Unified Odds Monitor"

import json
import os
import random
import re
import signal
import socket
import sqlite3
import sys
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import statistics

import requests
import pandas as pd
import pytz

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
from matplotlib.ticker import FuncFormatter

# Windows console safety — symbol glyphs (✓ ✗ ⚠ ℹ 📊) are not in cp1252
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# ==========================================================================
# CONFIGURATION
# ==========================================================================

# Local standalone default: run from the script's own directory.
# In HF Space deployments, set ABET_BASE_DIR explicitly.
BASE_DIR = Path(os.environ.get("ABET_BASE_DIR") or Path(__file__).resolve().parent)
LOG_DIR = BASE_DIR / "data" / "logs"
DB_PATH = BASE_DIR / "data" / "database" / "odds_nfl.db"
RAW_DIR = BASE_DIR / "data" / "runtime" / "nfl" / "raw_odds"
RUNTIME_DIR = BASE_DIR / "data" / "runtime" / "nfl"
CHARTS_DIR = RUNTIME_DIR / "charts"
API_STATE_FILE = RUNTIME_DIR / "api_state.json"

# Create directories
for _d in [DB_PATH.parent, RAW_DIR, RUNTIME_DIR, CHARTS_DIR, LOG_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ==========================================================================
# POST SIGNATURE — module/venue provenance on every Discord post
# ==========================================================================
# WHY THIS EXISTS. Four venues run this same script against the same Discord
# webhooks (GitHub Actions, HF Space, a cron host, local dev). Discord shows
# the WEBHOOK's display name, not the runner — so a post from any venue looks
# identical, and on 2026-10-12 a live post could not be attributed to a host.
# Every post now carries: visible author name + footer SIGNATURE, a zero-width
# covert fingerprint (ZWSP=0 / ZWJ=1 between ZWNJ sentinels) appended to the
# footer/content, and a local JSONL receipt row.
#
# SOURCE_ID is REQUIRED on every deployment (e.g. gh-actions, hf-space,
# vm-cron, local-dev). Missing -> "unknown" (loudly printed at startup).
SOURCE_ID = os.environ.get("SOURCE_ID", "").strip() or "unknown"
SOURCE_HOST = socket.gethostname()


def _git_sha7() -> str:
    """Short HEAD sha when running from a checkout; 'nogit' otherwise."""
    try:
        import subprocess
        r = subprocess.run(["git", "rev-parse", "--short=7", "HEAD"],
                           cwd=str(Path(__file__).resolve().parent),
                           capture_output=True, text=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except Exception:
        pass
    return "nogit"


SOURCE_SHA = os.environ.get("SOURCE_SHA", "").strip() or _git_sha7()
SIGNATURE = f"{SOURCE_ID}@{SOURCE_HOST}#{SOURCE_SHA}"

_ZWNJ = "\u200c"
_ZWSP = "\u200b"
_ZWJ = "\u200d"


def covert_signature(venue: str) -> str:
    """Invisible venue fingerprint: ZWSP/ZWJ bits between ZWNJ sentinels."""
    bits = "".join(f"{b:08b}" for b in venue.encode("utf-8"))
    return _ZWNJ + "".join(_ZWSP if b == "0" else _ZWJ for b in bits) + _ZWNJ


def decode_covert_signature(text: str) -> str:
    """Recover the venue string from a covert_signature() marker."""
    bits = "".join("0" if ch == _ZWSP else "1" for ch in text
                   if ch in (_ZWSP, _ZWJ))
    try:
        return bytes(int(bits[i:i + 8], 2)
                     for i in range(0, len(bits) - len(bits) % 8, 8)
                     ).decode("utf-8")
    except Exception:
        return ""


def signed_footer() -> str:
    """Visible signature + covert fingerprint for embed footers."""
    return f"{SCRIPT_NAME} [{SIGNATURE}]" + covert_signature(SIGNATURE)


def signed_author() -> str:
    """Visible Discord author (webhook username override)."""
    return f"{SCRIPT_NAME} [{SOURCE_ID}]"


def _post_receipt(kind: str, channel: str, detail: str = ""):
    """Append a JSONL receipt row for every successful post. Never raises."""
    try:
        p = Path(os.environ.get("POST_RECEIPT_LOG")
                 or (LOG_DIR / "post_receipts.jsonl"))
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "ts": datetime.now(timezone.utc).isoformat(),
                "venue": SOURCE_ID, "host": SOURCE_HOST, "sha": SOURCE_SHA,
                "kind": kind, "channel": channel, "detail": detail,
            }) + "\n")
    except Exception:
        pass


def _append_gate_log(gate):
    """Append every gate verdict (PASS/FAIL/SKIPPED) to data/logs/chart_quality_gate.jsonl."""
    try:
        row = {"ts_utc": datetime.now(timezone.utc).isoformat(),
               "venue": SOURCE_ID, "sha": SOURCE_SHA}
        row.update(gate if isinstance(gate, dict) else {"verdict": "SKIPPED"})
        p = LOG_DIR / "chart_quality_gate.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, default=str) + "\n")
    except Exception:
        pass


def _write_posted_manifest(ml, sp, to, games_n, records_n, elapsed_s, gate):
    """Per-run JSON manifest of exactly what this run published. Never raises."""
    try:
        man = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "source": SIGNATURE, "script_version": SCRIPT_VERSION,
            "games": games_n, "records": records_n,
            "elapsed_seconds": round(float(elapsed_s), 1),
            "gate": gate if isinstance(gate, dict) else {"verdict": "SKIPPED"},
            "charts": {
                "moneyline": [p.name for p, _ in ml],
                "spreads": [p.name for p, _ in sp],
                "totals": [p.name for p, _ in to],
            },
            "total_charts": len(ml) + len(sp) + len(to),
        }
        p = LOG_DIR / ("posted_manifest_"
                       + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + ".json")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(man, indent=2), encoding="utf-8")
        print(f"[manifest] wrote {p.name} ({man['total_charts']} charts)", flush=True)
    except Exception as e:
        print(f"[manifest] write failed: {e}", flush=True)

# ==========================================================================
# DURABLE ODDS HISTORY
# ==========================================================================
# WHY THIS EXISTS. odds_nfl.db is written to the GitHub Actions runner's disk,
# and that runner is DESTROYED when the job ends. Every hour this monitor
# therefore started from an empty database, stored one snapshot, drew a chart
# from a single point in time, and threw the database away. That is the whole
# explanation for the one-datapoint NFL charts.
#
# MLB does not have this problem because its Space process lives for hours, and
# because mlb_unified_odds_monitor snapshots to a HuggingFace Dataset and
# replays it after an ephemeral wipe. We use the SAME mechanism and the SAME
# dataset here — only the snapshot key differs, so NFL history is identifiable
# and can never collide with MLB's.
#
# CRITICAL DIFFERENCE FROM MLB. MLB restores ONCE per process because its
# process is long-lived. This monitor is invoked as `--once` from CI, so the
# process IS the cycle: it must restore at the START and snapshot at the END of
# every single run, or nothing ever accumulates.
_HAS_DURABLE = False
try:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import durable_state
    _HAS_DURABLE = True
except Exception as _e:                                    # pragma: no cover
    print(f"[WARN] durable_state unavailable: {_e} — NFL odds history is "
          f"EPHEMERAL and charts will show a single point", flush=True)

# Same dataset as MLB (ABET_STATE_REPO), NFL-labelled key.
HISTORY_SNAPSHOT = "nfl_odds_history_snapshot.json.gz"
HISTORY_RETAIN_DAYS = int(os.getenv("ODDS_HISTORY_RETAIN_DAYS", "7"))

# -- CHART QUALITY GATE (v1.0.0, 2026-10-14 0111 MDT) ------------------------
# Never post "movement" charts that cannot show movement. Proven in
# NFL_ODDS_FIX_PROOF_20261013_1455: live lane fails R1 (no history),
# created_at-collapse fails R2 (history stacked on one x-position),
# created_at-preserving build passes. A missing/broken gate never blocks.
CHART_GATE_FILE = "chart_quality_gate_v1.0.0_20261014_0111.py"


def _chart_gate_evaluate(db_path, charts_rendered=None):
    try:
        import importlib.util
        p = Path(__file__).with_name(CHART_GATE_FILE)
        if not p.exists():
            print(f"[gate] {CHART_GATE_FILE} not found - gate skipped", flush=True)
            return None
        spec = importlib.util.spec_from_file_location("chart_quality_gate", p)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        return m.evaluate(str(db_path), charts_rendered=charts_rendered)
    except Exception as e:
        print(f"[gate] evaluation failed, not blocking: {e}", flush=True)
        return None


def _norm_ct(v):
    """v004: normalize any timestamp-ish value to 'YYYY-MM-DD HH:MM:SS' (UTC)."""
    s = str(v).strip().replace("T", " ")
    return s[:19]

# File logger — writes to data/logs/ so the launcher dashboard picks it up
import logging as _logging
_file_logger = _logging.getLogger("nfl_odds_monitor")
_file_logger.setLevel(_logging.DEBUG)
_fh = _logging.FileHandler(LOG_DIR / "nfl_odds_monitor.log", encoding='utf-8')
_fh.setFormatter(_logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s',
                                     datefmt='%Y-%m-%d %H:%M:%S'))
_file_logger.addHandler(_fh)

# Discord Webhooks — from environment (discordapp.com domain for HF compatibility)
DISCORD_WEBHOOK_MONEYLINE = os.environ.get("DISCORD_WEBHOOK_NFL_MONEYLINE", "")
DISCORD_WEBHOOK_SPREADS   = os.environ.get("DISCORD_WEBHOOK_NFL_SPREADS", "")
DISCORD_WEBHOOK_TOTALS    = os.environ.get("DISCORD_WEBHOOK_NFL_TOTALS", "")

# TheOddsAPI — 9 keys loaded from environment for rotation.
# NOTE: shares the SAME key pool as the MLB suite (ODDS_API_KEY_1..9, MLB
# v006/v007 rotation algorithm). NFL-dedicated keys can be swapped in later
# by replacing the values — no code change needed.
API_KEYS = []
for _i in range(1, 11):
    _k = os.environ.get(f"ODDS_API_KEY_{_i}", "")
    if _k:
        API_KEYS.append((_k, f"API_{_i}"))

# Sport keys to try (in order of priority)
# Preseason runs Aug-Sep (HOF game + 4 weeks); books carry both sets of lines
# while preseason is underway, so try preseason key first, then regular season.
#
# CALENDAR TRIGGERS SILENTLY DOUBLE THE BILL. fetch_odds() returns on the FIRST
# sport key that yields data, so two keys cost 3 credits while the first still
# has games — and 6 the moment it goes empty. Preseason ends in late August but
# `_MONTH in (8, 9)` kept asking for it all through September: an empty 3-credit
# call, then 3 more for the real key. That flips NFL from 2,160 to 4,320
# credits/month with no code change and no alert, and takes the shared 9-key
# pool from 96% to 144% of its 4,500 allowance.
#
# PRESEASON STAYS IN. It is currently returning real games (17 on 2026-08-25),
# and dropping it would lose live coverage — so unlike MLB, the default here is
# ON. The doubling it can cause is affordable at the 2-hourly cadence: the
# worst case (preseason empty, so 2 calls x 3 credits x 12 cycles = 72/day for
# NFL, plus MLB's 36) is 3,348 per 31 days = 84% of the 4,000-credit live pool.
# The switch exists so it can be turned OFF deliberately once preseason ends,
# rather than a calendar silently changing the bill.
_MONTH = datetime.now().month
if _MONTH in (8, 9) and os.getenv("ODDS_INCLUDE_NFL_PRESEASON", "1") == "1":
    SPORT_KEYS = ["americanfootball_nfl_preseason", "americanfootball_nfl"]
else:
    SPORT_KEYS = ["americanfootball_nfl"]
SPORT_KEY = SPORT_KEYS[0]  # Primary (used for logging)
API_URL_BASE = "https://api.the-odds-api.com/v4/sports"
REGIONS = 'us'
MARKETS = 'h2h,spreads,totals'

# Feature flags
VERBOSE_LOGGING = True
SEND_DISCORD = True
GENERATE_CHARTS = True

# Continuous running.
#
# CREDIT-AWARE CADENCE. TheOddsAPI bills per call as:
#     cost = [unique markets returned] x [regions specified]
# Our call is markets=h2h,spreads,totals (3) x regions=us (1) = 3 credits.
# ODDS_API_KEY_1..9 x 500 = 4,500 credits/month, and this pool is SHARED with
# the MLB suite, which spends 2,160/month at its own hourly cadence.
#
#     1800s (30m) -> 48 cycles/day -> 144 credits/day -> 4,320/mo   <- was
#     3600s (60m) -> 24 cycles/day ->  72 credits/day -> 2,160/mo   <- now
#
# At 30 minutes, NFL alone took 96% of the shared pool and MLB's 2,160 put the
# pair at 144% — over budget every month. Both hourly is 4,320/mo, inside 4,500.
# Overridable from the Space settings without a redeploy, like the MLB monitor.
RUN_INTERVAL = int(os.getenv("ODDS_RUN_INTERVAL_SEC", "3600"))

# Team abbreviations (all 32 NFL teams)
TEAM_ABBR = {
    "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL", "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF", "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE", "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN", "Detroit Lions": "DET", "Green Bay Packers": "GB",
    "Houston Texans": "HOU", "Indianapolis Colts": "IND", "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC", "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LAR", "Miami Dolphins": "MIA", "Minnesota Vikings": "MIN",
    "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
    "New York Jets": "NYJ", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF", "Seattle Seahawks": "SEA",
    "Tampa Bay Buccaneers": "TB", "Tennessee Titans": "TEN",
    "Washington Commanders": "WAS",
}

# Bookmaker colors for charts (keyed by bookmaker_key from API)
BOOKMAKER_COLORS = {
    'draftkings': '#53D337', 'fanduel': '#1A73E8', 'betmgm': '#FDB913',
    'betrivers': '#003DA5', 'caesars': '#522398', 'pointsbet': '#00B5E2',
    'pointsbetau': '#00B5E2', 'pinnacle': '#FF6600', 'bovada': '#CC0000',
    'betonlineag': '#FF4500', 'mybookieag': '#800080', 'bet365': '#00A783',
    'espnbet': '#F5342E', 'hardrockbet': '#C7A86B', 'betus': '#006400',
    'lowvig': '#4B0082', 'fliff': '#00C4FF', 'windcreek': '#7CB342',
    'underdog': '#FF9800',
}

# Configure requests session with User-Agent (required for Discord from HF)
_session = requests.Session()
_session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; ABETMonitor/1.0)"})

# Global shutdown flag
shutdown_requested = False

# ==========================================================================
# LOGGING
# ==========================================================================

class Colors:
    RESET = '\033[0m'; RED = '\033[91m'; GREEN = '\033[92m'; YELLOW = '\033[93m'
    BLUE = '\033[94m'; MAGENTA = '\033[95m'; CYAN = '\033[96m'; GRAY = '\033[90m'
    BOLD = '\033[1m'; DIM = '\033[2m'

SYM = {'ok': '✓', 'err': '✗', 'warn': '⚠', 'info': 'ℹ', 'chart': '📊'}


def _ts():
    return datetime.now(timezone.utc).strftime('%H:%M:%S')


def log_error(msg, exc=None, critical=False):
    full = f"{msg} - {exc}" if exc else msg
    print(f"{Colors.RED}{SYM['err']} [{_ts()}] ERROR: {msg}{Colors.RESET}")
    if exc:
        print(f"{Colors.RED}  Exception: {exc}{Colors.RESET}")
    _file_logger.error(full)
    if critical and SEND_DISCORD:
        send_discord_alert(f"🚨 **CRITICAL**: {msg}", is_error=True)


def log_ok(msg):
    _file_logger.info(msg)
    if VERBOSE_LOGGING:
        print(f"{Colors.GREEN}{SYM['ok']} [{_ts()}] {msg}{Colors.RESET}")


def log_warn(msg):
    _file_logger.warning(msg)
    if VERBOSE_LOGGING:
        print(f"{Colors.YELLOW}{SYM['warn']} [{_ts()}] {msg}{Colors.RESET}")


def log_info(msg):
    _file_logger.info(msg)
    if VERBOSE_LOGGING:
        print(f"{Colors.CYAN}{SYM['info']} [{_ts()}] {msg}{Colors.RESET}")


# ==========================================================================
# ODDS MATH
# ==========================================================================

def decimal_to_american(d: float) -> int:
    try:
        return int((d - 1) * 100) if d >= 2.0 else int(-100 / (d - 1))
    except Exception:
        return 0


def american_to_decimal(a: int) -> float:
    try:
        return (a / 100) + 1 if a > 0 else (100 / abs(a)) + 1
    except Exception:
        return 1.0


def american_to_probability(odds: int) -> float:
    if odds == 0:
        return 0
    return 100 / (odds + 100) * 100 if odds > 0 else abs(odds) / (abs(odds) + 100) * 100


def probability_to_american(prob: float) -> int:
    if prob <= 0 or prob >= 100:
        return 0
    return int(100 / (prob / 100) - 100) if prob < 50 else int(-(prob / (100 - prob)) * 100)


def calculate_average_odds(odds_list: List[int]) -> int:
    """Average American odds via implied probability (proper method)."""
    probs = [american_to_probability(o) for o in odds_list if o != 0]
    if not probs:
        return 0
    return probability_to_american(sum(probs) / len(probs))


def abbreviate_team(name: str) -> str:
    return TEAM_ABBR.get(name, name[:3].upper())


def is_game_live(commence_time_str: str) -> bool:
    try:
        t = datetime.fromisoformat(commence_time_str.replace('Z', '+00:00'))
        return datetime.now(timezone.utc) > t
    except Exception:
        return False


# ==========================================================================
# SUPABASE CLIENT (optional — mirrors mlb suite; disabled when absent)
# ==========================================================================

_sb_client = None
try:
    from supabase_client import SupabaseClient
    _sb_client = SupabaseClient()
    if not _sb_client.connected:
        _sb_client = None
except Exception:
    pass


# ==========================================================================
# API KEY MANAGER — matches MLB v006/v007 algorithm
# ==========================================================================

class APIKeyManager:
    """Smart API key rotation with actual credit tracking from API response headers.
    Algorithm: sort by credits_remaining descending, pick randomly from
    keys with >= 80% of average credits remaining."""

    def __init__(self):
        self.env_keys = API_KEYS          # [(key_str, "API_N"), ...] from env
        self.state_file = API_STATE_FILE
        self.selected_key = None
        self.selected_id = None
        self.state = self._load_state()

    def _load_state(self) -> dict:
        """Build state from env vars, merge with persisted credit data.
        Priority: local file > Supabase > fresh defaults."""
        # Build base from env vars
        keys_list = []
        for key_val, kid in self.env_keys:
            keys_list.append({
                "id": kid,
                "key": key_val,
                "active": True,
                "credits_remaining": 500,
                "credits_used": 0,
                "total_requests": 0,
                "last_used": None,
            })

        # Try to merge persisted credit data
        persisted = None

        # Local file first (fastest)
        if self.state_file.exists():
            try:
                persisted = json.loads(self.state_file.read_text())
            except Exception:
                pass

        # Supabase fallback (persistent across container restarts)
        if not persisted and _sb_client:
            try:
                persisted = _sb_client.get_api_key_state()
            except Exception:
                pass

        if persisted:
            # Handle both list and dict formats for keys
            p_keys = persisted.get("keys", [])
            if isinstance(p_keys, list):
                credit_map = {k["id"]: k for k in p_keys if isinstance(k, dict)}
            elif isinstance(p_keys, dict):
                credit_map = p_keys
            else:
                credit_map = {}

            merged = 0
            for key_data in keys_list:
                kid = key_data["id"]
                if kid in credit_map:
                    p = credit_map[kid]
                    key_data["credits_remaining"] = p.get(
                        "credits_remaining", p.get("requests_remaining", 500))
                    key_data["credits_used"] = p.get("credits_used", 0)
                    key_data["total_requests"] = p.get("total_requests", 0)
                    key_data["last_used"] = p.get("last_used")
                    key_data["active"] = p.get("active", True)
                    merged += 1

            if merged > 0:
                src = "file" if self.state_file.exists() else "Supabase"
                log_info(f"Merged persisted credit data from {src} for {merged} keys")

        state = {
            "keys": keys_list,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save_state(state)
        return state

    def _save_state(self, state=None):
        """Save state to local JSON file."""
        if state is None:
            state = self.state
        try:
            state["updated_at"] = datetime.now(timezone.utc).isoformat()
            self.state_file.write_text(json.dumps(state, indent=2, default=str))
        except Exception:
            pass

    def get_key(self) -> Tuple[str, str]:
        """Select best key: most credits remaining, random from top pool."""
        all_keys = self.state.get("keys", [])

        # Filter to active keys with credits
        available = [k for k in all_keys
                     if k.get("active", True) and k.get("credits_remaining", 0) > 0]

        if not available:
            log_warn("All keys exhausted or inactive — resetting credits to 500")
            for k in all_keys:
                if k.get("active", True):
                    k["credits_remaining"] = 500
                    k["credits_used"] = 0
            available = [k for k in all_keys if k.get("active", True)]
            self._save_state()

        if not available:
            log_error("No active API keys!", critical=True)
            return "", ""

        # Sort by credits_remaining descending (most credits first)
        available.sort(key=lambda x: x.get("credits_remaining", 0), reverse=True)

        # Pool: keys with >= 80% of average credits remaining
        total_credits = sum(k["credits_remaining"] for k in available)
        avg_credits = total_credits / len(available)
        good_keys = [k for k in available
                     if k["credits_remaining"] >= avg_credits * 0.8]
        if not good_keys:
            good_keys = available

        selected = random.choice(good_keys)
        self.selected_key = selected["key"]
        self.selected_id = selected["id"]

        log_info(f"API Key: {selected['id']} — "
                 f"{selected['credits_remaining']} credits remaining "
                 f"(pool: {total_credits} across {len(available)} active keys)")
        return self.selected_key, self.selected_id

    def update_credits(self, credits_remaining: int, credits_used: int = None):
        """Update selected key with ACTUAL credits from x-requests-remaining header."""
        if not self.selected_id:
            return
        for key_data in self.state.get("keys", []):
            if key_data["id"] == self.selected_id:
                old_remaining = key_data.get("credits_remaining", 500)
                key_data["credits_remaining"] = credits_remaining
                if credits_used is not None:
                    key_data["credits_used"] = credits_used
                else:
                    key_data["credits_used"] = 500 - credits_remaining
                key_data["last_used"] = datetime.now(timezone.utc).isoformat()
                key_data["total_requests"] = key_data.get("total_requests", 0) + 1

                delta = old_remaining - credits_remaining
                if delta > 0:
                    log_info(f"Credits consumed: {delta} "
                             f"({old_remaining} → {credits_remaining})")
                break
        self._save_state()

    def mark_dead(self, key_id: str = None):
        """Mark a key as inactive (e.g., 401 unauthorized)."""
        kid = key_id or self.selected_id
        if not kid:
            return
        for key_data in self.state.get("keys", []):
            if key_data["id"] == kid:
                key_data["active"] = False
                key_data["credits_remaining"] = 0
                log_warn(f"Marked {kid} as INACTIVE (dead key)")
                break
        self._save_state()

    def push_to_supabase(self):
        """Push full key state to Supabase for persistence + dashboard."""
        if not _sb_client:
            return
        try:
            keys_info = []
            for key_data in self.state.get("keys", []):
                keys_info.append({
                    "id": key_data["id"],
                    "active": key_data.get("active", True),
                    "credits_remaining": key_data.get("credits_remaining", 0),
                    "credits_used": key_data.get("credits_used", 0),
                    "total_requests": key_data.get("total_requests", 0),
                    "last_used": key_data.get("last_used"),
                })
            active_keys = [k for k in keys_info if k["active"]]
            state = {
                "keys": keys_info,
                "total_keys": len(keys_info),
                "active_keys": len(active_keys),
                "total_credits": sum(k["credits_remaining"] for k in active_keys),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            _sb_client.push_api_key_state(state)
            log_info("API key state pushed to Supabase")
        except Exception as e:
            log_warn(f"Supabase key push failed: {e}")


# ==========================================================================
# DATABASE
# ==========================================================================

def setup_database() -> bool:
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute("""CREATE TABLE IF NOT EXISTS games (
            game_id TEXT PRIMARY KEY, sport_key TEXT, commence_time TEXT,
            home_team TEXT, away_team TEXT, last_update TEXT
        )""")

        c.execute("""CREATE TABLE IF NOT EXISTS odds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT, timestamp TEXT, bookmaker_key TEXT, bookmaker_title TEXT,
            market_key TEXT, outcome_name TEXT, price_decimal REAL,
            price_american INTEGER, point REAL,
            point_key REAL DEFAULT -999999,
            UNIQUE(game_id, bookmaker_key, market_key, outcome_name, point_key)
        )""")

        c.execute("""CREATE TABLE IF NOT EXISTS odds_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT, fetch_timestamp TEXT, bookmaker_key TEXT, bookmaker_title TEXT,
            market_key TEXT, outcome_name TEXT, price_decimal REAL,
            price_american INTEGER, point REAL, is_live_game BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        c.execute("""CREATE TABLE IF NOT EXISTS api_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, credits_used INTEGER, credits_remaining INTEGER,
            api_key_used TEXT, games_count INTEGER
        )""")

        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_game ON odds_history(game_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_ts ON odds_history(fetch_timestamp)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_mkt ON odds_history(market_key)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_odds_game ON odds(game_id)")

        conn.commit()
        conn.close()
        log_ok("Database ready")
        return True
    except Exception as e:
        log_error("Database setup failed", e)
        return False


# ==========================================================================
# DISCORD
# ==========================================================================

def send_discord_alert(message: str, is_error: bool = False):
    if not SEND_DISCORD:
        return
    color = 0xFF0000 if is_error else 0x00FF00
    title = f"{'⚠️' if is_error else '🏈'} NFL Odds Monitor v{SCRIPT_VERSION}"
    payload = {
        "username": signed_author(),
        "embeds": [{
            "title": title,
            "description": message,
            "color": color,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "footer": {"text": signed_footer()}
        }]
    }
    try:
        resp = _session.post(DISCORD_WEBHOOK_MONEYLINE, json=payload, timeout=10)
        if resp.status_code in (200, 204):
            _post_receipt("alert" if is_error else "text_embed",
                          "#nfl-moneylines", title)
            try:
                from discord_post_logger import log_post
                log_post("nfl_odds_monitor", "#nfl-moneylines",
                         "alert" if is_error else "text_embed",
                         title=title, description=message, color=color,
                         footer=signed_footer())
            except Exception:
                pass
    except Exception:
        pass


def post_chart_to_discord(chart_path: Path, title: str, webhook_url: str,
                          game_info: dict = None) -> bool:
    """Post chart to Discord with rich matchup text.
    game_info: optional dict with away_team, home_team, commence_time
    """
    if not SEND_DISCORD or not webhook_url:
        return False
    try:
        with open(chart_path, 'rb') as f:
            files = {'file': (chart_path.name, f, 'image/png')}

            # Build rich content text
            now_et = datetime.now(timezone.utc).astimezone(pytz.timezone('US/Eastern'))
            time_str = now_et.strftime('%I:%M %p ET').lstrip('0')

            if game_info:
                away = game_info.get('away_team', '?')
                home = game_info.get('home_team', '?')
                ct = game_info.get('commence_time', '')

                # Format game time
                try:
                    gt = datetime.fromisoformat(ct.replace('Z', '+00:00'))
                    gt_et = gt.astimezone(pytz.timezone('US/Eastern'))
                    game_time = gt_et.strftime('%I:%M %p ET').lstrip('0')
                except Exception:
                    game_time = 'TBD'

                content_text = (
                    f"**{SYM['chart']} {away} @ {home}** — {game_time}\n"
                    f"*{title}* • Updated {time_str} • `{SIGNATURE}`"
                )
            else:
                content_text = (f"**{SYM['chart']} {title}**\n"
                                f"`{time_str}` • `{SIGNATURE}`")

            data = {"content": content_text + covert_signature(SIGNATURE),
                    "username": signed_author()}
            # STALE-CONNECTION RETRY (ported from MLB v009, 2026-10-15 1320 MDT).
            # Proven in production Space logs: after ~60 min idle the pooled
            # keep-alive connection dies; the FIRST post of every cycle fails
            # with ConnectionResetError(104) and the chart was silently lost.
            # Retry once on a fresh connection so the first chart survives.
            resp = None
            for _attempt in (1, 2):
                try:
                    if _attempt > 1:
                        try:
                            f.seek(0)
                            _session.close()
                        except Exception:
                            pass
                        time.sleep(2)
                    resp = _session.post(webhook_url, files=files, data=data, timeout=20)
                    if resp.status_code in (200, 204):
                        break
                except OSError as _ce:      # ConnectionResetError / requests.ConnectionError
                    if _attempt == 2:
                        raise
                    log_warn(f"Discord post retry for {chart_path.name}: {_ce}")
            if resp is not None and resp.status_code in (200, 204):
                log_ok(f"Posted: {chart_path.name}")
                # Determine channel from webhook URL
                channel = "#nfl-moneylines"
                if webhook_url == os.getenv("DISCORD_WEBHOOK_NFL_SPREADS", ""):
                    channel = "#nfl-spreads"
                elif webhook_url == os.getenv("DISCORD_WEBHOOK_NFL_TOTALS", ""):
                    channel = "#nfl-totals"
                _post_receipt("chart", channel, chart_path.name)
                try:
                    from discord_post_logger import log_post, save_image
                    img_url = save_image("odds", str(chart_path), chart_path.stem)
                    log_post("nfl_odds_monitor", channel, "chart",
                             title=title, content=content_text,
                             has_image=True, image_name=chart_path.name,
                             image_url=img_url)
                except Exception:
                    pass
                return True
            else:
                log_warn(f"Discord {resp.status_code} for {chart_path.name}")
                return False
    except Exception as e:
        log_error("Discord post failed", e)
        return False


# ==========================================================================
# FETCH AND STORE ODDS
# ==========================================================================

def _log_api_usage(key_id: str, credits_used: int, credits_remaining: int,
                   games_count: int, bookmakers_count: int, sport_key: str):
    """Log API usage to BOTH SQLite (ephemeral cache) and Supabase (persistent)."""
    ts = datetime.now(timezone.utc).isoformat()

    # SQLite (ephemeral in-container cache)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""INSERT INTO api_usage
            (timestamp, credits_used, credits_remaining, api_key_used, games_count)
            VALUES (?,?,?,?,?)""",
            (ts, credits_used, credits_remaining, key_id, games_count))
        conn.commit()
        conn.close()
    except Exception as e:
        log_warn(f"SQLite api_usage write failed: {e}")

    # Supabase (persistent across restarts)
    if _sb_client:
        try:
            _sb_client.record_api_usage(
                api_key_id=key_id,
                credits_used=credits_used,
                credits_remaining=credits_remaining,
                games_count=games_count,
                bookmakers_count=bookmakers_count,
                regions=REGIONS,
                sport_key=sport_key,
            )
        except Exception as e:
            log_warn(f"Supabase api_usage write failed: {e}")


def fetch_odds(api_mgr: APIKeyManager) -> Tuple[Optional[list], Optional[int]]:
    """Fetch ALL odds from TheOddsAPI — single call for h2h/spreads/totals.
    Reads actual x-requests-remaining from headers.
    Handles dead/depleted keys (401) by marking inactive and cycling through
    ALL available keys until one works."""
    global SPORT_KEY

    # Try each sport key until we get games
    for sport_key in SPORT_KEYS:
        # Retry loop: cycle through keys on 401 (dead/depleted)
        max_retries = len(API_KEYS)
        for attempt in range(max_retries):
            api_key, key_id = api_mgr.get_key()
            if not api_key:
                log_error("No active API keys remaining", critical=True)
                return None, None

            try:
                url = f"{API_URL_BASE}/{sport_key}/odds"
                params = {
                    'apiKey': api_key,
                    'regions': REGIONS,
                    'markets': MARKETS,
                    'oddsFormat': 'decimal',
                    'dateFormat': 'iso'
                }
                log_info(f"Fetching odds ({sport_key}) with {key_id} "
                         f"(attempt {attempt + 1}/{max_retries})...")
                resp = _session.get(url, params=params, timeout=30)

                # 401 = dead key or depleted credits — mark inactive, try next
                if resp.status_code == 401:
                    log_warn(f"{key_id} returned 401 — marking as inactive")
                    api_mgr.mark_dead(key_id)
                    continue  # Try next key

                # Read ACTUAL credit info from API response headers
                credits_remaining = int(resp.headers.get('x-requests-remaining', 0))
                credits_used = int(resp.headers.get('x-requests-used', 0))

                # Update key state with real header values
                api_mgr.update_credits(credits_remaining, credits_used)

                if resp.status_code == 200:
                    data = resp.json()
                    if not data and len(SPORT_KEYS) > 1 and sport_key != SPORT_KEYS[-1]:
                        log_warn(f"{sport_key} returned 0 games — trying next sport key...")
                        break  # Break retry loop, continue sport key loop

                    # Count stats
                    ml = sp = to = 0
                    bookmaker_set = set()
                    live_games = 0
                    for g in data:
                        if is_game_live(g.get('commence_time', '')):
                            live_games += 1
                        for bm in g.get('bookmakers', []):
                            bookmaker_set.add(bm['key'])
                            for mkt in bm.get('markets', []):
                                n = len(mkt.get('outcomes', []))
                                if mkt['key'] == 'h2h': ml += n
                                elif mkt['key'] == 'spreads': sp += n
                                elif mkt['key'] == 'totals': to += n

                    SPORT_KEY = sport_key
                    log_ok(f"Fetched {len(data)} games from {sport_key} — "
                           f"ML:{ml} SP:{sp} TO:{to} — "
                           f"{credits_remaining} credits left ({key_id})")

                    # Save raw JSON with enriched metadata
                    ts_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
                    raw_file = RAW_DIR / f"raw_odds_unified_{ts_str}.json"
                    save_data = {
                        'fetch_timestamp': datetime.now(timezone.utc).isoformat(),
                        'script_version': SCRIPT_VERSION,
                        'sport_key': sport_key,
                        'credits_remaining': credits_remaining,
                        'credits_used': credits_used,
                        'regions': REGIONS,
                        'total_games': len(data),
                        'live_games': live_games,
                        'unique_bookmakers': sorted(list(bookmaker_set)),
                        'bookmakers_count': len(bookmaker_set),
                        'moneyline_count': ml,
                        'spread_count': sp,
                        'total_count': to,
                        'api_key_used': key_id,
                        'games': data,
                    }
                    with open(raw_file, 'w') as f:
                        json.dump(save_data, f, indent=2)
                    log_ok(f"Raw data saved: {raw_file.name}")

                    # Log API usage to BOTH SQLite and Supabase
                    _log_api_usage(key_id, credits_used, credits_remaining,
                                   len(data), len(bookmaker_set), sport_key)

                    return data, credits_remaining

                elif resp.status_code == 422:
                    log_warn(f"{sport_key} not available (422) — trying next...")
                    break  # Break retry loop, continue sport key loop
                else:
                    log_error(f"API error for {sport_key}: {resp.status_code} — "
                              f"{resp.text[:200]}")
                    return None, None

            except Exception as e:
                log_error(f"Fetch failed for {sport_key} with {key_id}", e)
                return None, None
        else:
            # All retries exhausted for this sport key
            log_error(f"All {max_retries} API keys returned 401 for {sport_key}",
                      critical=True)
            return None, None

    log_warn("All sport keys returned 0 games")
    return [], 0


def store_odds(data: list) -> int:
    """Store all odds with calculated averages per market."""
    if not data:
        return 0
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    ts = datetime.now(timezone.utc).isoformat()
    records = 0

    # Collectors for averages
    ml_coll = {}      # (game_id, team) -> [american_odds]
    sp_coll = {}      # (game_id, team, point) -> [american_odds]
    to_coll = {}      # (game_id, point) -> {'Over': [], 'Under': []}
    live_map = {}      # game_id -> bool

    try:
        for game in data:
            gid = game.get('id')
            commence = game.get('commence_time', '')
            is_live = is_game_live(commence)
            live_map[gid] = is_live
            home = game.get('home_team')
            away = game.get('away_team')

            c.execute("""INSERT OR REPLACE INTO games
                (game_id, sport_key, commence_time, home_team, away_team, last_update)
                VALUES (?,?,?,?,?,?)""",
                (gid, game.get('sport_key', SPORT_KEY), commence, home, away, ts))

            for bm in game.get('bookmakers', []):
                bk = bm.get('key')
                bt = bm.get('title')
                for mkt in bm.get('markets', []):
                    mk = mkt.get('key')
                    for out in mkt.get('outcomes', []):
                        pd_val = out.get('price', 1.0)
                        pa = decimal_to_american(pd_val)
                        pt = out.get('point')
                        on = out.get('name')

                        # Collect for averages
                        if mk == 'h2h':
                            ml_coll.setdefault((gid, on), []).append(pa)
                        elif mk == 'spreads' and pt is not None:
                            sp_coll.setdefault((gid, on, pt), []).append(pa)
                        elif mk == 'totals' and pt is not None:
                            to_coll.setdefault((gid, pt), {'Over': [], 'Under': []})
                            if on in ('Over', 'Under'):
                                to_coll[(gid, pt)][on].append(pa)

                        # History
                        c.execute("""INSERT INTO odds_history
                            (game_id,fetch_timestamp,bookmaker_key,bookmaker_title,
                             market_key,outcome_name,price_decimal,price_american,point,is_live_game)
                            VALUES (?,?,?,?,?,?,?,?,?,?)""",
                            (gid, ts, bk, bt, mk, on, pd_val, pa, pt, is_live))

                        # Upsert current odds (use point_key for UNIQUE constraint)
                        pt_key = pt if pt is not None else -999999
                        c.execute("""UPDATE odds SET price_decimal=?, price_american=?, timestamp=?, point=?
                            WHERE game_id=? AND bookmaker_key=? AND market_key=?
                            AND outcome_name=? AND point_key=?""",
                            (pd_val, pa, ts, pt, gid, bk, mk, on, pt_key))

                        if c.rowcount == 0:
                            try:
                                c.execute("""INSERT INTO odds
                                    (game_id,timestamp,bookmaker_key,bookmaker_title,
                                     market_key,outcome_name,price_decimal,price_american,point,point_key)
                                    VALUES (?,?,?,?,?,?,?,?,?,?)""",
                                    (gid, ts, bk, bt, mk, on, pd_val, pa, pt, pt_key))
                            except sqlite3.IntegrityError:
                                pass
                        records += 1

        # Store averages as '_average' bookmaker in history
        avg_count = 0

        for (gid, team), odds_list in ml_coll.items():
            if odds_list:
                avg_am = calculate_average_odds(odds_list)
                avg_dec = american_to_decimal(avg_am)
                c.execute("""INSERT INTO odds_history
                    (game_id,fetch_timestamp,bookmaker_key,bookmaker_title,
                     market_key,outcome_name,price_decimal,price_american,point,is_live_game)
                    VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (gid, ts, '_average', f'AVERAGE ({len(odds_list)} books)',
                     'h2h', team, avg_dec, avg_am, None, live_map.get(gid, False)))
                avg_count += 1

        for (gid, team, pt), odds_list in sp_coll.items():
            if odds_list:
                avg_am = calculate_average_odds(odds_list)
                avg_dec = american_to_decimal(avg_am)
                c.execute("""INSERT INTO odds_history
                    (game_id,fetch_timestamp,bookmaker_key,bookmaker_title,
                     market_key,outcome_name,price_decimal,price_american,point,is_live_game)
                    VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (gid, ts, '_average', f'AVERAGE ({len(odds_list)} books)',
                     'spreads', team, avg_dec, avg_am, pt, live_map.get(gid, False)))
                avg_count += 1

        for (gid, pt), sides in to_coll.items():
            for side_name in ('Over', 'Under'):
                odds_list = sides[side_name]
                if odds_list:
                    avg_am = calculate_average_odds(odds_list)
                    avg_dec = american_to_decimal(avg_am)
                    c.execute("""INSERT INTO odds_history
                        (game_id,fetch_timestamp,bookmaker_key,bookmaker_title,
                         market_key,outcome_name,price_decimal,price_american,point,is_live_game)
                        VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        (gid, ts, '_average', f'AVERAGE ({len(odds_list)} books)',
                         'totals', side_name, avg_dec, avg_am, pt, live_map.get(gid, False)))
                    avg_count += 1

        conn.commit()
        log_ok(f"Stored {records} records + {avg_count} averages (SQLite)")

        # ── Push to Supabase nfl_odds_history (persistent) ──
        if _sb_client:
            sb_records = []
            for game in data:
                gid = game.get('id')
                for bm in game.get('bookmakers', []):
                    bk = bm.get('key')
                    for mkt in bm.get('markets', []):
                        mk = mkt.get('key')
                        for out in mkt.get('outcomes', []):
                            pa = decimal_to_american(out.get('price', 1.0))
                            sb_records.append({
                                "game_id": gid,
                                "bookmaker": bk,
                                "market": mk,
                                "outcome": out.get('name'),
                                "price": pa,
                                "point": out.get('point'),
                            })
            # Also store averages
            for (gid, team), odds_list in ml_coll.items():
                if odds_list:
                    sb_records.append({
                        "game_id": gid, "bookmaker": "_average",
                        "market": "h2h", "outcome": team,
                        "price": calculate_average_odds(odds_list), "point": None,
                    })
            for (gid, team, pt), odds_list in sp_coll.items():
                if odds_list:
                    sb_records.append({
                        "game_id": gid, "bookmaker": "_average",
                        "market": "spreads", "outcome": team,
                        "price": calculate_average_odds(odds_list), "point": pt,
                    })
            for (gid, pt), sides in to_coll.items():
                for side_name in ('Over', 'Under'):
                    if sides[side_name]:
                        sb_records.append({
                            "game_id": gid, "bookmaker": "_average",
                            "market": "totals", "outcome": side_name,
                            "price": calculate_average_odds(sides[side_name]),
                            "point": pt,
                        })
            try:
                _sb_client.append_odds_history(sb_records)
            except Exception as e:
                log_warn(f"Supabase odds history push failed: {e}")

    except Exception as e:
        log_error("Store failed", e)
        conn.rollback()
    finally:
        conn.close()
    return records


# ==========================================================================
# CHART HELPERS
# ==========================================================================

def _setup_axes(ax):
    """Common dark-theme axis styling."""
    ax.set_facecolor('#1A1F2E')
    ax.tick_params(axis='x', colors='#CCCCCC', which='both')
    ax.tick_params(axis='y', colors='#CCCCCC', which='both')
    for spine in ax.spines.values():
        spine.set_edgecolor('#4A5568')
    ax.grid(True, alpha=0.3, axis='y', color='#4A5568')
    ax.grid(True, alpha=0.1, axis='x', color='#4A5568')


def _format_x_axis(ax, book_df):
    """Place x-axis ticks at each actual fetch timestamp so every data point
    is labeled with the time it was obtained."""
    if book_df.empty:
        return
    eastern = pytz.timezone('US/Eastern')
    # Convert to datetime — book_df from SQL has raw strings
    ts = pd.to_datetime(book_df['created_at'].astype(str).str.replace('T', ' ', regex=False).str.slice(0, 19), utc=True).dt.tz_convert(eastern)

    # Get unique fetch timestamps (rounded to the minute to collapse sub-second jitter)
    unique_ts = ts.dt.floor('min').drop_duplicates().sort_values()

    # If many fetch times (> 20), thin to avoid overlapping labels
    if len(unique_ts) > 20:
        step = max(1, len(unique_ts) // 15)
        unique_ts = unique_ts.iloc[::step]

    # Place ticks at the actual fetch times
    tick_positions = [t.to_pydatetime() for t in unique_ts]
    ax.set_xticks(tick_positions)

    # Format: time on top, date below
    time_range = ts.max() - ts.min()
    if time_range < pd.Timedelta(days=1):
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d\n%I:%M%p', tz=eastern))
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d\n%I:%M%p', tz=eastern))


def _american_formatter(y, pos):
    """Y-axis formatter: decimal odds → American label."""
    try:
        if y >= 2.0:
            return f"+{int((y-1)*100)}"
        elif y > 1.0:
            return f"{int(-100/(y-1))}"
    except Exception:
        pass
    return ""


def _plot_book_lines(ax, book_df, avg_df):
    """Plot individual bookmaker lines + average line on an axis."""
    eastern = pytz.timezone('US/Eastern')

    # Average line
    if not avg_df.empty:
        avg_df = avg_df.copy()
        avg_df['created_at'] = pd.to_datetime(avg_df['created_at'].astype(str).str.replace('T', ' ', regex=False).str.slice(0, 19), utc=True).dt.tz_convert(eastern)
        avg_df['price_decimal'] = avg_df['price_american'].apply(american_to_decimal)

        match = re.search(r'\((\d+) books?\)', str(avg_df['bookmaker_title'].iloc[0]))
        nbooks = match.group(1) if match else "?"

        if len(avg_df) > 1:
            ax.plot(avg_df['created_at'], avg_df['price_decimal'],
                    label=f'AVERAGE ({nbooks} books)', color='#FFD700',
                    linewidth=3.5, linestyle='--', alpha=0.9, zorder=10)
            ax.scatter(avg_df['created_at'].iloc[0], avg_df['price_decimal'].iloc[0],
                       color='#FFD700', s=150, zorder=20, edgecolor='white', linewidth=2, marker='s')
            ax.scatter(avg_df['created_at'].iloc[-1], avg_df['price_decimal'].iloc[-1],
                       color='#FFD700', s=150, zorder=20, edgecolor='yellow', linewidth=2, marker='^')
        else:
            ax.scatter(avg_df['created_at'].iloc[0], avg_df['price_decimal'].iloc[0],
                       label=f'AVERAGE ({nbooks} books)', color='#FFD700',
                       s=250, marker='D', edgecolor='white', linewidth=2, zorder=10)

    # Individual bookmakers
    if not book_df.empty:
        book_df = book_df.copy()
        book_df['created_at'] = pd.to_datetime(book_df['created_at'].astype(str).str.replace('T', ' ', regex=False).str.slice(0, 19), utc=True).dt.tz_convert(eastern)
        book_df['price_decimal'] = book_df['price_american'].apply(american_to_decimal)

        for bk in book_df['bookmaker_key'].unique():
            bd = book_df[book_df['bookmaker_key'] == bk].sort_values('created_at')
            clr = BOOKMAKER_COLORS.get(bk, '#888888')
            if len(bd) > 1:
                ax.plot(bd['created_at'], bd['price_decimal'],
                        label=bd['bookmaker_title'].iloc[0], color=clr,
                        linewidth=2, marker='o', markersize=6, alpha=0.9, zorder=12,
                        markeredgecolor='white', markeredgewidth=1)
            else:
                ax.scatter(bd['created_at'].iloc[0], bd['price_decimal'].iloc[0],
                           label=bd['bookmaker_title'].iloc[0], color=clr,
                           s=150, marker='o', edgecolor='white', linewidth=2, zorder=15)

    # Y-axis range
    all_dec = list(book_df['price_decimal']) if not book_df.empty else []
    if not avg_df.empty:
        all_dec.extend(list(avg_df['price_decimal']))
    if all_dec:
        ymin, ymax = min(all_dec), max(all_dec)
        pad = abs(ymax - ymin) * 0.15 or 0.05
        ax.set_ylim(ymax + pad, ymin - pad)

    ax.yaxis.set_major_formatter(FuncFormatter(_american_formatter))
    ax.yaxis.tick_right()
    ax.yaxis.set_label_position('right')
    ax.set_ylabel('American Odds', fontsize=10, color='#CCCCCC')


def _add_odds_header(ax, book_df, avg_df, prefix: str = "", label: str = ""):
    """Add MIN/AVG/MAX header text to chart panel."""
    if book_df.empty:
        return
    latest = book_df[book_df['created_at'] == book_df['created_at'].max()]['price_american'].values
    if len(latest) == 0:
        return
    lo, hi = min(latest), max(latest)
    parts = [label] if label else []
    if prefix:
        parts.append(prefix)
    parts.append(f"MIN: {lo:+d}")
    if not avg_df.empty:
        parts.append(f"AVG: {avg_df.iloc[-1]['price_american']:+d}")
    parts.append(f"MAX: {hi:+d}")
    txt = "  •  ".join(parts)
    import matplotlib.patheffects as _pe
    ax.text(0.5, 0.98, txt, transform=ax.transAxes, fontsize=11, color='#FFD700',
            weight='bold', va='top', ha='center',
            path_effects=[_pe.SimpleLineShadow(offset=(2, -2), alpha=0.6, rho=0.3), _pe.Normal()],
            bbox=dict(boxstyle='round,pad=0.4', facecolor='#1A1F2E',
                      edgecolor='#FFD700', linewidth=2, alpha=0.9))


def _game_title(game_row) -> str:
    """Format game date/time for chart title."""
    try:
        dt = pd.to_datetime(game_row['commence_time'])
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        return dt.astimezone(pytz.timezone('US/Eastern')).strftime('%m/%d %I:%M%p ET')
    except Exception:
        return ""


# ==========================================================================
# TEAM LOGOS — ported from mlb_unified_odds_monitor / mlb_daily
# ==========================================================================
# MLB resolves a logo through three tiers, verified at each step
# (mlb_daily._fetch_logo_uncached):
#   1. ESPN canonical  a.espncdn.com/i/teamlogos/mlb/500/{abbr}.png   <- primary
#   2. MLB static      www.mlbstatic.com/team-logos/{team_id}.svg
#   3. Supabase        mlb_team_assets.logo_espn_base64               <- last resort
#
# NFL has no numeric team_id scheme and no nfl_team_assets table exists, so tier
# 1 is the whole chain here. That is not a downgrade: tier 1 is what actually
# serves MLB logos in practice; 2 and 3 are fallbacks that rarely fire.
#
# TEAM_ABBR already holds all 32 NFL abbreviations and they ARE the ESPN
# abbreviations, so no second mapping table is needed. All 32 URLs verified
# reachable 2026-08-25 (32/32, every asset > 1 KB).
_logo_cache_mpl = {}          # {team_name: ndarray | None}
ESPN_NFL_LOGO = "https://a.espncdn.com/i/teamlogos/nfl/500/{abbr}.png"


def _get_team_logo_for_chart(team_name: str, size: int = 36):
    """Return an RGBA ndarray for matplotlib, or None. Never raises.

    Cached per team_name for the life of the process — a chart run draws the
    same handful of teams repeatedly and each miss is an HTTP round trip.
    A None result is cached too, so a dead team never re-fetches every panel.
    """
    if team_name in _logo_cache_mpl:
        return _logo_cache_mpl[team_name]

    logo = None
    try:
        abbr = TEAM_ABBR.get(team_name)
        if abbr:
            import numpy as np
            from PIL import Image
            url = ESPN_NFL_LOGO.format(abbr=abbr.lower())
            r = _session.get(url, timeout=8)
            # Verify it is actually an image before trusting it. ESPN answers
            # 200 with an HTML error page for an unknown abbreviation, which
            # would otherwise be embedded as a broken smear in the title band.
            if r.status_code == 200 and len(r.content) > 1000:
                im = Image.open(BytesIO(r.content)).convert("RGBA")
                im = im.resize((size, size), Image.LANCZOS)
                logo = np.array(im)
            else:
                log_warn(f"Logo fetch for {team_name} ({abbr}): "
                         f"HTTP {r.status_code}, {len(r.content)} bytes")
    except Exception as e:
        log_warn(f"Logo load failed for {team_name}: {e}")

    _logo_cache_mpl[team_name] = logo
    return logo



# League logo for THIS monitor's sport, loaded lazily from assets/<league>_logo.png.
# The filename used to be hardcoded to "mlb_logo.png" in all four league monitors, so
# NBA and NHL rendered the MLB badge and NFL rendered no logo at all (no assets/ dir).
_LEAGUE_SLUG = "NFL"
_LEAGUE_LOGO = None
_LEAGUE_LOGO_LEAGUE = None


def _get_league_logo():
    """Return this sport's league logo cropped to its opaque ink (no transparent padding).

    The natural aspect ratio is preserved. NFL/NBA/NHL marks are portrait, so the old
    forced resize to a fixed 269x142 canvas stretched them badly. The header scales by
    height and derives width from the measured extent, so no resize is needed here.
    """
    global _LEAGUE_LOGO, _LEAGUE_LOGO_LEAGUE
    if _LEAGUE_LOGO is not None and _LEAGUE_LOGO_LEAGUE == _LEAGUE_SLUG:
        return _LEAGUE_LOGO
    _LEAGUE_LOGO = False
    _LEAGUE_LOGO_LEAGUE = _LEAGUE_SLUG
    try:
        import os
        from pathlib import Path
        import numpy as np
        from PIL import Image
        fname = "%s_logo.png" % _LEAGUE_SLUG.lower()
        # Candidate asset roots, in order. A monitor can live in a scripts/ subdir
        # (HF Space: <repo>/scripts/x.py, so parent.parent IS the repo) or at a repo
        # root (the NFL GitHub Actions lane: <repo>/x.py, so parent IS the repo).
        # Checking only parent.parent made the NFL lane miss assets/ entirely.
        cands = []
        base = os.environ.get("ABET_BASE_DIR")
        if base:
            cands.append(Path(base))
        cands.append(Path(__file__).parent.parent)
        cands.append(Path(__file__).parent)
        p = None
        for c in cands:
            cand = c / "assets" / fname
            if cand.exists():
                p = cand
                break
        if p is None:
            log_warn(f"League logo asset missing: {fname} (looked in "
                     + ", ".join(str(c / "assets") for c in cands) + ")")
            return _LEAGUE_LOGO
        img = Image.open(p).convert("RGBA")
        ys, xs = np.where(np.array(img)[..., 3] > 8)
        if len(xs) == 0:
            log_warn(f"League logo has no opaque pixels: {fname}")
            return _LEAGUE_LOGO
        ink = img.crop((int(xs.min()), int(ys.min()), int(xs.max()) + 1, int(ys.max()) + 1))
        _LEAGUE_LOGO = np.array(ink)
    except Exception as e:
        log_warn(f"League logo load failed ({_LEAGUE_SLUG}): {e}")
    return _LEAGUE_LOGO

def _add_logo_title(fig, away_team: str, home_team: str, title_text: str, game_time: str):
    """Borderless header: MLB logo LEFT, team logos CENTERED, date/time stacked RIGHT.

    No boxes, no pills, no band spines - just the elements on the band background.
    Element sizes are MEASURED on a throwaway probe axes built with the band's exact
    geometry, then discarded: matplotlib's rendered extent does not match the analytic
    arr*zoom*(dpi/72) formula, and measuring on the real band leaves stray artists
    behind. Because the probe matches the band, measured px widths convert straight to
    band fractions and every final position is computed once.
    Call AFTER the final plt.tight_layout().
    """
    try:
        from matplotlib.offsetbox import OffsetImage, AnnotationBbox
        from matplotlib.textpath import TextPath
        from matplotlib.font_manager import FontProperties
        import numpy as np
        import re
        league = _LEAGUE_SLUG
        axes = fig.get_axes()
        if not axes:
            return
        left = axes[0].get_position().x0

        BAND_H = 0.13
        BAND_TOP = 0.87
        PAD_PX = 12.0
        GAP_PX = 14.0
        STACK_GAP_PX = 3.0
        GOLD = '#FFD700'

        fig_w, fig_h, dpi = fig.get_figwidth(), fig.get_figheight(), float(fig.dpi)
        band_w_px = (1.0 - left) * fig_w * dpi
        band_h_px = BAND_H * fig_h * dpi
        pad_x, gap_x = PAD_PX / band_w_px, GAP_PX / band_w_px
        # Right-align the date/time to the CHART's right edge, not the band's: the band
        # spans to the figure edge but the plot area stops short of it.
        pad_fig = 0.0                      # exact alignment to the chart edge
        text_right_b = ((axes[0].get_position().x1 - pad_fig) - left) / (1.0 - left)
        stack_gap = STACK_GAP_PX / band_h_px
        logo_h_b = (BAND_H * fig_h * dpi - 4.0) / band_h_px

        band = fig.add_axes([left, BAND_TOP, 1.0 - left, BAND_H])
        band.set_in_layout(False)
        band.set_xlim(0, 1); band.set_ylim(0, 1)
        band.set_facecolor('#0F1419')
        band.set_xticks([]); band.set_yticks([])
        for side in band.spines.values():
            side.set_visible(False)

        probe = fig.add_axes([left, BAND_TOP, 1.0 - left, BAND_H])
        probe.set_xlim(0, 1); probe.set_ylim(0, 1)
        probe.set_axis_off()
        probe.patch.set_visible(False)

        def measure_logo(arr):
            if not isinstance(arr, np.ndarray) or arr.ndim != 3 or arr.shape[2] != 4:
                return None, 0.0, 0.0
            ys, xs = np.where(arr[..., 3] > 8)
            if len(xs) == 0:
                return None, 0.0, 0.0
            arr = arr[ys.min():ys.max() + 1, xs.min():xs.max() + 1]
            z = (logo_h_b * band_h_px) * 72.0 / (arr.shape[0] * dpi)
            a = AnnotationBbox(OffsetImage(arr, zoom=z), (0, 0.5), frameon=False,
                               xycoords='axes fraction', box_alignment=(0, 0.5), pad=0)
            probe.add_artist(a)
            fig.canvas.draw()
            e = a.get_window_extent()
            a.remove()
            return arr, z, e.width / band_w_px

        def measure_text(txt, fsz):
            t = probe.text(0, 0.5, txt, transform=probe.transAxes, fontsize=fsz,
                           fontweight='bold', ha='left', va='center')
            fig.canvas.draw()
            e = t.get_window_extent()
            w_b, h_b = e.width / band_w_px, e.height / band_h_px
            t.remove()
            return w_b, h_b

        def put_logo(arr, z, x):
            if arr is None:
                return
            a = AnnotationBbox(OffsetImage(arr, zoom=z), (x, 0.5), frameon=False,
                               xycoords='axes fraction', box_alignment=(0, 0.5),
                               pad=0, clip_on=True)
            a.set_clip_path(band.patch)
            band.add_artist(a)

        def font_for(txt, ratio):
            return (logo_h_b * band_h_px * ratio) / max(
                float(TextPath((0, 0), txt, size=1.0).get_extents().height), 1e-6)

        bb_at = TextPath((0, 0), '@', size=1.0,
                         prop=FontProperties(weight='bold')).get_extents()
        fs_at = (logo_h_b * band_h_px * 0.42) / (float(bb_at.height) or 1.0)

        mlb_arr, mlb_z, mlb_w = measure_logo(_get_league_logo())
        away_arr, away_z, away_w = measure_logo(_get_team_logo_for_chart(away_team, size=256))
        home_arr, home_z, home_w = measure_logo(_get_team_logo_for_chart(home_team, size=256))
        at_w, at_h = measure_text('@', fs_at)
        grp_w = away_w + gap_x + at_w + gap_x + home_w

        date_s, time_s = '', ''
        if game_time:
            mm = re.match(r'^\s*(\d{2}/\d{2})\s+(.+?)\s*$', game_time)
            if mm:
                date_s, time_s = mm.group(1), mm.group(2)
            else:
                date_s = game_time

        fs_d = fs_t = None
        d_w = d_h = t_w = t_h = 0.0
        if date_s or time_s:
            fs_d = font_for(date_s or time_s, 0.22)
            fs_t = font_for(time_s or date_s, 0.17)
            # The stack must stay inside the band; shrink both until it does.
            for _ in range(12):
                if date_s:
                    d_w, d_h = measure_text(date_s, fs_d)
                if time_s:
                    t_w, t_h = measure_text(time_s, fs_t)
                if (d_h + stack_gap + t_h) <= 0.92 or fs_d <= 6.0:
                    break
                fs_d *= 0.92
                fs_t *= 0.92
        stack_h = d_h + stack_gap + t_h
        y_date = 0.5 + (stack_h - d_h) / 2.0
        y_time = 0.5 - stack_h / 2.0 + t_h / 2.0
        text_left = text_right_b - max(d_w, t_w)

        mlb_x = pad_x
        put_logo(mlb_arr, mlb_z, mlb_x)

        if away_arr is not None:
            avail_l = mlb_x + mlb_w + gap_x
            avail_r = text_left - gap_x
            gx = max((avail_l + avail_r) / 2.0 - grp_w / 2.0, avail_l)
            put_logo(away_arr, away_z, gx)
            band.text(gx + away_w + gap_x, 0.5, '@', transform=band.transAxes,
                      fontsize=fs_at, color='#9CA3AF', fontweight='bold',
                      ha='left', va='center', clip_on=True)
            put_logo(home_arr, home_z, gx + away_w + gap_x + at_w + gap_x)

        if date_s:
            band.text(text_right_b, y_date, date_s, transform=band.transAxes,
                      fontsize=fs_d, color=GOLD, fontweight='bold',
                      ha='right', va='center', clip_on=True)
        if time_s:
            band.text(text_right_b, y_time, time_s, transform=band.transAxes,
                      fontsize=fs_t, color='#9CA3AF', fontweight='bold',
                      ha='right', va='center', clip_on=True)
        probe.remove()
    except Exception as e:
        log_warn(f"Logo title failed: {e}")


# MONEYLINE CHARTS
# ==========================================================================

def generate_moneyline_charts() -> List[Tuple[Path, dict]]:
    """Generate moneyline charts. Returns list of (path, game_info) tuples."""
    log_info("Generating moneyline charts...")
    charts = []
    try:
        conn = sqlite3.connect(DB_PATH)
        games = pd.read_sql_query("""
            SELECT DISTINCT g.game_id, g.home_team, g.away_team, g.commence_time
            FROM games g JOIN odds_history oh ON g.game_id = oh.game_id
            WHERE g.last_update = (SELECT MAX(last_update) FROM games)
            AND oh.market_key='h2h' AND oh.bookmaker_key != '_average'
            GROUP BY g.game_id ORDER BY g.commence_time
        """, conn)

        for _, row in games.iterrows():
            gid = row['game_id']
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9), sharex=True)
            fig.patch.set_facecolor('#0F1419')

            for ax, team in zip([ax1, ax2], [row['away_team'], row['home_team']]):
                _setup_axes(ax)
                bdf = pd.read_sql_query(
                    "SELECT strftime('%Y-%m-%d %H:%M:%S', created_at) AS created_at, bookmaker_key, bookmaker_title, price_american "
                    "FROM odds_history WHERE game_id=? AND outcome_name=? AND market_key='h2h' "
                    "AND bookmaker_key!='_average' ORDER BY strftime('%Y-%m-%d %H:%M:%S', created_at)",
                    conn, params=(gid, team))
                adf = pd.read_sql_query(
                    "SELECT strftime('%Y-%m-%d %H:%M:%S', created_at) AS created_at, price_american, bookmaker_title "
                    "FROM odds_history WHERE game_id=? AND outcome_name=? AND market_key='h2h' "
                    "AND bookmaker_key='_average' ORDER BY strftime('%Y-%m-%d %H:%M:%S', created_at)",
                    conn, params=(gid, team))
                _plot_book_lines(ax, bdf, adf)
                _add_odds_header(ax, bdf, adf, label=f'{abbreviate_team(team)} MONEYLINE')
                if ax == ax1 and not bdf.empty:
                    leg = ax.legend(loc='upper left', fontsize=8, ncol=2,
                                    facecolor='#1A1F2E', edgecolor='#4A5568', framealpha=0.9)
                    if leg:
                        for t in leg.get_texts():
                            t.set_color('#CCCCCC')
                            if 'AVERAGE' in t.get_text():
                                t.set_weight('bold'); t.set_color('#FFD700')
                _format_x_axis(ax, bdf)

            gt = _game_title(row)
            # Enhanced title with team logos
            title_text = f"{row['away_team']} @ {row['home_team']} MONEYLINE — {gt}"

            ax2.set_xlabel('Time', fontsize=10, color='#CCCCCC')
            for lbl in ax2.xaxis.get_ticklabels():
                lbl.set_rotation(45); lbl.set_ha('right'); lbl.set_color('#CCCCCC')
            plt.tight_layout(rect=[0, 0, 1, 0.87])
            _add_logo_title(fig, row['away_team'], row['home_team'], title_text, gt)

            path = CHARTS_DIR / f"{gid[:8]}_ml_{datetime.now().strftime('%H%M%S')}.png"
            plt.savefig(path, dpi=130, facecolor='#0F1419', bbox_inches='tight')
            plt.close()

            game_info = {
                'away_team': row['away_team'], 'home_team': row['home_team'],
                'commence_time': row['commence_time'],
            }
            charts.append((path, game_info))
            log_ok(f"ML: {row['away_team']} @ {row['home_team']}")

        conn.close()
    except Exception as e:
        log_error("Moneyline chart generation failed", e)
        plt.close('all')
    return charts


# ==========================================================================
# SPREAD CHARTS
# ==========================================================================

def generate_spread_charts() -> List[Tuple[Path, dict]]:
    """Generate spread charts. Returns list of (path, game_info) tuples."""
    log_info("Generating spread charts...")
    charts = []
    try:
        conn = sqlite3.connect(DB_PATH)
        games = pd.read_sql_query("""
            SELECT DISTINCT g.game_id, g.home_team, g.away_team, g.commence_time,
                oh1.point as home_spread, oh2.point as away_spread
            FROM games g
            JOIN odds_history oh1 ON g.game_id=oh1.game_id AND oh1.outcome_name=g.home_team
            JOIN odds_history oh2 ON g.game_id=oh2.game_id AND oh2.outcome_name=g.away_team
            WHERE g.last_update=(SELECT MAX(last_update) FROM games)
            AND oh1.market_key='spreads' AND oh2.market_key='spreads'
            AND oh1.created_at=oh2.created_at AND oh1.bookmaker_key=oh2.bookmaker_key
            AND oh1.bookmaker_key!='_average'
            GROUP BY g.game_id, oh1.point, oh2.point
            ORDER BY g.commence_time, oh1.point
        """, conn)

        for _, row in games.iterrows():
            gid = row['game_id']
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9), sharex=True)
            fig.patch.set_facecolor('#0F1419')

            for ax, team, spread in zip([ax1, ax2],
                                         [row['home_team'], row['away_team']],
                                         [row['home_spread'], row['away_spread']]):
                _setup_axes(ax)
                bdf = pd.read_sql_query(
                    "SELECT strftime('%Y-%m-%d %H:%M:%S', created_at) AS created_at, bookmaker_key, bookmaker_title, price_american "
                    "FROM odds_history WHERE game_id=? AND outcome_name=? AND point=? "
                    "AND market_key='spreads' AND bookmaker_key!='_average' ORDER BY strftime('%Y-%m-%d %H:%M:%S', created_at)",
                    conn, params=(gid, team, spread))
                adf = pd.read_sql_query(
                    "SELECT strftime('%Y-%m-%d %H:%M:%S', created_at) AS created_at, price_american, bookmaker_title "
                    "FROM odds_history WHERE game_id=? AND outcome_name=? AND point=? "
                    "AND market_key='spreads' AND bookmaker_key='_average' ORDER BY strftime('%Y-%m-%d %H:%M:%S', created_at)",
                    conn, params=(gid, team, spread))
                _plot_book_lines(ax, bdf, adf)
                sp_txt = f"+{spread}" if spread > 0 else str(spread)
                _add_odds_header(ax, bdf, adf, prefix=f"SPREAD: {sp_txt}", label=f'{abbreviate_team(team)} SPREAD')
                if ax == ax1 and not bdf.empty:
                    leg = ax.legend(loc='upper left', fontsize=8, ncol=2,
                                    facecolor='#1A1F2E', edgecolor='#4A5568', framealpha=0.9)
                    if leg:
                        for t in leg.get_texts():
                            t.set_color('#CCCCCC')
                            if 'AVERAGE' in t.get_text():
                                t.set_weight('bold'); t.set_color('#FFD700')
                _format_x_axis(ax, bdf)

            hs = row['home_spread']
            label = f"SPREAD {hs:+g}/{row['away_spread']:+g}"
            gt = _game_title(row)
            title_text = f"{row['away_team']} @ {row['home_team']} {label} — {gt}"

            ax2.set_xlabel('Time', fontsize=10, color='#CCCCCC')
            for lbl in ax2.xaxis.get_ticklabels():
                lbl.set_rotation(45); lbl.set_ha('right'); lbl.set_color('#CCCCCC')
            plt.tight_layout(rect=[0, 0, 1, 0.87])
            _add_logo_title(fig, row['away_team'], row['home_team'], title_text, gt)

            hs_s = str(hs).replace('.', '_').replace('-', 'neg')
            path = CHARTS_DIR / f"{gid[:8]}_sp_{hs_s}_{datetime.now().strftime('%H%M%S')}.png"
            plt.savefig(path, dpi=130, facecolor='#0F1419', bbox_inches='tight')
            plt.close()

            game_info = {
                'away_team': row['away_team'], 'home_team': row['home_team'],
                'commence_time': row['commence_time'],
            }
            charts.append((path, game_info))
            log_ok(f"SP: {row['away_team']} @ {row['home_team']} {label}")

        conn.close()
    except Exception as e:
        log_error("Spread chart generation failed", e)
        plt.close('all')
    return charts


# ==========================================================================
# TOTALS CHARTS
# ==========================================================================

def generate_totals_charts() -> List[Tuple[Path, dict]]:
    """Generate totals charts. Returns list of (path, game_info) tuples."""
    log_info("Generating totals charts...")
    charts = []
    try:
        conn = sqlite3.connect(DB_PATH)
        games = pd.read_sql_query("""
            SELECT DISTINCT g.game_id, g.home_team, g.away_team, g.commence_time,
                oh.point as total_line
            FROM games g JOIN odds_history oh ON g.game_id=oh.game_id
            WHERE g.last_update=(SELECT MAX(last_update) FROM games)
            AND oh.market_key='totals' AND oh.bookmaker_key!='_average'
            GROUP BY g.game_id, oh.point
            ORDER BY g.commence_time, oh.point
        """, conn)

        for _, row in games.iterrows():
            gid = row['game_id']
            total = row['total_line']
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9), sharex=True)
            fig.patch.set_facecolor('#0F1419')

            for ax, outcome in zip([ax1, ax2], ['Over', 'Under']):
                _setup_axes(ax)
                bdf = pd.read_sql_query(
                    "SELECT strftime('%Y-%m-%d %H:%M:%S', created_at) AS created_at, bookmaker_key, bookmaker_title, price_american "
                    "FROM odds_history WHERE game_id=? AND outcome_name=? AND point=? "
                    "AND market_key='totals' AND bookmaker_key!='_average' ORDER BY strftime('%Y-%m-%d %H:%M:%S', created_at)",
                    conn, params=(gid, outcome, total))
                adf = pd.read_sql_query(
                    "SELECT strftime('%Y-%m-%d %H:%M:%S', created_at) AS created_at, price_american, bookmaker_title "
                    "FROM odds_history WHERE game_id=? AND outcome_name=? AND point=? "
                    "AND market_key='totals' AND bookmaker_key='_average' ORDER BY strftime('%Y-%m-%d %H:%M:%S', created_at)",
                    conn, params=(gid, outcome, total))
                _plot_book_lines(ax, bdf, adf)
                _add_odds_header(ax, bdf, adf, prefix=f"TOTAL: {total}", label=f'{outcome} TOTAL')
                if ax == ax1 and not bdf.empty:
                    leg = ax.legend(loc='upper left', fontsize=8, ncol=2,
                                    facecolor='#1A1F2E', edgecolor='#4A5568', framealpha=0.9)
                    if leg:
                        for t in leg.get_texts():
                            t.set_color('#CCCCCC')
                            if 'AVERAGE' in t.get_text():
                                t.set_weight('bold'); t.set_color('#FFD700')
                _format_x_axis(ax, bdf)

            gt = _game_title(row)
            title_text = f"{row['away_team']} @ {row['home_team']} TOTAL {total} — {gt}"

            ax2.set_xlabel('Time', fontsize=10, color='#CCCCCC')
            for lbl in ax2.xaxis.get_ticklabels():
                lbl.set_rotation(45); lbl.set_ha('right'); lbl.set_color('#CCCCCC')
            plt.tight_layout(rect=[0, 0, 1, 0.87])
            _add_logo_title(fig, row['away_team'], row['home_team'], title_text, gt)

            ts = str(total).replace('.', '_')
            path = CHARTS_DIR / f"{gid[:8]}_tot_{ts}_{datetime.now().strftime('%H%M%S')}.png"
            plt.savefig(path, dpi=130, facecolor='#0F1419', bbox_inches='tight')
            plt.close()

            game_info = {
                'away_team': row['away_team'], 'home_team': row['home_team'],
                'commence_time': row['commence_time'],
            }
            charts.append((path, game_info))
            log_ok(f"TO: {row['away_team']} @ {row['home_team']} O/U {total}")

        conn.close()
    except Exception as e:
        log_error("Totals chart generation failed", e)
        plt.close('all')
    return charts


# ==========================================================================
# MAIN ITERATION
# ==========================================================================

def snapshot_odds_history() -> bool:
    """Push games + recent odds_history to the shared HF Dataset. Never raises.

    Ported from mlb_unified_odds_monitor.snapshot_odds_history(); the only
    differences are the NFL-labelled HISTORY_SNAPSHOT key and a longer default
    retention, because NFL polls far less often than MLB and so needs a wider
    window to hold the same number of points per game.
    """
    if not _HAS_DURABLE or not durable_state.enabled():
        return False
    conn = None
    try:
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(days=HISTORY_RETAIN_DAYS)).isoformat()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        games = c.execute(
            "SELECT game_id, sport_key, commence_time, home_team, away_team, "
            "last_update FROM games").fetchall()
        hist = c.execute(
            "SELECT game_id, fetch_timestamp, bookmaker_key, bookmaker_title, "
            "market_key, outcome_name, price_decimal, price_american, point, "
            "is_live_game, created_at FROM odds_history WHERE fetch_timestamp >= ?",
            (cutoff,)).fetchall()
        ok = durable_state.save_gz_json(HISTORY_SNAPSHOT, {
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "sport": "nfl",
            "retain_days": HISTORY_RETAIN_DAYS,
            "games": games,
            "odds_history": hist,
        })
        if ok:
            log_info(f"NFL odds history snapshot: {len(games)} games, "
                     f"{len(hist)} history rows -> {HISTORY_SNAPSHOT}")
        return ok
    except Exception as e:
        log_warn(f"NFL odds history snapshot failed: {e}")
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def restore_from_supabase() -> int:
    """Replay nfl_odds_history out of Supabase into the local SQLite. Never raises.

    THIS IS THE PRIMARY RESTORE PATH, ahead of the HF Dataset snapshot, for one
    practical reason: SUPABASE_URL and SUPABASE_KEY are ALREADY secrets on this
    repo and HF_TOKEN is not. The rows are already being written every hour —
    1,022 of them in the 2026-08-25T04:33 run — so the data exists and only the
    read was missing.

    Supabase stores the AMERICAN price and no bookmaker_title, so both are
    reconstructed on the way in. price_decimal is derived because the chart
    plots decimal and only labels American (see _american_formatter).
    """
    if not _sb_client:
        return 0
    conn = None
    try:
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(days=HISTORY_RETAIN_DAYS)).isoformat()
        rows = _sb_client.get_odds_history(cutoff)
        if not rows:
            return 0

        def to_decimal(am):
            try:
                a = float(am)
            except (TypeError, ValueError):
                return None
            if a == 0:
                return None
            return a / 100.0 + 1.0 if a > 0 else 100.0 / abs(a) + 1.0

        payload = []
        for r in rows:
            am = r.get("price")
            payload.append((
                r.get("game_id"),
                r.get("fetched_at_pt") or r.get("created_at"),
                r.get("bookmaker"),
                r.get("bookmaker"),          # no title column upstream
                r.get("market"),
                r.get("outcome"),
                to_decimal(am),
                am,
                r.get("point"),
                0,
                _norm_ct(r.get("fetched_at_pt") or r.get("created_at")),  # v004: uniform format
            ))
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.executemany(
            "INSERT OR IGNORE INTO odds_history (game_id, fetch_timestamp, "
            "bookmaker_key, bookmaker_title, market_key, outcome_name, "
            "price_decimal, price_american, point, is_live_game, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)", payload)
        conn.commit()
        n = c.execute("SELECT COUNT(*) FROM odds_history").fetchone()[0]
        log_info(f"Restored {len(payload)} rows from Supabase "
                 f"(odds_history now holds {n})")
        return n
    except Exception as e:
        log_warn(f"Supabase odds history restore failed: {e}")
        return 0
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def restore_odds_history() -> int:
    """Repopulate games + odds_history from the snapshot. Never raises.

    INSERT OR IGNORE on both tables. MLB carries a _RESTORED module guard
    because its process loops for hours and replaying every cycle doubled the
    table (10.5x over-count, fixed 2026-08-19). That guard is unnecessary here
    while the entry point is `--once` — one process, one restore — but the
    OR IGNORE is kept for the loop-mode path in main(), and because the guard
    costs nothing if this ever becomes long-lived.
    """
    if not _HAS_DURABLE or not durable_state.enabled():
        return 0
    conn = None
    try:
        snap = durable_state.load_gz_json(HISTORY_SNAPSHOT, None)
        if not snap:
            log_info("No NFL odds history snapshot yet — first run seeds it")
            return 0
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.executemany(
            "INSERT OR IGNORE INTO games (game_id, sport_key, commence_time, "
            "home_team, away_team, last_update) VALUES (?,?,?,?,?,?)",
            snap.get("games", []))
        _rows = []
        for _r in snap.get("odds_history", []):
            _r = tuple(_r)
            # v004: created_at := the row's OWN fetch time (normalized). Heals any
            # historical collapse and guarantees ONE uniform format.
            _rows.append(_r[:10] + (_norm_ct(_r[1]),))
        c.executemany(
            "INSERT OR IGNORE INTO odds_history (game_id, fetch_timestamp, "
            "bookmaker_key, bookmaker_title, market_key, outcome_name, "
            "price_decimal, price_american, point, is_live_game, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            _rows)
        conn.commit()
        n = c.execute("SELECT COUNT(*) FROM odds_history").fetchone()[0]
        log_info(f"Restored NFL odds history: "
                 f"{len(snap.get('odds_history', []))} rows (db now holds {n})")
        return n
    except Exception as e:
        log_warn(f"NFL odds history restore failed: {e}")
        return 0
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def run_iteration() -> int:
    """Run one complete restore→fetch→store→chart→post→snapshot cycle."""
    try:
        if not setup_database():
            return 1

        # The runner is empty. Replay what previous runs collected BEFORE any
        # chart is drawn, or every chart shows exactly one point.
        # Supabase first — its credentials already exist on this repo. The HF
        # Dataset snapshot is additive and only fires if HF_TOKEN is present;
        # both use INSERT OR IGNORE so running both cannot double-count.
        restored = restore_from_supabase()
        try:
            if durable_state.load_gzipped_db("odds_nfl.db.gz", str(DB_PATH)):
                restored += 1
        except Exception:
            pass
        restored += restore_odds_history()
        if not restored:
            log_warn("No prior odds history restored — charts this run will show "
                     "only the points collected in this cycle. Check SUPABASE_KEY "
                     "and HF_TOKEN are set on the repo.")

        api_mgr = APIKeyManager()
        t0 = time.time()

        # Fetch
        data, credits = fetch_odds(api_mgr)
        if data is None:
            log_error("No odds data", critical=True)
            return 1

        # Store
        records = store_odds(data)

        # Movement charts (works with 1+ data points — single points render as dots)
        ml_charts = sp_charts = to_charts = []
        _gate = None
        _posted = 0
        _rendered = 0
        if GENERATE_CHARTS and records > 0:
            ml_charts = generate_moneyline_charts()
            sp_charts = generate_spread_charts()
            to_charts = generate_totals_charts()
            _rendered = len(ml_charts) + len(sp_charts) + len(to_charts)

            # ---- CHART QUALITY GATE (2026-10-14 0111 MDT) ------------------
            # Suppress posts when the charts cannot show movement (single
            # snapshot / collapsed x-axis). Snapshot + run still complete.
            _gate = _chart_gate_evaluate(DB_PATH, charts_rendered=
                                         len(ml_charts) + len(sp_charts) + len(to_charts))
            _append_gate_log(_gate)
            if _gate and _gate.get("verdict") == "FAIL":
                _n_suppressed = len(ml_charts) + len(sp_charts) + len(to_charts)
                log_warn(f"CHART QUALITY GATE FAIL: {'; '.join(_gate.get('reasons', []))} "
                         f"(series={_gate.get('series')} max_x={_gate.get('max_cx')} "
                         f"max_ts={_gate.get('max_ftx')}) - suppressing {_n_suppressed} chart posts")
                if SEND_DISCORD:
                    send_discord_alert(
                        f"CHART QUALITY GATE FAILED - {_n_suppressed} chart posts suppressed\n"
                        f"{'; '.join(_gate.get('reasons', []))}\n"
                        f"series={_gate.get('series')} max_x={_gate.get('max_cx')} "
                        f"max_ts={_gate.get('max_ftx')}")
                ml_charts = sp_charts = to_charts = []

            for ch_path, gi in ml_charts:
                if post_chart_to_discord(ch_path, "NFL Moneyline Movement", DISCORD_WEBHOOK_MONEYLINE, game_info=gi):
                    _posted += 1
                time.sleep(0.8)
            for ch_path, gi in sp_charts:
                if post_chart_to_discord(ch_path, "NFL Spread Movement", DISCORD_WEBHOOK_SPREADS, game_info=gi):
                    _posted += 1
                time.sleep(0.8)
            for ch_path, gi in to_charts:
                if post_chart_to_discord(ch_path, "NFL Totals Movement", DISCORD_WEBHOOK_TOTALS, game_info=gi):
                    _posted += 1
                time.sleep(0.8)

        # Push API key state to Supabase (persistent across restarts + dashboard)
        api_mgr.push_to_supabase()

        elapsed = time.time() - t0
        total_charts = len(ml_charts) + len(sp_charts) + len(to_charts)

        # ---- OUTPUT GUARD (v004, 2026-10-16): silence is how these failures
        # survived weeks. Shout when render or delivery fails. ----
        if records > 0 and GENERATE_CHARTS:
            if _rendered == 0:
                log_error(f"OUTPUT GUARD: 0 charts rendered from {records} records "
                          f"(chart generation failed silently?)", critical=True)
            elif _posted < total_charts:
                log_warn(f"OUTPUT GUARD: {_posted}/{total_charts} charts posted "
                         f"({total_charts - _posted} lost after retries)")
                if SEND_DISCORD:
                    send_discord_alert(
                        f"⚠ CHART DELIVERY SHORTFALL: {_posted}/{total_charts} charts "
                        f"posted this cycle - {total_charts - _posted} lost")
        log_ok(f"Iteration complete: {len(data)} games, {records} records, "
               f"{total_charts} charts in {elapsed:.1f}s")

        if SEND_DISCORD and records > 0:
            # Build matchup listing with start times
            matchup_lines = []
            for g in sorted(data, key=lambda x: x.get('commence_time', '')):
                away = g.get('away_team', '?')
                home = g.get('home_team', '?')
                aa = abbreviate_team(away)
                ha = abbreviate_team(home)
                ct = g.get('commence_time', '')
                try:
                    t = datetime.fromisoformat(ct.replace('Z', '+00:00'))
                    et = t.astimezone(pytz.timezone('US/Eastern'))
                    ts_str = et.strftime('%I:%M %p').lstrip('0')
                except Exception:
                    ts_str = 'TBD'
                live = " 🔴" if is_game_live(ct) else ""
                matchup_lines.append(f"🏈 `{ts_str}` **{aa} @ {ha}**{live}")

            matchup_text = "\n".join(matchup_lines) if matchup_lines else "No games"

            send_discord_alert(
                f"✅ **Odds Update Complete**\n"
                f"**{len(data)} Games** | {records} records | {elapsed:.1f}s\n"
                f"Charts: ML={len(ml_charts)} SP={len(sp_charts)} TO={len(to_charts)}\n"
                f"Credits remaining: {credits}\n"
                f"Source: {SIGNATURE}\n\n"
                f"{matchup_text}")

        # PUBLISHED MANIFEST (2026-10-14 1345 MDT): per-run JSON of what was
        # posted + gate verdict; uploaded with the charts as a workflow artifact.
        _write_posted_manifest(ml_charts, sp_charts, to_charts,
                               len(data), records, elapsed, _gate)

        # LAST ACT OF THE RUN. The runner is about to be destroyed; everything
        # this cycle collected exists only in a file that is about to vanish.
        snapshot_odds_history()
        try:
            durable_state.save_gzipped_db(str(DB_PATH), "odds_nfl.db.gz")
        except Exception:
            pass
        return 0

    except KeyboardInterrupt:
        raise
    except Exception as e:
        log_error(f"Iteration failed: {e}", e, critical=True)
        traceback.print_exc()
        return 1


# ==========================================================================
# CONTINUOUS LOOP
# ==========================================================================

def signal_handler(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    log_warn(f"Shutdown signal received ({signum})")


def main():
    global shutdown_requested
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    sport_str = " → ".join(SPORT_KEYS)
    print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}  {SCRIPT_NAME} v{SCRIPT_VERSION}{Colors.RESET}")
    print(f"{Colors.CYAN}  Sport keys: {sport_str} | Interval: {RUN_INTERVAL//60}min{Colors.RESET}")
    print(f"{Colors.CYAN}  Markets: {MARKETS} | Keys: {len(API_KEYS)}{Colors.RESET}")
    print(f"{Colors.CYAN}  Source: {SIGNATURE}{Colors.RESET}")
    if SOURCE_ID == "unknown":
        print("  [WARN] SOURCE_ID env not set — posts will be signed 'unknown'")
    print(f"{Colors.CYAN}{'='*60}{Colors.RESET}\n")

    if SEND_DISCORD:
        send_discord_alert(
            f"🚀 **Odds Monitor v{SCRIPT_VERSION} Started**\n"
            f"Sport keys: {sport_str}\n"
            f"Markets: {MARKETS}\n"
            f"Interval: {RUN_INTERVAL//60} min\n"
            f"Keys: {len(API_KEYS)}")

    run_count = 0
    consec_errors = 0

    while not shutdown_requested:
        run_count += 1
        print(f"\n{Colors.BLUE}{'─'*50}{Colors.RESET}")
        print(f"{Colors.BOLD}Run #{run_count} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Colors.RESET}")
        print(f"{Colors.BLUE}{'─'*50}{Colors.RESET}\n")

        try:
            code = run_iteration()
            if code == 0:
                consec_errors = 0
            else:
                consec_errors += 1
        except KeyboardInterrupt:
            shutdown_requested = True
            break
        except Exception as e:
            consec_errors += 1
            log_error(f"Run #{run_count} error", e)
            if consec_errors >= 5:
                log_error("Too many consecutive errors, stopping", critical=True)
                break

        if not shutdown_requested:
            nxt = datetime.now() + timedelta(seconds=RUN_INTERVAL)
            print(f"\n{Colors.DIM}Next run: {nxt.strftime('%H:%M:%S')} "
                  f"(sleeping {RUN_INTERVAL//60} min){Colors.RESET}")

            slept = 0
            while slept < RUN_INTERVAL and not shutdown_requested:
                chunk = min(30, RUN_INTERVAL - slept)
                time.sleep(chunk)
                slept += chunk

    # Shutdown
    print(f"\n{Colors.YELLOW}Shutting down...{Colors.RESET}")
    if SEND_DISCORD:
        send_discord_alert(f"🛑 **Odds Monitor Stopped**\nRan {run_count} iterations")
    print(f"{Colors.GREEN}Shutdown complete{Colors.RESET}")


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "--once":
            print(f"{Colors.YELLOW}Single iteration mode (--once){Colors.RESET}\n")
            sys.exit(run_iteration())
        else:
            main()
    except Exception as e:
        # Module-level crash protection — catches errors during startup/init
        import traceback as _tb
        import datetime as _dt
        ts = _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{ts}] [FATAL] Odds Monitor crashed during startup: {e}", flush=True)
        _tb.print_exc()
        try:
            if SEND_DISCORD:
                send_discord_alert(
                    f"🚨 **Odds Monitor FATAL CRASH**\n"
                    f"Process crashed during startup/initialization:\n```{str(e)[:300]}```"
                )
        except Exception:
            pass
        sys.exit(1)