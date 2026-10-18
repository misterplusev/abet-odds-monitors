"""
quota_gate.py - Sustain governor for hourly NFL odds polling.

Runs BEFORE the monitor on every CI invocation. Zero-credit checks only:
  1. GET /v4/sports (free, returns quota headers) for every ODDS_API_KEY_N
  2. Reads live pool remaining + detects dead keys + detects sport activity
  3. Reserves projected MLB burn until quota reset, applies safety margin
  4. EXIT 0 = run this hour | EXIT 7 = skip this hour (budget or no active sport)

Perpetual guarantee: hourly cadence holds whenever the pool projects to last
until reset; the gate degrades NFL polling automatically instead of letting
the pool go negative. MLB (separate runtime) is accounted as a reserve.

Env:
  ODDS_API_KEY_1..9   required (values never printed)
  QUOTA_RESET_DAY     day-of-month the pool resets (default 7)
  SAFETY_MARGIN       fraction of pool held as reserve (default 0.10)
  MLB_CREDITS_PER_DAY projected MLB burn (default 72 = hourly x 3)
  POLL_COST           credits per NFL fetch (default 3)
  SUPABASE_URL/KEY    optional; writes skip/run heartbeat to nfl_monitor_state
"""

import json
import os
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

RESET_DAY = int(os.environ.get("QUOTA_RESET_DAY", "7"))
SAFETY = float(os.environ.get("SAFETY_MARGIN", "0.10"))
MLB_PER_DAY = float(os.environ.get("MLB_CREDITS_PER_DAY", "72"))
POLL_COST = int(os.environ.get("POLL_COST", "3"))

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from supabase_client import SupabaseClient
    _sb = SupabaseClient()
    if not _sb.connected:
        _sb = None
except Exception:
    _sb = None


def heartbeat(state):
    if _sb:
        try:
            _sb.update_status("quota_gate", **state)
        except Exception:
            pass


def days_until_reset(now):
    if now.day < RESET_DAY:
        candidate = now.replace(day=RESET_DAY, hour=0, minute=0, second=0, microsecond=0)
    else:
        month = now.month + 1
        year = now.year + (1 if month > 12 else 0)
        month = 1 if month > 12 else month
        candidate = datetime(year, month, RESET_DAY, tzinfo=timezone.utc)
    return max((candidate - now).total_seconds() / 86400.0, 1 / 24.0)


API_BASE = os.environ.get("QUOTA_API_BASE", "https://api.the-odds-api.com/v4").rstrip("/")

def scan_keys():
    remaining, dead, used = 0, [], 0
    for i in range(1, 11):
        key = os.environ.get(f"ODDS_API_KEY_{i}", "")
        if not key:
            continue
        try:
            req = urllib.request.Request(
                f"{API_BASE}/sports/?apiKey={key}")
            with urllib.request.urlopen(req, timeout=20) as r:
                remaining += int(float(r.headers.get("x-requests-remaining", 0)))
                used += int(float(r.headers.get("x-requests-used", 0)))
        except Exception:
            dead.append(f"API_{i}")
    return remaining, used, dead


def scan_sports():
    """Free endpoint: is any NFL sport key currently offering odds?"""
    key = next((os.environ.get(f"ODDS_API_KEY_{i}") for i in range(1, 11)
                if os.environ.get(f"ODDS_API_KEY_{i}")), None)
    if not key:
        return []
    try:
        req = urllib.request.Request(
            f"{API_BASE}/sports/?apiKey={key}")
        with urllib.request.urlopen(req, timeout=20) as r:
            sports = json.loads(r.read().decode())
        return [s["key"] for s in sports
                if s.get("group") == "American Football"
                and "NFL" in s.get("title", "") and s.get("active")]
    except Exception:
        return []


def main():
    now = datetime.now(timezone.utc)
    remaining, used, dead = scan_keys()
    active_nfl = scan_sports()

    days_left = days_until_reset(now)
    mlb_reserve = (days_left * MLB_PER_DAY) * (1 + SAFETY)
    nfl_budget = remaining - mlb_reserve
    polls_afford = int(nfl_budget // POLL_COST) if nfl_budget > 0 else 0

    state = {
        "pool_remaining": remaining,
        "pool_used": used,
        "dead_keys": dead,
        "days_to_reset": round(days_left, 2),
        "mlb_reserve": round(mlb_reserve, 1),
        "nfl_budget": round(nfl_budget, 1),
        "polls_affordable": polls_afford,
        "active_nfl_sports": active_nfl,
        "decision": "",
    }

    if not active_nfl:
        state["decision"] = "SKIP_NO_ACTIVE_NFL_SPORT"
        print(f"[GATE] SKIP: no active NFL sport keys (offseason window). {json.dumps(state)}")
        heartbeat(state)
        sys.exit(7)

    if polls_afford < 1:
        state["decision"] = "SKIP_BUDGET"
        print(f"[GATE] SKIP: pool budget exhausted after MLB reserve. {json.dumps(state)}")
        heartbeat(state)
        sys.exit(7)

    state["decision"] = "RUN"
    print(f"[GATE] RUN: pool={remaining} mlb_reserve={mlb_reserve:.0f} "
          f"nfl_budget={nfl_budget:.0f} affordable_polls={polls_afford} "
          f"dead={dead or 'none'} reset_in={days_left:.1f}d")
    heartbeat(state)
    sys.exit(0)


if __name__ == "__main__":
    main()

