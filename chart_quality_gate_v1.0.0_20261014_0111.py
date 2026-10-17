"""chart_quality_gate_v1.0.0_20261014_0111 - pre-post chart quality gate (dev).

Blocks "movement" charts that cannot show movement. Proven in
E:\\abet\\.cache\\tmp\\opencode\\NFL_ODDS_FIX_PROOF_20261013_1455:
  variant A (live lane)             -> FAIL R1 (single snapshot, no history)
  variant B (created_at dropped)    -> FAIL R2 (history collapsed to one x-position)
  variant C (created_at preserved)  -> PASS

Rules (non-average book series: game, market, outcome, line, bookmaker):
  R1 no-history : >50% of series have <2 distinct fetch_timestamp
  R2 x-collapse : among series with >=3 distinct fetch_timestamp, >10% have
                  fewer distinct created_at than fetch_timestamp
  R3 no-charts  : charts_rendered must be > 0 when records > 0 (a gate that
                  cannot tell "gate passed" from "zero charts posted" is worse
                  than no gate). Added v004 (2026-10-16) per Part 7 of the
                  odds-monitors prompt: the existing DB-only gate could not
                  detect a 0-chart render and manufactured false confidence.

API : evaluate(db_path, charts_rendered=None) -> dict(verdict, reasons, ...)
CLI : python chart_quality_gate_v1.0.0_20261014_0111.py <db> [--json out]
      [--charts-rendered N]
      exit 0 = PASS, 1 = FAIL, 2 = error
"""
import json
import sqlite3
import sys
from pathlib import Path

R1_THRESHOLD = 0.50   # >50% series with <2 fetch timestamps = no history
R2_THRESHOLD = 0.10   # >10% deep series collapsed = x-axis broken
DEEP_MIN_FTX = 3      # "deep" series have >=3 distinct fetch timestamps

_SERIES_SQL = """
    SELECT game_id, market_key, outcome_name, COALESCE(point, -999999), bookmaker_key,
           COUNT(*), COUNT(DISTINCT created_at), COUNT(DISTINCT fetch_timestamp)
    FROM odds_history WHERE bookmaker_key != '_average'
    GROUP BY 1,2,3,4,5
"""


def evaluate(db_path, charts_rendered=None) -> dict:
    """Never raises: returns verdict='ERROR' with a reason on any failure.

    charts_rendered: number of charts the monitor actually rendered this cycle.
    When provided (and records are present), R3 requires it > 0.
    """
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            rows = conn.execute(_SERIES_SQL).fetchall()
            records = conn.execute("SELECT COUNT(*) FROM odds_history").fetchone()[0]
        finally:
            conn.close()
    except Exception as e:
        return {"verdict": "ERROR", "reasons": [f"gate db read failed: {e}"],
                "series": 0, "max_cx": 0, "max_ftx": 0}

    n = len(rows)
    if n == 0:
        return {"verdict": "ERROR", "reasons": ["no series rows in db"],
                "series": 0, "max_cx": 0, "max_ftx": 0}

    r1_bad = sum(1 for r in rows if r[7] < 2)
    deep = [r for r in rows if r[7] >= DEEP_MIN_FTX]
    r2_bad = sum(1 for r in deep if r[6] < r[7])
    r1_frac = r1_bad / n
    r2_frac = (r2_bad / len(deep)) if deep else 0.0

    verdict, reasons = "PASS", []
    if r1_frac > R1_THRESHOLD:
        verdict = "FAIL"
        reasons.append(f"R1 no-history: {r1_bad}/{n} series ({r1_frac:.0%}) have only one snapshot")
    if len(deep) and r2_frac > R2_THRESHOLD:
        verdict = "FAIL"
        reasons.append(f"R2 x-collapse: {r2_bad}/{len(deep)} deep series ({r2_frac:.0%}) lost "
                       f"created_at distinctness (chart draws one x position)")
    if charts_rendered is not None and records > 0 and int(charts_rendered) <= 0:
        verdict = "FAIL"
        reasons.append(f"R3 no-charts: 0 charts rendered from {records} records "
                       f"(chart generation failed silently?)")

    return {
        "verdict": verdict, "reasons": reasons, "series": n,
        "r1_single_snapshot": r1_bad, "r1_frac": round(r1_frac, 3),
        "deep_series": len(deep), "r2_collapsed": r2_bad, "r2_frac": round(r2_frac, 3),
        "max_cx": max((r[6] for r in rows), default=0),
        "max_ftx": max((r[7] for r in rows), default=0),
        "records": records, "charts_rendered": charts_rendered,
    }


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return 2
    db = Path(args[0])
    out_json = None
    charts_rendered = None
    if "--json" in args:
        out_json = args[args.index("--json") + 1]
    if "--charts-rendered" in args:
        charts_rendered = args[args.index("--charts-rendered") + 1]
    if not db.exists():
        print(f"GATE ERROR: db not found: {db}")
        return 2
    res = evaluate(db, charts_rendered=charts_rendered)
    print(f"[chart-quality-gate v1.0.0] {db}")
    print(f"  series={res['series']}  max distinct created_at={res['max_cx']}  "
          f"max distinct fetch_ts={res['max_ftx']}  records={res['records']}  "
          f"charts_rendered={res['charts_rendered']}")
    print(f"  R1 single-snapshot: {res.get('r1_single_snapshot')}/{res['series']} "
          f"({res.get('r1_frac', 0):.0%})   "
          f"R2 x-collapsed deep: {res.get('r2_collapsed')}/{res.get('deep_series', 0)} "
          f"({res.get('r2_frac', 0):.0%})   R3 charts_rendered>0: "
          f"{'FAIL' if res['charts_rendered'] is not None and res['records'] > 0 and int(res['charts_rendered'] or 0) <= 0 else 'OK'}")
    print(f"  VERDICT: {res['verdict']}" + ("" if res["verdict"] == "PASS"
                                            else " - " + "; ".join(res["reasons"])))
    if out_json:
        Path(out_json).write_text(json.dumps(res, indent=2), encoding="utf-8")
    return 0 if res["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
