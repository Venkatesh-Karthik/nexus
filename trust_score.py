"""
trust_score.py — Inventory reliability scoring using Inventory_Audits data.

Formula:
    mismatch_pct = avg( abs(recorded - actual) / max(actual, 1) ) * 100
    trust_score  = max(0, round(100 - mismatch_pct * FACTOR, 2))

FACTOR (default 1.5) amplifies the penalty for large discrepancies.
  - A 10% average mismatch → score = 100 - 15 = 85  (High)
  - A 40% average mismatch → score = 100 - 60 = 40  (Low)
  - A 70%+ average mismatch → score = 0             (Critical)

Labels:
  >= 80  → "High"      (reliable)
  >= 50  → "Medium"    (moderate drift)
  >= 20  → "Low"       (frequent mismatch)
  <  20  → "Critical"  (data untrustworthy)
"""

import sqlite3

DB_FILE = "inventory.db"
MISMATCH_FACTOR = 1.5   # penalty amplifier (tunable)


def compute_trust_score(product_id: str, db_path: str = DB_FILE) -> dict:
    """
    Compute trust score for a single product using Inventory_Audits.
    Returns a dict with score, label, breakdown, and mismatch details.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT recorded_stock, actual_stock
        FROM Inventory_Audits
        WHERE product_id = ?
        """,
        (product_id,),
    ).fetchall()
    conn.close()

    if not rows:
        return {
            "product_id": product_id,
            "trust_score": None,
            "label": "No Data",
            "audits_total": 0,
            "audits_with_mismatch": 0,
            "avg_mismatch_pct": None,
            "message": "No audit records found for this product.",
        }

    total_audits     = len(rows)
    mismatch_count   = 0
    divergence_sum   = 0.0

    for r in rows:
        recorded = r["recorded_stock"]
        actual   = r["actual_stock"]

        if recorded != actual:
            mismatch_count += 1

        # Proportional divergence: how far off recorded is relative to actual
        divergence = abs(recorded - actual) / max(actual, 1)
        divergence_sum += divergence

    avg_mismatch_pct = round((divergence_sum / total_audits) * 100, 2)
    raw_score        = 100 - (avg_mismatch_pct * MISMATCH_FACTOR)
    trust_score      = max(0.0, round(raw_score, 2))

    if trust_score >= 80:
        label = "High"
    elif trust_score >= 50:
        label = "Medium"
    elif trust_score >= 20:
        label = "Low"
    else:
        label = "Critical"

    return {
        "product_id"          : product_id,
        "trust_score"         : trust_score,
        "label"               : label,
        "audits_total"        : total_audits,
        "audits_with_mismatch": mismatch_count,
        "avg_mismatch_pct"    : avg_mismatch_pct,
        "mismatch_factor"     : MISMATCH_FACTOR,
        "formula"             : f"100 - ({avg_mismatch_pct} * {MISMATCH_FACTOR}) = {trust_score}",
    }


def compute_all_trust_scores(db_path: str = DB_FILE) -> list[dict]:
    """
    Compute trust scores for every product that has audit data.
    Sorted by trust_score ascending (least reliable first).
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    product_ids = conn.execute(
        "SELECT DISTINCT product_id FROM Inventory_Audits ORDER BY product_id"
    ).fetchall()
    conn.close()

    results = [compute_trust_score(r["product_id"], db_path) for r in product_ids]
    results.sort(key=lambda x: (x["trust_score"] is None, x["trust_score"]))
    return results


# ─── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sqlite3 as _s, os

    if not os.path.exists(DB_FILE):
        print(f"ERROR: {DB_FILE} not found. Run load_data.py first.")
    else:
        scores = compute_all_trust_scores()
        print(f"\nTrust Scores for {len(scores)} products:\n")
        print(f"  {'product_id':<12} {'score':>7} {'label':<10} {'mismatches':>10}  formula")
        print(f"  {'─'*12} {'─'*7} {'─'*10} {'─'*10}  {'─'*30}")
        for s in scores[:15]:
            print(
                f"  {s['product_id']:<12} {str(s['trust_score']):>7} "
                f"{s['label']:<10} {s['audits_with_mismatch']:>5}/{s['audits_total']:<5}  "
                f"{s['formula']}"
            )
