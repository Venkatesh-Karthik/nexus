"""
simulate.py — Real-world event simulation using the actual dataset.

Phase 1 — Online Demand (Online_Orders):
  For each Fulfilled/Pending order → POST /reserve, then POST /confirm
  For each Cancelled order         → POST /reserve, then POST /release
  If stock is insufficient         → logs "OUT OF STOCK (skipped)"

Phase 2 — In-Store Sales (Store_Inventory_Records):
  Simulates X% of recorded stock being sold in-store via POST /simulate_store_sale

Phase 3 — Expiry Demo:
  Creates a reservation, waits, then shows it being detected as expired.

All actions go through the live FastAPI server (must be running on port 8000).
"""

import sqlite3
import time
import random
import httpx

BASE_URL = "http://127.0.0.1:8000"
DB_FILE  = "inventory.db"

# How many orders / store records to sample (keep simulation fast)
MAX_ONLINE_ORDERS  = 30
MAX_STORE_SALES    = 20
STORE_SALE_PCT     = 0.05   # simulate 5% of recorded store stock sold in-store

# ─────────────────────────────────────────────────────────────────────────────
# Pretty log helpers
# ─────────────────────────────────────────────────────────────────────────────
SEP  = "─" * 62
TICK = "✅"
WARN = "⚠️ "
FAIL = "❌"
INFO = "ℹ️ "

def log(symbol, msg):
    print(f"  {symbol}  {msg}")

def section(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: call API
# ─────────────────────────────────────────────────────────────────────────────
def post(endpoint, payload):
    try:
        r = httpx.post(f"{BASE_URL}{endpoint}", json=payload, timeout=10)
        return r.json()
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1: Simulate Online Orders
# ─────────────────────────────────────────────────────────────────────────────
def simulate_online_orders():
    section("PHASE 1 — Online Orders → Reservations")

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    orders = conn.execute("""
        SELECT order_id, product_id, quantity, order_status
        FROM Online_Orders
        ORDER BY order_time
        LIMIT ?
    """, (MAX_ONLINE_ORDERS,)).fetchall()
    conn.close()

    reserved_count   = 0
    confirmed_count  = 0
    released_count   = 0
    out_of_stock     = 0

    for order in orders:
        pid    = order["product_id"]
        qty    = order["quantity"]
        status = order["order_status"]

        print(f"\n  Order {order['order_id']} | {pid} | qty={qty} | status={status}")

        # --- Step 1: Try to reserve ---
        res = post("/reserve", {"product_id": pid, "quantity": qty})

        if not res.get("success"):
            log(FAIL, f"RESERVE FAILED — {res.get('message', res)}")
            out_of_stock += 1
            continue

        rsv_id = res["reservation_id"]
        log(TICK, f"RESERVED  reservation_id={rsv_id}  "
                  f"available_before={res['available_stock_before']}  "
                  f"available_after={res['available_stock_after']}  "
                  f"expires_at={res['expires_at']}")
        reserved_count += 1

        # --- Step 2: Confirm (Fulfilled) or Release (Cancelled) ---
        if status == "Fulfilled":
            conf = post("/confirm", {"reservation_id": rsv_id})
            if conf.get("success"):
                log(TICK, f"CONFIRMED  reservation_id={rsv_id}  "
                          f"qty_sold={conf['quantity']}  "
                          f"total_stock permanently reduced.")
                confirmed_count += 1
            else:
                log(WARN, f"CONFIRM FAILED — {conf.get('detail', conf)}")

        elif status == "Cancelled":
            rel = post("/release", {"reservation_id": rsv_id})
            if rel.get("success"):
                log(INFO, f"RELEASED   reservation_id={rsv_id}  "
                          f"qty_restored={rel['quantity_released']}  "
                          f"stock returned to available.")
                released_count += 1
            else:
                log(WARN, f"RELEASE FAILED — {rel.get('detail', rel)}")

        else:
            log(INFO, f"Pending order — reservation held (expires in 10 min).")

    print(f"\n  Summary → Reserved: {reserved_count} | Confirmed: {confirmed_count} | "
          f"Released: {released_count} | Out-of-stock: {out_of_stock}")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2: Simulate In-Store Sales
# ─────────────────────────────────────────────────────────────────────────────
def simulate_store_sales():
    section("PHASE 2 — In-Store Sales (simulate_store_sale)")

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row

    # Pick a sample of store records and sell a fraction of their stock
    records = conn.execute("""
        SELECT store_id, product_id, recorded_stock_level
        FROM Store_Inventory_Records
        WHERE recorded_stock_level > 0
        ORDER BY RANDOM()
        LIMIT ?
    """, (MAX_STORE_SALES,)).fetchall()
    conn.close()

    sold_count   = 0
    failed_count = 0

    for rec in records:
        pid      = rec["product_id"]
        store_id = rec["store_id"]
        qty_sell = max(1, int(rec["recorded_stock_level"] * STORE_SALE_PCT))

        print(f"\n  Store {store_id} | {pid} | "
              f"recorded_stock={rec['recorded_stock_level']} | selling={qty_sell}")

        res = post("/simulate_store_sale", {"product_id": pid, "quantity": qty_sell})

        if res.get("success"):
            log(TICK, f"SOLD  qty={qty_sell}  "
                      f"stock_before={res['stock_before']}  "
                      f"stock_after={res['stock_after']}")
            sold_count += 1
        else:
            log(FAIL, f"SALE FAILED — {res.get('detail', res)}")
            failed_count += 1

    print(f"\n  Summary → Sold: {sold_count} | Failed: {failed_count}")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3: Expiry Demo (short reservation that we detect as expired)
# ─────────────────────────────────────────────────────────────────────────────
def simulate_expiry_demo():
    section("PHASE 3 — Expiry Demo")

    # Pick any product with sufficient stock
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT product_id FROM inventory WHERE total_stock - reserved_stock >= 1 LIMIT 1"
    ).fetchone()
    conn.close()

    if not row:
        log(WARN, "No available product found for expiry demo.")
        return

    pid = row["product_id"]
    log(INFO, f"Creating reservation for {pid} qty=1 ...")
    res = post("/reserve", {"product_id": pid, "quantity": 1})

    if not res.get("success"):
        log(FAIL, f"Could not reserve for demo: {res}")
        return

    rsv_id = res["reservation_id"]
    exp_at = res["expires_at"]
    log(TICK, f"Reservation {rsv_id} created. expires_at={exp_at}")

    # Manually expire it in the DB to demo the detection logic
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        "UPDATE reservations SET expires_at = datetime('now', '-1 second') WHERE id = ?",
        (rsv_id,),
    )
    conn.commit()
    conn.close()
    log(INFO, "Forced reservation to be expired in DB (set expires_at to past).")

    log(INFO, "Waiting 35s for expiry_loop to sweep ... (loop runs every 30s)")
    for i in range(35, 0, -5):
        print(f"    {i}s remaining ...", end="\r")
        time.sleep(5)
    print()

    # Check the result
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    r = conn.execute(
        "SELECT status FROM reservations WHERE id = ?", (rsv_id,)
    ).fetchone()
    conn.close()

    final_status = r["status"] if r else "NOT FOUND"
    if final_status == "expired":
        log(TICK, f"EXPIRY CONFIRMED — reservation {rsv_id} status='{final_status}'. "
                  f"reserved_stock has been restored.")
    else:
        log(WARN, f"Reservation {rsv_id} status='{final_status}' "
                  f"(expiry loop may not have run yet — check server logs).")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 4: Waitlist Demo
# ─────────────────────────────────────────────────────────────────────────────
def simulate_waitlist_demo():
    section("PHASE 4 — Waitlist Demo")

    # 1. Find a product with 0 available stock (or make it 0)
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT i.product_id, i.total_stock - i.reserved_stock AS avail
        FROM inventory i
        ORDER BY avail ASC
        LIMIT 1
    """).fetchone()
    conn.close()

    if not row:
        log(WARN, "No products found.")
        return

    pid = row["product_id"]
    avail = row["avail"]

    # If it has stock, reserve it all to make it 0
    if avail > 0:
        log(INFO, f"Product {pid} has {avail} units. Reserving all to make it 0...")
        temp_res = post("/reserve", {"product_id": pid, "quantity": avail})
        temp_id = temp_res.get("reservation_id")
    else:
        log(INFO, f"Product {pid} is already out of stock (avail=0).")
        temp_id = None

    # 2. Try to join waitlist
    email = "customer@example.com"
    log(INFO, f"Joining waitlist for {pid} with {email}...")
    w_res = post("/join_waitlist", {"product_id": pid, "email": email})
    
    if w_res.get("success"):
        log(TICK, f"Joined waitlist! Message: {w_res['message']}")
    else:
        log(FAIL, f"Failed to join waitlist: {w_res.get('detail', w_res)}")
        return

    # 3. Increase stock (release the reservation we just made)
    if temp_id:
        log(INFO, f"Releasing 1 unit of {pid} to trigger notification...")
        # Note: /release expects reservation_id
        post("/release", {"reservation_id": temp_id})
        log(TICK, "Reservation released! Check the SERVER CONSOLE for the [NOTIFICATION] log.")
    else:
        log(WARN, "Waitlist demo requires a product that goes from 0 to >0. "
                  "Try running the simulation again after some orders happen.")


# ─────────────────────────────────────────────────────────────────────────────
# Final inventory snapshot
# ─────────────────────────────────────────────────────────────────────────────
def print_inventory_snapshot():
    section("FINAL INVENTORY SNAPSHOT (top 10 by available_stock)")
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT product_id, total_stock, reserved_stock,
               total_stock - reserved_stock AS available_stock
        FROM inventory
        ORDER BY available_stock DESC
        LIMIT 10
    """).fetchall()
    conn.close()

    print(f"  {'product_id':<12} {'total':>8} {'reserved':>10} {'available':>11}")
    print(f"  {'─'*12} {'─'*8} {'─'*10} {'─'*11}")
    for r in rows:
        print(f"  {r['product_id']:<12} {r['total_stock']:>8} "
              f"{r['reserved_stock']:>10} {r['available_stock']:>11}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "═" * 62)
    print("  INVENTORY SIMULATION — Using Real Dataset")
    print("  Make sure the FastAPI server is running:")
    print("  uvicorn main:app --reload")
    print("═" * 62)

    # Check server is reachable
    try:
        r = httpx.get(f"{BASE_URL}/stock", timeout=5)
        log(TICK, f"Server reachable — {len(r.json())} products in inventory.")
    except Exception:
        log(FAIL, "Cannot reach server at localhost:8000. Start it first.")
        exit(1)

    simulate_online_orders()
    simulate_store_sales()
    simulate_expiry_demo()
    simulate_waitlist_demo()
    print_inventory_snapshot()

    print("\n" + "═" * 62)
    print("  Simulation complete.")
    print("═" * 62 + "\n")
