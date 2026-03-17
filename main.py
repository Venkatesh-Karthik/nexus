"""
main.py — FastAPI application for Real-Time Inventory Management System.

Key design decisions:
- SQLite row-level locking via BEGIN IMMEDIATE prevents concurrent overselling.
- Reservations expire after 10 minutes; background task cleans them up.
- available_stock is always computed live: total_stock - active_reservations.
- Trust score = audit accuracy rate from Inventory_Audits sheet.
"""

import asyncio
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path
from pydantic import BaseModel
import trust_score as ts_logic
import demand_prediction


DB_FILE = "inventory.db"
RESERVATION_TTL_MINUTES = 10

def verify_admin(is_admin: bool):
    if not is_admin:
        raise HTTPException(status_code=403, detail="Unauthorized")


def require_admin(
    x_is_admin: Optional[str] = Header(default=None, alias="x-is-admin"),
) -> None:
    # Simple session flag. Frontend sets localStorage.isAdmin=true after /admin_login success.
    is_admin = str(x_is_admin or "").strip().lower() in ("true", "1", "yes")
    verify_admin(is_admin)


def _next_id(conn: sqlite3.Connection, table: str, column: str, prefix: str, default_start: int) -> str:
    row = conn.execute(
        f"""
        SELECT {column} AS id
        FROM {table}
        WHERE {column} LIKE ?
        ORDER BY {column} DESC
        LIMIT 1
        """,
        (f"{prefix}-%",),
    ).fetchone()
    if not row or not row["id"]:
        return f"{prefix}-{default_start}"

    try:
        n = int(str(row["id"]).split("-", 1)[1])
    except Exception:
        n = default_start - 1
    return f"{prefix}-{n + 1}"


# ---------------------------------------------------------------------------
# Startup: create extra tables & seed inventory
# ---------------------------------------------------------------------------
def init_db():
    """
    Create app tables if they don't exist.

    IMPORTANT:
    - Reservation logic remains centralized in `inventory` + `reservations`.
    - Store-level stock is maintained in `store_stock` (seeded from dataset once).
      Dataset tables like Store_Inventory_Records remain read-only snapshots.
    """
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS reservations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id  TEXT    NOT NULL,
            store_id    TEXT    NOT NULL DEFAULT 'ALL',
            quantity    INTEGER NOT NULL,
            customer    TEXT,
            status      TEXT    NOT NULL DEFAULT 'active',
            created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
            expires_at  TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS waitlist (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id         TEXT    NOT NULL,
            store_id           TEXT    NOT NULL DEFAULT 'ALL',
            customer_email     TEXT    NOT NULL,
            requested_quantity INTEGER NOT NULL DEFAULT 1,
            created_at         TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        -- Operational store stock that we can mutate on purchases/transfers.
        CREATE TABLE IF NOT EXISTS store_stock (
            store_id   TEXT    NOT NULL,
            product_id TEXT    NOT NULL,
            stock      INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (store_id, product_id)
        );

        -- Activity log for admin analytics/feed.
        CREATE TABLE IF NOT EXISTS activity_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ts         TEXT    NOT NULL DEFAULT (datetime('now')),
            event_type TEXT    NOT NULL,
            message    TEXT    NOT NULL,
            product_id TEXT,
            store_id   TEXT,
            quantity   INTEGER
        );
    """)

    # Seed store_stock once from dataset snapshot if empty.
    try:
        cnt = conn.execute("SELECT COUNT(1) AS n FROM store_stock").fetchone()["n"]
        if cnt == 0:
            conn.execute(
                """
                INSERT INTO store_stock (store_id, product_id, stock)
                SELECT
                    store_id,
                    product_id,
                    SUM(CASE WHEN recorded_stock_level < 0 THEN 0 ELSE recorded_stock_level END) AS stock
                FROM Store_Inventory_Records
                GROUP BY store_id, product_id
                """
            )
            conn.commit()
    except Exception:
        # If dataset tables don't exist yet, skip seeding (app will still run).
        conn.rollback()
    finally:
        conn.close()


def log_activity(
    conn: sqlite3.Connection,
    event_type: str,
    message: str,
    product_id: Optional[str] = None,
    store_id: Optional[str] = None,
    quantity: Optional[int] = None,
):
    try:
        conn.execute(
            """
            INSERT INTO activity_log (event_type, message, product_id, store_id, quantity)
            VALUES (?, ?, ?, ?, ?)
            """,
            (event_type, message, product_id, store_id, quantity),
        )
    except Exception:
        # Never let analytics logging break core flows.
        pass


# ---------------------------------------------------------------------------
# Background expiry loop — runs every 30 seconds
# ---------------------------------------------------------------------------
EXPIRY_CHECK_INTERVAL_SECONDS = 30


async def expiry_loop():
    """
    Async background task that runs forever.
    Every EXPIRY_CHECK_INTERVAL_SECONDS it:
      1. Finds all reservations where status='active' AND expires_at <= now.
      2. Decrements inventory.reserved_stock by each expired quantity.
         MAX(0, ...) guard ensures reserved_stock never goes negative.
      3. Marks those reservations status='expired'.
    All done inside a single BEGIN IMMEDIATE transaction per cycle.
    """
    while True:
        await asyncio.sleep(EXPIRY_CHECK_INTERVAL_SECONDS)
        try:
            conn = sqlite3.connect(DB_FILE, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")

            # Fetch all active reservations that have passed their expiry time
            expired = conn.execute(
                """
                SELECT id, product_id, quantity
                FROM reservations
                WHERE status = 'active'
                  AND expires_at <= datetime('now')
                """
            ).fetchall()

            if expired:
                for rsv in expired:
                    # Decrement reserved_stock — MAX(0, ...) prevents negatives
                    conn.execute(
                        """
                        UPDATE inventory
                        SET reserved_stock = MAX(0, reserved_stock - ?)
                        WHERE product_id = ?
                        """,
                        (rsv["quantity"], rsv["product_id"]),
                    )

                # Bulk-mark all expired reservations in one UPDATE
                ids = tuple(r["id"] for r in expired)
                placeholders = ",".join("?" * len(ids))
                conn.execute(
                    f"UPDATE reservations SET status='expired' WHERE id IN ({placeholders})",
                    ids,
                )
                print(
                    f"[expiry_loop] Expired {len(expired)} reservation(s) and "
                    f"restored stock for: "
                    f"{list(set(r['product_id'] for r in expired))}"
                )
                
                # Notify waitlist for each unique product that gained stock
                for pid in set(r["product_id"] for r in expired):
                    notify_waitlist(pid)


            conn.execute("COMMIT")
            conn.close()

        except Exception as e:
            print(f"[expiry_loop] Error during expiry sweep: {e}")
            try:
                conn.execute("ROLLBACK")
                conn.close()
            except Exception:
                pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # Start the expiry loop as a non-blocking background task
    task = asyncio.create_task(expiry_loop())
    print(f"[lifespan] Expiry loop started — checking every {EXPIRY_CHECK_INTERVAL_SECONDS}s.")
    yield
    # Graceful shutdown: cancel the loop when the server stops
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("[lifespan] Expiry loop stopped.")


app = FastAPI(
    title="Real-Time Inventory Management API",
    description="Prevents overselling via reservation locks and real-time stock calculation.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static frontend files
app.mount("/static", StaticFiles(directory="static"), name="static")


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def compute_available_stock(conn: sqlite3.Connection, product_id: str) -> dict:
    """
    Returns a dict with total_stock, reserved_stock, available_stock, stock_status.
    Only counts active reservations that haven't expired.
    """
    row = conn.execute(
        "SELECT total_stock FROM inventory WHERE product_id = ?", (product_id,)
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found.")

    total = row["total_stock"]

    res = conn.execute(
        """SELECT COALESCE(SUM(quantity), 0) AS rsv
           FROM reservations
           WHERE product_id = ? AND status = 'active' AND expires_at > datetime('now')""",
        (product_id,),
    ).fetchone()
    reserved = res["rsv"]

    available = total - reserved
    if available <= 0:
        status = "red"
    elif available <= 10:
        status = "yellow"
    else:
        status = "green"

    return {
        "product_id": product_id,
        "total_stock": total,
        "reserved_stock": reserved,
        "available_stock": available,
        "stock_status": status,
    }


def notify_waitlist(product_id: str):
    """
    Simulates sending notifications to customers on the waitlist.
    Triggered when available_stock increases.
    """
    conn = get_conn()
    waiters = conn.execute(
        "SELECT customer_email FROM waitlist WHERE product_id = ?", (product_id,)
    ).fetchall()
    
    if waiters:
        print(f"\n[NOTIFICATION] Stock available for {product_id}!")
        for row in waiters:
            print(f"  >> Email sent to: {row['customer_email']}")
            
        # Clear the waitlist after notifying
        conn.execute("DELETE FROM waitlist WHERE product_id = ?", (product_id,))
        conn.commit()
    conn.close()



def expire_reservations(product_id: Optional[str] = None):
    """Mark expired active reservations as 'expired'."""
    conn = get_conn()
    if product_id:
        conn.execute(
            """UPDATE reservations SET status = 'expired'
               WHERE status = 'active' AND expires_at <= datetime('now')
               AND product_id = ?""",
            (product_id,),
        )
    else:
        conn.execute(
            """UPDATE reservations SET status = 'expired'
               WHERE status = 'active' AND expires_at <= datetime('now')"""
        )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Pydantic request schemas
# ---------------------------------------------------------------------------
class ReserveRequest(BaseModel):
    product_id: str
    quantity: int
    customer: Optional[str] = "anonymous"
    store_id: Optional[str] = "ALL"


class BuyRequest(BaseModel):
    reservation_id: int


class WaitlistRequest(BaseModel):
    product_id: str
    customer_email: str
    requested_quantity: Optional[int] = 1
    store_id: Optional[str] = "ALL"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
def customer_app():
    base = Path(__file__).resolve().parent
    return FileResponse(base / "static" / "customer.html", headers={"Cache-Control": "no-store"})


@app.get("/admin")
def admin_page():
    base = Path(__file__).resolve().parent
    return FileResponse(base / "static" / "admin_app.html", headers={"Cache-Control": "no-store"})


@app.get("/__debug_mainfile")
def __debug_mainfile():
    return {"main_file": str(Path(__file__).resolve())}


# ---- Inventory ------------------------------------------------------------ #

@app.get("/api/inventory")
def get_inventory():
    """Return full inventory with live available stock and color-coded status."""
    expire_reservations()
    conn = get_conn()
    rows = conn.execute("""
        SELECT
            i.product_id,
            -- Dataset schema: Products does not include product_name; expose a stable name anyway.
            COALESCE(p.category, i.product_id)                 AS product_name,
            p.category                                         AS category,
            p.price_usd                                        AS price,
            p.collection_season                                AS collection_season,
            i.total_stock,
            COALESCE(r.reserved_stock, 0)                      AS reserved_stock,
            i.total_stock - COALESCE(r.reserved_stock, 0)      AS available_stock,
            CASE
                WHEN (i.total_stock - COALESCE(r.reserved_stock, 0)) <= 0  THEN 'red'
                WHEN (i.total_stock - COALESCE(r.reserved_stock, 0)) <= 10 THEN 'yellow'
                ELSE 'green'
            END AS stock_status
        FROM inventory AS i
        LEFT JOIN Products AS p ON i.product_id = p.product_id
        LEFT JOIN (
            SELECT product_id, SUM(quantity) AS reserved_stock
            FROM reservations
            WHERE status = 'active' AND expires_at > datetime('now')
            GROUP BY product_id
        ) AS r ON i.product_id = r.product_id
        ORDER BY i.product_id
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/inventory/{product_id}")
def get_product_inventory(product_id: str):
    """Return live stock info for a single product."""
    expire_reservations(product_id)
    conn = get_conn()
    result = compute_available_stock(conn, product_id)
    conn.close()
    return result


# ---- Reservation ---------------------------------------------------------- #

@app.post("/api/reserve")
def reserve_stock(req: ReserveRequest, background_tasks: BackgroundTasks):
    """
    Lock stock for 10 minutes.
    Uses BEGIN IMMEDIATE to prevent race conditions / overselling.
    """
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be > 0.")

    conn = get_conn()
    try:
        # BEGIN IMMEDIATE acquires write lock immediately, blocking other writers
        conn.execute("BEGIN IMMEDIATE")

        stock = compute_available_stock(conn, req.product_id)
        if stock["available_stock"] < req.quantity:
            conn.execute("ROLLBACK")
            conn.close()
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Not enough stock. Available: {stock['available_stock']}, "
                    f"Requested: {req.quantity}"
                ),
            )

        expires_at = (
            datetime.now(timezone.utc) + timedelta(minutes=RESERVATION_TTL_MINUTES)
        ).strftime("%Y-%m-%d %H:%M:%S")

        cursor = conn.execute(
            """INSERT INTO reservations (product_id, store_id, quantity, customer, expires_at)
               VALUES (?, ?, ?, ?, ?)""",
            (req.product_id, req.store_id, req.quantity, req.customer, expires_at),
        )
        reservation_id = cursor.lastrowid
        conn.execute("COMMIT")

    except HTTPException:
        raise
    except Exception as e:
        conn.execute("ROLLBACK")
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    # Schedule background expiry sweep
    background_tasks.add_task(expire_reservations)

    return {
        "success": True,
        "reservation_id": reservation_id,
        "product_id": req.product_id,
        "quantity": req.quantity,
        "expires_at": expires_at,
        "message": f"Stock reserved for {RESERVATION_TTL_MINUTES} minutes.",
    }


@app.post("/api/buy")
def complete_purchase(req: BuyRequest):
    """
    Confirm a reservation and permanently deduct stock.
    Marks reservation as 'completed' and decrements total_stock in inventory.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        rsv = conn.execute(
            "SELECT * FROM reservations WHERE id = ?", (req.reservation_id,)
        ).fetchone()

        if not rsv:
            raise HTTPException(status_code=404, detail="Reservation not found.")
        if rsv["status"] != "active":
            raise HTTPException(
                status_code=409, detail=f"Reservation is {rsv['status']}, not active."
            )
        if rsv["expires_at"] <= datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"):
            conn.execute(
                "UPDATE reservations SET status='expired' WHERE id=?",
                (req.reservation_id,),
            )
            conn.execute("COMMIT")
            raise HTTPException(status_code=409, detail="Reservation has expired.")

        # Permanently deduct from inventory
        conn.execute(
            "UPDATE inventory SET total_stock = total_stock - ? WHERE product_id = ?",
            (rsv["quantity"], rsv["product_id"]),
        )
        # Mark reservation as completed
        conn.execute(
            "UPDATE reservations SET status='completed' WHERE id=?",
            (req.reservation_id,),
        )
        conn.execute("COMMIT")

    except HTTPException:
        raise
    except Exception as e:
        conn.execute("ROLLBACK")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return {
        "success": True,
        "message": "Purchase confirmed. Stock permanently deducted.",
        "reservation_id": req.reservation_id,
        "product_id": rsv["product_id"],
        "quantity_purchased": rsv["quantity"],
    }


@app.delete("/api/reserve/{reservation_id}")
def cancel_reservation(reservation_id: int):
    """Cancel a reservation and release the locked stock."""
    conn = get_conn()
    rsv = conn.execute(
        "SELECT * FROM reservations WHERE id = ?", (reservation_id,)
    ).fetchone()
    if not rsv:
        conn.close()
        raise HTTPException(status_code=404, detail="Reservation not found.")
    if rsv["status"] != "active":
        conn.close()
        raise HTTPException(status_code=409, detail=f"Cannot cancel: status is {rsv['status']}.")

    conn.execute(
        "UPDATE reservations SET status='cancelled' WHERE id=?", (reservation_id,)
    )
    conn.commit()
    conn.close()
    return {"success": True, "message": "Reservation cancelled. Stock released."}


# ---- Waitlist ------------------------------------------------------------- #

@app.post("/api/waitlist")
def join_waitlist(req: WaitlistRequest):
    """Add customer to waitlist for an out-of-stock product."""
    conn = get_conn()
    # Verify product exists
    p = conn.execute(
        "SELECT product_id FROM inventory WHERE product_id = ?", (req.product_id,)
    ).fetchone()
    if not p:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Product {req.product_id} not found.")

    conn.execute(
        """INSERT INTO waitlist (product_id, store_id, customer_email, requested_quantity)
           VALUES (?, ?, ?, ?)""",
        (req.product_id, req.store_id, req.customer_email, req.requested_quantity),
    )
    conn.commit()
    conn.close()
    return {"success": True, "message": "Added to waitlist successfully."}


@app.get("/api/waitlist/{product_id}")
def get_waitlist(product_id: str):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM waitlist WHERE product_id = ? ORDER BY created_at",
        (product_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---- Trust Score ---------------------------------------------------------- #

@app.get("/stock")
def get_stock():
    """
    GET /stock
    Returns product_name, total_stock, reserved_stock, available_stock, trust_score
    for every product in the inventory.
    """
    expire_reservations()
    conn = get_conn()
    try:
        rows = conn.execute("""
        SELECT
            i.product_id,
            COALESCE(p.category, i.product_id)                  AS product_name,
            i.total_stock,
            COALESCE(r.reserved_stock, 0)                 AS reserved_stock,
            i.total_stock - COALESCE(r.reserved_stock, 0) AS available_stock
        FROM inventory AS i
        LEFT JOIN Products AS p ON i.product_id = p.product_id
        LEFT JOIN (
            SELECT product_id, SUM(quantity) AS reserved_stock
            FROM reservations
            WHERE status = 'active' AND expires_at > datetime('now')
            GROUP BY product_id
        ) AS r ON i.product_id = r.product_id
        ORDER BY i.product_id
    """).fetchall()
    finally:
        conn.close()
    
    # Enrich with trust scores and demand predictions
    predictions = demand_prediction.get_predicted_demand()
    results = []
    for row in rows:
        d = dict(row)
        pid = d["product_id"]
        
        # Trust Score
        ts = ts_logic.compute_trust_score(pid)
        d["trust_score"] = ts["trust_score"]
        d["trust_label"] = ts["label"]
        
        # Demand Prediction
        d["predicted_demand"] = predictions.get(pid, 0.0)
        
        results.append(d)
        
    return results


@app.get("/api/trust-score/{product_id}")
def get_trust_score(product_id: str):
    """
    Trust Score based on mismatch percentage from audits.
    """
    return ts_logic.compute_trust_score(product_id)


@app.get("/api/trust-scores")
def get_all_trust_scores():
    """Returns trust scores for all products."""
    return ts_logic.compute_all_trust_scores()


# ---- Stores & Products ---------------------------------------------------- #

@app.get("/api/stores")
def get_stores():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM Stores").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/products")
def get_products():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM Products").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---- Admin login + admin controls ---------------------------------------- #

class AdminLoginRequest(BaseModel):
    password: str


@app.post("/admin_login")
def admin_login(req: AdminLoginRequest):
    if req.password == "6969":
        return {"success": True, "admin": True}
    raise HTTPException(status_code=401, detail="Invalid password")


class AddProductRequest(BaseModel):
    product_name: str


@app.post("/add_product")
def add_product(req: AddProductRequest, _: None = Depends(require_admin)):
    name = (req.product_name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="product_name is required.")

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        product_id = _next_id(conn, "Products", "product_id", "PROD", 2200)
        conn.execute(
            """
            INSERT INTO Products (product_id, category, price_usd, collection_season)
            VALUES (?, ?, ?, ?)
            """,
            (product_id, name, 0, "AdminAdded"),
        )
        conn.execute(
            """
            INSERT INTO inventory (product_id, total_stock, reserved_stock, available_stock)
            VALUES (?, 0, 0, 0)
            """,
            (product_id,),
        )
        log_activity(conn, "product", "Product added.", product_id=product_id)
        conn.execute("COMMIT")
        return {"success": True, "product_id": product_id, "product_name": name}

    except sqlite3.IntegrityError as e:
        conn.execute("ROLLBACK")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.delete("/delete_product/{product_id}")
def delete_product(product_id: str, _: None = Depends(require_admin)):
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        active = conn.execute(
            """
            SELECT COUNT(1) AS n
            FROM reservations
            WHERE product_id = ?
              AND status = 'active'
              AND expires_at > datetime('now')
            """,
            (product_id,),
        ).fetchone()
        if active and active["n"] > 0:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail="Cannot delete: active reservations exist.")

        conn.execute("DELETE FROM waitlist WHERE product_id = ?", (product_id,))
        conn.execute("DELETE FROM reservations WHERE product_id = ?", (product_id,))

        # Dataset-derived tables (safe cleanup; does not affect centralized reservation logic)
        conn.execute("DELETE FROM Store_Inventory_Records WHERE product_id = ?", (product_id,))
        conn.execute("DELETE FROM store_stock WHERE product_id = ?", (product_id,))
        conn.execute("DELETE FROM Online_Orders WHERE product_id = ?", (product_id,))
        conn.execute("DELETE FROM Inventory_Audits WHERE product_id = ?", (product_id,))
        conn.execute("DELETE FROM Store_Transfers WHERE product_id = ?", (product_id,))
        conn.execute("DELETE FROM Website_Inventory_View WHERE product_id = ?", (product_id,))

        conn.execute("DELETE FROM inventory WHERE product_id = ?", (product_id,))
        cur = conn.execute("DELETE FROM Products WHERE product_id = ?", (product_id,))
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=404, detail="Product not found.")

        log_activity(conn, "product", "Product deleted.", product_id=product_id)
        conn.execute("COMMIT")
        return {"success": True, "product_id": product_id}

    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


class AddStoreRequest(BaseModel):
    store_name: str


@app.post("/add_store")
def add_store(req: AddStoreRequest, _: None = Depends(require_admin)):
    name = (req.store_name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="store_name is required.")

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        store_id = _next_id(conn, "Stores", "store_id", "STORE", 150)
        conn.execute(
            """
            INSERT INTO Stores (store_id, city, store_type, staff_count)
            VALUES (?, ?, ?, ?)
            """,
            (store_id, name, "AdminAdded", 0),
        )
        log_activity(conn, "store", "Store added.", store_id=store_id)
        conn.execute("COMMIT")
        return {"success": True, "store_id": store_id, "store_name": name}

    except sqlite3.IntegrityError as e:
        conn.execute("ROLLBACK")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.delete("/delete_store/{store_id}")
def delete_store(store_id: str, _: None = Depends(require_admin)):
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        # Cleanup dataset tables referencing the store
        conn.execute("DELETE FROM Store_Inventory_Records WHERE store_id = ?", (store_id,))
        conn.execute("DELETE FROM store_stock WHERE store_id = ?", (store_id,))
        conn.execute("DELETE FROM Website_Inventory_View WHERE store_id = ?", (store_id,))
        conn.execute("DELETE FROM Inventory_Audits WHERE store_id = ?", (store_id,))
        conn.execute("DELETE FROM Online_Orders WHERE fulfillment_store = ?", (store_id,))
        conn.execute(
            "DELETE FROM Store_Transfers WHERE source_store = ? OR destination_store = ?",
            (store_id, store_id),
        )

        # Cleanup app tables (rarely used but safe)
        conn.execute("DELETE FROM waitlist WHERE store_id = ?", (store_id,))
        conn.execute("DELETE FROM reservations WHERE store_id = ?", (store_id,))

        cur = conn.execute("DELETE FROM Stores WHERE store_id = ?", (store_id,))
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=404, detail="Store not found.")

        log_activity(conn, "store", "Store deleted.", store_id=store_id)
        conn.execute("COMMIT")
        return {"success": True, "store_id": store_id}

    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


class UpdateStockRequest(BaseModel):
    product_id: str
    quantity: int


@app.post("/update_stock")
def update_stock(req: UpdateStockRequest, _: None = Depends(require_admin)):
    """
    Admin inventory update (centralized).
    quantity is a delta: positive adds stock, negative reduces stock.
    """
    if req.quantity == 0:
        raise HTTPException(status_code=400, detail="Quantity must be non-zero.")

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT total_stock, reserved_stock FROM inventory WHERE product_id = ?",
            (req.product_id,),
        ).fetchone()
        if not row:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=404, detail=f"Product {req.product_id} not found.")

        total_before = row["total_stock"]
        reserved = row["reserved_stock"]
        total_after = total_before + req.quantity

        if total_after < 0:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail="Cannot set total_stock below 0.")
        if total_after < reserved:
            conn.execute("ROLLBACK")
            raise HTTPException(
                status_code=409,
                detail=f"Cannot reduce below reserved_stock ({reserved}).",
            )

        conn.execute(
            "UPDATE inventory SET total_stock = ? WHERE product_id = ?",
            (total_after, req.product_id),
        )

        log_activity(
            conn,
            "stock_update",
            "Central inventory stock updated.",
            product_id=req.product_id,
            quantity=req.quantity,
        )
        conn.execute("COMMIT")
        return {
            "success": True,
            "product_id": req.product_id,
            "total_stock_before": total_before,
            "total_stock_after": total_after,
            "reserved_stock": reserved,
            "available_stock_after": total_after - reserved,
        }

    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ---- Store-level visibility (display-only) -------------------------------- #

@app.get("/store_inventory/{product_id}")
def get_store_inventory(product_id: str):
    """
    Display-only store-wise stock for a product using dataset tables.
    Does NOT affect centralized reservation/total stock logic.
    """
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                COALESCE(s.city, ss.store_id) AS store_name,
                SUM(MAX(0, ss.stock))         AS stock
            FROM store_stock AS ss
            LEFT JOIN Stores AS s ON s.store_id = ss.store_id
            WHERE ss.product_id = ?
            GROUP BY store_name
            ORDER BY stock DESC, store_name ASC
            """,
            (product_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/all_store_inventory")
def get_all_store_inventory():
    """
    Display-only full matrix (product_name, store_name, stock) using dataset tables.
    product_name is derived from Products.category (dataset has no separate name field).
    """
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                COALESCE(p.category, ss.product_id)    AS product_name,
                COALESCE(s.city, ss.store_id)          AS store_name,
                SUM(MAX(0, ss.stock))                  AS stock,
                ss.product_id                          AS product_id
            FROM store_stock AS ss
            LEFT JOIN Stores AS s ON s.store_id = ss.store_id
            LEFT JOIN Products AS p ON p.product_id = ss.product_id
            GROUP BY ss.product_id, product_name, store_name
            ORDER BY product_name ASC, store_name ASC
            """
        ).fetchall()
        # Keep response minimal but include ids for debugging/usefulness
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ---- Simplified endpoints (step-by-step build) ---------------------------- #

class StoreSaleRequest(BaseModel):
    product_id: str
    quantity: int


class StockAdjustRequest(BaseModel):
    product_id: str
    quantity: int


@app.post("/add_stock")
def add_stock(req: StockAdjustRequest, _: None = Depends(require_admin)):
    """
    Manager/Admin: Increase total_stock for a product.
    Uses BEGIN IMMEDIATE to serialize writes and keep reservation logic safe.
    """
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be > 0.")

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        row = conn.execute(
            "SELECT total_stock, reserved_stock FROM inventory WHERE product_id = ?",
            (req.product_id,),
        ).fetchone()

        if not row:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=404, detail=f"Product {req.product_id} not found.")

        conn.execute(
            "UPDATE inventory SET total_stock = total_stock + ? WHERE product_id = ?",
            (req.quantity, req.product_id),
        )
        conn.execute("COMMIT")

        updated = {
            "product_id": req.product_id,
            "total_stock_before": row["total_stock"],
            "total_stock_after": row["total_stock"] + req.quantity,
            "reserved_stock": row["reserved_stock"],
            "available_stock_after": (row["total_stock"] + req.quantity) - row["reserved_stock"],
        }
        log_activity(
            conn,
            "stock_update",
            "Central inventory stock increased.",
            product_id=req.product_id,
            quantity=req.quantity,
        )
        return {"success": True, **updated, "message": "Stock added successfully."}

    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/reduce_stock")
def reduce_stock(req: StockAdjustRequest, _: None = Depends(require_admin)):
    """
    Manager/Admin: Decrease total_stock for a product.
    Guarantees:
      - total_stock never goes negative
      - total_stock never drops below reserved_stock (prevents breaking reservations)
    """
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be > 0.")

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        row = conn.execute(
            "SELECT total_stock, reserved_stock FROM inventory WHERE product_id = ?",
            (req.product_id,),
        ).fetchone()

        if not row:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=404, detail=f"Product {req.product_id} not found.")

        total_before = row["total_stock"]
        reserved = row["reserved_stock"]

        if total_before < req.quantity:
            conn.execute("ROLLBACK")
            raise HTTPException(
                status_code=409,
                detail=f"Cannot reduce by {req.quantity}. Total stock is only {total_before}.",
            )

        total_after = total_before - req.quantity
        if total_after < reserved:
            conn.execute("ROLLBACK")
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Cannot reduce stock below reserved units. "
                    f"reserved_stock={reserved}, requested_total_after={total_after}."
                ),
            )

        conn.execute(
            "UPDATE inventory SET total_stock = total_stock - ? WHERE product_id = ?",
            (req.quantity, req.product_id),
        )
        conn.execute("COMMIT")

        updated = {
            "product_id": req.product_id,
            "total_stock_before": total_before,
            "total_stock_after": total_after,
            "reserved_stock": reserved,
            "available_stock_after": total_after - reserved,
        }
        log_activity(
            conn,
            "stock_update",
            "Central inventory stock reduced.",
            product_id=req.product_id,
            quantity=-req.quantity,
        )
        return {"success": True, **updated, "message": "Stock reduced successfully."}

    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Admin: waitlist, store control, activity feed, cashier mode
# ---------------------------------------------------------------------------

@app.get("/admin/waitlist")
def admin_get_waitlist(_: None = Depends(require_admin)):
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, product_id, store_id, customer_email, requested_quantity, created_at FROM waitlist ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.delete("/admin/waitlist/{waitlist_id}")
def admin_remove_waitlist(waitlist_id: int, _: None = Depends(require_admin)):
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute("DELETE FROM waitlist WHERE id = ?", (waitlist_id,))
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=404, detail="Waitlist entry not found.")
        log_activity(conn, "waitlist", "Waitlist entry removed.", quantity=waitlist_id)
        conn.execute("COMMIT")
        return {"success": True, "id": waitlist_id}
    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/admin/waitlist/notify/{product_id}")
def admin_notify_waitlist(product_id: str, _: None = Depends(require_admin)):
    # Uses existing notify_waitlist() behavior (prints + clears waitlist for product).
    notify_waitlist(product_id)
    conn = get_conn()
    try:
        log_activity(conn, "waitlist", "Waitlist notified.", product_id=product_id)
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "product_id": product_id}


@app.get("/admin/activity")
def admin_activity(limit: int = 50, _: None = Depends(require_admin)):
    limit = max(1, min(int(limit), 200))
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT id, ts, event_type, message, product_id, store_id, quantity
            FROM activity_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/admin/store_stock/{product_id}")
def admin_store_stock(product_id: str, _: None = Depends(require_admin)):
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT ss.store_id, COALESCE(s.city, ss.store_id) AS store_name, ss.stock
            FROM store_stock ss
            LEFT JOIN Stores s ON s.store_id = ss.store_id
            WHERE ss.product_id = ?
            ORDER BY ss.stock DESC
            """,
            (product_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


class TransferStockRequest(BaseModel):
    product_id: str
    from_store_id: str
    to_store_id: str
    quantity: int


@app.post("/admin/transfer_stock")
def admin_transfer_stock(req: TransferStockRequest, _: None = Depends(require_admin)):
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be > 0.")
    if req.from_store_id == req.to_store_id:
        raise HTTPException(status_code=400, detail="from_store_id and to_store_id must differ.")

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        src = conn.execute(
            "SELECT stock FROM store_stock WHERE store_id=? AND product_id=?",
            (req.from_store_id, req.product_id),
        ).fetchone()
        if not src:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=404, detail="Source store stock not found.")
        if int(src["stock"]) < req.quantity:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail="Not enough stock in source store.")

        # Ensure destination row exists
        conn.execute(
            "INSERT OR IGNORE INTO store_stock (store_id, product_id, stock) VALUES (?, ?, 0)",
            (req.to_store_id, req.product_id),
        )

        conn.execute(
            "UPDATE store_stock SET stock = stock - ? WHERE store_id=? AND product_id=?",
            (req.quantity, req.from_store_id, req.product_id),
        )
        conn.execute(
            "UPDATE store_stock SET stock = stock + ? WHERE store_id=? AND product_id=?",
            (req.quantity, req.to_store_id, req.product_id),
        )

        log_activity(
            conn,
            "transfer",
            "Stock transferred between stores.",
            product_id=req.product_id,
            store_id=f"{req.from_store_id}->{req.to_store_id}",
            quantity=req.quantity,
        )

        conn.execute("COMMIT")
        return {"success": True}

    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


class CashierPurchaseRequest(BaseModel):
    product_id: str
    store_id: str
    quantity: int


@app.post("/admin/cashier_purchase")
def admin_cashier_purchase(req: CashierPurchaseRequest, _: None = Depends(require_admin)):
    """
    Offline cashier mode purchase:
    - Deducts central total_stock (never below reserved)
    - Deducts store_stock for the selected store
    """
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be > 0.")

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        inv = conn.execute(
            "SELECT total_stock, reserved_stock FROM inventory WHERE product_id=?",
            (req.product_id,),
        ).fetchone()
        if not inv:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=404, detail="Product not found.")

        total_before = int(inv["total_stock"])
        reserved = int(inv["reserved_stock"])
        total_after = total_before - req.quantity
        if total_after < 0:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail="Not enough central stock.")
        if total_after < reserved:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail="Cannot sell below reserved units.")

        ss = conn.execute(
            "SELECT stock FROM store_stock WHERE store_id=? AND product_id=?",
            (req.store_id, req.product_id),
        ).fetchone()
        if not ss:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=404, detail="Store stock not found.")
        if int(ss["stock"]) < req.quantity:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail="Not enough store stock.")

        conn.execute(
            "UPDATE inventory SET total_stock = total_stock - ? WHERE product_id=?",
            (req.quantity, req.product_id),
        )
        conn.execute(
            "UPDATE store_stock SET stock = stock - ? WHERE store_id=? AND product_id=?",
            (req.quantity, req.store_id, req.product_id),
        )

        log_activity(
            conn,
            "cashier_purchase",
            "Cashier purchase completed.",
            product_id=req.product_id,
            store_id=req.store_id,
            quantity=req.quantity,
        )

        conn.execute("COMMIT")
        return {"success": True}

    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/simulate_store_sale")
def simulate_store_sale(req: StoreSaleRequest):
    """
    POST /simulate_store_sale
    Simulates a physical in-store sale by directly reducing total_stock.
    This keeps online inventory in sync with what actually sold in the store.
    """
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be > 0.")

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        row = conn.execute(
            "SELECT total_stock FROM inventory WHERE product_id = ?",
            (req.product_id,),
        ).fetchone()

        if not row:
            conn.execute("ROLLBACK")
            conn.close()
            raise HTTPException(status_code=404, detail=f"Product {req.product_id} not found.")

        if row["total_stock"] < req.quantity:
            conn.execute("ROLLBACK")
            conn.close()
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Not enough stock. Available: {row['total_stock']}, "
                    f"Requested: {req.quantity}"
                ),
            )

        conn.execute(
            "UPDATE inventory SET total_stock = total_stock - ? WHERE product_id = ?",
            (req.quantity, req.product_id),
        )
        conn.execute("COMMIT")

        new_stock = row["total_stock"] - req.quantity
        return {
            "success": True,
            "product_id": req.product_id,
            "quantity": req.quantity,
            "stock_before": row["total_stock"],
            "stock_after": new_stock
        }
    except HTTPException:
        raise
    except Exception as e:
        conn.execute("ROLLBACK")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# POST /reserve  — clean step-by-step reservation endpoint
# ---------------------------------------------------------------------------

class SimpleReserveRequest(BaseModel):
    product_id: str
    quantity: int


@app.post("/reserve")
def reserve(req: SimpleReserveRequest):
    """
    POST /reserve
    Input : product_id, quantity
    Logic :
      1. BEGIN IMMEDIATE — SQLite write-lock (blocks all other writers instantly).
      2. Compute available_stock = total_stock - reserved_stock (from inventory table).
      3. If available_stock >= quantity:
            - Increment inventory.reserved_stock
            - Insert reservation row with expires_at = now + 10 min
            - COMMIT
      4. If not: ROLLBACK → return "Out of stock".

    Why BEGIN IMMEDIATE?
      Without it, two simultaneous requests could both read the same
      available_stock = 5, both pass the check, and both reserve 5 units
      → overselling of 5. IMMEDIATE prevents this by serialising writes.
    """
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be > 0.")

    conn = get_conn()
    try:
        # Acquire write-lock immediately — no other writer can proceed until COMMIT/ROLLBACK
        conn.execute("BEGIN IMMEDIATE")

        row = conn.execute(
            "SELECT total_stock, reserved_stock FROM inventory WHERE product_id = ?",
            (req.product_id,),
        ).fetchone()

        if not row:
            conn.execute("ROLLBACK")
            conn.close()
            raise HTTPException(status_code=404, detail=f"Product '{req.product_id}' not found.")

        total_stock    = row["total_stock"]
        reserved_stock = row["reserved_stock"]
        available_stock = total_stock - reserved_stock

        # ---- Out of stock check -----------------------------------------
        if available_stock < req.quantity:
            conn.execute("ROLLBACK")
            conn.close()
            return {
                "success": False,
                "product_id": req.product_id,
                "available_stock": available_stock,
                "requested": req.quantity,
                "message": "Out of stock. Please join the waitlist.",
            }

        # ---- Enough stock → reserve it ----------------------------------
        expires_at = (
            datetime.now(timezone.utc) + timedelta(minutes=RESERVATION_TTL_MINUTES)
        ).strftime("%Y-%m-%d %H:%M:%S")

        # 1. Increment reserved_stock in inventory table
        conn.execute(
            "UPDATE inventory SET reserved_stock = reserved_stock + ? WHERE product_id = ?",
            (req.quantity, req.product_id),
        )

        # 2. Create reservation record (for expiry tracking & audit)
        cursor = conn.execute(
            """INSERT INTO reservations (product_id, store_id, quantity, status, expires_at)
               VALUES (?, 'ALL', ?, 'active', ?)""",
            (req.product_id, req.quantity, expires_at),
        )
        reservation_id = cursor.lastrowid

        conn.execute("COMMIT")

    except HTTPException:
        raise
    except Exception as e:
        conn.execute("ROLLBACK")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return {
        "success": True,
        "reservation_id": reservation_id,
        "product_id": req.product_id,
        "quantity_reserved": req.quantity,
        "available_stock_before": available_stock,
        "available_stock_after": available_stock - req.quantity,
        "expires_at": expires_at,
        "message": f"Stock locked for {RESERVATION_TTL_MINUTES} minutes.",
    }


# ---------------------------------------------------------------------------
# POST /confirm  — complete a purchase: deduct stock permanently
# ---------------------------------------------------------------------------

class ConfirmRequest(BaseModel):
    reservation_id: int


@app.post("/confirm")
def confirm_reservation(req: ConfirmRequest):
    """
    POST /confirm
    Input : reservation_id
    Logic :
      1. Load reservation — must be status='active' and not expired.
      2. BEGIN IMMEDIATE write-lock.
      3. Reduce inventory.total_stock  by quantity  (permanent sale deduction).
      4. Reduce inventory.reserved_stock by quantity (release the lock).
         MAX(0, ...) guards prevent negatives on both columns.
      5. Mark reservation status='completed'.
      6. COMMIT.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        rsv = conn.execute(
            "SELECT * FROM reservations WHERE id = ?", (req.reservation_id,)
        ).fetchone()

        if not rsv:
            conn.execute("ROLLBACK")
            conn.close()
            raise HTTPException(status_code=404, detail="Reservation not found.")

        if rsv["status"] != "active":
            conn.execute("ROLLBACK")
            conn.close()
            raise HTTPException(
                status_code=409,
                detail=f"Cannot confirm: reservation status is '{rsv['status']}'.",
            )

        if rsv["expires_at"] <= datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"):
            # Mark as expired, then reject
            conn.execute(
                "UPDATE reservations SET status='expired' WHERE id=?",
                (req.reservation_id,),
            )
            conn.execute("COMMIT")
            conn.close()
            raise HTTPException(status_code=409, detail="Reservation has expired.")

        qty = rsv["quantity"]
        pid = rsv["product_id"]

        # Permanently deduct from total_stock (the actual sale)
        conn.execute(
            "UPDATE inventory SET total_stock = MAX(0, total_stock - ?) WHERE product_id = ?",
            (qty, pid),
        )
        # Release the reservation lock from reserved_stock
        conn.execute(
            "UPDATE inventory SET reserved_stock = MAX(0, reserved_stock - ?) WHERE product_id = ?",
            (qty, pid),
        )
        # Mark reservation as completed
        conn.execute(
            "UPDATE reservations SET status='completed' WHERE id=?",
            (req.reservation_id,),
        )
        conn.execute("COMMIT")

    except HTTPException:
        raise
    except Exception as e:
        conn.execute("ROLLBACK")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return {
        "success": True,
        "reservation_id": req.reservation_id,
        "product_id": pid,
        "quantity": qty,
        "message": "Purchase confirmed. total_stock and reserved_stock updated.",
    }


# ---------------------------------------------------------------------------
# Customer purchase with store selection (does not modify reservation logic)
# ---------------------------------------------------------------------------

class PurchaseConfirmRequest(BaseModel):
    reservation_id: int
    store_id: Optional[str] = "ANY"  # store_id from Stores table, or "ANY"


@app.post("/purchase")
def purchase_with_store(req: PurchaseConfirmRequest):
    """
    Customer purchase confirmation that also decrements store_stock.

    - Uses same checks as /confirm: reservation must be active and not expired.
    - Atomically updates:
        inventory.total_stock -= qty
        inventory.reserved_stock -= qty
        reservations.status = 'completed'
        store_stock.stock -= qty  (chosen store or best-available store)
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        rsv = conn.execute(
            "SELECT * FROM reservations WHERE id = ?",
            (req.reservation_id,),
        ).fetchone()

        if not rsv:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=404, detail="Reservation not found.")

        if rsv["status"] != "active":
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail=f"Reservation is {rsv['status']}, not active.")

        if rsv["expires_at"] <= datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"):
            conn.execute("UPDATE reservations SET status='expired' WHERE id=?", (req.reservation_id,))
            conn.execute("COMMIT")
            raise HTTPException(status_code=409, detail="Reservation has expired.")

        qty = int(rsv["quantity"])
        pid = rsv["product_id"]

        # Ensure inventory won't go negative or below reserved units.
        inv = conn.execute(
            "SELECT total_stock, reserved_stock FROM inventory WHERE product_id = ?",
            (pid,),
        ).fetchone()
        if not inv:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=404, detail=f"Product {pid} not found in inventory.")

        total_before = inv["total_stock"]
        reserved_before = inv["reserved_stock"]
        total_after = total_before - qty
        reserved_after = reserved_before - qty
        if total_after < 0 or reserved_after < 0:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail="Inventory underflow. Please refresh and retry.")

        # Choose store
        store_id = (req.store_id or "ANY").strip()
        chosen_store_id = None

        if store_id.upper() == "ANY":
            row = conn.execute(
                """
                SELECT store_id, stock
                FROM store_stock
                WHERE product_id = ?
                ORDER BY stock DESC
                LIMIT 1
                """,
                (pid,),
            ).fetchone()
            if not row or int(row["stock"]) < qty:
                conn.execute("ROLLBACK")
                raise HTTPException(status_code=409, detail="Selected product is not available in any store.")
            chosen_store_id = row["store_id"]
        else:
            row = conn.execute(
                "SELECT stock FROM store_stock WHERE store_id = ? AND product_id = ?",
                (store_id, pid),
            ).fetchone()
            if not row:
                conn.execute("ROLLBACK")
                raise HTTPException(status_code=404, detail="Store stock record not found for this product.")
            if int(row["stock"]) < qty:
                conn.execute("ROLLBACK")
                raise HTTPException(status_code=409, detail="Not enough stock in selected store.")
            chosen_store_id = store_id

        # Apply store stock decrement
        conn.execute(
            "UPDATE store_stock SET stock = stock - ? WHERE store_id = ? AND product_id = ?",
            (qty, chosen_store_id, pid),
        )

        # Apply centralized inventory + reservation updates (same intent as /confirm)
        conn.execute(
            "UPDATE inventory SET total_stock = MAX(0, total_stock - ?) WHERE product_id = ?",
            (qty, pid),
        )
        conn.execute(
            "UPDATE inventory SET reserved_stock = MAX(0, reserved_stock - ?) WHERE product_id = ?",
            (qty, pid),
        )
        conn.execute("UPDATE reservations SET status='completed' WHERE id=?", (req.reservation_id,))

        log_activity(
            conn,
            "purchase",
            "Purchase confirmed (store stock updated).",
            product_id=pid,
            store_id=chosen_store_id,
            quantity=qty,
        )

        conn.execute("COMMIT")
        return {
            "success": True,
            "reservation_id": req.reservation_id,
            "product_id": pid,
            "quantity": qty,
            "store_id": chosen_store_id,
            "message": "Purchase confirmed. total_stock and store stock updated.",
        }

    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# POST /release  — cancel a reservation: restore available stock
# ---------------------------------------------------------------------------

class ReleaseRequest(BaseModel):
    reservation_id: int


@app.post("/release")
def release_reservation(req: ReleaseRequest):
    """
    POST /release
    Input : reservation_id
    Logic :
      1. Load reservation — must be status='active'.
      2. BEGIN IMMEDIATE write-lock.
      3. Reduce inventory.reserved_stock by quantity (stock restored to available).
         total_stock is NOT changed — no sale happened.
      4. Mark reservation status='cancelled'.
      5. COMMIT.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        rsv = conn.execute(
            "SELECT * FROM reservations WHERE id = ?", (req.reservation_id,)
        ).fetchone()

        if not rsv:
            conn.execute("ROLLBACK")
            conn.close()
            raise HTTPException(status_code=404, detail="Reservation not found.")

        if rsv["status"] != "active":
            conn.execute("ROLLBACK")
            conn.close()
            raise HTTPException(
                status_code=409,
                detail=f"Cannot release: reservation status is '{rsv['status']}'.",
            )

        qty = rsv["quantity"]
        pid = rsv["product_id"]

        # Only release the lock — total_stock stays the same (no sale)
        conn.execute(
            "UPDATE inventory SET reserved_stock = MAX(0, reserved_stock - ?) WHERE product_id = ?",
            (qty, pid),
        )
        conn.execute(
            "UPDATE reservations SET status='cancelled' WHERE id=?",
            (req.reservation_id,),
        )
        conn.execute("COMMIT")
        
        # Notify waitlist that stock is now available
        notify_waitlist(pid)


    except HTTPException:
        raise
    except Exception as e:
        conn.execute("ROLLBACK")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return {
        "success": True,
        "reservation_id": req.reservation_id,
        "product_id": pid,
        "quantity_released": qty,
        "message": "Reservation cancelled. Stock restored to available.",
    }


# ---------------------------------------------------------------------------
# POST /join_waitlist — simplified waitlist endpoint
# ---------------------------------------------------------------------------

class SimpleWaitlistRequest(BaseModel):
    product_id: str
    email: str


@app.post("/join_waitlist")
def join_waitlist_simplified(req: SimpleWaitlistRequest):
    """
    POST /join_waitlist
    Logic:
      1. Check available_stock.
      2. If stock = 0: allow joining.
      3. Else: return error (stock still exists).
    """
    conn = get_conn()
    stock = compute_available_stock(conn, req.product_id)
    
    if stock["available_stock"] > 0:
        conn.close()
        raise HTTPException(
            status_code=400, 
            detail=f"Stock is still available ({stock['available_stock']} units). Please reserve instead."
        )

    conn.execute(
        "INSERT INTO waitlist (product_id, customer_email) VALUES (?, ?)",
        (req.product_id, req.email)
    )
    conn.commit()
    conn.close()
    
    return {
        "success": True,
        "product_id": req.product_id,
        "email": req.email,
        "message": "Added to waitlist. We will notify you when stock returns."
    }

