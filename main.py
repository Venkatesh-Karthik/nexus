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
from fastapi.responses import StreamingResponse
from pathlib import Path
import json
import time
from pydantic import BaseModel
import trust_score as ts_logic
import demand_prediction
import startup


DB_FILE = "inventory.db"
RESERVATION_TTL_MINUTES = 10
CENTRAL_STORE_ID = "CENTRAL"

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

        -- Operational mismatch history for trust/visibility.
        CREATE TABLE IF NOT EXISTS inventory_mismatches (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            ts             TEXT    NOT NULL DEFAULT (datetime('now')),
            product_id     TEXT    NOT NULL,
            total_stock    INTEGER NOT NULL,
            sum_store_stock INTEGER NOT NULL,
            delta          INTEGER NOT NULL
        );

        -- App-level product catalog (realistic retail naming), decoupled from dataset Products table.
        CREATE TABLE IF NOT EXISTS product_catalog (
            product_id   TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category     TEXT NOT NULL,
            brand        TEXT NOT NULL
        );
    """)

    # Ensure a "Central Warehouse" store exists for balancing total vs store-level stock.
    # Stores is a dataset table; we insert a row if possible, but do not require it.
    try:
        conn.execute(
            "INSERT OR IGNORE INTO Stores (store_id, city) VALUES (?, ?)",
            (CENTRAL_STORE_ID, "Central Warehouse"),
        )
        conn.commit()
    except Exception:
        conn.rollback()

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

    # After seeding (or if store_stock already exists), enforce the invariant:
    #   inventory.total_stock == SUM(store_stock.stock) for each product,
    # by assigning any difference into CENTRAL store_stock (never negative).
    try:
        _seed_product_catalog(conn)
        _sync_all_products_to_central(conn)
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()


def _seed_product_catalog(conn: sqlite3.Connection) -> None:
    """
    Populate product_catalog for all inventory products if missing.
    Uses a deterministic cycle of realistic products across 15 retail categories.
    """
    # If table is empty, seed for all product_ids in inventory.
    cnt = conn.execute("SELECT COUNT(1) AS n FROM product_catalog").fetchone()
    if cnt and int(cnt["n"]) > 0:
        return

    categories = [
        ("Watches", [
            ("Rolex", "Submariner"), ("Omega", "Seamaster"), ("Tissot", "PRX"), ("Casio", "G-Shock"),
            ("Seiko", "Prospex Diver"), ("Fossil", "Gen 6 Hybrid"), ("Citizen", "Eco-Drive"),
            ("Tag Heuer", "Carrera"), ("Garmin", "Fenix"), ("Apple", "Watch Series"),
        ]),
        ("Shoes", [
            ("Nike", "Air Max 90"), ("Adidas", "Ultraboost"), ("Puma", "RS-X"), ("Reebok", "Classic Leather"),
            ("New Balance", "574"), ("ASICS", "Gel-Kayano"), ("Vans", "Old Skool"),
            ("Converse", "Chuck 70"), ("Jordan", "Retro High"), ("Skechers", "Go Walk"),
        ]),
        ("Apparel", [
            ("Levi's", "501 Original Jeans"), ("Uniqlo", "Supima Cotton Tee"), ("Zara", "Slim Fit Shirt"),
            ("H&M", "Everyday Hoodie"), ("Nike", "Dri-FIT Tee"), ("Adidas", "Essentials Track Pants"),
            ("Ralph Lauren", "Polo Shirt"), ("Calvin Klein", "Logo Sweatshirt"),
            ("Tommy Hilfiger", "Classic Jacket"), ("The North Face", "Puffer Jacket"),
        ]),
        ("Accessories", [
            ("Bellroy", "Slim Wallet"), ("Herschel", "Chapter Toiletry Kit"), ("Nike", "Everyday Cap"),
            ("Adidas", "Performance Socks"), ("Fjällräven", "Kånken Keyring"), ("Ray-Ban", "Leather Case"),
            ("Samsonite", "Luggage Tag"), ("Anker", "USB-C Cable"), ("Apple", "MagSafe Wallet"),
            ("Victorinox", "Swiss Card"),
        ]),
        ("Electronics", [
            ("Apple", "AirPods Pro"), ("Sony", "WH-1000XM Headphones"), ("Samsung", "Galaxy Watch"),
            ("Bose", "QuietComfort Earbuds"), ("Nintendo", "Switch Console"), ("Kindle", "Paperwhite"),
            ("GoPro", "HERO Action Cam"), ("JBL", "Flip Speaker"), ("Logitech", "MX Master Mouse"),
            ("Dell", "UltraSharp Monitor"),
        ]),
        ("Bags", [
            ("Samsonite", "Carry-On Spinner"), ("Tumi", "Alpha Brief Pack"), ("Herschel", "Little America Backpack"),
            ("Nike", "Brasilia Duffel"), ("Adidas", "Classic Backpack"), ("Fjällräven", "Kånken Backpack"),
            ("Eastpak", "Padded Pak'r"), ("Michael Kors", "Jet Set Tote"),
            ("Coach", "Signature Crossbody"), ("Bellroy", "Transit Workpack"),
        ]),
        ("Sunglasses", [
            ("Ray-Ban", "Wayfarer"), ("Oakley", "Holbrook"), ("Persol", "PO0649"), ("Maui Jim", "Peahi"),
            ("Prada", "Linea Rossa"), ("Gucci", "Square Frame"), ("Tom Ford", "FT0237"),
            ("Polaroid", "Classic Shades"), ("Carrera", "Aviator"), ("Vogue", "Everyday Sunglasses"),
        ]),
        ("Perfumes", [
            ("Dior", "Sauvage"), ("Chanel", "Bleu de Chanel"), ("Giorgio Armani", "Acqua di Giò"),
            ("Versace", "Eros"), ("YSL", "La Nuit de L'Homme"), ("Paco Rabanne", "1 Million"),
            ("Tom Ford", "Oud Wood"), ("Jo Malone", "Wood Sage & Sea Salt"),
            ("Calvin Klein", "CK One"), ("Burberry", "Hero"),
        ]),
        ("Jewelry", [
            ("Tiffany & Co.", "Sterling Pendant"), ("Swarovski", "Crystal Studs"), ("Pandora", "Charm Bracelet"),
            ("Cartier", "Love Ring"), ("Bvlgari", "Serpenti Bracelet"), ("Mejuri", "Gold Hoops"),
            ("David Yurman", "Cable Bracelet"), ("Thomas Sabo", "Silver Chain"),
            ("Tous", "Signature Pendant"), ("Mikimoto", "Pearl Earrings"),
        ]),
        ("Sportswear", [
            ("Nike", "Tech Fleece Joggers"), ("Adidas", "Training Shorts"), ("Under Armour", "HeatGear Top"),
            ("Puma", "Running Tee"), ("Reebok", "Workout Tank"), ("Lululemon", "Pace Breaker Shorts"),
            ("Gymshark", "Training Leggings"), ("ASICS", "Running Jacket"),
            ("New Balance", "Sport Hoodie"), ("The North Face", "Active Windbreaker"),
        ]),
        ("Formal Footwear", [
            ("Clarks", "Oxford Leather"), ("Allen Edmonds", "Park Avenue"), ("Cole Haan", "GrandPro"),
            ("Johnston & Murphy", "Tyndall Cap Toe"), ("Ecco", "Helsinki"), ("Loake", "1880 Oxford"),
            ("Bata", "Classic Derby"), ("Hush Puppies", "Dress Shoe"),
            ("Tod's", "Leather Loafer"), ("Salvatore Ferragamo", "Gancini Loafer"),
        ]),
        ("Smart Devices", [
            ("Apple", "AirTag 4-Pack"), ("Google", "Nest Hub"), ("Amazon", "Echo Dot"),
            ("Fitbit", "Charge Tracker"), ("Samsung", "SmartTag2"), ("Xiaomi", "Mi Smart Band"),
            ("Ring", "Video Doorbell"), ("TP-Link", "Smart Plug"),
            ("Philips Hue", "Starter Kit"), ("Anker", "MagGo Charger"),
        ]),
        ("Luxury Items", [
            ("Louis Vuitton", "Monogram Wallet"), ("Gucci", "Leather Belt"), ("Prada", "Saffiano Cardholder"),
            ("Burberry", "Check Scarf"), ("Hermès", "Silk Tie"), ("Cartier", "Signature Pen"),
            ("Montblanc", "Meisterstück Pen"), ("Bottega Veneta", "Intrecciato Wallet"),
            ("Saint Laurent", "SLP Card Case"), ("Balenciaga", "Logo Cap"),
        ]),
        ("Travel Gear", [
            ("Samsonite", "Travel Pillow"), ("Cabeau", "Neck Pillow"), ("Away", "Carry-On"),
            ("Osprey", "Farpoint Backpack"), ("Nomad", "Travel Adapter"), ("Anker", "Power Bank"),
            ("GoPro", "Travel Kit"), ("Peak Design", "Packing Cubes"),
            ("Hydro Flask", "Travel Bottle"), ("Eagle Creek", "Compression Sacs"),
        ]),
        ("Fitness Equipment", [
            ("Bowflex", "Adjustable Dumbbells"), ("Nike", "Training Mat"), ("Adidas", "Resistance Bands"),
            ("Fitbit", "Smart Scale"), ("TheraBand", "Loop Bands"), ("TriggerPoint", "Foam Roller"),
            ("Hyperice", "Massage Gun"), ("Garmin", "HRM-Pro Strap"),
            ("Kettlebell Kings", "Kettlebell"), ("Under Armour", "Jump Rope"),
        ]),
    ]

    # Flatten to a deterministic list of 150 SKUs.
    sku = []
    for cat, items in categories:
        for brand, name in items:
            sku.append((cat, brand, f"{brand} {name}"))

    inv_ids = [r["product_id"] for r in conn.execute("SELECT product_id FROM inventory ORDER BY product_id").fetchall()]
    for i, pid in enumerate(inv_ids):
        cat, brand, pname = sku[i % len(sku)]
        conn.execute(
            "INSERT OR REPLACE INTO product_catalog (product_id, product_name, category, brand) VALUES (?, ?, ?, ?)",
            (pid, pname, cat, brand),
        )


def _sum_store_stock(conn: sqlite3.Connection, product_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(SUM(MAX(0, stock)), 0) AS s FROM store_stock WHERE product_id = ?",
        (product_id,),
    ).fetchone()
    return int(row["s"] if row and row["s"] is not None else 0)


def _ensure_store_stock_row(conn: sqlite3.Connection, store_id: str, product_id: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO store_stock (store_id, product_id, stock) VALUES (?, ?, 0)",
        (store_id, product_id),
    )


def _sync_product_to_central(conn: sqlite3.Connection, product_id: str) -> None:
    inv = conn.execute(
        "SELECT total_stock FROM inventory WHERE product_id = ?",
        (product_id,),
    ).fetchone()
    if not inv:
        return

    total_stock = int(inv["total_stock"])
    sum_store = _sum_store_stock(conn, product_id)
    delta = total_stock - sum_store

    # Record mismatch any time we see divergence (helps trust score).
    if delta != 0:
        try:
            conn.execute(
                """
                INSERT INTO inventory_mismatches (product_id, total_stock, sum_store_stock, delta)
                VALUES (?, ?, ?, ?)
                """,
                (product_id, total_stock, sum_store, delta),
            )
        except Exception:
            pass

    # If store sum is higher than total_stock, bring total_stock up (physical stock exists in stores).
    # This avoids a negative CENTRAL bucket and prevents "phantom underflow" on store sales.
    if delta < 0:
        conn.execute(
            "UPDATE inventory SET total_stock = ? WHERE product_id = ?",
            (sum_store, product_id),
        )
        total_stock = sum_store
        delta = 0

    _ensure_store_stock_row(conn, CENTRAL_STORE_ID, product_id)
    central = conn.execute(
        "SELECT stock FROM store_stock WHERE store_id = ? AND product_id = ?",
        (CENTRAL_STORE_ID, product_id),
    ).fetchone()
    central_before = int(central["stock"]) if central else 0
    central_after = max(0, central_before + delta)
    conn.execute(
        "UPDATE store_stock SET stock = ? WHERE store_id = ? AND product_id = ?",
        (central_after, CENTRAL_STORE_ID, product_id),
    )


def _sync_all_products_to_central(conn: sqlite3.Connection) -> None:
    pids = conn.execute("SELECT product_id FROM inventory").fetchall()
    for r in pids:
        _sync_product_to_central(conn, r["product_id"])


def _apply_stock_delta(
    conn: sqlite3.Connection,
    product_id: str,
    total_delta: int,
    store_id: str = CENTRAL_STORE_ID,
    store_delta: Optional[int] = None,
    *,
    forbid_below_reserved: bool = True,
) -> dict:
    """
    Single atomic stock mutation primitive.

    Guarantees:
    - Updates inventory.total_stock by total_delta
    - Updates store_stock.stock for store_id by store_delta (defaults to total_delta)
    - Prevents negative totals and negative store stock
    - Prevents total_stock dropping below inventory.reserved_stock (unless forbid_below_reserved=False)
    """
    if store_delta is None:
        store_delta = total_delta

    inv = conn.execute(
        "SELECT total_stock, reserved_stock FROM inventory WHERE product_id = ?",
        (product_id,),
    ).fetchone()
    if not inv:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found.")

    total_before = int(inv["total_stock"])
    reserved = int(inv["reserved_stock"])
    total_after = total_before + int(total_delta)
    if total_after < 0:
        raise HTTPException(status_code=409, detail="Cannot set total_stock below 0.")
    if forbid_below_reserved and total_after < reserved:
        raise HTTPException(status_code=409, detail=f"Cannot reduce below reserved_stock ({reserved}).")

    _ensure_store_stock_row(conn, store_id, product_id)
    ss = conn.execute(
        "SELECT stock FROM store_stock WHERE store_id=? AND product_id=?",
        (store_id, product_id),
    ).fetchone()
    store_before = int(ss["stock"]) if ss else 0
    store_after = store_before + int(store_delta)
    if store_after < 0:
        raise HTTPException(status_code=409, detail="Not enough store stock for this operation.")

    conn.execute(
        "UPDATE inventory SET total_stock = ? WHERE product_id = ?",
        (total_after, product_id),
    )
    conn.execute(
        "UPDATE store_stock SET stock = ? WHERE store_id=? AND product_id=?",
        (store_after, store_id, product_id),
    )

    # Keep invariant healthy for this product (assign drift into CENTRAL).
    _sync_product_to_central(conn, product_id)

    return {
        "product_id": product_id,
        "total_stock_before": total_before,
        "total_stock_after": total_after,
        "reserved_stock": reserved,
        "store_id": store_id,
        "store_stock_before": store_before,
        "store_stock_after": store_after,
        "available_stock_after": total_after - reserved,
    }


def _sse_format(data: dict, event: str = "message", event_id: Optional[int] = None) -> str:
    lines = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    if event:
        lines.append(f"event: {event}")
    payload = json.dumps(data, ensure_ascii=False)
    for line in payload.splitlines():
        lines.append(f"data: {line}")
    return "\n".join(lines) + "\n\n"


def _event_stream(last_id: int = 0, admin_only: bool = False):
    """
    Server-Sent Events stream of activity_log changes.
    Clients can pass ?last_id=123 to resume.
    """
    last = max(0, int(last_id or 0))
    while True:
        try:
            conn = get_conn()
            rows = conn.execute(
                """
                SELECT id, ts, event_type, message, product_id, store_id, quantity
                FROM activity_log
                WHERE id > ?
                ORDER BY id ASC
                LIMIT 50
                """,
                (last,),
            ).fetchall()
            conn.close()

            if rows:
                for r in rows:
                    last = int(r["id"])
                    yield _sse_format(dict(r), event="activity", event_id=last)
            else:
                # Keep connection alive (also helps proxies)
                yield "event: ping\ndata: {}\n\n"
        except Exception:
            # If DB temporarily unavailable, don't crash the stream.
            yield "event: ping\ndata: {}\n\n"

        time.sleep(1.0)


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
    # Step 1: Seed database from Excel dataset if empty (fixes ephemeral FS on Render).
    startup.ensure_database_ready()
    # Step 2: Create operational tables and sync stock.
    init_db()
    # Step 3: Start the background expiry loop.
    task = asyncio.create_task(expiry_loop())
    print(f"[lifespan] Expiry loop started — checking every {EXPIRY_CHECK_INTERVAL_SECONDS}s.")
    yield
    # Graceful shutdown: cancel the loop when the server stops.
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
app.mount("/images", StaticFiles(directory="static/images"), name="images")


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


@app.get("/offline")
def offline_page():
    base = Path(__file__).resolve().parent
    return FileResponse(base / "static" / "offline.html", headers={"Cache-Control": "no-store"})


@app.get("/events")
def events(last_id: int = 0):
    """
    Public event stream used by customer UI for instant refresh.
    """
    return StreamingResponse(_event_stream(last_id=last_id, admin_only=False), media_type="text/event-stream")


@app.get("/admin/events")
def admin_events(last_id: int = 0, _: None = Depends(require_admin)):
    """
    Admin event stream (same payload; separate path for auth + future filtering).
    """
    return StreamingResponse(_event_stream(last_id=last_id, admin_only=True), media_type="text/event-stream")


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
            COALESCE(pc.product_name, p.category, i.product_id) AS product_name,
            COALESCE(pc.category, p.category)                  AS category,
            pc.brand                                           AS brand,
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
        LEFT JOIN product_catalog AS pc ON i.product_id = pc.product_id
        LEFT JOIN (
            SELECT product_id, SUM(quantity) AS reserved_stock
            FROM reservations
            WHERE status = 'active' AND expires_at > datetime('now')
            GROUP BY product_id
        ) AS r ON i.product_id = r.product_id
        ORDER BY i.product_id
    """).fetchall()
    # Demand prediction + trust score are computed in Python to avoid schema coupling.
    demand = demand_prediction.get_predicted_demand(DB_FILE)
    out = []
    for r in rows:
        d = dict(r)
        pid = d.get("product_id")
        d["demand_per_day"] = float(demand.get(pid, 0.0) or 0.0)
        try:
            t = ts_logic.compute_trust_score(pid, DB_FILE)
            d["trust_score"] = t.get("trust_score")
            d["trust_label"] = t.get("label")
        except Exception:
            d["trust_score"] = None
            d["trust_label"] = "No Data"
        out.append(d)
    conn.close()
    return out


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
            COALESCE(pc.product_name, p.category, i.product_id) AS product_name,
            COALESCE(pc.category, p.category)                  AS category,
            pc.brand                                           AS brand,
            i.total_stock,
            COALESCE(r.reserved_stock, 0)                 AS reserved_stock,
            i.total_stock - COALESCE(r.reserved_stock, 0) AS available_stock
        FROM inventory AS i
        LEFT JOIN Products AS p ON i.product_id = p.product_id
        LEFT JOIN product_catalog AS pc ON i.product_id = pc.product_id
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
    predictions = demand_prediction.get_predicted_demand(DB_FILE)
    results = []
    for row in rows:
        d = dict(row)
        pid = d["product_id"]
        
        # Trust Score
        ts = ts_logic.compute_trust_score(pid, DB_FILE)
        d["trust_score"] = ts["trust_score"]
        d["trust_label"] = ts["label"]
        
        # Demand Prediction
        d["demand_per_day"] = predictions.get(pid, 0.0)
        
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
    category: Optional[str] = None
    brand: Optional[str] = None


@app.post("/add_product")
def add_product(req: AddProductRequest, _: None = Depends(require_admin)):
    name = (req.product_name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="product_name is required.")

    category = (req.category or "").strip() or "Apparel"
    brand = (req.brand or "").strip() or (name.split(" ", 1)[0] if " " in name else "Generic")

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
        conn.execute(
            """
            INSERT OR REPLACE INTO product_catalog (product_id, product_name, category, brand)
            VALUES (?, ?, ?, ?)
            """,
            (product_id, name, category, brand),
        )
        log_activity(conn, "product", "Product added.", product_id=product_id)
        conn.execute("COMMIT")
        return {"success": True, "product_id": product_id, "product_name": name, "category": category, "brand": brand}

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
        updated = _apply_stock_delta(conn, req.product_id, req.quantity, CENTRAL_STORE_ID)

        log_activity(
            conn,
            "stock_update",
            "Central inventory stock updated (synced to store_stock).",
            product_id=req.product_id,
            quantity=req.quantity,
        )
        conn.execute("COMMIT")
        return {"success": True, **updated}

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
                COALESCE(pc.product_name, p.category, ss.product_id) AS product_name,
                COALESCE(s.city, ss.store_id)          AS store_name,
                SUM(MAX(0, ss.stock))                  AS stock,
                ss.product_id                          AS product_id
            FROM store_stock AS ss
            LEFT JOIN Stores AS s ON s.store_id = ss.store_id
            LEFT JOIN Products AS p ON p.product_id = ss.product_id
            LEFT JOIN product_catalog AS pc ON pc.product_id = ss.product_id
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
        updated = _apply_stock_delta(conn, req.product_id, +req.quantity, CENTRAL_STORE_ID)
        log_activity(
            conn,
            "stock_update",
            "Central inventory stock increased (synced to store_stock).",
            product_id=req.product_id,
            quantity=req.quantity,
        )
        conn.execute("COMMIT")
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
        updated = _apply_stock_delta(conn, req.product_id, -req.quantity, CENTRAL_STORE_ID)
        log_activity(
            conn,
            "stock_update",
            "Central inventory stock reduced (synced to store_stock).",
            product_id=req.product_id,
            quantity=-req.quantity,
        )
        conn.execute("COMMIT")
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


@app.get("/admin/analytics")
def admin_analytics(days: int = 14, _: None = Depends(require_admin)):
    """
    Business analytics + KPI metrics for HQ dashboard.
    Uses available DB data; simulates cost/profit margin when not present.
    """
    days = max(7, min(int(days or 14), 90))
    conn = get_conn()
    try:
        # KPI: total products
        total_products = conn.execute("SELECT COUNT(1) AS n FROM inventory").fetchone()["n"]

        # KPI: low stock items (available between 1..9)
        low_stock = conn.execute(
            """
            SELECT COUNT(1) AS n
            FROM (
              SELECT
                i.product_id,
                i.total_stock - COALESCE(r.reserved_stock,0) AS available
              FROM inventory i
              LEFT JOIN (
                SELECT product_id, SUM(quantity) AS reserved_stock
                FROM reservations
                WHERE status='active' AND expires_at > datetime('now')
                GROUP BY product_id
              ) r ON r.product_id=i.product_id
            )
            WHERE available > 0 AND available < 10
            """
        ).fetchone()["n"]

        # KPI: stock value (uses dataset price_usd)
        stock_value = conn.execute(
            """
            SELECT COALESCE(SUM(i.total_stock * COALESCE(p.price_usd,0)), 0) AS v
            FROM inventory i
            LEFT JOIN Products p ON p.product_id=i.product_id
            """
        ).fetchone()["v"]

        # Sales metrics from activity_log (operational events)
        sales = conn.execute(
            """
            SELECT
              COALESCE(SUM(COALESCE(a.quantity,0) * COALESCE(p.price_usd,0)), 0) AS revenue,
              COALESCE(SUM(COALESCE(a.quantity,0)), 0) AS units
            FROM activity_log a
            LEFT JOIN Products p ON p.product_id=a.product_id
            WHERE a.event_type IN ('purchase','cashier_purchase','qr_scan_purchase')
            """
        ).fetchone()
        revenue = float(sales["revenue"] or 0)
        units_sold = int(sales["units"] or 0)

        # Simulated profit: assume 60% COGS if unknown.
        profit = conn.execute(
            """
            SELECT
              COALESCE(SUM(COALESCE(a.quantity,0) * COALESCE(p.price_usd,0) * 0.40), 0) AS profit
            FROM activity_log a
            LEFT JOIN Products p ON p.product_id=a.product_id
            WHERE a.event_type IN ('purchase','cashier_purchase','qr_scan_purchase')
            """
        ).fetchone()["profit"]
        profit = float(profit or 0)

        # Sales over time
        sales_over_time = conn.execute(
            """
            SELECT date(ts) AS d, COALESCE(SUM(COALESCE(a.quantity,0) * COALESCE(p.price_usd,0)), 0) AS revenue
            FROM activity_log a
            LEFT JOIN Products p ON p.product_id=a.product_id
            WHERE a.event_type IN ('purchase','cashier_purchase','qr_scan_purchase')
              AND ts >= datetime('now', ?)
            GROUP BY date(ts)
            ORDER BY d ASC
            """,
            (f"-{days} days",),
        ).fetchall()

        # Top selling products (by units)
        top = conn.execute(
            """
            SELECT
              a.product_id,
              COALESCE(pc.product_name, p.category, a.product_id) AS product_name,
              COALESCE(SUM(COALESCE(a.quantity,0)),0) AS units
            FROM activity_log a
            LEFT JOIN product_catalog pc ON pc.product_id=a.product_id
            LEFT JOIN Products p ON p.product_id=a.product_id
            WHERE a.event_type IN ('purchase','cashier_purchase','qr_scan_purchase')
            GROUP BY a.product_id, product_name
            ORDER BY units DESC
            LIMIT 5
            """
        ).fetchall()

        # Stock distribution by category (total stock)
        stock_by_cat = conn.execute(
            """
            SELECT COALESCE(pc.category, 'Uncategorized') AS category,
                   COALESCE(SUM(i.total_stock),0) AS stock
            FROM inventory i
            LEFT JOIN product_catalog pc ON pc.product_id=i.product_id
            GROUP BY category
            ORDER BY stock DESC
            """
        ).fetchall()

        # Low vs healthy pie
        health = conn.execute(
            """
            SELECT
              SUM(CASE WHEN available <= 0 THEN 1 ELSE 0 END) AS out_n,
              SUM(CASE WHEN available > 0 AND available < 10 THEN 1 ELSE 0 END) AS low_n,
              SUM(CASE WHEN available >= 10 THEN 1 ELSE 0 END) AS healthy_n
            FROM (
              SELECT
                i.product_id,
                i.total_stock - COALESCE(r.reserved_stock,0) AS available
              FROM inventory i
              LEFT JOIN (
                SELECT product_id, SUM(quantity) AS reserved_stock
                FROM reservations
                WHERE status='active' AND expires_at > datetime('now')
                GROUP BY product_id
              ) r ON r.product_id=i.product_id
            )
            """
        ).fetchone()

        # Demand trend: simulate over time using current demand_per_day per category.
        demand_now = demand_prediction.get_predicted_demand(DB_FILE)
        # Map product->category then aggregate current demand by category.
        cat_rows = conn.execute(
            """
            SELECT i.product_id, COALESCE(pc.category, 'Uncategorized') AS category
            FROM inventory i
            LEFT JOIN product_catalog pc ON pc.product_id=i.product_id
            """
        ).fetchall()
        by_cat = {}
        for r in cat_rows:
            pid = r["product_id"]
            cat = r["category"]
            by_cat[cat] = by_cat.get(cat, 0.0) + float(demand_now.get(pid, 0.0) or 0.0)

        # Pick top 4 categories by demand for chart readability.
        top_cats = sorted(by_cat.items(), key=lambda x: x[1], reverse=True)[:4]
        cats = [c for c, _ in top_cats] or ["All"]

        # Build deterministic series (no randomness) for last N days.
        # Use a simple cosine modulation so it looks like a trend.
        import math
        labels = []
        series = {c: [] for c in cats}
        for i in range(days - 1, -1, -1):
            d = conn.execute("SELECT date(datetime('now', ?)) AS d", (f"-{i} days",)).fetchone()["d"]
            labels.append(d)
            for idx, c in enumerate(cats):
                base = float(by_cat.get(c, 0.0) if c != "All" else sum(by_cat.values()))
                mod = 1.0 + 0.10 * math.cos((i + idx) * 0.6)
                series[c].append(round(base * mod, 2))

        return {
            "kpis": {
                "total_products": int(total_products),
                "stock_value_usd": round(float(stock_value or 0.0), 2),
                "total_sales_usd": round(revenue, 2),
                "total_profit_usd": round(profit, 2),
                "low_stock_items": int(low_stock),
                "units_sold": units_sold,
            },
            "sales_over_time": {
                "labels": [r["d"] for r in sales_over_time],
                "revenue": [float(r["revenue"] or 0.0) for r in sales_over_time],
            },
            "demand_trend": {
                "labels": labels,
                "series": series,
            },
            "top_selling": {
                "labels": [r["product_name"] for r in top],
                "units": [int(r["units"] or 0) for r in top],
            },
            "stock_by_category": {
                "labels": [r["category"] for r in stock_by_cat],
                "stock": [int(r["stock"] or 0) for r in stock_by_cat],
            },
            "stock_health": {
                "out": int(health["out_n"] or 0),
                "low": int(health["low_n"] or 0),
                "healthy": int(health["healthy_n"] or 0),
            },
        }
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


class ScanPurchaseRequest(BaseModel):
    product_id: str
    store_id: str
    quantity: int


@app.post("/scan_purchase")
def scan_purchase(req: ScanPurchaseRequest, _: None = Depends(require_admin)):
    """
    Offline QR scan purchase:
    - product_id comes from QR
    - store_id comes from cashier selection (or device assignment)
    - quantity is entered by cashier

    Guarantees:
    - total_stock decreases
    - store_stock for that store decreases
    - never sells below reserved_stock
    - never allows negative store stock
    """
    if (req.product_id or "").strip() == "":
        raise HTTPException(status_code=400, detail="product_id is required.")
    if (req.store_id or "").strip() == "":
        raise HTTPException(status_code=400, detail="store_id is required.")
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="quantity must be > 0.")

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        # Ensure store row exists; treat missing as 0 stock.
        conn.execute(
            "INSERT OR IGNORE INTO store_stock (store_id, product_id, stock) VALUES (?, ?, 0)",
            (req.store_id, req.product_id),
        )
        ss = conn.execute(
            "SELECT stock FROM store_stock WHERE store_id=? AND product_id=?",
            (req.store_id, req.product_id),
        ).fetchone()
        store_stock = int(ss["stock"]) if ss else 0
        if store_stock < req.quantity:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail="Not enough stock in selected store.")

        # Apply atomic deltas
        updated = _apply_stock_delta(
            conn,
            req.product_id,
            total_delta=-req.quantity,
            store_id=req.store_id,
            store_delta=-req.quantity,
            forbid_below_reserved=True,
        )

        log_activity(
            conn,
            "qr_scan_purchase",
            "QR scan purchase completed.",
            product_id=req.product_id,
            store_id=req.store_id,
            quantity=req.quantity,
        )

        conn.execute("COMMIT")
        return {"success": True, **updated}

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
    Simulates a physical in-store sale.
    NOTE: This is legacy/simplified. Prefer /scan_purchase or /admin/cashier_purchase
    because they update both total_stock and store_stock.
    """
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be > 0.")

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        # Assume it came from CENTRAL when store is not specified.
        updated = _apply_stock_delta(conn, req.product_id, -req.quantity, CENTRAL_STORE_ID)
        conn.execute("COMMIT")
        return {
            "success": True,
            **updated,
            "quantity": req.quantity,
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

