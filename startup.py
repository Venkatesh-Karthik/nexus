"""
startup.py — Database initialization and seeding for production deployments.

Why this file:
  On cloud hosts (e.g. Render) the filesystem is ephemeral — inventory.db
  disappears between redeploys.  This module detects that situation and
  re-populates the database from the bundled Excel dataset before the app
  handles its first request.

Entry point called from main.py lifespan:
  from startup import ensure_database_ready
  ensure_database_ready()
"""

import os
import sqlite3
from pathlib import Path

# Paths relative to the project root (where main.py lives).
BASE_DIR   = Path(__file__).parent
DB_FILE    = BASE_DIR / "inventory.db"
EXCEL_FILE = BASE_DIR / "retail_dataset.xlsx"


def _db_has_data() -> bool:
    """Return True if the inventory table already exists and has rows."""
    if not DB_FILE.exists():
        return False
    try:
        conn = sqlite3.connect(str(DB_FILE))
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='inventory'"
        ).fetchone()
        if not row:
            conn.close()
            return False
        count = conn.execute("SELECT COUNT(1) FROM inventory").fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False


def _load_excel_to_sqlite() -> None:
    """
    Read all expected sheets from the Excel dataset and write them into
    SQLite using pandas.  Uses if_exists='replace' so repeated calls are
    idempotent (each sheet is fully overwritten, never duplicated).
    """
    try:
        import pandas as pd
    except ImportError:
        print("[startup] WARNING: pandas not installed — skipping Excel seeding.")
        return

    expected_sheets = [
        "Stores",
        "Products",
        "Store_Inventory_Records",
        "Website_Inventory_View",
        "Online_Orders",
        "Inventory_Audits",
        "Store_Transfers",
    ]

    print(f"[startup] Reading dataset from {EXCEL_FILE} …")
    try:
        all_sheets = pd.read_excel(str(EXCEL_FILE), sheet_name=None)
    except Exception as exc:
        print(f"[startup] ERROR reading Excel file: {exc}")
        print("[startup] App will start — some features may not have data.")
        return

    conn = sqlite3.connect(str(DB_FILE))
    try:
        for sheet in expected_sheets:
            if sheet in all_sheets:
                df = all_sheets[sheet]
                df.to_sql(sheet, conn, if_exists="replace", index=False)
                print(f"[startup]   ✓ Loaded '{sheet}' ({len(df)} rows)")
            else:
                print(f"[startup]   ⚠ Sheet '{sheet}' not found — skipped.")

        # Create the inventory table from Website_Inventory_View if it exists
        if "Website_Inventory_View" in all_sheets:
            df_inv = all_sheets["Website_Inventory_View"]
            # Build a clean inventory table: product_id, total_stock, reserved_stock
            if "product_id" in df_inv.columns and "available_stock" in df_inv.columns:
                inv = df_inv[["product_id"]].copy()
                inv["total_stock"]    = df_inv.get("available_stock", 100)
                inv["reserved_stock"] = 0
                inv = inv.drop_duplicates(subset=["product_id"])
                inv.to_sql("inventory", conn, if_exists="replace", index=False)
                print(f"[startup]   ✓ Built 'inventory' table ({len(inv)} products)")

        conn.commit()
        print("[startup] Dataset loaded successfully.")
    except Exception as exc:
        conn.rollback()
        print(f"[startup] ERROR during seeding: {exc}")
    finally:
        conn.close()


def ensure_database_ready() -> None:
    """
    Main entry point.

    - If the DB already has data: skip (fast path, no re-seeding on normal restarts).
    - If the DB is missing or empty: load from Excel, then let main's init_db()
      create the operational tables on top.
    """
    if _db_has_data():
        print("[startup] Database already populated — skipping seed.")
        return

    if not EXCEL_FILE.exists():
        print(
            f"[startup] WARNING: Dataset file not found at {EXCEL_FILE}.\n"
            "          The app will start but inventory data will be empty."
        )
        return

    print("[startup] Initializing database from dataset …")
    _load_excel_to_sqlite()
    print("[startup] Database initialization complete.")
