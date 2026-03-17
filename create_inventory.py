"""
create_inventory.py

Reads Store_Inventory_Records from inventory.db, aggregates stock per product
across all stores, and creates an `inventory` table:

    inventory(product_id, total_stock, reserved_stock, available_stock)

- total_stock    = SUM of stock_level across all stores for that product
- reserved_stock = 0 initially (will be updated live by reservation system)
- available_stock = total_stock - reserved_stock  (computed column / view)

This table becomes the single source of truth for the API.
"""

import sqlite3
import pandas as pd

DB_FILE = "inventory.db"


def build_inventory_table(db_path: str = DB_FILE) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)

    # 1. Aggregate stock per product across all stores
    query = """
        SELECT
            product_id,
            SUM(recorded_stock_level) AS total_stock,
            0               AS reserved_stock
        FROM Store_Inventory_Records
        GROUP BY product_id
        ORDER BY product_id
    """
    df = pd.read_sql_query(query, conn)

    # 2. Compute available_stock column
    df["available_stock"] = df["total_stock"] - df["reserved_stock"]

    # 3. Write to the `inventory` table (replace on every run so it stays fresh)
    df.to_sql("inventory", conn, if_exists="replace", index=False)
    print(f"[inventory] Table created/updated with {len(df)} products.")

    # 4. Verify: show sample
    sample = pd.read_sql_query(
        "SELECT * FROM inventory ORDER BY total_stock DESC LIMIT 10", conn
    )
    print("\nTop 10 products by total stock:")
    print(sample.to_string(index=False))

    conn.close()
    return df


def get_inventory_for_api(db_path: str = DB_FILE) -> list[dict]:
    """
    Returns the inventory table as a list of dicts — ready for API response.
    Also dynamically computes reserved_stock from the live reservations table.
    """
    conn = sqlite3.connect(db_path)

    # Join inventory with live reservation sums so the API always sees
    # the real available stock even if reservations were made.
    query = """
        SELECT
            i.product_id,
            i.total_stock,
            COALESCE(r.reserved_stock, 0)                       AS reserved_stock,
            i.total_stock - COALESCE(r.reserved_stock, 0)       AS available_stock,
            CASE
                WHEN (i.total_stock - COALESCE(r.reserved_stock, 0)) <= 0  THEN 'red'
                WHEN (i.total_stock - COALESCE(r.reserved_stock, 0)) <= 10 THEN 'yellow'
                ELSE 'green'
            END AS stock_status
        FROM inventory AS i
        LEFT JOIN (
            SELECT product_id, SUM(quantity) AS reserved_stock
            FROM reservations
            WHERE status = 'active'
              AND expires_at > datetime('now')
            GROUP BY product_id
        ) AS r ON i.product_id = r.product_id
        ORDER BY i.product_id
    """
    try:
        df = pd.read_sql_query(query, conn)
    except Exception:
        # reservations table may not exist yet before full app init
        df = pd.read_sql_query(
            """
            SELECT product_id, total_stock,
                   0 AS reserved_stock,
                   total_stock AS available_stock,
                   CASE WHEN total_stock = 0 THEN 'red'
                        WHEN total_stock <= 10 THEN 'yellow'
                        ELSE 'green' END AS stock_status
            FROM inventory ORDER BY product_id
            """,
            conn
        )
    conn.close()
    return df.to_dict(orient="records")


if __name__ == "__main__":
    build_inventory_table()

    print("\n--- Structured API Output (first 5 rows) ---")
    records = get_inventory_for_api()
    for r in records[:5]:
        print(r)
