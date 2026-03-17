import sqlite3


def main() -> None:
    conn = sqlite3.connect("inventory.db")
    cur = conn.cursor()

    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
    views = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name").fetchall()]
    print("tables:", tables)
    print("views:", views)

    for t in ["inventory", "Products", "Stores", "Store_Inventory_Records", "Online_Orders", "Inventory_Audits"]:
        cols = cur.execute(f"PRAGMA table_info('{t}')").fetchall()
        print(f"\n{t} cols:", [f"{c[1]}:{c[2]}" for c in cols])

    for v in ["stores", "store_inventory_records"]:
        cols = cur.execute(f"PRAGMA table_info('{v}')").fetchall()
        print(f"\n{v} cols:", [f"{c[1]}:{c[2]}" for c in cols])

    conn.close()


if __name__ == "__main__":
    main()

