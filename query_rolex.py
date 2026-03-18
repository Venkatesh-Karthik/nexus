import sqlite3
import pandas as pd

def query_rolex():
    DB_FILE = "inventory.db"
    try:
        conn = sqlite3.connect(DB_FILE)
        query = """
        SELECT 
            pc.product_id, 
            pc.product_name, 
            s.city AS store_location, 
            ss.stock 
        FROM product_catalog pc
        JOIN store_stock ss ON pc.product_id = ss.product_id
        JOIN Stores s ON ss.store_id = s.store_id
        WHERE pc.product_name LIKE '%Rolex Submariner%'
          AND ss.stock > 0;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            print("No Rolex Submariner found in stock in any store.")
        else:
            print(df.to_string(index=False))
    except Exception as e:
        print(f"Error querying database: {e}")

if __name__ == "__main__":
    query_rolex()
