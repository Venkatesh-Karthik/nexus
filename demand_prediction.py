import sqlite3
import pandas as pd

def get_predicted_demand(db_path="inventory.db"):
    """
    Calculates simple demand prediction per product based on Online_Orders.
    Calculates (Total Quantity / Total Unique Days) for each product.
    """
    conn = sqlite3.connect(db_path)
    try:
        # Load orders into DataFrame
        query = "SELECT order_time, product_id, quantity FROM Online_Orders"
        df = pd.read_sql_query(query, conn)
        
        # Convert order_time to date (strip time)
        df['order_date'] = pd.to_datetime(df['order_time']).dt.date
        
        # Calculate total days in the dataset
        total_days = df['order_date'].nunique()
        if total_days == 0:
            return {}

        # Aggregate total quantity per product
        product_totals = df.groupby('product_id')['quantity'].sum()
        
        # Calculate daily average demand
        # We round to 2 decimal places for better display
        predictions = (product_totals / total_days).round(2).to_dict()
        
        return predictions
    except Exception as e:
        print(f"Error calculating demand: {e}")
        return {}
    finally:
        conn.close()

if __name__ == "__main__":
    preds = get_predicted_demand()
    print("Sample Demand Predictions:")
    for pid, val in list(preds.items())[:5]:
        print(f"{pid}: {val} units/day")
