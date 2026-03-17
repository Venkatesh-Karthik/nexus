import pandas as pd
import sqlite3
import os

def load_excel_to_sqlite(excel_file_path: str, db_file_path: str):
    """
    Loads multiple sheets from an Excel file into a SQLite database.
    Prints sample data from each inserted table.
    """
    if not os.path.exists(excel_file_path):
        print(f"Error: Excel file not found at {excel_file_path}")
        return

    expected_sheets = [
        "Stores",
        "Products",
        "Store_Inventory_Records",
        "Website_Inventory_View",
        "Online_Orders",
        "Inventory_Audits",
        "Store_Transfers"
    ]

    print(f"Connecting to SQLite database at {db_file_path}...")
    # Connect to SQLite database (creates it if it doesn't exist)
    conn = sqlite3.connect(db_file_path)
    
    try:
        # Define Excel file to extract data
        print(f"Reading Excel file at {excel_file_path}...")
        
        # We can read all sheets by passing sheet_name=None, returning a dictionary of DataFrames
        all_sheets = pd.read_excel(excel_file_path, sheet_name=None)
        
        for sheet_name in expected_sheets:
            if sheet_name in all_sheets:
                print(f"\nProcessing sheet: '{sheet_name}'...")
                df = all_sheets[sheet_name]
                
                # Write to SQLite
                # if_exists="replace" will overwrite the table if it already exists
                df.to_sql(sheet_name, conn, if_exists="replace", index=False)
                
                print(f"Successfully loaded '{sheet_name}' into table '{sheet_name}'.")
                
                # Print a small sample from the DataFrame
                print(f"Sample data from '{sheet_name}':")
                print(df.head(3))
                print("-" * 50)
            else:
                print(f"Warning: Expected sheet '{sheet_name}' was not found in the Excel file.")
        
        print("\nAll sheets processed successfully!")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        
    finally:
        # Close the connection
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    # Example usage:
    # Set the path to the Excel dataset and the target SQLite database file.
    # Update 'retail_dataset.xlsx' to the actual name of your file.
    EXCEL_FILE = "retail_dataset.xlsx"  
    DB_FILE = "inventory.db"
    
    load_excel_to_sqlite(EXCEL_FILE, DB_FILE)
