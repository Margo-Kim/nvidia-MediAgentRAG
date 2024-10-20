import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_poe():
    try:
        # db connection
        # Get the path of the current script (where the Python script is located)
        script_dir = os.path.dirname(__file__)

        # Construct the relative path to the mimic.db file
        db_path = os.path.join(script_dir, '..', 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

        # Connect to the database using the relative path
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Drop the old table if it exists
        drop_table_query = "DROP TABLE IF EXISTS poe_clean;"
        cursor.execute(drop_table_query)
        print(" 'poe_clean' table dropped if existed. ")
        
        # create new table
        create_table_query = """
        CREATE TABLE poe_clean (
            poe_id TEXT PRIMARY KEY,
            poe_seq INTEGER,
            subject_id INTEGER,
            hadm_id INTEGER,
            ordertime DATE,
            order_type TEXT,
            order_subtype TEXT,
            transaction_type TEXT,
            discontinue_of_poe_id TEXT,
            discontinued_by_poe_id TEXT,
            order_provider_id TEXT,
            order_status TEXT
        );
        """
        cursor.execute(create_table_query)
        print(" 'poe_clean' is successfully created. ")
        
        # copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO poe_clean (poe_id,
            poe_seq,
            subject_id,
            hadm_id,
            ordertime,
            order_type,
            order_subtype,
            transaction_type,
            discontinue_of_poe_id,
            discontinued_by_poe_id,
            order_provider_id,
            order_status)
        SELECT poe_id,
            poe_seq,
            subject_id,
            hadm_id,
            ordertime,
            order_type,
            order_subtype,
            transaction_type,
            discontinue_of_poe_id,
            discontinued_by_poe_id,
            order_provider_id,
            order_status
        FROM poe;
        """
        cursor.execute(copy_data_query)
        print(" 'poe' is successfully copied. ")

        # Drop the old table using the reusable function
        drop_table(cursor, "poe")
        
        # Rename the new table to the original name
        rename_table(cursor, "poe_clean", "poe")
        
        # Use the new function to check the table schema
        check_table_schema(conn, "poe")
        
        # commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # close the connection
        if conn:
            conn.close()
            print("Database connection closed.")