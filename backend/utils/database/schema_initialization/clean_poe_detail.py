import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_poe_detail():
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
        drop_table_query = "DROP TABLE IF EXISTS poe_detail_clean;"
        cursor.execute(drop_table_query)
        print(" 'poe_detail_clean' table dropped if existed. ")
        
        # create new table
        create_table_query = """
        CREATE TABLE poe_detail_clean (
            poe_id,
            poe_seq,
            subject_id,
            field_name,
            field_value,
            PRIMARY KEY (poe_id, poe_seq, subject_id, field_name)
        );
        """
        cursor.execute(create_table_query)
        print(" 'poe_detail_clean' is successfully created. ")
        
        # copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO poe_detail_clean (poe_id,
            poe_seq,
            subject_id,
            field_name,
            field_value)
        SELECT poe_id,
            poe_seq,
            subject_id,
            field_name,
            field_value
        FROM poe_detail;
        """
        cursor.execute(copy_data_query)
        print(" 'poe_detail_clean' is successfully copied. ")

        # drop the old table
        cursor.execute("DROP TABLE IF EXISTS poe_detail;")
        print(" 'poe_detail' is successfully dropped. ")
        
        # look into table info using PRAGMA table_info
        table_name = 'poe_detail_clean'
        query = f"PRAGMA table_info({table_name});"
        table_info = pd.read_sql(query, conn)

        print(f" '{table_name}' : ")
        print(table_info)
        
        # rename the new table to the old table name
        rename_table_query = "ALTER TABLE poe_detail_clean RENAME TO poe_detail;"
        cursor.execute(rename_table_query)
        print(" rename 'poe_detail_clean' to 'poe_detail'. ")
        
        # commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # close the connection
        if conn:
            conn.close()
            print("Database connection closed.")