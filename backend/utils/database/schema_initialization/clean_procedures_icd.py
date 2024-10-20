import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_procedures_icd():
        
    # Remove duplicates from the 'procedures_icd' table
    remove_duplicates_from_table("procedures_icd")
    
    remove_duplicates_from_table_with_key("procedures_icd", ['subject_id', 'hadm_id', 'seq_num'])
    
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
        drop_table_query = "DROP TABLE IF EXISTS procedures_icd_clean;"
        cursor.execute(drop_table_query)
        print(" 'procedures_icd_clean' table dropped if existed. ")

        # Create a new table 'procedures_icd_clean'
        create_table_query = """
        CREATE TABLE procedures_icd_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            seq_num INTEGER,
            chartdate DATE,
            icd_code TEXT,
            icd_version INTEGER,
            PRIMARY KEY (hadm_id, seq_num)
        );
        """

        cursor.execute(create_table_query)
        print("'procedures_icd_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO procedures_icd_clean (subject_id,
            hadm_id,
            seq_num,
            chartdate,
            icd_code,
            icd_version)
        SELECT subject_id,
            hadm_id,
            seq_num,
            chartdate,
            icd_code,
            icd_version
        FROM procedures_icd;
        """
        cursor.execute(copy_data_query)
        print("'procedures_icd' data is successfully copied to 'procedures_icd_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "procedures_icd")
        
        # Rename the new table to the original name
        rename_table(cursor, "procedures_icd_clean", "procedures_icd")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "procedures_icd")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")