import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_diagnoses_icd():
    
    # Remove duplicates from the 'diagnoses_icd' table
    remove_duplicates_from_table("diagnoses_icd")
        
    try:
        # Get the path of the current script (where the Python script is located)
        script_dir = os.path.dirname(__file__)

        # Construct the relative path to the mimic.db file
        db_path = os.path.join(script_dir, '..', 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

        # Connect to the database using the relative path
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Drop the old table if it exists
        drop_table_query = "DROP TABLE IF EXISTS diagnoses_icd_clean;"
        cursor.execute(drop_table_query)
        print(" 'diagnoses_icd_clean' table dropped if existed. ")
    
        # Create a new table 'diagnoses_icd_clean'
        create_table_query = """
        CREATE TABLE diagnoses_icd_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            seq_num INTEGER,
            icd_code TEXT,
            icd_version INTEGER,
            PRIMARY KEY (hadm_id, seq_num, icd_code)
        );
        """
        cursor.execute(create_table_query)
        print("'diagnoses_icd_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO diagnoses_icd_clean (subject_id,
            hadm_id,
            seq_num,
            icd_code,
            icd_version)
        SELECT subject_id,
            hadm_id,
            seq_num,
            icd_code,
            icd_version
        FROM diagnoses_icd;
        """
        cursor.execute(copy_data_query)
        print("'diagnoses_icd' data is successfully copied to 'diagnoses_icd_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "diagnoses_icd")
        
        # Rename the new table to the original name
        rename_table(cursor, "diagnoses_icd_clean", "diagnoses_icd")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "diagnoses_icd")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")