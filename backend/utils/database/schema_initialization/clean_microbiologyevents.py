import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_microbiologyevents():
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
        drop_table_query = "DROP TABLE IF EXISTS microbiologyevents_clean;"
        cursor.execute(drop_table_query)
        print(" 'microbiologyevents_clean' table dropped if existed. ")

        # create new table
        create_table_query = """
        CREATE TABLE microbiologyevents_clean (
            microevent_id INTEGER PRIMARY KEY,
            subject_id INTEGER,
            hadm_id INTEGER,
            micro_specimen_id INTEGER,
            order_provider_id TEXT,
            chartdate DATE,
            charttime DATE,
            spec_itemid INTEGER,
            spec_type_desc TEXT,
            test_seq INTEGER,
            storedate DATE,
            storetime DATE,
            test_itemid INTEGER,
            test_name TEXT,
            org_itemid INTEGER,
            org_name TEXT,
            isolate_num INTEGER,
            quantity TEXT,
            ab_itemid INTEGER,
            ab_name TEXT,
            dilution_text TEXT,
            dilution_comparison TEXT,
            dilution_value REAL,
            interpretation TEXT,
            comments TEXT
        );
        """
        cursor.execute(create_table_query)
        print(" 'microbiologyevents_clean' is successfully created. ")
        
        # copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO microbiologyevents_clean (microevent_id,
            subject_id,
            hadm_id,
            micro_specimen_id,
            order_provider_id,
            chartdate,
            charttime,
            spec_itemid,
            spec_type_desc,
            test_seq,
            storedate,
            storetime,
            test_itemid,
            test_name,
            org_itemid,
            org_name,
            isolate_num,
            quantity,
            ab_itemid,
            ab_name,
            dilution_text,
            dilution_comparison,
            dilution_value,
            interpretation,
            comments)
        SELECT microevent_id,
            subject_id,
            hadm_id,
            micro_specimen_id,
            order_provider_id,
            chartdate,
            charttime,
            spec_itemid,
            spec_type_desc,
            test_seq,
            storedate,
            storetime,
            test_itemid,
            test_name,
            org_itemid,
            org_name,
            isolate_num,
            quantity,
            ab_itemid,
            ab_name,
            dilution_text,
            dilution_comparison,
            dilution_value,
            interpretation,
            comments
        FROM microbiologyevents;
        """
        cursor.execute(copy_data_query)
        print(" 'microbiologyevents' is successfully copied. ")

        # Drop the old table using the reusable function
        drop_table(cursor, "microbiologyevents")
        
        # Rename the new table to the original name
        rename_table(cursor, "microbiologyevents_clean", "microbiologyevents")
        
        # Use the new function to check the table schema
        check_table_schema(conn, "microbiologyevents")
        
        # commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # close the connection
        if conn:
            conn.close()
            print("Database connection closed.")