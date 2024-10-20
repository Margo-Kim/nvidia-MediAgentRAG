import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_pharmacy():
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
        drop_table_query = "DROP TABLE IF EXISTS pharmacy_clean;"
        cursor.execute(drop_table_query)
        print(" 'pharmacy_clean' table dropped if existed. ")
        
        # create new table
        create_table_query = """
        CREATE TABLE pharmacy_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            pharmacy_id INTEGER PRIMARY KEY,
            poe_id TEXT,
            starttime DATE,
            stoptime DATE,
            medication TEXT,
            proc_type TEXT,
            status TEXT,
            entertime DATE,
            verifiedtime DATE,
            route TEXT,
            frequency TEXT,
            disp_sched TEXT,
            infusion_type TEXT,
            sliding_scale TEXT,
            lockout_interval TEXT,
            basal_rate REAL,
            one_hr_max TEXT,
            doses_per_24_hrs REAL,
            duration TEXT,
            duration_interval TEXT,
            expiration_value INTEGER,
            expiration_unit TEXT,
            expirationdate DATE,
            dispensation TEXT,
            fill_quantity TEXT
        );
        """
        cursor.execute(create_table_query)
        print(" 'pharmacy_clean' is successfully created. ")
        
        # copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO pharmacy_clean (
            subject_id,
            hadm_id,
            pharmacy_id,
            poe_id,
            starttime,
            stoptime,
            medication,
            proc_type,
            status,
            entertime,
            verifiedtime,
            route,
            frequency,
            disp_sched,
            infusion_type,
            sliding_scale,
            lockout_interval,
            basal_rate,
            one_hr_max,
            doses_per_24_hrs,
            duration,
            duration_interval,
            expiration_value,
            expiration_unit,
            expirationdate,
            dispensation,
            fill_quantity)
        SELECT subject_id,
            hadm_id,
            pharmacy_id,
            poe_id,
            starttime,
            stoptime,
            medication,
            proc_type,
            status,
            entertime,
            verifiedtime,
            route,
            frequency,
            disp_sched,
            infusion_type,
            sliding_scale,
            lockout_interval,
            basal_rate,
            one_hr_max,
            doses_per_24_hrs,
            duration,
            duration_interval,
            expiration_value,
            expiration_unit,
            expirationdate,
            dispensation,
            fill_quantity
        FROM pharmacy;
        """
        cursor.execute(copy_data_query)
        print(" 'pharmacy' is successfully copied. ")

        # Drop the old table using the reusable function
        drop_table(cursor, "pharmacy")
        
        # Rename the new table to the original name
        rename_table(cursor, "pharmacy_clean", "pharmacy")
        
        # Use the new function to check the table schema
        check_table_schema(conn, "pharmacy")
        
        # commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # close the connection
        if conn:
            conn.close()
            print("Database connection closed.")