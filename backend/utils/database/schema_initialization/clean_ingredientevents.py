import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_ingredientevents():
        
    # Remove duplicates from the 'ingredientevents' table
    remove_duplicates_from_table("ingredientevents")

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
        drop_table_query = "DROP TABLE IF EXISTS ingredientevents_clean;"
        cursor.execute(drop_table_query)
        print(" 'ingredientevents_clean' table dropped if existed. ")

        # Create a new table 'ingredientevents_clean'
        create_table_query = """
        CREATE TABLE ingredientevents_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            stay_id INTEGER,
            caregiver_id INTEGER,
            starttime DATE,
            endtime DATE,
            storetime DATE,
            itemid INTEGER,
            amount REAL,
            amountuom TEXT,
            rate REAL,
            rateuom TEXT,
            orderid INTEGER,
            linkorderid INTEGER,
            statusdescription TEXT,
            originalamount REAL,
            originalrate REAL,
            PRIMARY KEY (orderid,itemid)
        );
        """
        
        cursor.execute(create_table_query)
        print("'ingredientevents_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO ingredientevents_clean (subject_id,
            hadm_id,
            stay_id,
            caregiver_id,
            starttime,
            endtime,
            storetime,
            itemid,
            amount,
            amountuom,
            rate,
            rateuom,
            orderid,
            linkorderid,
            statusdescription,
            originalamount,
            originalrate)
        SELECT subject_id,
            hadm_id,
            stay_id,
            caregiver_id,
            starttime,
            endtime,
            storetime,
            itemid,
            amount,
            amountuom,
            rate,
            rateuom,
            orderid,
            linkorderid,
            statusdescription,
            originalamount,
            originalrate
        FROM ingredientevents;
        """
        cursor.execute(copy_data_query)
        print("'ingredientevents' data is successfully copied to 'ingredientevents_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "ingredientevents")
        
        # Rename the new table to the original name
        rename_table(cursor, "ingredientevents_clean", "ingredientevents")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "ingredientevents")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")