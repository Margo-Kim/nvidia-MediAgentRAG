import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def drop_duplicate_in_emar_detail():
    # Connect to the SQLite database
    conn = sqlite3.connect('mimic.db')
    cursor = conn.cursor()

    # Query to find duplicate combinations of emar_id and parent_field_ordinal
    query_duplicates = """
    SELECT emar_id, parent_field_ordinal, COUNT(*) AS duplicate_count
    FROM emar_detail
    GROUP BY emar_id, parent_field_ordinal
    HAVING COUNT(*) > 1;
    """

    # Execute the query and load the results into a DataFrame
    duplicates_df = pd.read_sql(query_duplicates, conn)

    print("Duplicate combinations of emar_id and parent_field_ordinal:")
    print(duplicates_df)

    # Delete duplicate records, keeping one record per combination
    delete_duplicates_query = """
    DELETE FROM emar_detail
    WHERE ROWID NOT IN (
        SELECT MIN(ROWID)
        FROM emar_detail
        GROUP BY emar_id, parent_field_ordinal
    );
    """

    # Execute the delete operation
    cursor.execute(delete_duplicates_query)
    conn.commit()

    print("Excess duplicate records have been deleted, retaining only one record per combination.")

    # Verify if any duplicate combinations remain
    verify_query = """
    SELECT emar_id, parent_field_ordinal, COUNT(*) AS duplicate_count
    FROM emar_detail
    GROUP BY emar_id, parent_field_ordinal
    HAVING COUNT(*) > 1;
    """

    # Execute the verification query and load the results into a DataFrame
    remaining_duplicates_df = pd.read_sql(verify_query, conn)

    print("Checking if any duplicate combinations remain:")
    print(remaining_duplicates_df)

    if remaining_duplicates_df.empty:
        print("All duplicate records have been successfully removed.")
    else:
        print("Some duplicate records still exist, please check the data.")

    # Close the database connection
    conn.close()
    print("Database connection closed.")

def clean_emar():
    drop_duplicate_in_emar_detail()
    try:
        # Get the path of the current script (where the Python script is located)
        script_dir = os.path.dirname(__file__)

        # Construct the relative path to the mimic.db file
        db_path = os.path.join(script_dir, '..', 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

        # Connect to the database using the relative path
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Drop the old table if it exists
        drop_table_query = "DROP TABLE IF EXISTS emar_clean;"
        cursor.execute(drop_table_query)
        print(" 'emar_clean' table dropped if existed. ")
        
        # create new table
        create_table_query = """
        CREATE TABLE emar_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            emar_id TEXT PRIMARY KEY,
            emar_seq INTEGER,
            poe_id TEXT,
            pharmacy_id INTEGER,
            enter_provider_id TEXT,
            charttime DATE,
            medication TEXT,
            event_txt TEXT,
            scheduletime DATE,
            storetime DATE
        );
        """
        cursor.execute(create_table_query)
        print(" 'emar_clean' is successfully created. ")
        
        # copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO emar_clean (subject_id,
            hadm_id,
            emar_id,
            emar_seq,
            poe_id,
            pharmacy_id,
            enter_provider_id,
            charttime,
            medication,
            event_txt,
            scheduletime,
            storetime)
        SELECT subject_id,
            hadm_id,
            emar_id,
            emar_seq,
            poe_id,
            pharmacy_id,
            enter_provider_id,
            charttime,
            medication,
            event_txt,
            scheduletime,
            storetime
        FROM emar;
        """
        cursor.execute(copy_data_query)
        print(" 'emar' is successfully copied. ")

        # Drop the old table using the reusable function
        drop_table(cursor, "emar")
        
        # Rename the new table to the original name
        rename_table(cursor, "emar_clean", "emar")
        
        # Use the new function to check the table schema
        check_table_schema(conn, "emar")
        
        # commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # close the connection
        if conn:
            conn.close()
            print("Database connection closed.")