import pandas as pd
import sqlite3
import glob
import os

def batch_import_to_sqlite(db_name, folder_paths, chunksize=100000):
    """
    Batch import all CSV files from multiple folders into an SQLite database.

    Parameters:
    db_name (str): The SQLite database file name, e.g., 'mimic.db'
    folder_paths (list): A list of folder paths containing the CSV files
    chunksize (int): The number of rows to read at a time
    """
    # Create an SQLite database connection
    conn = sqlite3.connect(db_name)
    
    # Iterate through each folder
    for folder_path in folder_paths:
        # Get all CSV file paths in the folder
        csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

        # Batch import all CSV files in the folder
        for csv_file in csv_files:
            # Extract the file name (without extension) as the table name
            table_name = os.path.basename(csv_file).replace('.csv', '')

            # Read the CSV file in chunks and import it into the SQLite database
            try:
                chunk_iter = pd.read_csv(csv_file, chunksize=chunksize)
                for i, chunk in enumerate(chunk_iter):
                    chunk.to_sql(table_name, conn, if_exists='append', index=False)
                    print(f"Successfully imported chunk {i+1} of file {csv_file} into table '{table_name}'")
            except Exception as e:
                print(f"Error importing file {csv_file}: {e}")
    
    # Close the database connection
    conn.close()
    print("All files have been imported, and the database connection is closed.")

def check_table_row_counts(db_name):
    """
    Connect to an SQLite database and check the row counts for each table.

    Parameters:
    db_name (str): The SQLite database file name, e.g., 'mimic.db'
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db_name)

    # Query all table names
    query_tables = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql(query_tables, conn)

    print("Tables in the database:")
    print(tables)

    # Iterate through all tables and check the number of rows in each table
    for table_name in tables['name']:
        query_count = f"SELECT COUNT(*) as row_count FROM {table_name};"
        result = pd.read_sql(query_count, conn)
        print(f"Number of rows in table '{table_name}': {result['row_count'][0]}")

    # Close the database connection
    conn.close()

def data_import():
    # Define the folder paths and database name
    folder_paths = ['./mimic-iv-2.2/hosp', './mimic-iv-2.2/icu']
    db_name = 'mimic.db'

    # Batch import the CSV files to SQLite
    batch_import_to_sqlite(db_name, folder_paths)

    # Check rows
    check_table_row_counts(db_name)

def print_one():
    print("1")
    
if __name__ == "__main__":
    data_import()