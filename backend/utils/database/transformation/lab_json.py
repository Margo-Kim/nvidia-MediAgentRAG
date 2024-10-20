
from collections import defaultdict
import sqlite3
import json
import os

def fetch_labevents_data(cursor, hadm_id):
    """Fetch labevents data for the given hadm_id."""
    query = """
    SELECT labevents.charttime, labevents.value, labevents.valueuom, labevents.flag, d_labitems.label, d_labitems.fluid
    FROM labevents
    LEFT JOIN d_labitems ON labevents.itemid = d_labitems.itemid
    WHERE labevents.hadm_id = ?
    """
    # Execute the query and return all results
    cursor.execute(query, (hadm_id,))
    return cursor.fetchall()

def export_admissions_to_json_lab(subject_id, output_dir):
    try:
        # Connect to the database
        conn = sqlite3.connect('mimic.db')
        cursor = conn.cursor()

        # Query all admissions data for the given subject_id, joining with the patients table
        query = """
        SELECT admissions.hadm_id, admissions.admittime, admissions.dischtime, admissions.admission_type, 
               admissions.insurance, admissions.marital_status, 
               patients.subject_id, patients.gender, patients.anchor_age
        FROM admissions
        JOIN patients ON admissions.subject_id = patients.subject_id
        WHERE admissions.subject_id = ?
        """
        cursor.execute(query, (subject_id,))
        rows = cursor.fetchall()

        # Create a JSON file for each hadm_id
        for row in rows:
            hadm_id, admittime, dischtime, admission_type, insurance, marital_status, subj_id, gender, age = row

            # Fetch labevents data
            labevents_list = fetch_labevents_data(cursor, hadm_id)

            # Merge data by charttime and place fluid at the same level
            labevents_dict = defaultdict(lambda: defaultdict(list))
            for charttime, value, valueuom, flag, label, fluid in labevents_list:
                labevents_dict[charttime]["fluid"] = fluid  # Add fluid at the same level as charttime
                labevents_dict[charttime]["events"].append({
                    "value": value,
                    "valueuom": valueuom,
                    "flag": flag,
                    "label": label
                })

            # Convert to JSON-friendly format
            labevents_data = {
                "description": "The following information is about laboratory events during the admission.",
                "charttimes": {charttime: dict(details) for charttime, details in labevents_dict.items()}  # Grouped by charttime
            }

            # Build the final JSON data structure
            data = {
                str(hadm_id): {
                    "subject_id": subj_id,
                    "gender": gender,
                    "age": age,
                    "admission": {
                        "description": "The following information is about patient admission.",
                        "admission_type": admission_type,
                        "insurance": insurance,
                        "marital_status": marital_status,
                        "admission_time": admittime,
                        "discharge_time": dischtime,
                        "labevents": labevents_data  # Add labevents section
                    }
                }
            }

            # Create a JSON file using the hadm_id as the filename
            filename = os.path.join(output_dir, f"{hadm_id}-lab.json")
            with open(filename, 'w') as json_file:
                json.dump(data, json_file, indent=4, default=str)  # Use default=str to handle datetime formats
            
            print(f"JSON file created: {filename}")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the database connection
        if conn:
            conn.close()
            print("Database connection closed.")
