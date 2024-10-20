from transformation.json_output import * 
import os

def get_files(subject_id):
    # db connection
    # Get the path of the current script (where the Python script is located)
    script_dir = os.path.dirname(__file__)

    # Construct the relative path to the mimic.db file
    db_path = os.path.join(script_dir, 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

    export_admissions_to_json(subject_id)     # Generate  JSON file
    export_chartevents_to_json(subject_id)    # Generate chartevents JSON file
    export_labevents_to_json(subject_id)      # Generate labevents JSON file

get_files(13180007)