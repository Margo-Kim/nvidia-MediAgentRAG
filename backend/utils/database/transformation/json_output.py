import pandas as pd
import sqlite3
import glob
import os


import sqlite3
import json

# main.py
import sqlite3
import json
from collections import defaultdict

from transformation.data_fetch import *
from transformation.lab_json import *
from transformation.chartevent_json import *

def build_json_data(admission_row, transfers_list, services_list, procedures_list, prescriptions_list, icustays_list, cursor):
    """Build the final JSON data format."""
    hadm_id, admittime, dischtime, admission_type, insurance, marital_status, subj_id, gender, age = admission_row

    # Build ICU stays data
    if icustays_list:
        stays_data = []
        for stay_id, first_careunit, last_careunit, intime, outtime, los in icustays_list:
            # Fetch datetimeevents data and group by charttime
            datetimeevents_list = fetch_datetimeevents_data(cursor, stay_id)
            datetimeevents_dict = defaultdict(list)
            for charttime, value, label, abbreviation, category, unitname in datetimeevents_list:
                datetimeevents_dict[charttime].append({
                    "value": value,
                    "label": label,
                    "abbreviation": abbreviation,
                    "category": category,
                    "unitname": unitname
                })
            datetimeevents_data = {
                "description": "The following ida ta anformation is about datetime events during the ICU stay",
                "data": dict(datetimeevents_dict)  # Grouped by charttime
            }

            # Fetch inputevents data and group by starttime and endtime
            inputevents_list = fetch_inputevents_data(cursor, stay_id)
            inputevents_dict = defaultdict(list)
            for starttime, endtime, amount, amountuom, rate, rateuom, patientweight, label, category, _ in inputevents_list:
                inputevents_dict[f"{starttime} - {endtime}"].append({
                    "total_amount": f"{amount} {amountuom}" if amount and amountuom else amount or amountuom,
                    "infusion_rate": f"{rate} {rateuom}" if rate and rateuom else rate or rateuom,
                    "patientweight": patientweight,
                    "label": label,
                    "category": category
                })
            inputevents_data = {
                "description": "The following information is about input events during the ICU stay",
                "data": dict(inputevents_dict)  # Grouped by starttime and endtime
            }

            # Fetch ingredientevents data and group by starttime and endtime
            ingredientevents_list = fetch_ingredientevents_data(cursor, stay_id)
            ingredientevents_dict = defaultdict(list)
            for starttime, endtime, amount, amountuom, rate, rateuom, label, _ in ingredientevents_list:
                ingredientevents_dict[f"{starttime} - {endtime}"].append({
                    "total_amount": f"{amount} {amountuom}" if amount and amountuom else amount or amountuom,
                    "infusion_rate": f"{rate} {rateuom}" if rate and rateuom else rate or rateuom,
                    "label": label
                })
            ingredientevents_data = {
                "description": "The following information is about ingredient events during the ICU stay",
                "data": dict(ingredientevents_dict)  # Grouped by starttime and endtime
            }

            # Fetch outputevents data and group by charttime
            outputevents_list = fetch_outputevents_data(cursor, stay_id)
            outputevents_dict = defaultdict(list)
            for charttime, value, label, unitname in outputevents_list:
                outputevents_dict[charttime].append({
                    "total_output": f"{value} {unitname}" if value and unitname else value or unitname,
                    "label": label
                })
            outputevents_data = {
                "description": "The following information is about output events during the ICU stay",
                "data": dict(outputevents_dict)  # Grouped by charttime
            }

            # Fetch procedureevents data and group by starttime and endtime
            procedureevents_list = fetch_procedureevents_data(cursor, stay_id)
            procedureevents_dict = defaultdict(list)
            for starttime, endtime, value, valueuom, location, locationcategory, patientweight, label, abbreviation, category, _ in procedureevents_list:
                procedureevents_dict[f"{starttime} - {endtime}"].append({
                    "duration": f"{value} {valueuom}" if value and valueuom else value or valueuom,
                    "location": location,
                    "locationcategory": locationcategory,
                    "patientweight": patientweight,
                    "label": label,
                    "abbreviation": abbreviation,
                    "category": category
                })
            procedureevents_data = {
                "description": "The following information is about procedure events during the ICU stay",
                "data": dict(procedureevents_dict)  # Grouped by starttime and endtime
            }

            stays_data.append({
                "stay_id": stay_id,
                "first_careunit": first_careunit,
                "last_careunit": last_careunit,
                "intime": intime,
                "outtime": outtime,
                "los": los,
                "events": [
                    {"datetimeevents": datetimeevents_data},
                    {"inputevents": inputevents_data},
                    {"ingredientevents": ingredientevents_data},
                    {"outputevents": outputevents_data},
                    {"procedureevents": procedureevents_data}
                ]
            })

        icustays_data = {
            "description": "The following information is about patient ICU stays during the admission",
            "stays": stays_data
        }
    else:
        icustays_data = {
            "description": "There are no ICU stays for this admission."
        }

    # Build transfer data section
    transfers_data = {
        "description": "The following information is about patient transfers during the admission",
        "data": [{
            "transfer_id": transfer_id,
            "eventtype": eventtype,
            "careunit": careunit,
            "intime": intime,
            "outtime": outtime
        } for transfer_id, eventtype, careunit, intime, outtime in transfers_list]
    }

    # Build services data section
    services_data = {
        "description": "The following information is about patient services during the admission",
        "data": [{
            "transfertime": transfertime,
            "prev_service": prev_service,
            "curr_service": curr_service
        } for transfertime, prev_service, curr_service in services_list]
    }

    # Build procedures data section
    if procedures_list:
        procedures_data = {
            "description": "The following information is about patient procedures during the admission",
            "data": [{
                "chartdate": chartdate,
                "procedure_title": long_title
            } for chartdate, long_title in procedures_list]
        }
    else:
        procedures_data = {
            "description": "There are no procedures for this admission."
        }

    # Build prescriptions data section
    prescriptions_data = {
        "description": "The following information is about patient prescriptions during the admission",
        "data": [{
            "starttime": starttime,
            "stoptime": stoptime,
            "drug_type": drug_type,
            "drug_info": f"{drug}, {prod_strength}" if drug and prod_strength else drug or prod_strength,
            "form_rx": form_rx,
            "prescribed_dose": f"{dose_val_rx} {dose_unit_rx}" if dose_val_rx and dose_unit_rx else dose_val_rx or dose_unit_rx,
            "formulary_dose": f"{form_val_disp} {form_unit_disp}" if form_val_disp and form_unit_disp else form_val_disp or form_unit_disp,
            "doses_per_24_hrs": doses_per_24_hrs,
            "route": route
        } for starttime, stoptime, drug_type, drug, prod_strength, form_rx, dose_val_rx, dose_unit_rx, form_val_disp, form_unit_disp, doses_per_24_hrs, route in prescriptions_list]
    }

    # Build the final JSON structure
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
                "discharge_time": dischtime
            },
            "transfer": transfers_data,
            "services": services_data,
            "procedures": procedures_data,
            "prescriptions": prescriptions_data,
            "icustays": icustays_data
        }
    }
    return data


def export_admissions_to_json(subject_id):
    """Main function: Query and generate JSON file for admissions-related data."""
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect('mimic.db')
        cursor = conn.cursor()

        # Fetch admissions data for the given subject_id
        admissions_rows = fetch_admission_data(cursor, subject_id)

        # Define the output directory and ensure it exists
        output_dir = './json_output/'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Process each admission record
        for admission_row in admissions_rows:
            hadm_id = admission_row[0]

            # Fetch related data: transfers, ICU stays, services, procedures, prescriptions
            transfers_list = fetch_transfer_data(cursor, hadm_id)
            icustays_list = fetch_icustays_data(cursor, hadm_id)
            service_list = fetch_services_data(cursor, hadm_id)
            procedures_list = fetch_procedures_data(cursor, hadm_id)
            prescriptions_list = fetch_prescriptions_data(cursor, hadm_id)
            
            # Build the JSON structure with fetched data
            data = build_json_data(admission_row, transfers_list, service_list, procedures_list, prescriptions_list, icustays_list, cursor)

            # Save JSON file with hadm_id as the filename in the specified output directory
            filename = os.path.join(output_dir, f"{hadm_id}.json")
            with open(filename, 'w') as json_file:
                json.dump(data, json_file, indent=4, default=str)  # Handle datetime objects with default=str
            
            print(f"Admissions JSON file created: {filename}")
            
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")

    finally:
        # Ensure the database connection is closed
        if conn:
            conn.close()
            print("Database connection closed.")


def export_chartevents_to_json(subject_id):
    """Generate a JSON file for chartevents data."""
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect('mimic.db')
        cursor = conn.cursor()

        # Define the output directory and ensure it exists
        output_dir = './json_output/'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Fetch admissions data for the subject
        admissions_rows = fetch_admission_data(cursor, subject_id)

        # Process each admission to generate chartevents data
        for admission_row in admissions_rows:
            hadm_id = admission_row[0]
            icustays_list = fetch_icustays_data(cursor, hadm_id)

            # Save chartevents data for each ICU stay to a JSON file in the specified output directory
            save_chartevents_json(admission_row, icustays_list, cursor, output_dir)
            
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")

    finally:
        # Ensure the database connection is closed
        if conn:
            conn.close()
            print("Database connection closed.")


def export_labevents_to_json(subject_id):
    """Generate a JSON file for labevents data."""
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect('mimic.db')
        cursor = conn.cursor()

        # Define the output directory and ensure it exists
        output_dir = './json_output/'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Export lab events data for the given subject and save it in the specified output directory
        export_admissions_to_json_lab(subject_id, output_dir)
        
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")

    finally:
        # Ensure the database connection is closed
        if conn:
            conn.close()
            print("Database connection closed.")