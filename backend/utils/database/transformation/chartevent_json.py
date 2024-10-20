from collections import defaultdict
import json
import os

def fetch_chartevents_data(cursor, stay_id):
    query = """
    SELECT ce.charttime, ce.value, ce.valueuom, di.label, di.category, di.unitname
    FROM chartevents AS ce
    LEFT JOIN d_items AS di ON ce.itemid = di.itemid
    WHERE ce.stay_id = ?
    """
    cursor.execute(query, (stay_id,))
    return sorted(cursor.fetchall(), key=lambda x: x[0])

def save_chartevents_json(admission_row, icustays_list, cursor, output_dir):
    """Save a JSON file containing chartevents data for each admission."""
    hadm_id, admittime, dischtime, admission_type, insurance, marital_status, subj_id, gender, age = admission_row

    # Build ICU stays data
    stays_data = []
    if icustays_list:
        for stay_id, first_careunit, last_careunit, intime, outtime, los in icustays_list:
            # Fetch chartevents data for the ICU stay
            chartevents_list = fetch_chartevents_data(cursor, stay_id)

            # Create a dictionary to group data by charttime and category
            charttime_dict = defaultdict(lambda: defaultdict(list))
            for charttime, value, valueuom, label, category, unitname in chartevents_list:
                charttime_dict[charttime][category].append({
                    "value": value,
                    "valueuom": valueuom,
                    "label": label,
                    "unitname": unitname
                })

            # Convert the data into a format suitable for JSON
            chartevents_data = {
                "description": "The following information is about chart events during the ICU stay",
                "charttimes": {charttime: dict(categories) for charttime, categories in charttime_dict.items()}
            }

            stays_data.append({
                "stay_id": stay_id,
                "first_careunit": first_careunit,
                "last_careunit": last_careunit,
                "intime": intime,
                "outtime": outtime,
                "los": los,
                "chartevents": chartevents_data  # Add chartevents data to ICU stays
            })
    else:
        stays_data = [{"description": "There are no ICU stays for this admission."}]

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
            "icustays": {
                "description": "The following information is about patient ICU stays during the admission",
                "stays": stays_data
            }
        }
    }
    
    # Save JSON file
    filename = os.path.join(output_dir, f"{hadm_id}-chartevents.json")
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4, default=str)  # Ensure datetime objects are handled
    
    print(f"Chartevents JSON file created: {filename}")
