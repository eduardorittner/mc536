import os
import csv
import json

ENERGY_DATASET_FILE = "energy.csv"
EDUCATION_DATASET_FILE = "education.csv"
PROCESSED_ENERGY_FILE = "energy.json"
PROCESSED_EDUCATION_FILE = "education.json"


def process_energy(input_path, output_path):
    data = []
    with open(input_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            processed_row = {}
            for key, value in row.items():
                match key:
                    # Ignore ISO code and population
                    # Country is the only string value
                    # Year is the only int value
                    # All the others are float
                    case "iso_code" | "population":
                        continue
                    case "country":
                        processed_row[key] = value
                    case "year":
                        processed_row[key] = int(value)
                    case _:
                        if value != "":
                            processed_row[key] = float(value)
            data.append(processed_row)

    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)


def process_education(input_path, output_path):
    # Map from the original dataset keys to our (more readable) keys
    renamed_keys = {
        "pop": "population",
        "lu": "no_schooling",
        "lp": "primary_schooling",
        "lpc": "primary_schooling_complete",
        "ls": "secondary_schooling",
        "lsc": "secondary_schooling_complete",
        "lh": "tertiary_schooling",
        "lhc": "tertiary_schooling_complete",
        "yr_sch": "schooling_years_avg",
        "yr_sch_pri": "primary_schooling_years_avg",
        "yr_sch_sec": "secondary_schooling_years_avg",
        "yr_sch_ter": "tertiary_schooling_years_avg",
    }

    data = []
    with open(input_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            processed_row = {}
            for key, value in row.items():
                match key:
                    # Ignored
                    case "BLcode" | "WBcode" | "region_code" | "sex":
                        continue
                    # Only string value
                    case "country":
                        processed_row[key] = value
                    # Int values without renaming
                    case "agefrom" | "ageto" | "year":
                        processed_row[key] = int(value)
                    # Renamed float values
                    case _:
                        if value != "":
                            processed_row[renamed_keys[key]] = float(value)
            data.append(processed_row)

    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)


def connect():
    uri = "mongodb://localhost:27017/"
    client = MongoClient(uri)
    database = client["education_and_energy"]
    return database


def import_collection(file, db, collection):
    if db[collection] is None:
        print(f"Collection {collection} does not exist, creating one")
        db.create_collection(collection)
    else:
        print(f"Collection {collection} already exists, clearing it")
        db[collection].delete_many({})

    print(f"Reading json from {os.path.basename(file.name)}")
    document = json.load(file)
    print("Importing data into database")
    db[collection].insert_many(document)


if __name__ == "__main__":
    db = connect()

    if not os.path.exists(PROCESSED_ENERGY_FILE):
        print(
            f"{PROCESSED_ENERGY_FILE} doesn't exist, processing data from {ENERGY_DATASET_FILE}"
        )
        processed = process_energy(ENERGY_DATASET_FILE, PROCESSED_ENERGY_FILE)
        print("Data processed and saved")

    else:
        print(f"Found processed file {PROCESSED_ENERGY_FILE}")

    print("Importing energy data")
    with open(PROCESSED_ENERGY_FILE, "r") as f:
        import_collection(f, db, "collection")

    if not os.path.exists(PROCESSED_EDUCATION_FILE):
        print(
            f"{PROCESSED_EDUCATION_FILE} doesn't exist, processing data from {EDUCATION_DATASET_FILE}"
        )
        processed = process_education(EDUCATION_DATASET_FILE, PROCESSED_EDUCATION_FILE)
        print("Data processed and saved")

    else:
        print(f"Found processed file {PROCESSED_EDUCATION_FILE}")

    print("Importing educationdata")
    with open(PROCESSED_EDUCATION_FILE, "r") as f:
        import_collection(f, db, "education")
