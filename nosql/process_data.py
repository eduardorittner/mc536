import os
import csv
import json
from pymongo import MongoClient

ENERGY_DATASET_FILE = "energy.csv"
EDUCATION_DATASET_FILE = "education.csv"
PROCESSED_ENERGY_FILE = "energy.json"
PROCESSED_EDUCATION_FILE = "education.json"

# Campos desejados para energy
ENERGY_FIELDS = [
    "country", "year",
    "coal_electricity", "coal_consumption",
    "wind_electricity", "wind_consumption",
    "solar_electricity", "solar_consumption",
    "gas_electricity", "gas_consumption",
    "hydro_electricity", "hydro_consumption",
    "nuclear_electricity", "nuclear_consumption",
    "oil_electricity", "oil_consumption",
    "other_renewable_electricity", "other_renewable_consumption"
]

def process_energy(input_path, output_path):
    data = []
    with open(input_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            processed_row = {}
            for key in ENERGY_FIELDS:
                if key not in row:
                    continue
                value = row[key]
                if value == "":
                    continue
                if key == "country":
                    processed_row["country"] = value
                elif key == "year":
                    processed_row["year"] = int(value)
                else:
                    processed_row[key] = float(value)
            if processed_row:
                data.append(processed_row)

    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)

def process_education(input_path, output_path):
    renamed_keys = {
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
                    case "BLcode" | "WBcode" | "region_code" | "sex":
                        continue
                    case "country":
                        processed_row[key] = value
                    case "agefrom" | "ageto" | "year":
                        processed_row[key] = int(value)
                    case "pop":
                        if value != "":
                            processed_row["population"] = 1000 * float(value)
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
    print(f"Importing collection: {collection}")
    db[collection].delete_many({})  # Limpa a coleção
    document = json.load(file)
    db[collection].insert_many(document)
    print(f"{len(document)} documentos inseridos na coleção '{collection}'.")

if __name__ == "__main__":
    db = connect()

    # Processa e importa energy
    if not os.path.exists(PROCESSED_ENERGY_FILE):
        print(f"{PROCESSED_ENERGY_FILE} não existe. Processando {ENERGY_DATASET_FILE}")
        process_energy(ENERGY_DATASET_FILE, PROCESSED_ENERGY_FILE)
        print("Energy processado e salvo em JSON")
    else:
        print(f"Arquivo {PROCESSED_ENERGY_FILE} já existe")

    print("Importando dados de energy")
    with open(PROCESSED_ENERGY_FILE, "r") as f:
        import_collection(f, db, "energy")

    # Processa e importa education
    if not os.path.exists(PROCESSED_EDUCATION_FILE):
        print(f"{PROCESSED_EDUCATION_FILE} não existe. Processando {EDUCATION_DATASET_FILE}")
        process_education(EDUCATION_DATASET_FILE, PROCESSED_EDUCATION_FILE)
        print("Education processado e salvo em JSON")
    else:
        print(f"Arquivo {PROCESSED_EDUCATION_FILE} já existe")

    print("Importando dados de education")
    with open(PROCESSED_EDUCATION_FILE, "r") as f:
        import_collection(f, db, "education")

