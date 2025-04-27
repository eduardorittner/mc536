import psycopg2
from psycopg2 import sql
import pandas as pd
import argparse
from tqdm import tqdm


def erase_tables(conn):
    with conn.cursor() as cur:
        try:
            # Drop tables in reverse dependency order
            drop_order = [
                "Producao",
                "Media_Estudo",
                "Escolaridade",
                "Populacao",
                "Fonte",
                "Eletricidade",
                "Energia",
                "Pais",
            ]

            for table in drop_order:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                        sql.Identifier(table)
                    )
                )

            conn.commit()
            print("Tables successfully erased!")

        except Exception as e:
            conn.rollback()
            raise e


def create_tables(conn, sql_file):
    with open(sql_file, "r") as f:
        sql_script = f.read()
        cursor = conn.cursor()

        try:
            cursor.execute(sql_script)
            conn.commit()

        except Exception as e:
            conn.rollback()
            print(f"Erro: {e}")
            exit(1)

    print("Tables created!")


def insert_countries(countries, conn):
    print(f"Inserting {len(countries)} countries")
    country_ids = {}

    with conn.cursor() as cursor:
        for country in countries:
            insert_sql = """
                INSERT INTO "Pais" ("nome")
                VALUES (%s)
                RETURNING "id" """
            cursor.execute(
                insert_sql,
                (country,),
            )
            country_id = cursor.fetchone()[0]
            country_ids[country] = country_id
        conn.commit()

    print("All countries inserted successfully")

    return country_ids


def import_education(file, conn):
    return


def import_energy(file, conn):
    df = pd.read_csv(file)

    # Unique list of all countries
    countries = df["country"].drop_duplicates()
    country_ids = insert_countries(countries, conn)

    # TODO add oil to .sql schema and here
    energy_types = [
        "biofuel",
        "coal",
        "solar",
        "wind",
        "gas",
        "fossil_fuel",
        "hydro",
        "nuclear",
    ]

    energy_keys = ["consumption", "production", "prod_change_twh", "cons_change_twh"]

    electricity_keys = ["electricity", "share_elec"]

    print("Inserting Energia, Eletricidade and Producao tables")
    with conn.cursor() as cur:
        for _, row in tqdm(df.iterrows(), total=len(df), colour="green"):
            year = row["year"]
            country = country_ids[row["country"]]

            producao = [year, country]

            energia_sql = """
            INSERT INTO "Energia" ("producao", "consumo", "mudanca_anual_consumo", "mudanca_anual_producao")
            VALUES (%s, %s, %s, %s)
            RETURNING "id" """

            eletricidade_sql = """
            INSERT INTO "Eletricidade" ("producao", "porcentagem")
            VALUES (%s, %s)
            RETURNING "id" """

            fonte_sql = """
            INSERT INTO "Fonte" ("energia", "eletricidade")
            VALUES (%s, %s)
            RETURNING "id" """

            producao_sql = """
            INSERT INTO "Producao" ("ano", "pais_id", "biocombustivel", "carvao", "solar", "eolica", "gas", "combustivel_fossil", "hidro", "nuclear")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING "id" """

            for energy in energy_types:
                values = [row.get(energy + "_" + b) for b in energy_keys]
                cur.execute(energia_sql, (values))
                energia_id = cur.fetchone()[0]

                # NOTE we use `get` here because it can be null
                values = [row.get(energy + "_" + b) for b in electricity_keys]
                cur.execute(eletricidade_sql, (values))
                eletricidade_id = cur.fetchone()[0]

                # Create fonte table
                cur.execute(fonte_sql, (energia_id, eletricidade_id))
                fonte_id = cur.fetchone()[0]

                producao.append(fonte_id)

            cur.execute(producao_sql, (producao))

        conn.commit()


args = argparse.ArgumentParser()
args.add_argument(
    "--reset",
    type=str,
    default=None,
    help="Reset database schema using provided .sql file",
)
args.add_argument(
    "--energy",
    action="store_true",
    help="Import data from energy dataset",
)
args.add_argument(
    "--education", action="store_true", help="Import data from education dataset"
)

if __name__ == "__main__":
    opt = args.parse_args()

    with psycopg2.connect(
        dbname="Educacao-e-energia",
        user="postgres",
        password="",
        host="localhost",
        port="5432",
    ) as conn:
        if opt.reset is not None:
            erase_tables(conn)
            create_tables(conn, opt.reset)

        if opt.energy:
            import_energy("energy.csv", conn)

        if opt.education:
            import_education("education.csv", conn)
