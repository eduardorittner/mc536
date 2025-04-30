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
            # Only add countries that are not yet present, otherwise do nothing
            insert_sql = """
            INSERT INTO "Pais" ("nome")
            VALUES (%s)
            ON CONFLICT ("nome") DO NOTHING; """
            cursor.execute(
                insert_sql,
                (country,),
            )
        conn.commit()

        cursor.execute('SELECT id, nome FROM "Pais"')
        results = cursor.fetchall()

    print("All countries inserted successfully")

    countries = {}

    for id, country in results:
        countries[country] = id

    return countries


def import_education(file, conn):
    df = pd.read_csv(file)

    # Unique list of all countries
    countries = df["country"].drop_duplicates()
    country_ids = insert_countries(countries, conn)

    print("Importing education dataset")

    with conn.cursor() as cur:
        for _, row in tqdm(df.iterrows(), total=len(df), colour="green"):
            year = row["year"]
            country = country_ids[row["country"]]
            sex = row["sex"]
            agefrom = str(row["agefrom"])
            ageto = str(row["ageto"])
            faixa_etaria = agefrom + "_" + ageto
            quantidade = int(row["pop"]) * 1000

            populacao_sql = """
            INSERT INTO "Populacao" ("ano", "sexo", "faixa_etaria", "quantidade", "pais_id")
            VALUES (%s, %s, %s, %s, %s)
            RETURNING "id" """

            cur.execute(populacao_sql, (year, sex, faixa_etaria, quantidade, country))
            populacao_id = cur.fetchone()[0]

            media_estudo_sql = """
            INSERT INTO "Media_Estudo" (
            "media_anos_primario",
            "media_anos_secundario",
            "media_anos_superior",
            "media_anos_estudo",
            "populacao_id")
            VALUES (%s, %s, %s, %s, %s)
            """

            media_primario = row["yr_sch_pri"]
            media_secundario = row["yr_sch_sec"]
            media_superior = row["yr_sch_ter"]
            media_estudo = row["yr_sch"]

            cur.execute(
                media_estudo_sql,
                (
                    media_primario,
                    media_secundario,
                    media_superior,
                    media_estudo,
                    populacao_id,
                ),
            )

            escolaridade_sql = """
            INSERT INTO "Escolaridade" ("porc_sem_escolaridade",
            "porc_primario_alcançado",
            "porc_primario_completo",
            "porc_secundario_alcançado",
            "porc_secundario_completo",
            "porc_superior_alcançado",
            "porc_superior_completo",
            "populacao_id")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            sem_escolaridade = row["lu"]
            primario = row["lp"]
            primario_completo = row["lpc"]
            secundario = row["ls"]
            secundario_completo = row["lsc"]
            superior = row["lh"]
            superior_completo = row["lhc"]

            cur.execute(
                escolaridade_sql,
                (
                    sem_escolaridade,
                    primario,
                    primario_completo,
                    secundario,
                    secundario_completo,
                    superior,
                    superior_completo,
                    populacao_id,
                ),
            )

        conn.commit()


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

    print("Importing energy dataset")
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


def highest_education_variation(conn):
    sql = """
SELECT 
    pop.pais_id,
    MAX(CASE WHEN pop.ano = 2000 THEN me.media_anos_estudo END) AS media_2000,
    MAX(CASE WHEN pop.ano = 2010 THEN me.media_anos_estudo END) AS media_2010
FROM "Media_Estudo" me
JOIN "Populacao" pop ON me.populacao_id = pop.id
WHERE pop.ano IN (2000, 2010)
GROUP BY pop.pais_id
    """
    sql = """
    WITH EducationData AS (
    SELECT 
        pop.id AS pop_id,
        pop.pais_id AS pais_id,
        MAX(CASE WHEN pop.ano = 2000 THEN me.media_anos_estudo END) AS media_inicial,
        MAX(CASE WHEN pop.ano = 2001 THEN me.media_anos_estudo END) AS media_final
    FROM "Media_Estudo" me
    JOIN "Populacao" pop ON me.populacao_id = pop.id
    WHERE pop.ano IN (2000, 2001)
    GROUP BY pop.id, pop.pais_id
    ),
    EducationDifference AS (
      SELECT 
        edata.pop_id,
        edata.pais_id,
        (edata.media_final - edata.media_inicial) AS diferenca_media_estudo
      FROM EducationData edata
      JOIN "Pais" pais ON edata.pais_id = pais.id
    )
    SELECT 
      ed.pop_id,
      ed.media_inicial,
      ed.media_final
    FROM EducationData ed
    """
    # JOIN "Pais" p ON ed.pais_id = p.id
    # ORDER BY ed.diferenca_media_estudo DESC;

    sqla = """WITH EducationData AS (
      SELECT 
        pop.pais_id,
        MAX(CASE WHEN pop.ano = 2000 THEN me.media_anos_estudo END) AS media_inicial,
        MAX(CASE WHEN pop.ano = 2020 THEN me.media_anos_estudo END) AS media_final
      FROM "Media_Estudo" me
      JOIN "Populacao" pop ON me.populacao_id = pop.id
      WHERE pop.ano IN (2000, 2020)
      GROUP BY pop.pais_id
    ),
    EducationDifference AS (
      SELECT 
        pais_id,
        (media_final - media_inicial) AS diferenca_media_estudo
      FROM EducationData
      WHERE media_inicial IS NOT NULL AND media_final IS NOT NULL
    ),
    EnergyAggregated AS (
      SELECT 
        pais_id,
        SUM(biocombustivel) AS total_biocombustivel,
        SUM(carvao) AS total_carvao,
        SUM(solar) AS total_solar,
        SUM(eolica) AS total_eolica,
        SUM(gas) AS total_gas,
        SUM(combustivel_fossil) AS total_combustivel_fossil,
        SUM(hidro) AS total_hidro,
        SUM(nuclear) AS total_nuclear
      FROM "Producao"
      WHERE ano BETWEEN 1970 AND 2020
      GROUP BY pais_id
    )
    SELECT 
      p.nome AS pais,
      ed.diferenca_media_estudo,
      ea.total_biocombustivel,
      ea.total_carvao,
      ea.total_solar,
      ea.total_eolica,
      ea.total_gas,
      ea.total_combustivel_fossil,
      ea.total_hidro,
      ea.total_nuclear
    FROM EducationDifference ed
    JOIN "Pais" p ON ed.pais_id = p.id
    LEFT JOIN EnergyAggregated ea ON ed.pais_id = ea.pais_id
    ORDER BY ed.diferenca_media_estudo DESC;"""

    print("Running highest education variation query")

    with conn.cursor() as cur:
        cur.execute(sql)
        results = cur.fetchall()

    print(results)


args = argparse.ArgumentParser()
args.add_argument(
    "--reset-schema",
    action="store_true",
    help="Reset database schema using vendored .sql file",
)
args.add_argument(
    "--clean",
    action="store_true",
    help="Reset database schema and re-import all data",
)
args.add_argument(
    "--energy",
    action="store_true",
    help="Import data from energy dataset",
)
args.add_argument(
    "--education", action="store_true", help="Import data from education dataset"
)

args.add_argument("--queries", type=str, help="Queries to perform, delimited by commas")

if __name__ == "__main__":
    opt = args.parse_args()

    with psycopg2.connect(
        dbname="Educacao-e-energia",
        user="postgres",
        password="",
        host="localhost",
        port="5432",
    ) as conn:
        if opt.reset_schema or opt.clean:
            erase_tables(conn)
            create_tables(conn, "schema.sql")

        if opt.energy or opt.clean:
            import_energy("energy.csv", conn)

        if opt.education or opt.clean:
            import_education("education.csv", conn)

        if opt.queries is not None:
            for query in opt.queries.split(","):
                match query:
                    case "education-variation":
                        highest_education_variation(conn)
                    case default:
                        print(f"Skipping unknown query: {query}")
