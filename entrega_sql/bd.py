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
                "Media_Estudo",
                "Escolaridade",
                "Populacao",
                "Tipo_Eletricidade",
                "Eletricidade",
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
            "porc_primario_alcancado",
            "porc_primario_completo",
            "porc_secundario_alcancado",
            "porc_secundario_completo",
            "porc_superior_alcancado",
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


def insert_energy_types(conn):
    """
    Insere os tipos de eletricidade na tabela Tipo_Eletricidade se ainda não estiverem cadastrados.
    """
    energy_types = [
        "coal",
        "solar",
        "wind",
        "gas",
        "hydro",
        "nuclear",
        "oil",
        "other_renewable"
    ]

    with conn.cursor() as cur:
        cur.execute('SELECT "id", "tipo" FROM "Tipo_Eletricidade"')
        existing = dict(cur.fetchall())

        type_ids = {}
        id_counter = max(existing.keys(), default=0) + 1

        for energy in energy_types:
            if energy not in existing.values():
                cur.execute(
                    'INSERT INTO "Tipo_Eletricidade" ("id", "tipo") VALUES (%s, %s) RETURNING "id"',
                    (id_counter, energy)
                )
                type_id = cur.fetchone()[0]
                type_ids[energy] = type_id
                id_counter += 1
            else:
                # Recupera o id já existente
                type_id = [k for k, v in existing.items() if v == energy][0]
                type_ids[energy] = type_id

        conn.commit()
        return type_ids


def import_energy(file, conn):
    df = pd.read_csv(file)

    # Lista única de países
    countries = df["country"].drop_duplicates()
    country_ids = insert_countries(countries, conn)

    # Inserir tipos de energia
    type_ids = insert_energy_types(conn)

    print("Importando energia para a tabela Eletricidade...")
    with conn.cursor() as cur:
        for _, row in tqdm(df.iterrows(), total=len(df), colour="green"):
            year = row["year"]
            country = country_ids[row["country"]]

            for energy in type_ids.keys():
                tipo_id = type_ids[energy]

                producao = row.get(f"{energy}_electricity")
                consumo = row.get(f"{energy}_consumption")

                 # Transforma 0.000 e NaN em 0
                producao = 0 if pd.isna(producao) or producao == 0.0 else producao
                consumo = 0 if pd.isna(consumo) or consumo == 0.0 else consumo

                # Se ambos forem zero (sem dados reais), pula
                if producao == 0 and consumo == 0:
                    continue

                cur.execute(
                    """
                    INSERT INTO "Eletricidade" (
                    "ano", "pais_id", "tipo_eletricidade_id", "producao", "consumo"
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (year, country, tipo_id, producao, consumo)
                )


        conn.commit()


def highest_education_variation(conn):
    sql = """
    SELECT 
    pa."nome" AS pais,

    -- Soma todos os valores não-nulos, incluindo 0.000
    COALESCE(SUM(el."consumo"), 0) AS consumo_total_eletricidade,

    -- Média dos anos de estudo
    ROUND(AVG(me."media_anos_estudo"), 2) AS media_anos_estudo,
    ROUND(AVG(me."media_anos_primario"), 2) AS media_anos_primario,
    ROUND(AVG(me."media_anos_secundario"), 2) AS media_anos_secundario,
    ROUND(AVG(me."media_anos_superior"), 2) AS media_anos_superior

FROM 
    "Pais" pa

JOIN "Eletricidade" el 
    ON el."pais_id" = pa."id"

LEFT JOIN "Populacao" p 
    ON p."pais_id" = pa."id" AND p."ano" = el."ano"

LEFT JOIN "Media_Estudo" me 
    ON me."populacao_id" = p."id"

-- Filtro de ano ele mostra os dados de 1965 até 2010 pulando de 5 em 5


GROUP BY 
    pa."nome"

HAVING 
    SUM(el."consumo") > 0 -- Apenas ignora países com 100% dos dados de consumo nulos
    AND COUNT(me."id") > 0
    AND AVG(me."media_anos_estudo") IS NOT NULL
    AND AVG(me."media_anos_primario") IS NOT NULL
    AND AVG(me."media_anos_secundario") IS NOT NULL
    AND AVG(me."media_anos_superior") IS NOT NULL
	

ORDER BY 
    consumo_total_eletricidade DESC;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        results = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(results, columns=colnames)
    print(df)


def consumo_educacao(conn):
    query = """
SELECT
  p."faixa_etaria",
  e."porc_sem_escolaridade",
  e."porc_primario_alcancado",
  e."porc_primario_completo",
  e."porc_secundario_alcancado",
  e."porc_secundario_completo",
  e."porc_superior_alcancado",
  e."porc_superior_completo",
  m."media_anos_estudo"
FROM "Escolaridade" e
JOIN "Populacao" p ON p."id" = e."populacao_id"
JOIN "Media_Estudo" m ON m."populacao_id" = p."id"
JOIN "Pais" pa ON pa."id" = p."pais_id"
WHERE p."ano" = 2010
  AND pa."nome" = 'Brazil'
ORDER BY m."media_anos_estudo" DESC;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(results, columns=colnames)
    print(df)


def producao_educacao(conn):
    query = """WITH escolaridade_base AS (
    SELECT
        pa."nome" AS pais,
        p."ano",
        AVG(es."porc_secundario_completo") AS secundario_completo
    FROM "Pais" pa
    JOIN "Populacao" p ON p."pais_id" = pa."id"
    JOIN "Escolaridade" es ON es."populacao_id" = p."id"
    WHERE p."ano" IN (1965, 1980, 1995, 2010)
      AND p."faixa_etaria" = '15_999'
    GROUP BY pa."nome", p."ano"
),

energia_base AS (
    SELECT
        pa."nome" AS pais,
        e."ano",
        SUM(COALESCE(e."producao", 0)) AS producao_total
    FROM "Pais" pa
    JOIN "Eletricidade" e ON e."pais_id" = pa."id"
    WHERE e."ano" IN (1965, 1980, 1995, 2010)
    GROUP BY pa."nome", e."ano"
),

escolaridade_pares AS (
    SELECT
        e1.pais,
        e1.ano AS ano_inicial,
        e2.ano AS ano_final,
        ROUND(e2.secundario_completo - e1.secundario_completo, 2) AS variacao_secundario_completo
    FROM escolaridade_base e1
    JOIN escolaridade_base e2 
        ON e1.pais = e2.pais AND e2.ano = e1.ano + 15
),

energia_pares AS (
    SELECT
        e1.pais,
        e1.ano AS ano_inicial,
        e2.ano AS ano_final,
        ROUND(e2.producao_total - e1.producao_total, 2) AS variacao_producao
    FROM energia_base e1
    JOIN energia_base e2 
        ON e1.pais = e2.pais AND e2.ano = e1.ano + 15
),

combinado AS (
    SELECT
        e.ano_inicial,
        e.ano_final,
        e.pais,
        e.variacao_secundario_completo,
        ep.variacao_producao
    FROM escolaridade_pares e
    LEFT JOIN energia_pares ep
        ON e.pais = ep.pais AND e.ano_inicial = ep.ano_inicial
),

ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY ano_inicial ORDER BY ABS(variacao_secundario_completo) DESC) AS rk
    FROM combinado
)

SELECT 
    ano_inicial,
    ano_final,
    pais,
    variacao_secundario_completo,
    variacao_producao
FROM ranked
WHERE rk = 1
ORDER BY ano_inicial;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(results, columns=colnames)
    print(df)


def correlacao_educacao_energia(conn):
    query = """
    SELECT 
    te."tipo" AS tipo_energia,
    CORR(e."producao", es."media_anos_superior") AS correlacao_superior
FROM "Eletricidade" e
JOIN "Tipo_Eletricidade" te ON e."tipo_eletricidade_id" = te."id"
JOIN "Pais" pa ON pa."id" = e."pais_id"
JOIN "Populacao" p ON p."pais_id" = pa."id" AND p."ano" = e."ano"
JOIN "Media_Estudo" es ON es."populacao_id" = p."id"
WHERE 
    es."media_anos_superior" IS NOT NULL
    AND e."producao" IS NOT NULL
GROUP BY te."tipo"
ORDER BY correlacao_superior DESC;  
    """
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(results, columns=colnames)
    print(df)


def education_disparity_energy(conn):
    query = """
WITH tipo_categoria AS (
    SELECT 
        id,
        tipo,
        CASE 
            WHEN tipo IN ('coal', 'oil', 'gas') THEN 'fossil'
            WHEN tipo IN ('nuclear', 'wind', 'solar', 'other_renewable', 'hydro') THEN 'renewable'
            ELSE 'outros'
        END AS categoria
    FROM "Tipo_Eletricidade"
),

energia_base AS (
    SELECT
        pa."id" AS pais_id,
        pa."nome" AS pais,
        te."tipo",
        te."categoria",
        SUM(COALESCE(e."producao", 0) + COALESCE(e."consumo", 0)) AS total_energia
    FROM "Eletricidade" e
    JOIN tipo_categoria te ON te.id = e."tipo_eletricidade_id"
    JOIN "Pais" pa ON pa."id" = e."pais_id"
    WHERE e."ano" = 2010
    GROUP BY pa."id", pa."nome", te."tipo", te."categoria"
),

energia_pivot AS (
    SELECT
        pais_id,
        pais,
        MAX(CASE WHEN tipo = 'coal' THEN total_energia ELSE 0 END) AS coal,
        MAX(CASE WHEN tipo = 'oil' THEN total_energia ELSE 0 END) AS oil,
        MAX(CASE WHEN tipo = 'gas' THEN total_energia ELSE 0 END) AS gas,
        MAX(CASE WHEN tipo = 'nuclear' THEN total_energia ELSE 0 END) AS nuclear,
        MAX(CASE WHEN tipo = 'wind' THEN total_energia ELSE 0 END) AS wind,
        MAX(CASE WHEN tipo = 'solar' THEN total_energia ELSE 0 END) AS solar,
        MAX(CASE WHEN tipo = 'hydro' THEN total_energia ELSE 0 END) AS hydro,
        MAX(CASE WHEN tipo = 'other_renewable' THEN total_energia ELSE 0 END) AS other_renewable,
        SUM(CASE WHEN categoria = 'fossil' THEN total_energia ELSE 0 END) AS energia_fossil,
        SUM(CASE WHEN categoria = 'renewable' THEN total_energia ELSE 0 END) AS energia_renovavel
    FROM energia_base
    GROUP BY pais_id, pais
),

populacao_2010 AS (
    SELECT
        pa."id" AS pais_id,
        SUM(p."quantidade") AS populacao_15_999
    FROM "Populacao" p
    JOIN "Pais" pa ON pa."id" = p."pais_id"
    WHERE p."ano" = 2010 AND p."faixa_etaria" = '15_999' -- se não restringir ele soma a pop duas vezes
    GROUP BY pa."id"
),

pegada_final AS (
    SELECT
        e.pais,
        coal, oil, gas,
        nuclear, wind, solar, hydro, other_renewable,
        energia_fossil,
        energia_renovavel,
        p.populacao_15_999,
        ROUND(
            (energia_fossil - energia_renovavel) / NULLIF(p.populacao_15_999, 0),
            6
        ) AS pegada_ecologica_per_capita
    FROM energia_pivot e
    JOIN populacao_2010 p ON p.pais_id = e.pais_id
)

SELECT *
FROM pegada_final
ORDER BY pegada_ecologica_per_capita ASC;


    """

    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(results, columns=colnames)
    print(df)

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
args.add_argument("--dbname", type=str, default="Educacao-e-energia")
args.add_argument("--user", type=str, default="postgres")
args.add_argument("--password", type=str, default="")
args.add_argument("--host", type=str, default="localhost")
args.add_argument("--port", type=str, default="5432")

args.add_argument("--queries", type=str, help="Queries to perform, delimited by commas")

if __name__ == "__main__":
    opt = args.parse_args()

    with psycopg2.connect(
        dbname=opt.dbname,
        user=opt.user,
        password=opt.password,
        host=opt.host,
        port=opt.port,
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
            case "all":
                highest_education_variation(conn)
                consumo_educacao(conn)
                producao_educacao(conn)
                correlacao_educacao_energia(conn)
                education_disparity_energy(conn)
            case "education-variation":
                highest_education_variation(conn)
            case "consumo-educacao":
                consumo_educacao(conn)
            case "producao-educacao":
                producao_educacao(conn)
            case "correlacao":
                correlacao_educacao_energia(conn)
            case "education-disparity-energy":
                education_disparity_energy(conn)
            case _:
                print(f"Skipping unknown query: {query}")
