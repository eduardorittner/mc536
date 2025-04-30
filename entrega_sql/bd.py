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
    WITH Media2000 AS (
  SELECT 
    pop.pais_id,
    AVG(me.media_anos_estudo) AS media_inicial
  FROM "Media_Estudo" me
  JOIN "Populacao" pop ON me.populacao_id = pop.id
  WHERE pop.ano = 2000
  GROUP BY pop.pais_id
),
Media2010 AS (
  SELECT 
    pop.pais_id,
    AVG(me.media_anos_estudo) AS media_final
  FROM "Media_Estudo" me
  JOIN "Populacao" pop ON me.populacao_id = pop.id
  WHERE pop.ano = 2010
  GROUP BY pop.pais_id
),
EducationData AS (
  SELECT 
    m2000.pais_id,
    m2000.media_inicial,
    m2010.media_final
  FROM Media2000 m2000
  JOIN Media2010 m2010 
    ON m2000.pais_id = m2010.pais_id
),
EducationDifference AS (
  SELECT 
    edata.pais_id,
    edata.media_inicial,
    edata.media_final,
    (edata.media_final - edata.media_inicial) AS diferenca_media_estudo
  FROM EducationData edata
  WHERE edata.media_inicial IS NOT NULL AND edata.media_final IS NOT NULL
),
EnergyAggregated AS (
  SELECT 
    p.pais_id,
    SUM(e1.producao) AS total_biocombustivel,
    SUM(e2.producao) AS total_carvao,
    SUM(e3.producao) AS total_solar,
    SUM(e4.producao) AS total_eolica,
    SUM(e5.producao) AS total_gas,
    SUM(e6.producao) AS total_combustivel_fossil,
    SUM(e7.producao) AS total_hidro,
    SUM(e8.producao) AS total_nuclear
  FROM "Producao" p
  LEFT JOIN "Energia" e1 ON p.biocombustivel = e1.id
  LEFT JOIN "Energia" e2 ON p.carvao = e2.id
  LEFT JOIN "Energia" e3 ON p.solar = e3.id
  LEFT JOIN "Energia" e4 ON p.eolica = e4.id
  LEFT JOIN "Energia" e5 ON p.gas = e5.id
  LEFT JOIN "Energia" e6 ON p.combustivel_fossil = e6.id
  LEFT JOIN "Eletricidade" e7 ON p.hidro = e7.id
  LEFT JOIN "Eletricidade" e8 ON p.nuclear = e8.id
  WHERE p.ano BETWEEN 1970 AND 2010
  GROUP BY p.pais_id
)
SELECT 
  pais.nome AS pais,
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
JOIN "Pais" pais ON ed.pais_id = pais.id
JOIN EnergyAggregated ea ON ed.pais_id = ea.pais_id
WHERE 
  ed.diferenca_media_estudo IS NOT NULL AND
  ea.total_biocombustivel IS NOT NULL AND
  ea.total_carvao IS NOT NULL AND
  ea.total_solar IS NOT NULL AND
  ea.total_eolica IS NOT NULL AND
  ea.total_gas IS NOT NULL AND
  ea.total_combustivel_fossil IS NOT NULL AND
  ea.total_hidro IS NOT NULL AND
  ea.total_nuclear IS NOT NULL
ORDER BY ed.diferenca_media_estudo DESC;
    """
    print("Running highest education variation query")

    with conn.cursor() as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            print(row)


def consumo_educacao(conn):
    query = """
    WITH MediaEducacao AS (
      SELECT pop.pais_id, me.media_anos_estudo
      FROM "Populacao" pop
      JOIN "Media_Estudo" me ON pop.id = me.populacao_id
      WHERE pop.ano = 2010
    ),
    ConsumoEnergia AS (
      SELECT pr.pais_id, SUM(e.producao) AS producao_total, SUM(e.consumo) AS consumo_total
      FROM "Producao" pr
      JOIN "Fonte" f ON f.id = ANY (ARRAY[pr.biocombustivel, pr.carvao, pr.solar, pr.eolica, pr.gas, pr.combustivel_fossil, pr.hidro, pr.nuclear])
      JOIN "Energia" e ON f.energia = e.id
      GROUP BY pr.pais_id
    )
    SELECT p.nome, ce.consumo_total, me.media_anos_estudo
    FROM ConsumoEnergia ce
    JOIN "Pais" p ON p.id = ce.pais_id
    LEFT JOIN MediaEducacao me ON me.pais_id = ce.pais_id
    ORDER BY ce.consumo_total DESC;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        for row in cur.fetchall():
            print(row)


def producao_educacao(conn):
    query = """
    WITH MediaEducacao AS (
      SELECT pop.pais_id, me.media_anos_estudo
      FROM "Populacao" pop
      JOIN "Media_Estudo" me ON pop.id = me.populacao_id
      WHERE pop.ano = 2010
    ),
    ProducaoEnergia AS (
      SELECT pr.pais_id, SUM(e.producao) AS producao_total
      FROM "Producao" pr
      JOIN "Fonte" f ON f.id = ANY (ARRAY[pr.biocombustivel, pr.carvao, pr.solar, pr.eolica, pr.gas, pr.combustivel_fossil, pr.hidro, pr.nuclear])
      JOIN "Energia" e ON f.energia = e.id
      GROUP BY pr.pais_id
    )
    SELECT p.nome, pe.producao_total, me.media_anos_estudo
    FROM ProducaoEnergia pe
    JOIN "Pais" p ON p.id = pe.pais_id
    LEFT JOIN MediaEducacao me ON me.pais_id = pe.pais_id
    ORDER BY pe.producao_total DESC;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        for row in cur.fetchall():
            print(row)


def correlacao_educacao_energia(conn):
    query = """
    WITH Educacao AS (
      SELECT pop.pais_id, me.media_anos_estudo
      FROM "Populacao" pop
      JOIN "Media_Estudo" me ON pop.id = me.populacao_id
      WHERE pop.ano = 2010
    ),
    Producoes AS (
      SELECT pais_id, 
        SUM(biocombustivel) AS bio,
        SUM(carvao) AS carvao,
        SUM(solar) AS solar,
        SUM(eolica) AS eolica,
        SUM(gas) AS gas,
        SUM(combustivel_fossil) AS fossil,
        SUM(hidro) AS hidro,
        SUM(nuclear) AS nuclear
      FROM "Producao"
      GROUP BY pais_id
    )
    SELECT
      'bio' AS fonte, corr(p.bio, e.media_anos_estudo) AS correlacao FROM Producoes p JOIN Educacao e ON p.pais_id = e.pais_id
    UNION
    SELECT 'carvao', corr(p.carvao, e.media_anos_estudo) FROM Producoes p JOIN Educacao e ON p.pais_id = e.pais_id
    UNION
    SELECT 'solar', corr(p.solar, e.media_anos_estudo) FROM Producoes p JOIN Educacao e ON p.pais_id = e.pais_id
    UNION
    SELECT 'eolica', corr(p.eolica, e.media_anos_estudo) FROM Producoes p JOIN Educacao e ON p.pais_id = e.pais_id
    UNION
    SELECT 'gas', corr(p.gas, e.media_anos_estudo) FROM Producoes p JOIN Educacao e ON p.pais_id = e.pais_id
    UNION
    SELECT 'fossil', corr(p.fossil, e.media_anos_estudo) FROM Producoes p JOIN Educacao e ON p.pais_id = e.pais_id
    UNION
    SELECT 'hidro', corr(p.hidro, e.media_anos_estudo) FROM Producoes p JOIN Educacao e ON p.pais_id = e.pais_id
    UNION
    SELECT 'nuclear', corr(p.nuclear, e.media_anos_estudo) FROM Producoes p JOIN Educacao e ON p.pais_id = e.pais_id
    ORDER BY correlacao DESC;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        for row in cur.fetchall():
            print(row)


def education_disparity_energy(conn):
    query = """
    WITH media_por_sexo AS (
      SELECT
        p.id AS pais_id,
        p.nome AS pais,
        pop.sexo,
        AVG(me.media_anos_estudo) AS media_anos_estudo
      FROM "Pais" p
      JOIN "Populacao" pop ON pop.pais_id = p.id
      JOIN "Media_Estudo" me ON me.populacao_id = pop.id
      WHERE pop.sexo IN ('M', 'F')
      GROUP BY p.id, p.nome, pop.sexo
    ),
    disparidade AS (
      SELECT
        m1.pais,
        ABS(m1.media_anos_estudo - m2.media_anos_estudo) AS disparidade_educacional
      FROM media_por_sexo m1
      JOIN media_por_sexo m2 ON m1.pais = m2.pais
      WHERE m1.sexo = 'M' AND m2.sexo = 'F'
    )
    SELECT
      d.pais,
      d.disparidade_educacional,
      e.producao,
      e.consumo
    FROM disparidade d
    JOIN "Pais" p ON p.nome = d.pais
    LEFT JOIN "Producao" pr ON pr.pais_id = p.id
    LEFT JOIN "Fonte" f ON f.id = pr.solar  -- substitua por outra fonte se necessário
    LEFT JOIN "Energia" e ON e.id = f.energia
    ORDER BY d.disparidade_educacional DESC;
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
                    case "education-variation":
                        highest_education_variation(conn)
                    case "consumo-educacao":
                        consumo_educacao(conn)
                    case "producao-educacao":
                        producao_educacao(conn)
                    case "correlacao":
                        correlacao_educacao_energia(conn)
                    case default:
                        print(f"Skipping unknown query: {query}")
        
        if opt.queries is not None:
            for query in opt.queries.split(","):
                match query:
                    case "education-variation":
                        highest_education_variation(conn)
                    case "education-disparity-energy":
                        education_disparity_energy(conn)
                    case default:
                        print(f"Skipping unknown query: {query}")

