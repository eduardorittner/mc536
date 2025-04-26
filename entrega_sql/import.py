import psycopg2
import pandas as pd

def create_tables(conn):
    sql_file = "./teste_db.sql"

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

def import_csv(file):
    # TODO read csv data and change columns names to match db

    df = pd.read_csv(file)


def insert_into_db(frame):
    # Get a pandas frame and insert data into db

with psycopg2.connect(
    dbname="Educacao-e-energia",
    user="postgres",
    password="",
    host="localhost",
    port="5432",) as conn:

    create_tables(conn)

