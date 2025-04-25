import psycopg2

sql_file = "./teste_db.sql"

with open(sql_file, "r") as f:
    sql_script = f.read()

with psycopg2.connect(
    dbname="Educacao-e-energia",
    user="postgres",
    password="",
    host="localhost",
    port="5432",
) as conn:
    cursor = conn.cursor()

    try:
        cursor.execute(sql_script)
        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Erro: {e}")
        exit(1)
