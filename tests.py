import psycopg2

database_info = {
    'DB_HOST': "localhost",
    'DB_NAME': "khipo_db",
    'DB_USER': "khipo_user",
    'DB_PASSWORD': "khipo"
}


def check_duplicates(database_info, table_name):
    conn = psycopg2.connect(
        host=database_info['DB_HOST'],
        dbname=database_info['DB_NAME'],
        user=database_info['DB_USER'],
        password=database_info['DB_PASSWORD']
    )

    cur = conn.cursor()

    check_duplicates_query = f"""
    SELECT *
    FROM (
        SELECT *,
               COUNT(*) OVER (PARTITION BY id, createdAt, description, local, propositionId) AS count_duplicates
        FROM {table_name}
    ) subquery
    WHERE count_duplicates > 1;
    """

    cur.execute(check_duplicates_query)
    duplicates = cur.fetchall()

    cur.close()
    conn.close()

    return duplicates

duplicates = check_duplicates(database_info, 'Tramitacao')

if duplicates:
    print("Linhas duplicadas encontradas:")
    for row in duplicates:
        print(row)
else:
    print("Nenhuma linha duplicada encontrada.")
