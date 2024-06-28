import pandas as pd
import psycopg2
import numpy as np

proposicoes = pd.read_csv('proposicoes_clean.csv')
tramitacoes = pd.read_csv('tramitacoes.csv')

database_info = {
    'DB_HOST': "localhost",
    'DB_NAME': "khipo_db",
    'DB_USER': "khipo_user",
    'DB_PASSWORD': "khipo"
}


def create_tables(database_info):

    conn = psycopg2.connect(
        host=database_info['DB_HOST'],
        dbname=database_info['DB_NAME'],
        user=database_info['DB_USER'],
        password=database_info['DB_PASSWORD']
    )

    cur = conn.cursor()

    create_table_proposicao = """
    CREATE TABLE IF NOT EXISTS Proposicao (
        id INTEGER PRIMARY KEY,
        author TEXT,
        presentationDate TIMESTAMP,
        ementa TEXT,
        regime TEXT,
        situation TEXT,
        propositionType TEXT,
        number TEXT,
        year INTEGER,
        city VARCHAR(500),
        state VARCHAR(500)
    );
    """

    create_table_tramitacao = """
    CREATE TABLE IF NOT EXISTS Tramitacao (
        id INTEGER PRIMARY KEY,
        createdAt TIMESTAMP,
        description TEXT,
        local TEXT,
        propositionId INTEGER REFERENCES Proposicao(id)
    );
    """

    cur.execute(create_table_proposicao)
    cur.execute(create_table_tramitacao)
    conn.commit()

    cur.close()
    conn.close()


def insert_data(proposicoes, tramitacoes, database_info):
    proposicoes['presentationDate'] = proposicoes['presentationDate'].apply(
        lambda date: pd.to_datetime(date) if date is not None else None)
    tramitacoes['createdAt'] = tramitacoes['createdAt'].apply(
        lambda date: pd.to_datetime(date) if date is not None else None)

    proposicoes['presentationDate'] = proposicoes['presentationDate'].replace({pd.NaT: None, np.nan: None})
    proposicoes['year'] = proposicoes['year'].replace({pd.NaT: None, np.nan: None})

    conn = psycopg2.connect(
        host=database_info['DB_HOST'],
        dbname=database_info['DB_NAME'],
        user=database_info['DB_USER'],
        password=database_info['DB_PASSWORD']
    )

    cur = conn.cursor()

    for index, row in proposicoes.iterrows():
        cur.execute("""
        INSERT INTO Proposicao (id, author, presentationDate, ementa, regime, situation, propositionType, number, year, city, state)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            author = EXCLUDED.author,
            presentationDate = EXCLUDED.presentationDate,
            ementa = EXCLUDED.ementa,
            regime = EXCLUDED.regime,
            situation = EXCLUDED.situation,
            propositionType = EXCLUDED.propositionType,
            number = EXCLUDED.number,
            year = EXCLUDED.year,
            city = EXCLUDED.city,
            state = EXCLUDED.state
        """, (row['id'], row['author'], row['presentationDate'], row['ementa'], row['regime'],
              row['situation'], row['propositionType'], row['number'], row['year'],
              row['city'], row['state']))

    conn.commit()

    for index, row in tramitacoes.iterrows():
        cur.execute("""
        INSERT INTO Tramitacao (id, createdAt, description, local, propositionId)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            createdAt = EXCLUDED.createdAt,
            description = EXCLUDED.description,
            local = EXCLUDED.local,
            propositionId = EXCLUDED.propositionId
        """, (row['id'], row['createdAt'], row['description'], row['local'], row['propositionId']))

    conn.commit()
    cur.close()
    conn.close()


def load_main(proposicoes, tramitacoes, database_info):
    create_tables(database_info)
    insert_data(proposicoes, tramitacoes, database_info)

    return 'Dados carregados'

print(load_main(proposicoes, tramitacoes, database_info))

