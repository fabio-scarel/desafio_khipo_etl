import pandas as pd
import psycopg2
import numpy as np
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

db_host = os.environ.get('DB_HOST')
db_port = os.environ.get('DB_PORT')
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')
db_conn_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'


def add_constraint(conn, table_name):
    cursor = conn.cursor()

    prop_query = f"""
                        ALTER TABLE proposicao
                        ADD CONSTRAINT unique_proposicao_city 
                        UNIQUE (author, presentationdate, ementa, regime, situation, propositiontype, number, year, city, state)
                    """
    
    tram_query = f"""
                        ALTER TABLE tramitacao
                        ADD CONSTRAINT unique_tramitacao 
                        UNIQUE (createdat, description, local, propositionid)
                    """

    if table_name == 'proposicao':    
        cursor.execute(prop_query)
    else:
        cursor.execute(tram_query)

    conn.commit()
    cursor.close()


def correct_typing(proposicoes, tramitacoes, load=True):

    if load:
        proposicoes['presentationdate'] = proposicoes['presentationdate'].apply(
            lambda date: pd.to_datetime(date) if date is not None else None)
        tramitacoes['createdat'] = tramitacoes['createdat'].apply(
            lambda date: pd.to_datetime(date) if date is not None else None)

        proposicoes['presentationdate'] = proposicoes['presentationdate'].replace({pd.NaT: None, np.nan: None})
        proposicoes['year'] = proposicoes['year'].replace({pd.NaT: None, np.nan: None})

        return proposicoes, tramitacoes
    
    else:
        proposicoes['year'] = proposicoes['year'].replace({pd.NaT: None, np.nan: None})

        return proposicoes, tramitacoes


def create_tables(db_conn_uri):

    create_table_proposicao = """
    CREATE TABLE IF NOT EXISTS Proposicao (
        id SERIAL PRIMARY KEY,
        author TEXT,
        presentationdate TIMESTAMP,
        ementa TEXT,
        regime TEXT,
        situation TEXT,
        propositiontype TEXT,
        number TEXT,
        year INTEGER,
        city VARCHAR(500),
        state VARCHAR(500)
    );
    """

    create_table_tramitacao = """
    CREATE TABLE IF NOT EXISTS tramitacao (
        id SERIAL PRIMARY KEY,
        createdAt TIMESTAMP,
        description TEXT,
        local TEXT,
        propositionid INTEGER REFERENCES proposicao(id)
    );
    """

    with psycopg2.connect(db_conn_uri) as conn:
        cur = conn.cursor()

        cur.execute(create_table_proposicao)
        cur.execute(create_table_tramitacao)
        conn.commit()

        cur.close()


def insert_data(proposicoes, tramitacoes, db_conn_uri):

    with psycopg2.connect(db_conn_uri) as conn:

        cur = conn.cursor()

        for index, row in proposicoes.iterrows():
            cur.execute("""
            INSERT INTO Proposicao (id, author, presentationdate, ementa, regime, situation, propositiontype, number, year, city, state)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                author = EXCLUDED.author,
                presentationdate = EXCLUDED.presentationdate,
                ementa = EXCLUDED.ementa,
                regime = EXCLUDED.regime,
                situation = EXCLUDED.situation,
                propositiontype = EXCLUDED.propositiontype,
                number = EXCLUDED.number,
                year = EXCLUDED.year,
                city = EXCLUDED.city,
                state = EXCLUDED.state
            """, (row['id'], row['author'], row['presentationdate'], row['ementa'], row['regime'],
                row['situation'], row['propositiontype'], row['number'], row['year'],
                row['city'], row['state']))

        conn.commit()

        for index, row in tramitacoes.iterrows():
            cur.execute("""
            INSERT INTO Tramitacao (id, createdat, description, local, propositionid)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                createdat = EXCLUDED.createdat,
                description = EXCLUDED.description,
                local = EXCLUDED.local,
                propositionid = EXCLUDED.propositionid
            """, (row['id'], row['createdat'], row['description'], row['local'], row['propositionid']))

        
        conn.commit()
        cur.close()


def load_main(proposicoes, tramitacoes, db_conn_uri):

    proposicoes, tramitacoes = correct_typing(proposicoes, tramitacoes)
    create_tables(db_conn_uri)
    insert_data(proposicoes, tramitacoes, db_conn_uri)

    with psycopg2.connect(db_conn_uri) as conn:
        add_constraint(conn, 'proposicao')
        add_constraint(conn, 'tramitacao')

    return 'Dados carregados'


if __name__ == '__main__':

    proposicoes = pd.read_csv('proposicoes_clean.csv')
    tramitacoes = pd.read_csv('tramitacoes.csv')
    
    print(load_main(proposicoes, tramitacoes, db_conn_uri))

    return 'Dados carregados'

print(load_main(proposicoes, tramitacoes, database_info))

