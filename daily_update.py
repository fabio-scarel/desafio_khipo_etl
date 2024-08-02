import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import schedule
import time
from dotenv import load_dotenv
import os

from extract import extract_proposicoes, create_tramitacoes, url
from transform import transform_data, colunas
from load import db_host, db_port, db_name, db_user, db_pass, db_conn_uri

prop_update_testing = {
    'id': [1, 8, 9], 
    'author': [
        'Deputado Noraldino Júnior', 'Novo Deputado', 'Outro Deputado'
    ],
    'presentationdate': [
        '2023-05-25 00:00:00', '2024-03-10 00:00:00', '2023-12-31 00:00:00'
    ],
    'ementa': [
        'Altera a Lei 18692, de 30 de dezembro de 2009, que uniformiza os critérios de gestão e execução para transferência gratuita de bens, valores ou benefícios por órgãos e entidades da administração pública estadual, compreendidos no âmbito dos programas sociais que especifica. (Altera art 2º e anexo para acrescentar como objetivo de programa social sujeito a transferência de bens, valores ou benefícios a promoção de políticas de proteção e bem-estar de animais domésticos.)', 'Nova proposta de lei...', 'Outro projeto de lei...'
    ],
    'regime': [
        'Deliberação em dois turnos no Plenário', 'Deliberação em turno único nas comissões',
        'Deliberação em dois turnos no Plenário'
    ],
    'situation': [
        'Arquivado', 'Em tramitação', 'Em tramitação'
    ],
    'propositiontype': ['PL', 'PL', 'PL'],
    'number': [699, 2000, 2021],
    'year': [2023, 2034, 2033],
    'city': ['Belo Horizonte', 'Nova Lima', 'Contagem'],
    'state': ['Minas Gerais', 'Minas Gerais', 'Minas Gerais']
}

df_prop_update_testing = pd.DataFrame(prop_update_testing)

tram_update_testing = {
    'id': [1, 2, 3],
    'createdat': [
        '2024-08-01 10:00:00', 
        '2024-08-02 11:00:00', 
        '2024-08-03 12:00:00'
    ],
    'description': [
        'Testando.',
        'as',
        'atualizações'
    ],
    'local': [
        'Rua', 
        'das Palmeiras',
        '1234'
    ],
    'propositionid': [101, 102, 103]
}

df_tram_update_testing = pd.DataFrame(tram_update_testing)

def get_new_id(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(id) FROM {table_name}")
    max_id = cursor.fetchone()[0]
    cursor.close()
    return max_id + 1 if max_id else 1

def upsert_proposicoes_data(df_new, table_name, conn):
    try:
        cursor = conn.cursor()

        # Loop through the dataframe and upsert each record
        for index, row in df_new.iterrows():
            try:
                # Check for null or incorrect values
                if 'author' not in row or pd.isnull(row['author']) or pd.isnull(row['presentationdate']) or pd.isnull(row['ementa']):
                    print(f"Skipping row {index} due to null values in critical columns.")
                    continue

                # Get a new ID if needed
                new_id = get_new_id(conn, table_name)

                # Upsert query using ON CONFLICT
                upsert_proposicoes_query = f"""
                    INSERT INTO {table_name} (id, author, presentationdate, ementa, regime, situation, propositiontype, number, year, city, state)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (author, presentationdate, ementa, regime, situation, propositiontype, number, year, city, state)
                    DO NOTHING
                    RETURNING id
                """
                
                cursor.execute(upsert_proposicoes_query, (
                    new_id, row['author'], row['presentationdate'], row['ementa'],
                    row['regime'], row['situation'], row['propositiontype'], row['number'],
                    row['year'], row['city'], row['state']
                ))

            except Exception as e:
                print(f"Error inserting row {index}: {e}")
                # Rollback the transaction in case of an error to prevent lockup
                conn.rollback()

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print("Error updating database:", e)
    finally:
        cursor.close()

def upsert_tramitacoes_data(df_new, table_name, conn):
    try:
        cursor = conn.cursor()

        # Loop through the dataframe and upsert each record
        for index, row in df_new.iterrows():
            try:
                # Check for null or incorrect values in : id, createdat, description, local, propositionid
                if 'propositionid' not in row or pd.isnull(row['createdat']) or pd.isnull(row['description']) or pd.isnull(row['propositionid']):
                    print(f"Skipping row {index} due to null values in critical columns.")
                    continue

                # Get a new ID if needed
                new_id = get_new_id(conn, table_name)

                # Upsert query using ON CONFLICT
                upsert_tramitacoes_query = f"""
                    INSERT INTO {table_name} (id, createdat, description, local, propositionid)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (createdat, description, local, propositionid)
                    DO NOTHING
                    RETURNING id
                """
                
                cursor.execute(upsert_tramitacoes_query, (
                    new_id, row['createdat'], row['description'], row['local'],
                    row['propositionid']
                ))

            except Exception as e:
                print(f"Error inserting row {index}: {e}")
                # Rollback the transaction in case of an error to prevent lockup
                conn.rollback()

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print("Error updating database:", e)
    finally:
        cursor.close()

def extract_update(url, colunas):

    df_proposicoes = extract_proposicoes(url).reset_index(names='id')
    df_tramitacoes = create_tramitacoes(df_proposicoes)

    proposicoes_final, tramitacoes_final = transform_data(df_proposicoes, colunas, df_tramitacoes)

    return proposicoes_final, tramitacoes_final


def update_main(url, colunas):

    df_proposicoes, df_tramitacoes = extract_update(url, colunas)

    with psycopg2.connect(db_conn_uri) as conn:
        upsert_proposicoes_data(df_proposicoes, 'proposicao', conn)
        upsert_tramitacoes_data(df_tramitacoes, 'tramitacao', conn)

    with psycopg2.connect(db_conn_uri) as conn:
        upsert_proposicoes_data(df_prop_update_testing, 'proposicao', conn)
        upsert_tramitacoes_data(df_tram_update_testing, 'tramitacao', conn)

    return 'Dados Atualizados'

def job():
    print("Iniciando o job de atualização às", datetime.now())
    update_main(url, colunas)
    print("Job de atualização concluído às", datetime.now())


if __name__ == '__main__':
    schedule.every().day.at("17:31").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)
