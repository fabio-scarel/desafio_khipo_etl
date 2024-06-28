import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import schedule
import time

from extract import extract_main
from transform import transform_main
from load import insert_data

url = 'https://dadosabertos.almg.gov.br/ws/proposicoes/pesquisa/direcionada'

database_info = {
    'DB_HOST': "localhost",
    'DB_NAME': "khipo_db",
    'DB_USER': "khipo_user",
    'DB_PASSWORD': "khipo"
}

colunas = [
    'id',
    'autor',
    'dataPublicacao',
    'ementa',
    'regime',
    'situacao',
    'siglaTipoProjeto',
    'numero',
]

data_atual = pd.Timestamp(datetime.now().date())
data_ontem = pd.Timestamp(datetime.now().date() - timedelta(1))


def get_tramitacoes(df_update, data_atual):
    tramitacoes_list = []

    for _, row in df_update.iterrows():
        numero = row['id']
        if isinstance(row['listaHistoricoTramitacoes'], list):
            for passo in row['listaHistoricoTramitacoes']:
                if pd.to_datetime(passo['data']) == data_atual:
                    add_row = {
                        'createdAt': passo['data'],
                        'description': passo['historico'],
                        'local': passo['local'],
                        'propositionId': numero
                    }
                    tramitacoes_list.append(add_row)

    tramitacoes = pd.DataFrame(tramitacoes_list, columns=['createdAt', 'description', 'local', 'propositionId'])

    tramitacoes = tramitacoes.reset_index(names='id')

    return tramitacoes


def update_main(url, colunas, database_info):

    proposicoes, tramitacoes = extract_main(url)

    # Organização dos dados
    proposicoes['dataUltimaAcao'] = proposicoes['dataUltimaAcao'].apply(
        lambda date: pd.to_datetime(date) if date is not None else None)
    proposicoes['dataPublicacao'] = proposicoes['dataPublicacao'].apply(
        lambda date: pd.to_datetime(date) if date is not None else None)

    conn = psycopg2.connect(
        host=database_info['DB_HOST'],
        dbname=database_info['DB_NAME'],
        user=database_info['DB_USER'],
        password=database_info['DB_PASSWORD']
    )

    cur = conn.cursor()

    count_query = "SELECT COUNT(*) FROM Proposicao;"
    cur.execute(count_query)

    row_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    # Separação dos dados que precisam ser atualizados na base
    resultado_ultima_acao = proposicoes[proposicoes['dataUltimaAcao'] == data_atual]

    # Novos registros que precisam ser adicionados com a sua indexação correta
    resultado_data_publicacao_hoje = proposicoes[proposicoes['dataPublicacao'] == data_atual]
    resultado_data_publicacao_hoje['id'] = range(row_count + 1, row_count + len(resultado_data_publicacao_hoje) + 1)

    # Junção dos dataframes para atualização
    df_proposicoes_update = pd.concat([resultado_ultima_acao, resultado_data_publicacao_hoje], axis=0)

    tramitacoes_update_final = get_tramitacoes(df_proposicoes_update, data_atual)

    resultado_data_publicacao_hoje, tramitacoes_update_final = transform_main(df_proposicoes_update, tramitacoes_update_final, colunas)

    return insert_data(resultado_data_publicacao_hoje, tramitacoes_update_final, database_info)

update_main(database_info=database_info, colunas=colunas, url=url)


def job():
    print("Iniciando o job de atualização às", datetime.now())
    update_main(url, colunas, database_info)
    print("Job de atualização concluído às", datetime.now())

schedule.every().day.at("01:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
