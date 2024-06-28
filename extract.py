import requests
import time
import pandas as pd

url = 'https://dadosabertos.almg.gov.br/ws/proposicoes/pesquisa/direcionada'


def extract_data(base_url, ano=2023, tp=100, formato='json', order=3):
    global response
    todas_proposicoes = []
    pagina = 1
    max_retries = 5

    while True:
        query = {
            'tp': tp,
            'formato': formato,
            'ano': ano,
            'ord': order,
            'p': pagina  # Adiciona o número da página
        }

        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(base_url, params=query)
                response.raise_for_status()

                data = response.json()

                lista_proposicoes = data.get('resultado', {}).get('listaItem', [])
                if not lista_proposicoes:
                    return pd.DataFrame(todas_proposicoes)

                todas_proposicoes.extend(lista_proposicoes)
                pagina += 1

                print(f"Página {pagina - 1} processada, {len(lista_proposicoes)} proposições adicionadas.")

                tamanho_pagina = data.get('resultado', {}).get('tamanhoPagina', 100)
                if len(lista_proposicoes) < tamanho_pagina:
                    return pd.DataFrame(todas_proposicoes)

                time.sleep(1)
                break

            except requests.exceptions.HTTPError:
                if response.status_code in [500, 502, 503, 504, 429]:
                    retries += 1
                    wait = 2 ** retries
                    print(
                        f"Erro {response.status_code}. Aguardando {wait} segundos antes de tentar novamente."
                    )
                    time.sleep(wait)
                else:
                    raise


def extract_main(api_url):
    df_proposicoes = extract_data(api_url)
    df_proposicoes = df_proposicoes.reset_index(names='id')

    tramitacoes_list = []

    for _, row in df_proposicoes.iterrows():
        numero = row['id']
        for passo in row['listaHistoricoTramitacoes']:
            add_row = {
                'createdAt': passo['data'],
                'description': passo['historico'],
                'local': passo['local'],
                'propositionId': numero
            }
            tramitacoes_list.append(add_row)

    df_tramitacoes = pd.DataFrame(tramitacoes_list, columns=['createdAt', 'description', 'local', 'propositionId'])

    df_tramitacoes = df_tramitacoes.reset_index(names='id')

    return df_proposicoes, df_tramitacoes

proposicoes_final, tramitacoes_final = extract_main(url)

proposicoes_final.to_csv('proposicoes.csv', index=False)
tramitacoes_final.to_csv('tramitacoes.csv', index=False)