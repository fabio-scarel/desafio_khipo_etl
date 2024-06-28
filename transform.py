import pandas as pd


def remove_nova_linha(texto):
    return texto.replace('\n', ' ')


def remove_espacos_extras(text):
    return ' '.join(text.split())


def remove_partidos(remove_partido, lista_partidos):
    for partido in lista_partidos:
        remove_partido = str(remove_partido).replace(partido, '')
    return remove_partido.strip()


df_proposicoes = pd.read_csv('proposicoes.csv')
df_tramitacoes = pd.read_csv('tramitacoes.csv')

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


def transform_main(all_proposicoes, tramitacoes, colunas_desejadas):
    proposicoes_tratadas = all_proposicoes[colunas_desejadas]
    proposicoes_tratadas.columns = ['id', 'author', 'presentationDate', 'ementa', 'regime', 'situation',
                                    'propositionType', 'number']

    proposicoes_tratadas['city'] = 'Belo Horizonte'
    proposicoes_tratadas['state'] = 'Minas Gerais'

    proposicoes_tratadas['year'] = proposicoes_tratadas['presentationDate'].apply(
        lambda year: str(year)[:4])

    proposicoes_tratadas['ementa'] = proposicoes_tratadas['ementa'].apply(
        lambda ementa: 'Informação Não Disponível' if type(ementa) != str else ementa)

    proposicoes_tratadas['ementa'] = proposicoes_tratadas['ementa'].apply(
        lambda ementa: ementa.replace('\n', ' '))

    palavras = []
    for autor in proposicoes_tratadas['author'].apply(lambda autor_partidos: str(autor_partidos).split(' ')):
        palavras.append(autor[-1])
    palavras = list(set(palavras))

    partidos = []
    for palavra in palavras:
        if palavra.upper() == palavra or palavra == 'PCdoB':
            partidos.append(palavra)

    partidos.sort(reverse=True)

    proposicoes_tratadas['author'] = proposicoes_tratadas['author'].apply(remove_partidos, lista_partidos=partidos)

    proposicoes_tratadas['author'] = proposicoes_tratadas['author'].apply(
        lambda author: author.replace('\n', ', '))

    proposicoes_tratadas['author'] = proposicoes_tratadas['author'].apply(
        lambda author: author.replace('PRD', ', '))

    proposicoes_tratadas['author'] = proposicoes_tratadas['author'].apply(remove_espacos_extras)

    tramitacoes['description'] = tramitacoes['description'].apply(
        lambda description: description.replace('\n', ' '))

    return proposicoes_tratadas, tramitacoes


proposicoes_final, tramitacoes_final = transform_main(df_proposicoes, df_tramitacoes, colunas)

proposicoes_final.to_csv('proposicoes_clean.csv', index=False)
tramitacoes_final.to_csv('tramitacoes.csv', index=False)
