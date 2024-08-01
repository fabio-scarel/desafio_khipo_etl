import pandas as pd

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

def remove_nova_linha(texto):
    return texto.replace('\n', ' ')


def remove_espacos_extras(text):
    return ' '.join(text.split())


def remove_partidos(remove_partido, lista_partidos):
    for partido in lista_partidos:
        remove_partido = str(remove_partido).replace(partido, '')
    return remove_partido.strip()


def select_columns(all_proposicoes, colunas_desejadas):
    """
    Select the columns you want from the original database
    """

    proposicoes = all_proposicoes[colunas_desejadas]
    proposicoes.columns = ['id', 'author', 'presentationdate', 'ementa', 'regime', 'situation',
                            'propositiontype', 'number']

    return proposicoes


def add_city_state(proposicoes):
    """
    Add the location from the information
    """
    proposicoes = proposicoes.assign(city='Belo Horizonte', state='Minas Gerais')

    return proposicoes


def transform_ementa_year(proposicoes):
    """
    Given the Presentation Date on the table, set the year of the proposition or inform that this information
    is not available. Also removes the paragraphs " \ n " present on the text.
    """
    
    proposicoes.loc[:, 'year'] = proposicoes.loc[:, 'presentationdate'].apply(
        lambda year: str(year)[:4])

    proposicoes.loc[:, 'ementa'] = proposicoes.loc[:, 'ementa'].apply(
        lambda ementa: 'Informação Não Disponível' if type(ementa) != str else ementa)

    proposicoes.loc[:, 'ementa'] = proposicoes.loc[:, 'ementa'].apply(
        lambda ementa: ementa.replace('\n', ' '))

    return proposicoes


def transform_author(proposicoes):
    """
    Cleans up the information from the author, removing their party, the spaces and the paragraphs.
    """

    palavras = []
    for autor in proposicoes['author'].apply(lambda autor_partidos: str(autor_partidos).split(' ')):
        palavras.append(autor[-1])
    palavras = list(set(palavras))

    partidos = []
    for palavra in palavras:
        if palavra.upper() == palavra or palavra == 'PCdoB':
            partidos.append(palavra)

    partidos.sort(reverse=True)

    proposicoes.loc[:, 'author'] = proposicoes['author'].apply(
        remove_partidos, lista_partidos=partidos)

    proposicoes.loc[:, 'author'] = proposicoes['author'].apply(
        lambda author: author.replace('\n', ', '))

    proposicoes.loc[:, 'author'] = proposicoes['author'].apply(
        lambda author: author.replace('PRD', ', '))

    proposicoes.loc[:, 'author'] = proposicoes['author'].apply(
        remove_espacos_extras)

    return proposicoes


def transform_tramitacoes(tramitacoes):
    """
    Removes the paragraphs present on the descriptions.
    """

    tramitacoes.loc[:, 'description'] = tramitacoes['description'].apply(
        lambda description: description.replace('\n', ' '))

    tramitacoes = tramitacoes.drop_duplicates(subset=['createdat', 'description', 'local', 'propositionid'])

    return tramitacoes


def transform_data(df_proposicoes, colunas, df_tramitacoes):
    
    proposicoes_0 = select_columns(df_proposicoes, colunas)
    proposicoes_1 = add_city_state(proposicoes_0)
    proposicoes_2 = transform_ementa_year(proposicoes_1)
    proposicoes_final = transform_author(proposicoes_2)
    tramitacoes_final = transform_tramitacoes(df_tramitacoes)

    return proposicoes_final, tramitacoes_final

if __name__ == '__main__':

    df_proposicoes = pd.read_csv('proposicoes.csv')
    df_tramitacoes = pd.read_csv('tramitacoes.csv')

    proposicoes_final, tramitacoes_final = transform_data(df_proposicoes, colunas, df_tramitacoes)

    proposicoes_final.to_csv('proposicoes_clean.csv', index=False)
    tramitacoes_final.to_csv('tramitacoes.csv', index=False)
