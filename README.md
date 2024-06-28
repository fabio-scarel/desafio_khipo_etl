# Desafio de Engenharia de Dados | ETL de Proposições Legislativas

O projeto Desafio_Eng_Dados foi desenvolvido para extrair, transformar e carregar dados relacionados a proposições legislativas de uma API externa para um banco de dados PostgreSQL. Este projeto utiliza Docker e Docker Compose para gerenciar o ambiente da aplicação e suas dependências.

## Estrutura do Projeto

Desafio_Eng_Dados/
│
├── app/
│   ├── daily_update.py
│   ├── extract.py
│   ├── load.py
│   ├── transform.py
│   ├── proposicoes.csv
│   ├── proposicoes_clean.csv
│   ├── tramitacoes.csv
│
├── test/
│   ├── tests.py
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md

## Executando o Projeto
Pré-requisitos
Certifique-se de ter o Docker e o Docker Compose instalados em sua máquina.

### Construindo e Executando os Serviços

#### Para construir e executar os contêineres Docker, execute o seguinte comando no diretório raiz do projeto:
docker-compose up --build

#### Parando os Serviços
Para parar os contêineres em execução, use:
docker-compose down

## Arquivos e Scripts
#### daily_update.py
Este script é o ponto de entrada principal para a aplicação. Ele coordena a extração, transformação e carregamento dos dados.

#### extract.py
Contém a lógica para extrair dados da API externa.

#### transform.py
Contém a lógica para transformar os dados extraídos no formato necessário para o carregamento.

#### load.py
Contém a lógica para carregar os dados transformados no banco de dados PostgreSQL.

#### Os arquivos csv servem para transferir as tabelas entre os scripts python

#### tests.py
O teste verifica a presença de registros duplicados no banco de dados PostgreSQL.
