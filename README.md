# Pipeline de Dados ETL: Corridas de Táxi de NYC

![Status](https://img.shields.io/badge/status-conclu%C3%ADdo-brightgreen)

## 1. Visão Geral

Este projeto implementa um pipeline de dados ETL completo para processar o dataset público de corridas de táxi de Nova York. O objetivo é demonstrar as melhores práticas de engenharia de dados, utilizando PySpark e serviços da nuvem AWS para transformar e carregar milhões de registros de forma eficiente e escalável.

O pipeline segue a arquitetura de medalhão (Bronze, Silver, Gold) para garantir a qualidade e a rastreabilidade dos dados em cada etapa do processo.

## 2. Arquitetura da Solução

O fluxo de dados foi desenhado para ser modular e robusto:

1.  **Ingestão (Bronze):** Dados brutos são extraídos do Kaggle e carregados no Amazon S3 (camada Bronze).
2.  **Transformação (Silver):** Um job PySpark lê os dados brutos, aplica um rigoroso processo de limpeza e enriquecimento, e salva o resultado em formato Parquet particionado no S3 (camada Silver).
3.  **Carga (Load):** Os dados limpos da camada Silver são carregados em uma tabela em um banco de dados AWS RDS PostgreSQL para consumo analítico.
4.  **Governança:** O AWS Glue Data Catalog é utilizado para catalogar os schemas das tabelas, permitindo que sejam descobertas e consultadas por outras ferramentas como o Amazon Athena. As credenciais são gerenciadas de forma segura pelo AWS Secrets Manager.

## 3. Estrutura do Projeto

O código foi organizado em uma estrutura modular para promover reusabilidade e clareza:

```
aws-etl-pipeline-nyc-taxi/
├── configs/                # Configuração centralizada para o pipeline
│   └── settings.py
├── src/                    # Código fonte da aplicação
│   ├── common/             # Módulos reutilizáveis (Spark, Logger, AWS)
│   ├── jobs/               # Lógica de cada etapa do ETL
├── notebooks/              # Notebooks para exploração de dados
├── main.py                 # Orquestrador principal do pipeline
├── .env                    # Arquivo para variáveis de ambiente (credenciais)
└── requirements.txt        # Dependências do projeto
```

## 4. Configuração do Ambiente

Siga os passos abaixo para configurar e executar o projeto.

### 4.1. Pré-requisitos

* Conta na AWS
* Python 3.10+
* [AWS CLI](https://aws.amazon.com/cli/) instalado

### 4.2. Instalação

1.  **Clone o Repositório:**
    ```bash
    git clone https://github.com/LuanAccioly/aws-etl-pipeline-nyc-taxi.git
    cd aws-etl-pipeline-nyc-taxi
    ```

2.  **Crie e Ative o Ambiente Virtual:**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Instale as Dependências:**
    ```bash
    pip install -r requirements.txt
    ```

## 4. Como Executar o Pipeline

Para executar o pipeline completo, do download dos dados à carga no banco de dados, utilize o orquestrador principal. Certifique-se de estar na raiz do projeto com o ambiente virtual ativado.

```bash
python main.py
```

## 5. Decisões de Modelagem e Particionamento

Esta seção detalha as escolhas técnicas feitas durante a etapa de transformação, conforme solicitado no desafio.

### 5.1. Colunas Derivadas (Enriquecimento)

Para agregar valor analítico ao dataset, as seguintes colunas foram criadas:

* `trip_duration_minutes` (Double): Calcula a duração total da viagem em minutos. É uma métrica fundamental para análises de eficiência e para identificar outliers.
* `day_part` (String): Classifica a hora de início da viagem em períodos (`morning`, `afternoon`, `evening`, `late_night`), permitindo a análise de padrões de demanda por período do dia.
* `is_valid` (Boolean): Um campo booleano que aplica um conjunto de regras de negócio para validar a integridade de cada registro (ex: duração e custo positivos). Isso garante que as análises sejam feitas apenas sobre dados confiáveis.

### 5.2. Estratégia de Particionamento na Camada Silver

A camada Silver, que armazena os dados transacionais limpos, foi **particionada por ano e mês** (`/year=YYYY/month=MM/`).

* **Justificativa:** Esta é uma das otimizações de performance mais importantes para um data lake. Ao particionar por data, consultas que filtram por um período específico (ex: "apenas corridas de Janeiro de 2015") se tornam ordens de magnitude mais rápidas e baratas. As engines de query (Spark, Athena, Redshift Spectrum) utilizam a técnica de *Partition Pruning*, lendo apenas os arquivos das pastas relevantes e ignorando terabytes de dados desnecessários.

### 5.3. Outras Decisões

* **Formato Parquet:** Adotado para as camadas Silver e Gold por sua eficiência de compressão e por ser um formato colunar, o que acelera consultas analíticas.
* **Limpeza de Dados:** O pipeline realiza um tratamento robusto de tipos de dados, valores nulos e registros duplicados para garantir a qualidade do dado final.

## 6. Validação dos Dados

Após a carga no RDS, a integridade dos dados pode ser validada com uma consulta SQL, conforme solicitado. A query abaixo, por exemplo, verifica a contagem de registros válidos por mês e ano na tabela final.

```sql
SELECT
    EXTRACT(YEAR FROM tpep_pickup_datetime) AS trip_year,
    EXTRACT(MONTH FROM tpep_pickup_datetime) AS trip_month,
    is_valid,
    COUNT(*) AS total_trips
FROM
    silver_nyc_taxi_trips
GROUP BY
    1, 2, 3
ORDER BY
    1, 2;
```