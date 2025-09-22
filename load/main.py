import json

import boto3
from pyspark.sql import SparkSession


# --- Etapa 1: Obter Credenciais do Banco de Dados do Secrets Manager ---
def get_rds_credentials():
    secret_name = "aws-etl-pipeline-nyc-taxi/postgres"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        return json.loads(secret)
    except Exception as e:
        print(f"Erro ao buscar o secret: {e}")
        raise e


# --- Etapa 2: Configurar a Sessão Spark Corretamente ---
print("Configurando a sessão Spark para carga no RDS...")
db_credentials = get_rds_credentials()

spark = (
    SparkSession.builder.appName("LoadToRDS")
    .config("spark.driver.memory", "12g")
    # Linha ÚNICA e CONSOLIDADA com todos os pacotes necessários nas versões estáveis
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.874,org.postgresql:postgresql:42.5.0",
    )
    # Configuração explícita do provedor de credenciais para evitar outros erros
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")  # 30s
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")  # 60s
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000")  # 200s
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")  # 24h em ms
    .getOrCreate()
)

# --- Etapa 3: Ler os Dados da Camada Silver do S3 ---
print("Lendo dados particionados da camada Silver...")
bucket_name = "aws-etl-pipeline-nyc-taxi"
# CAMINHO CORRIGIDO PARA A CAMADA SILVER 👇
silver_s3_path = f"s3a://{bucket_name}/silver/nyc_taxi_trips/"

# O Spark lerá todos os arquivos Parquet da estrutura particionada automaticamente
df_silver = spark.read.parquet(silver_s3_path)

# --- Etapa 4: Configurar a Conexão JDBC e Escrever no RDS ---
jdbc_url = (
    f"jdbc:postgresql://{db_credentials['host']}:{db_credentials['port']}/postgres"
)

connection_properties = {
    "user": db_credentials["username"],
    "password": db_credentials["password"],
    "driver": "org.postgresql.Driver",
}

# NOME DA TABELA AJUSTADO PARA REFLETIR A ORIGEM DOS DADOS 👇
table_name = "silver_nyc_taxi_trips"

# Antes de escrever, garantimos que a tabela já existe no PostgreSQL com o schema correto.
print(
    f"Iniciando a escrita de {df_silver.count():,} registros na tabela '{table_name}'..."
)

(
    df_silver.write.mode("overwrite").jdbc(
        url=jdbc_url, table=table_name, properties=connection_properties
    )
)

print("Carga no RDS PostgreSQL concluída com sucesso!")
spark.stop()
