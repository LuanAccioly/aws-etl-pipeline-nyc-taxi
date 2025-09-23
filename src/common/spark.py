from pyspark.sql import SparkSession


def create_s3_spark_session():
    """
    Cria uma SparkSession otimizada para a etapa Bronze -> Silver.
    Usa a configuração com hadoop-aws:3.3.1.
    """
    print("Configurando a Spark Session para operações S3 (Bronze -> Silver)...")
    spark = (
        SparkSession.builder.appName("BronzeToSilver")
        .config("spark.driver.memory", "12g")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
        .getOrCreate()
    )
    print("Sessão Spark S3 configurada.")
    return spark


def create_rds_spark_session():
    """
    Cria uma SparkSession otimizada para a etapa Silver -> RDS.
    Usa a configuração com hadoop-aws:3.2.0 e o driver do PostgreSQL.
    """
    print("Configurando a Spark Session para carga no RDS (Silver -> RDS)...")
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
    print("Sessão Spark RDS configurada.")
    return spark
