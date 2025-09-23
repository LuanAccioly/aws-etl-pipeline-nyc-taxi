from src.common.aws import get_secret
from src.common.logger import get_logger

STEP_NAME = "SILVER_TO_RDS"
logger = get_logger(__name__, step=STEP_NAME)


def run(spark, settings):
    """
    Executa o job de carga dos dados da camada Silver (S3) para uma tabela
    no RDS PostgreSQL.
    """
    logger.info("Iniciando o job: Carga da camada Silver para o RDS.")

    try:
        # Etapa 1: Obter credenciais do banco de dados de forma segura
        db_credentials = get_secret(settings.RDS_SECRET_NAME, settings.AWS_REGION)

        # Etapa 2: Ler os dados da camada Silver do S3
        logger.info(
            f"Lendo dados particionados da camada Silver de: {settings.SILVER_S3_PATH}"
        )
        df_silver = spark.read.parquet(settings.SILVER_S3_PATH)

        # Opcional: Cache para acelerar a contagem e a escrita
        df_silver.cache()
        record_count = df_silver.count()
        logger.info(f"Leitura concluída. {record_count:,} registros serão carregados.")

        # Etapa 3: Configurar a conexão JDBC e escrever no RDS
        jdbc_url = f"jdbc:postgresql://{db_credentials['host']}:{db_credentials['port']}/postgres"

        connection_properties = {
            "user": db_credentials["username"],
            "password": db_credentials["password"],
            "driver": "org.postgresql.Driver",
        }

        logger.info(
            f"Iniciando a escrita na tabela '{settings.RDS_TABLE_NAME}' do RDS."
        )

        (
            df_silver.write.mode("overwrite").jdbc(
                url=jdbc_url,
                table=settings.RDS_TABLE_NAME,
                properties=connection_properties,
            )
        )

        # Limpar o cache
        df_silver.unpersist()

        logger.info("Carga no RDS PostgreSQL concluída com sucesso.")

    except Exception as e:
        logger.error(f"Ocorreu um erro no job {STEP_NAME}: {e}", exc_info=True)
        raise


# Bloco para permitir a execução direta do script para testes
if __name__ == "__main__":
    import configs.settings as app_settings
    from src.common.spark import create_rds_spark_session

    spark_session = create_rds_spark_session()
    run(spark_session, app_settings)
