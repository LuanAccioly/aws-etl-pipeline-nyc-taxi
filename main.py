import configs.settings as settings
from src.common.logger import get_logger
from src.common.spark import create_rds_spark_session, create_s3_spark_session
from src.jobs import bronze_to_silver, kaggle_to_bronze, silver_to_rds

STEP_NAME = "PIPELINE_ORCHESTRATOR"
logger = get_logger(__name__, step=STEP_NAME)


def main():
    """
    Orquestra a execução do pipeline de ETL, utilizando sessões Spark
    específicas para cada etapa que necessita.
    """
    spark = None
    try:
        logger.info("Iniciando o pipeline de dados NYC Taxi...")

        # # --- Job 1: Ingestão (Não precisa de Spark) ---
        # logger.info("--- Executando Job 1: Kaggle para Bronze ---")
        # kaggle_to_bronze.run(settings)
        # logger.info("--- Job 1 concluído ---")

        # # --- Job 2: Transformação (Usa a sessão S3) ---
        # logger.info("--- Executando Job 2: Bronze para Silver ---")
        # spark = create_s3_spark_session()
        # bronze_to_silver.run(spark, settings)
        # logger.info("Parando a sessão Spark S3...")
        # spark.stop()
        # spark = None  # Garante que a variável está limpa
        # logger.info("--- Job 2 concluído ---")

        # --- Job 3: Carga (Usa a sessão RDS) ---
        logger.info("--- Executando Job 3: Silver para RDS ---")
        spark = create_rds_spark_session()
        silver_to_rds.run(spark, settings)
        logger.info("--- Job 3 concluído ---")

        logger.info("Pipeline de dados NYC Taxi concluído com sucesso!")

    except Exception as e:
        logger.error(f"O pipeline falhou. Erro: {e}", exc_info=True)
    finally:
        if spark:
            logger.info("Encerrando a sessão Spark final.")
            spark.stop()


if __name__ == "__main__":
    main()
