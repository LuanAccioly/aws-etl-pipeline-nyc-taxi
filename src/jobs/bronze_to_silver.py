from pyspark.sql.functions import col, hour, month, when, year
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

from src.common.logger import get_logger

STEP_NAME = "BRONZE_TO_SILVER"
logger = get_logger(__name__, step=STEP_NAME)


def run(spark, settings):
    """
    Executa o job completo de leitura da camada Bronze, limpeza, enriquecimento
    e escrita na camada Silver.
    """
    logger.info("Iniciando o job de transformação da camada Bronze para a Silver.")

    try:
        # Leitura da camada Bronze
        logger.info(f"Lendo dados da camada Bronze de: {settings.BRONZE_S3_PATH}")
        df_bronze = spark.read.option("header", "true").csv(settings.BRONZE_S3_PATH)
        initial_count = df_bronze.count()
        logger.info(
            f"Leitura concluída. Contagem inicial de registros: {initial_count:,}"
        )

        # Conversão de Tipos
        logger.info("Iniciando a conversão de tipos de dados.")
        df_typed = (
            df_bronze.withColumn("VendorID", col("VendorID").cast(IntegerType()))
            .withColumn(
                "tpep_pickup_datetime",
                col("tpep_pickup_datetime").cast(TimestampType()),
            )
            .withColumn(
                "tpep_dropoff_datetime",
                col("tpep_dropoff_datetime").cast(TimestampType()),
            )
            .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
            .withColumn("trip_distance", col("trip_distance").cast(DoubleType()))
            .withColumn("RateCodeID", col("RateCodeID").cast(IntegerType()))
            .withColumn("payment_type", col("payment_type").cast(IntegerType()))
            .withColumn("fare_amount", col("fare_amount").cast(DoubleType()))
            .withColumn("extra", col("extra").cast(DoubleType()))
            .withColumn("mta_tax", col("mta_tax").cast(DoubleType()))
            .withColumn("tip_amount", col("tip_amount").cast(DoubleType()))
            .withColumn("tolls_amount", col("tolls_amount").cast(DoubleType()))
            .withColumn(
                "improvement_surcharge", col("improvement_surcharge").cast(DoubleType())
            )
            .withColumn("total_amount", col("total_amount").cast(DoubleType()))
        )

        # Limpeza de dados
        logger.info("Iniciando a limpeza de dados (duplicados e nulos).")
        df_deduplicated = df_typed.dropDuplicates()

        critical_columns = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "total_amount",
        ]
        df_cleaned = df_deduplicated.dropna(subset=critical_columns)

        count_after_cleaning = df_cleaned.count()
        rows_removed = initial_count - count_after_cleaning
        logger.info(
            f"Limpeza concluída. Total de {rows_removed:,} registros removidos."
        )

        # Enriquecimento
        logger.info("Iniciando o enriquecimento dos dados com novas colunas.")
        df_enriched = df_cleaned.withColumn(
            "trip_duration_minutes",
            (
                col("tpep_dropoff_datetime").cast("long")
                - col("tpep_pickup_datetime").cast("long")
            )
            / 60,
        ).withColumn(
            "day_part",
            when(
                (hour(col("tpep_pickup_datetime")) >= 6)
                & (hour(col("tpep_pickup_datetime")) < 12),
                "morning",
            )
            .when(
                (hour(col("tpep_pickup_datetime")) >= 12)
                & (hour(col("tpep_pickup_datetime")) < 18),
                "afternoon",
            )
            .when(
                (hour(col("tpep_pickup_datetime")) >= 18)
                & (hour(col("tpep_pickup_datetime")) < 23),
                "evening",
            )
            .otherwise("late_night"),
        )

        is_valid_condition = (
            (col("trip_duration_minutes") > 0)
            & (col("passenger_count") > 0)
            & (col("total_amount") > 0)
            & (col("trip_distance") > 0)
        )
        df_final = df_enriched.withColumn("is_valid", is_valid_condition)

        # Adicionar colunas de partição para escrita
        df_for_silver = df_final.withColumn(
            "year", year(col("tpep_pickup_datetime"))
        ).withColumn("month", month(col("tpep_pickup_datetime")))

        # Escrita na camada Silver
        logger.info(
            f"Escrevendo dados limpos na camada Silver em: {settings.SILVER_S3_PATH}"
        )
        (
            df_for_silver.write.mode("overwrite")
            .partitionBy("year", "month")
            .parquet(settings.SILVER_S3_PATH)
        )
        logger.info("Job Bronze -> Silver concluído com sucesso.")

    except Exception as e:
        logger.error(f"Ocorreu um erro no job {STEP_NAME}: {e}", exc_info=True)
        raise


# Bloco para permitir a execução direta do script para testes
if __name__ == "__main__":
    import configs.settings as app_settings
    from src.common.spark import create_spark_session

    spark_session = create_spark_session()
    run(spark_session, app_settings)
