import os

import boto3
import kagglehub

from src.common.logger import get_logger

STEP_NAME = "KAGGLE_TO_BRONZE"
logger = get_logger(__name__, step=STEP_NAME)


def run(settings):
    """
    Executa o download do dataset do Kaggle e o upload para a camada Bronze no S3.
    """
    logger.info("Iniciando o job: Download do Kaggle para a camada Bronze.")

    try:
        logger.info("Baixando o dataset do KaggleHub...")
        path = kagglehub.dataset_download("elemento/nyc-yellow-taxi-trip-data")
        logger.info(f"Dataset baixado com sucesso no diretório local: {path}")

        s3 = boto3.client("s3")

        logger.info(f"Iniciando upload para o bucket '{settings.S3_BUCKET_NAME}'...")
        for root, _, files in os.walk(path):
            for file in files:
                local_file_path = os.path.join(root, file)

                # Constrói a chave S3 usando o prefixo das configurações
                relative_path = os.path.relpath(local_file_path, path)
                s3_key = f"{settings.BRONZE_S3_PREFIX}/{relative_path}"

                s3.upload_file(local_file_path, settings.S3_BUCKET_NAME, s3_key)
                logger.info(
                    f"Upload de '{file}' para 's3://{settings.S3_BUCKET_NAME}/{s3_key}' concluído."
                )

        logger.info(
            "Upload de todos os arquivos para a camada Bronze finalizado com sucesso."
        )

    except Exception as e:
        logger.error(f"Ocorreu um erro no job {STEP_NAME}: {e}", exc_info=True)
        raise


# Bloco para permitir a execução direta do script para testes
if __name__ == "__main__":
    import configs.settings as app_settings

    run(app_settings)
