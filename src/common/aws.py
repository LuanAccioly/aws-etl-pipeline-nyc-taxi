import json

import boto3

from src.common.logger import get_logger

# Não precisamos de um STEP_NAME aqui, pois é um módulo de utilidade
logger = get_logger(__name__, step="AWS_COMMON")


def get_secret(secret_name: str, region_name: str = "us-east-1"):
    """
    Busca um segredo do AWS Secrets Manager.

    Args:
        secret_name (str): O nome ou ARN do segredo.
        region_name (str): A região da AWS onde o segredo está.

    Returns:
        dict: O segredo como um dicionário Python.
    """
    logger.info(f"Buscando segredo '{secret_name}' do AWS Secrets Manager.")
    session = boto3.session.Session()  # type: ignore
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        logger.info("Segredo obtido com sucesso.")
        return json.loads(secret)
    except Exception as e:
        logger.error(f"Erro ao buscar o segredo '{secret_name}': {e}", exc_info=True)
        raise e
