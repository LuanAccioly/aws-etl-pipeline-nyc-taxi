import logging

import colorlog


class StepFilter(logging.Filter):
    """
    Filtro de logging para injetar o nome da etapa (step) em cada registro de log.
    """

    def __init__(self, step):
        super().__init__()
        self.step = step

    def filter(self, record):
        record.step = self.step
        return True


def get_logger(name: str, step: str):
    """
    Cria e configura um logger colorido e contextual.

    Args:
        name (str): O nome do logger, geralmente __name__ do módulo que o chama.
        step (str): O nome da etapa do pipeline (ex: "BRONZE_TO_SILVER").

    Returns:
        logging.Logger: Uma instância de logger configurada.
    """
    # Cria o logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Evita a duplicação de handlers se a função for chamada múltiplas vezes
    if logger.hasHandlers():
        logger.handlers.clear()

    # Cria um handler de console com cores
    handler = colorlog.StreamHandler()

    # Define o formato do log
    log_format = (
        "[%(step)s] - %(asctime)s - "
        "%(log_color)s%(levelname)-8s%(reset)s - "
        "[%(name)s] - %(message)s"
    )

    # Define as cores para cada nível de log
    formatter = colorlog.ColoredFormatter(
        log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    )

    handler.setFormatter(formatter)

    # Adiciona o filtro e o handler ao logger
    logger.addFilter(StepFilter(step))
    logger.addHandler(handler)

    return logger
