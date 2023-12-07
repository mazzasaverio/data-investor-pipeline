import sys


from loguru import logger


def configure_logger():
    """
    Configure the Loguru logger settings.
    """
    logger.remove()
    logger.add(
        lambda msg: sys.stderr.write(msg),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {function}:{line} - {message}",
    )
    logger.add(
        "./logs/data_pipeline.log", rotation="1 day", level="INFO", serialize=True
    )
