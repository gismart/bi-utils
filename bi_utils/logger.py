import logging


FORMAT = "[%(name)s] [%(asctime)s] %(levelname)s: %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"


def setup_root_logger(name: str = "bi_utils", level: str = "info") -> None:
    logging.basicConfig(format=FORMAT, datefmt=DATEFMT, level=logging.INFO)
    logger = logging.getLogger(name)
    logger.setLevel(level.upper())
    if logger.handlers:
        logger.handlers.clear()
    formatter = logging.Formatter(FORMAT, DATEFMT)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False


setup_root_logger()
setup_root_logger("locopy", level="warning")
