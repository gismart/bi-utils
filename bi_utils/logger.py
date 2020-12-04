import locopy
import logging


FORMAT = '[%(name)s] [%(asctime)s] %(levelname)s: %(message)s'
DATEFMT = '%Y-%m-%d %H:%M:%S'

locopy_logger = locopy.logger.get_logger()
formatter = logging.Formatter(FORMAT, DATEFMT)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
locopy_logger.handlers.clear()
locopy_logger.addHandler(handler)
locopy_logger.setLevel(logging.WARN)


def get_logger(name: str) -> logging.Logger:
    '''Create logger with basic config'''
    logging.basicConfig(format=FORMAT, datefmt=DATEFMT, level=logging.INFO)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger
