import logging


FORMAT = '[%(name)s] [%(asctime)s] %(levelname)s: %(message)s'
DATEFMT = '%Y-%m-%d %H:%M:%S'

logging.basicConfig(format=FORMAT, datefmt=DATEFMT, level=logging.WARN)
locopy_logger = logging.getLogger('locopy')
locopy_logger.setLevel(logging.WARN)


def get_logger(name: str) -> logging.Logger:
    '''Create logger with basic config'''
    logging.basicConfig(format=FORMAT, datefmt=DATEFMT, level=logging.INFO)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger
