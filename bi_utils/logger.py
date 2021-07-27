import logging


FORMAT = '[%(name)s] [%(asctime)s] %(levelname)s: %(message)s'
DATEFMT = '%Y-%m-%d %H:%M:%S'


def get_logger(name: str, level: str = 'info') -> logging.Logger:
    '''Create logger with basic config'''
    logger = logging.getLogger(name)
    logger.setLevel(level.upper())
    return logger


def config_root_logger(logger: logging.Logger) -> None:
    if logger.handlers:
        logger.handlers.clear()
    formatter = logging.Formatter(FORMAT, DATEFMT)
    handler = logging.StreamHandler()
    handler.setLevel(logger.level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False


config_root_logger(get_logger('bi_utils'))
config_root_logger(get_logger('locopy', level='warning'))
