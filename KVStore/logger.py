import logging.config
import os

def setup_logger():

    stream = 'ext://sys.stderr'

    config_dict = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': "%(asctime)s [%(levelname)s] %(filename)s:%(lineno)s -- %(message)s"
            },
        },
        'handlers': {
            'console_handler': {
                'level': "INFO",
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': stream
            },
            'file_handler': {
                'level': "INFO",
                'formatter': 'standard',
                'class': 'logging.FileHandler',
                'filename': os.devnull,
                'mode': 'a',
            },
        },
        'loggers': {
            'KVStore': {
                'handlers': ['console_handler'],
                'level': "INFO",
                'propagate': False
            },
        }
    }

    logging.config.dictConfig(config_dict)
