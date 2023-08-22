# logging.py - cephadm specific logging behavior

import logging
import logging.config
import os
import sys

from typing import List

from .context import CephadmContext
from .constants import QUIET_LOG_LEVEL, LOG_DIR


class _ExcludeErrorsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        """Only lets through log messages with log level below WARNING ."""
        return record.levelno < logging.WARNING


_common_formatters = {
    'cephadm': {
        'format': '%(asctime)s %(thread)x %(levelname)s %(message)s'
    },
}


_log_file_handler = {
    'level': 'DEBUG',
    'class': 'logging.handlers.WatchedFileHandler',
    'formatter': 'cephadm',
    'filename': '%s/cephadm.log' % LOG_DIR,
}


# During normal cephadm operations (cephadm ls, gather-facts, etc ) we use:
# stdout: for JSON output only
# stderr: for error, debug, info, etc
_logging_config = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': _common_formatters,
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
        },
        'log_file': _log_file_handler,
    },
    'loggers': {
        '': {
            'level': 'DEBUG',
            'handlers': ['console', 'log_file'],
        }
    }
}


# When cephadm is used as standard binary (bootstrap, rm-cluster, etc) we use:
# stdout: for debug and info
# stderr: for errors and warnings
_interactive_logging_config = {
    'version': 1,
    'filters': {
        'exclude_errors': {
            '()': _ExcludeErrorsFilter
        }
    },
    'disable_existing_loggers': True,
    'formatters': _common_formatters,
    'handlers': {
        'console_stdout': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'filters': ['exclude_errors'],
            'stream': sys.stdout
        },
        'console_stderr': {
            'level': 'WARNING',
            'class': 'logging.StreamHandler',
            'stream': sys.stderr
        },
        'log_file': _log_file_handler,
    },
    'loggers': {
        '': {
            'level': 'DEBUG',
            'handlers': ['console_stdout', 'console_stderr', 'log_file'],
        }
    }
}


_logrotate_data = """# created by cephadm
/var/log/ceph/cephadm.log {
    rotate 7
    daily
    compress
    missingok
    notifempty
    su root root
}
"""


def cephadm_init_logging(
    ctx: CephadmContext, logger: logging.Logger, args: List[str]
) -> None:
    """Configure the logging for cephadm as well as updating the system
    to have the expected log dir and logrotate configuration.
    """
    logging.addLevelName(QUIET_LOG_LEVEL, 'QUIET')
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    operations = ['bootstrap', 'rm-cluster']
    if any(op in args for op in operations):
        logging.config.dictConfig(_interactive_logging_config)
    else:
        logging.config.dictConfig(_logging_config)

    logger.setLevel(QUIET_LOG_LEVEL)

    if not os.path.exists(ctx.logrotate_dir + '/cephadm'):
        with open(ctx.logrotate_dir + '/cephadm', 'w') as f:
            f.write(_logrotate_data)

    if ctx.verbose:
        for handler in logger.handlers:
            if handler.name in ['console', 'log_file', 'console_stdout']:
                handler.setLevel(QUIET_LOG_LEVEL)
    logger.debug('%s\ncephadm %s' % ('-' * 80, args))
