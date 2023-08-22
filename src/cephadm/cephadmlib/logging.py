# logging.py - cephadm specific logging behavior

import enum
import logging
import logging.config
import os
import sys

from typing import List, Any, Dict, cast

from .context import CephadmContext
from .constants import QUIET_LOG_LEVEL, LOG_DIR


class _ExcludeErrorsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        """Only lets through log messages with log level below WARNING ."""
        return record.levelno < logging.WARNING


class _termcolors(str, enum.Enum):
    yellow = '\033[93m'
    red = '\033[31m'
    end = '\033[0m'


class Highlight(enum.Enum):
    FAILURE = 1
    WARNING = 2

    def extra(self) -> Dict[str, 'Highlight']:
        """Return logging extra for the current kind of highlight."""
        return {'highlight': self}

    def _highlight(self, s: str) -> str:
        color = {
            self.FAILURE: _termcolors.red.value,
            self.WARNING: _termcolors.yellow.value,
        }[
            cast(int, self)
        ]  # cast works around mypy confusion wrt enums
        return f'{color}{s}{_termcolors.end.value}'


class _Colorizer(logging.Formatter):
    def format(self, record: Any) -> str:
        res = super().format(record)
        highlight = getattr(record, 'highlight', None)
        # checking sys.stderr here is a bit dirty but we know exactly
        # how _Colorizer will be used, so it works for now
        if highlight is not None and sys.stderr.isatty():
            res = highlight._highlight(res)
        return res


_common_formatters = {
    'cephadm': {
        'format': '%(asctime)s %(thread)x %(levelname)s %(message)s',
    },
    'colorized': {
        '()': _Colorizer,
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
    },
}


# When cephadm is used as standard binary (bootstrap, rm-cluster, etc) we use:
# stdout: for debug and info
# stderr: for errors and warnings
_interactive_logging_config = {
    'version': 1,
    'filters': {
        'exclude_errors': {
            '()': _ExcludeErrorsFilter,
        },
    },
    'disable_existing_loggers': True,
    'formatters': _common_formatters,
    'handlers': {
        'console_stdout': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'filters': ['exclude_errors'],
            'stream': sys.stdout,
        },
        'console_stderr': {
            'level': 'WARNING',
            'class': 'logging.StreamHandler',
            'stream': sys.stderr,
            'formatter': 'colorized',
        },
        'log_file': _log_file_handler,
    },
    'loggers': {
        '': {
            'level': 'DEBUG',
            'handlers': ['console_stdout', 'console_stderr', 'log_file'],
        }
    },
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
