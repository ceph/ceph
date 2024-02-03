# logging.py - cephadm specific logging behavior

import enum
import logging
import logging.config
import logging.handlers
import os
import sys

from typing import List, Any, Dict, Optional, cast

from .context import CephadmContext
from .constants import QUIET_LOG_LEVEL, LOG_DIR

from cephadmlib.file_utils import write_new

from cephadmlib import templating


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


class LogDestination(str, enum.Enum):
    file = 'log_file'
    syslog = 'syslog'


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

_syslog_handler = {
    'level': 'DEBUG',
    'class': 'logging.handlers.SysLogHandler',
    'formatter': 'cephadm',
    'address': '/dev/log',
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
        'syslog': _syslog_handler,
    },
    'loggers': {
        '': {
            'level': 'DEBUG',
            'handlers': ['console'],
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
        }
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
        'syslog': _syslog_handler,
    },
    'loggers': {
        '': {
            'level': 'DEBUG',
            'handlers': ['console_stdout', 'console_stderr'],
        }
    },
}


_VERBOSE_HANDLERS = [
    'console',
    'console_stdout',
    LogDestination.file.value,
    LogDestination.syslog.value,
]


_INTERACTIVE_CMDS = ['bootstrap', 'rm-cluster']


def _copy(obj: Any) -> Any:
    """Recursively copy mutable items in the logging config dictionaries."""
    # copy.deepcopy fails to pickle the config dicts (sys.stderr, etc)
    # so it's either implement our own basic recursive copy or allow
    # the global objects to be mutated by _complete_logging_config
    if isinstance(obj, dict):
        return {k: _copy(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_copy(v) for v in obj]
    return obj


def _complete_logging_config(
    interactive: bool, destinations: Optional[List[str]]
) -> Dict[str, Any]:
    """Return a logging configuration dict, based on the runtime parameters
    cephadm was invoked with.
    """
    # Use _copy to avoid mutating the global dicts
    lc = _copy(_logging_config)
    if interactive:
        lc = _copy(_interactive_logging_config)

    handlers = lc['loggers']['']['handlers']
    if not destinations:
        handlers.append(LogDestination.file.value)
    for dest in destinations or []:
        handlers.append(LogDestination[dest])

    return lc


def cephadm_init_logging(
    ctx: CephadmContext, logger: logging.Logger, args: List[str]
) -> None:
    """Configure the logging for cephadm as well as updating the system
    to have the expected log dir and logrotate configuration.

    The context's log_dest attribute, if not None, determines what
    persistent logging destination to use. The LogDestination
    enum provides valid destination values.
    """
    logging.addLevelName(QUIET_LOG_LEVEL, 'QUIET')
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    lc = _complete_logging_config(
        any(op in args for op in _INTERACTIVE_CMDS),
        getattr(ctx, 'log_dest', None),
    )
    logging.config.dictConfig(lc)

    logger.setLevel(QUIET_LOG_LEVEL)

    write_cephadm_logrotate_config(ctx)

    for handler in logger.handlers:
        # the following little hack ensures that no matter how cephadm is named
        # (eg. suffixed by a hash when copied by the mgr) we set a consistent
        # syslog identifier. This way one can do things like run
        # `journalctl -t cephadm`.
        if handler.name == LogDestination.syslog:
            # the space after the colon in the ident is significant!
            cast(logging.handlers.SysLogHandler, handler).ident = 'cephadm: '
        # set specific handlers to log extra level of detail when the verbose
        # option is set
        if ctx.verbose and handler.name in _VERBOSE_HANDLERS:
            handler.setLevel(QUIET_LOG_LEVEL)
    logger.debug('%s\ncephadm %s' % ('-' * 80, args))


def write_cephadm_logrotate_config(ctx: CephadmContext) -> None:
    if not os.path.exists(ctx.logrotate_dir + '/cephadm'):
        with open(ctx.logrotate_dir + '/cephadm', 'w') as f:
            cephadm_logrotate_config = templating.render(
                ctx, templating.Templates.cephadm_logrotate_config
            )
            f.write(cephadm_logrotate_config)


def write_cluster_logrotate_config(ctx: CephadmContext, fsid: str) -> None:
    # logrotate for the cluster
    with write_new(ctx.logrotate_dir + f'/ceph-{fsid}', perms=None) as f:
        """
        See cephadm/cephadmlib/templates/cluster.logrotate.config.j2 to
        get a better idea what this comment is referring to

        This is a bit sloppy in that the killall/pkill will touch all ceph daemons
        in all containers, but I don't see an elegant way to send SIGHUP *just* to
        the daemons for this cluster.  (1) systemd kill -s will get the signal to
        podman, but podman will exit.  (2) podman kill will get the signal to the
        first child (bash), but that isn't the ceph daemon.  This is simpler and
        should be harmless.
        """
        targets: List[str] = [
            'ceph-mon',
            'ceph-mgr',
            'ceph-mds',
            'ceph-osd',
            'ceph-fuse',
            'radosgw',
            'rbd-mirror',
            'cephfs-mirror',
            'tcmu-runner',
        ]

        logrotate_config = templating.render(
            ctx,
            templating.Templates.cluster_logrotate_config,
            fsid=fsid,
            targets=targets,
        )

        f.write(logrotate_config)
