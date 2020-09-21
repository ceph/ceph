import logging
import os
from ceph_volume import terminal
from ceph_volume import conf

BASE_FORMAT = "[%(name)s][%(levelname)-6s] %(message)s"
FILE_FORMAT = "[%(asctime)s]" + BASE_FORMAT


def setup(name='ceph-volume.log', log_path=None):
    log_path = log_path or conf.log_path
    # if a non-root user calls help or other no-sudo-required command the
    # logger will fail to write to /var/lib/ceph/ so this /tmp/ path is used as
    # a fallback
    tmp_log_file = os.path.join('/tmp/', name)
    root_logger = logging.getLogger()
    # The default path is where all ceph log files are, and will get rotated by
    # Ceph's logrotate rules.

    root_logger.setLevel(logging.DEBUG)

    try:
        fh = logging.FileHandler(log_path)
    except (OSError, IOError) as err:
        terminal.warning("Falling back to /tmp/ for logging. Can't use %s" % log_path)
        terminal.warning(str(err))
        conf.log_path = tmp_log_file
        fh = logging.FileHandler(tmp_log_file)

    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(FILE_FORMAT))

    root_logger.addHandler(fh)


def setup_console():
    # TODO: At some point ceph-volume should stop using the custom logger
    # interface that exists in terminal.py and use the logging module to
    # produce output for the terminal
    # Console Logger
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('[terminal] %(message)s'))
    sh.setLevel(logging.DEBUG)

    terminal_logger = logging.getLogger('terminal')

    # allow all levels at root_logger, handlers control individual levels
    terminal_logger.addHandler(sh)
