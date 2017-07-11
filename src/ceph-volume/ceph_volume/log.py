import logging
import os
from ceph_volume import terminal
from ceph_volume import conf

BASE_FORMAT = "[%(name)s][%(levelname)-6s] %(message)s"
FILE_FORMAT = "[%(asctime)s]" + BASE_FORMAT


def setup(name='ceph-volume.log'):
    # if a non-root user calls help or other no-sudo-required command the
    # logger will fail to write to /var/lib/ceph/ so this /tmp/ path is used as
    # a fallback
    tmp_log_file = os.path.join('/tmp/', name)
    root_logger = logging.getLogger()
    # The default path is where all ceph log files are, and will get rotated by
    # Ceph's logrotate rules.

    root_logger.setLevel(logging.DEBUG)

    try:
        fh = logging.FileHandler(conf.log_path)
    except (OSError, IOError) as err:
        terminal.warning("Falling back to /tmp/ for logging. Can't use %s" % conf.log_path)
        terminal.warning(str(err))
        conf.log_path = tmp_log_file
        fh = logging.FileHandler(tmp_log_file)

    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(FILE_FORMAT))

    root_logger.addHandler(fh)
