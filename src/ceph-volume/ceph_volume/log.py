import logging
import os
from ceph_volume import config

BASE_FORMAT = "[%(name)s][%(levelname)-6s] %(message)s"
FILE_FORMAT = "[%(asctime)s]" + BASE_FORMAT


def setup(config=None, name='ceph-volume.log'):
    # if a non-root user calls help or other no-sudo-required command the
    # logger will fail to write to /var/lib/ceph/ so this /tmp/ path is used as
    # a fallback
    tmp_log_file = os.path.join('/tmp/', name)
    root_logger = logging.getLogger()
    # The default path is where all ceph log files are, and will get rotated by
    # Ceph's logrotate rules.
    default_log_path = os.environ.get('CEPH_VOLUME_LOG_PATH', '/var/log/ceph/')
    log_path = config.get('--log-path', default_log_path)
    log_file = os.path.join(log_path, 'ceph-volume.log')

    root_logger.setLevel(logging.DEBUG)

    # File Logger
    config['log_path'] = log_file
    try:
        fh = logging.FileHandler(log_file)
    except (OSError, IOError):
        config['log_path'] = tmp_log_file
        fh = logging.FileHandler(tmp_log_file)

    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(FILE_FORMAT))

    root_logger.addHandler(fh)
