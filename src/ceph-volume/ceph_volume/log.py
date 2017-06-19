from datetime import datetime
import logging
import os

BASE_FORMAT = "[%(name)s][%(levelname)-6s] %(message)s"
FILE_FORMAT = "[%(asctime)s]" + BASE_FORMAT


def setup(config=None):
    root_logger = logging.getLogger()
    log_path = config.get('--log-path', '/var/log/ceph/')
    if not os.path.exists(log_path):
        raise RuntimeError('configured ``--log-path`` value does not exist: %s' % log_path)
    date = datetime.strftime(datetime.utcnow(), '%Y-%m-%d')
    log_file = os.path.join(log_path, 'ceph-volume-%s.log' % date)

    root_logger.setLevel(logging.DEBUG)

    # File Logger
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(FILE_FORMAT))

    root_logger.addHandler(fh)
