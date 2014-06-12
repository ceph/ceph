from gevent import monkey
monkey.patch_all(dns=False)
from .orchestra import monkey
monkey.patch_all()

import logging

# We don't need to see log entries for each connection opened
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(
    logging.WARN)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s:%(name)s:%(message)s')
log = logging.getLogger(__name__)


def setup_log_file(logger, log_path):
    log_formatter = logging.Formatter(
        fmt=u'%(asctime)s.%(msecs)03d %(levelname)s:%(name)s:%(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S')
    log_handler = logging.FileHandler(filename=log_path)
    log_handler.setFormatter(log_formatter)
    logger.addHandler(log_handler)
