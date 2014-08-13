from gevent import monkey
monkey.patch_all(dns=False)
from .orchestra import monkey
monkey.patch_all()

import logging


__version__ = '0.1.0'

# We don't need to see log entries for each connection opened
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(
    logging.WARN)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s:%(name)s:%(message)s')
log = logging.getLogger(__name__)


def setup_log_file(log_path):
    root_logger = logging.getLogger()
    handlers = root_logger.handlers
    for handler in handlers:
        if isinstance(handler, logging.FileHandler) and \
                handler.stream.name == log_path:
            log.debug("Already logging to %s; not adding new handler",
                      log_path)
            return
    formatter = logging.Formatter(
        fmt=u'%(asctime)s.%(msecs)03d %(levelname)s:%(name)s:%(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S')
    handler = logging.FileHandler(filename=log_path)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
