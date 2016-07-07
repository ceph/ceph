from gevent import monkey
monkey.patch_all(
    dns=False,
    # Don't patch subprocess to avoid http://tracker.ceph.com/issues/14990
    subprocess=False,
)
import sys

# Don't write pyc files
sys.dont_write_bytecode = True

from .orchestra import monkey
monkey.patch_all()

import logging
import os


__version__ = '0.1.0'


# If we are running inside a virtualenv, ensure we have its 'bin' directory in
# our PATH. This doesn't happen automatically if scripts are called without
# first activating the virtualenv.
exec_dir = os.path.abspath(os.path.dirname(sys.argv[0]))
if os.path.split(exec_dir)[-1] == 'bin' and exec_dir not in os.environ['PATH']:
    os.environ['PATH'] = ':'.join((exec_dir, os.environ['PATH']))

# We don't need to see log entries for each connection opened
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(
    logging.WARN)
# if requests doesn't bundle it, shut it up anyway
logging.getLogger('urllib3.connectionpool').setLevel(
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
