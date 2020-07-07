from __future__ import print_function
import os

# Tell gevent not to patch os.waitpid() since it is susceptible to race
# conditions. See:
# http://www.gevent.org/gevent.monkey.html#gevent.monkey.patch_os
os.environ['GEVENT_NOWAITPID'] = 'true'

# Use manhole to give us a way to debug hung processes
# https://pypi.python.org/pypi/manhole
import manhole
manhole.install(
    verbose=False,
    # Listen for SIGUSR1
    oneshot_on="USR1"
)
from gevent import monkey
monkey.patch_all(
    dns=False,
    # Don't patch subprocess to avoid http://tracker.ceph.com/issues/14990
    subprocess=False,
)
import sys
from gevent.hub import Hub

# Don't write pyc files
sys.dont_write_bytecode = True

from teuthology.orchestra import monkey
monkey.patch_all()

import logging
import subprocess

__version__ = '1.0.0'

# do our best, but if it fails, continue with above

try:
    teuthology_dir = os.path.dirname(os.path.realpath(__file__))
    site_dir = os.path.dirname(teuthology_dir)
    git_dir = os.path.join(site_dir, '.git')
    # make sure we use git repo otherwise it is a released version
    if os.path.exists(git_dir):
        __version__ += '-' + str(subprocess.check_output(
            'git rev-parse --short HEAD'.split(),
            cwd=site_dir
        ).decode()).strip()
except Exception as e:
    # before logging; should be unusual
    print("Can't get version from git rev-parse %s" % e, file=sys.stderr)

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

log.debug('teuthology version: %s', __version__)


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
    root_logger.info('teuthology version: %s', __version__)


def install_except_hook():
    """
    Install an exception hook that first logs any uncaught exception, then
    raises it.
    """
    def log_exception(exc_type, exc_value, exc_traceback):
        if not issubclass(exc_type, KeyboardInterrupt):
            log.critical("Uncaught exception", exc_info=(exc_type, exc_value,
                                                         exc_traceback))
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
    sys.excepthook = log_exception


def patch_gevent_hub_error_handler():
    Hub._origin_handle_error = Hub.handle_error

    def custom_handle_error(self, context, type, value, tb):
        if context is None or issubclass(type, Hub.SYSTEM_ERROR):
            self.handle_system_error(type, value)
        elif issubclass(type, Hub.NOT_ERROR):
            pass
        else:
            log.error("Uncaught exception (Hub)", exc_info=(type, value, tb))

    Hub.handle_error = custom_handle_error

patch_gevent_hub_error_handler()
