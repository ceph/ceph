import logging
import time

from teuthology import misc as teuthology
from ..orchestra import run

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Sleep for some number of seconds.

    Example::


       tasks:
       - install:
       - ceph:
       - sleep:
           duration: 10
       - interactive:

    """
    if not config:
        config = {}
    assert isinstance(config, dict)
    duration = int(config.get('duration', 5))
    log.info('Sleeping for %d', duration)
    time.sleep(duration)
