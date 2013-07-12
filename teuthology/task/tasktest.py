import logging
import contextlib
import time

from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Task that just displays information when it is create and when it is
    destroyed/cleaned up.  This task was used to test parallel and
    sequential task options.

    example:
    
    tasks:
    - sequential:
        - tasktest:
            - id: 'foo'
        - tasktest:
            - id: 'bar'
            - delay:5
    - tasktest:

    The above yaml will sequentially start a test task named foo and a test
    task named bar.  Bar will take 5 seconds to complete.  After foo and bar
    have finished, an unidentified tasktest task will run.
    """
    try:
        delay = config.get('delay', 0)
        id = config.get('id', 'UNKNOWN')    
    except AttributeError:
        delay = 0
        id = 'UNKNOWN'
    try:
        log.info('**************************************************')
        log.info('Started task test -- %s' % id)
        log.info('**************************************************')
        time.sleep(delay)
        yield

    finally:
        log.info('**************************************************')
        log.info('Task test is being cleaned up -- %s' % id)
        log.info('**************************************************')

