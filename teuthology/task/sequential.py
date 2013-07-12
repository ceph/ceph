import sys
import logging
import contextlib

from teuthology import run_tasks
from ..orchestra import run

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Sequentialize a group of tasks into one executable block

    example:
    - sequential:
       - tasktest:
       - tasktest:

    Sequential task and Parallel tasks can be nested.
    """
    stack = []
    try:
        for entry in config:
            ((taskname, confg),) = entry.iteritems()
            log.info('In sequential, running task %s...' % taskname)
            mgr = run_tasks.run_one_task(taskname, ctx=ctx, config=confg)
            if hasattr(mgr, '__enter__'):
                mgr.__enter__()
                stack.append(mgr)
    finally:
        try:
            exc_info = sys.exc_info()
            while stack:
                mgr = stack.pop()
                endr = mgr.__exit__(*exc_info)
        finally:
            del exc_info
