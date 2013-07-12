import sys
import logging
import contextlib

from teuthology import run_tasks
from teuthology import parallel
from ..orchestra import run

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run a group of tasks in parallel.

    example:
    - parallel:
       - tasktest:
       - tasktest:

    Sequential task and Parallel tasks can be nested.
    """
    
    log.info('starting parallel...')
    with parallel.parallel() as p:
        for entry in config:
            ((taskname, confg),) = entry.iteritems()
            p.spawn(_run_spawned, ctx, confg, taskname)

def _run_spawned(ctx,config,taskname):
    mgr = {}
    try:
        log.info('In parallel, running task %s...' % taskname)
        mgr = run_tasks.run_one_task(taskname, ctx=ctx, config=config)
        if hasattr(mgr, '__enter__'):
            mgr.__enter__()
    finally:
        exc_info = sys.exc_info()
        if hasattr(mgr, '__exit__'):
            mgr.__exit__(*exc_info)
        del exc_info
