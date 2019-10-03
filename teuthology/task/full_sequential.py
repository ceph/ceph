"""
Task sequencer - full
"""
import sys
import logging

from teuthology import run_tasks

log = logging.getLogger(__name__)


def task(ctx, config):
    """
    Run a set of tasks to completion in order.  __exit__ is called on a task
    before __enter__ on the next

    example::
        - full_sequential:
           - tasktest:
           - tasktest:

    :param ctx: Context
    :param config: Configuration
    """
    for entry in config:
        if not isinstance(entry, dict):
            entry = ctx.config.get(entry, {})
        ((taskname, confg),) = entry.items()
        log.info('In full_sequential, running task %s...' % taskname)
        mgr = run_tasks.run_one_task(taskname, ctx=ctx, config=confg)
        if hasattr(mgr, '__enter__'):
            try:
                mgr.__enter__()
            finally:
                try:
                    exc_info = sys.exc_info()
                    mgr.__exit__(*exc_info)
                finally:
                    del exc_info
