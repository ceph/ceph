"""
Task to loop a list of items
"""
import sys
import logging

from teuthology import run_tasks

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Loop a sequential group of tasks

    example::

        - loop:
           count: 10
           body:
             - tasktest:
             - tasktest:

    :param ctx: Context
    :param config: Configuration
    """
    for i in range(config.get('count', 1)):
        stack = []
        try:
            for entry in config.get('body', []):
                if not isinstance(entry, dict):
                    entry = ctx.config.get(entry, {})
                ((taskname, confg),) = entry.items()
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
                    mgr.__exit__(*exc_info)
            finally:
                del exc_info
