import sys
import logging

from teuthology import run_tasks

log = logging.getLogger(__name__)


def task(ctx, config):
    """
    Sequentialize a group of tasks into one executable block

    example:
    - sequential:
       - tasktest:
       - tasktest:

    You can also reference the job from elsewhere:

    foo:
      tasktest:
    tasks:
    - sequential:
      - tasktest:
      - foo
      - tasktest:

    That is, if the entry is not a dict, we will look it up in the top-level
    config.

    Sequential task and Parallel tasks can be nested.
    """
    stack = []
    try:
        for entry in config:
            if not isinstance(entry, dict):
                entry = ctx.config.get(entry, {})
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
                mgr.__exit__(*exc_info)
        finally:
            del exc_info
