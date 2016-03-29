"""
Task to group parallel running tasks
"""
import sys
import logging

from teuthology import run_tasks
from teuthology import parallel

log = logging.getLogger(__name__)


def task(ctx, config):
    """
    Run a group of tasks in parallel.

    example:
    - parallel:
       - tasktest:
       - tasktest:

    You can also define tasks in a top-level section outside of
    'tasks:', and reference them here.

    The referenced section must contain a list of tasks to run
    sequentially, or a single task as a dict. The latter is only
    available for backwards compatibility with existing suites:

    tasks:
    - parallel:
      - tasktest: # task inline
      - foo       # reference to top-level 'foo' section
      - bar       # reference to top-level 'bar' section
    foo:
    - tasktest1:
    - tasktest2:
    bar:
      tasktest: # note the list syntax from 'foo' is preferred

    That is, if the entry is not a dict, we will look it up in the top-level
    config.

    Sequential tasks and Parallel tasks can be nested.
    """

    log.info('starting parallel...')
    with parallel.parallel() as p:
        for entry in config:
            if not isinstance(entry, dict):
                entry = ctx.config.get(entry, {})
                # support the usual list syntax for tasks
                if isinstance(entry, list):
                    entry = dict(sequential=entry)
            ((taskname, confg),) = entry.iteritems()
            p.spawn(_run_spawned, ctx, confg, taskname)


def _run_spawned(ctx, config, taskname):
    """Run one of the tasks (this runs in parallel with others)"""
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
