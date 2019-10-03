"""
Task sequencer finally
"""
import sys
import logging
import contextlib

from teuthology import run_tasks

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
    Sequentialize a group of tasks into one executable block, run on cleanup

    example::

      tasks:
      - foo:
      - full_sequential_finally:
        - final1:
        - final2:
      - bar:
      - baz:

    The final1 and final2 tasks will run when full_sequentiall_finally is torn
    down, after the nested bar and baz tasks have run to completion, and right
    before the preceding foo task is torn down.  This is useful if there are
    additional steps you want to interject in a job during the shutdown (instead
    of startup) phase.

    :param ctx: Context
    :param config: Configuration
    """
    try:
        yield
    finally:
        for entry in config:
            if not isinstance(entry, dict):
                entry = ctx.config.get(entry, {})
            ((taskname, confg),) = entry.items()
            log.info('In full_sequential_finally, running task %s...' % taskname)
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
