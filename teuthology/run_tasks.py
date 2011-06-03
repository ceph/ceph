import sys
import logging

log = logging.getLogger(__name__)

def _run_one_task(taskname, **kwargs):
    parent = __import__('teuthology.task', globals(), locals(), [taskname], 0)
    mod = getattr(parent, taskname)
    fn = getattr(mod, 'task')
    return fn(**kwargs)

def run_tasks(tasks, ctx):
    stack = []
    try:
        for taskdict in tasks:
            try:
                ((taskname, config),) = taskdict.iteritems()
            except ValueError:
                raise RuntimeError('Invalid task definition: %s' % taskdict)
            log.info('Running task %s...', taskname)
            manager = _run_one_task(taskname, ctx=ctx, config=config)
            if hasattr(manager, '__enter__'):
                manager.__enter__()
                stack.append(manager)
    except:
        log.exception('Saw exception from tasks')
    finally:
        try:
            exc_info = sys.exc_info()
            while stack:
                manager = stack.pop()
                log.debug('Unwinding manager %s', manager)
                try:
                    suppress = manager.__exit__(*exc_info)
                except:
                    log.exception('Manager failed: %s', manager)
                else:
                    if suppress:
                        sys.exc_clear()
                        exc_info = (None, None, None)

            if exc_info != (None, None, None):
                log.debug('Exception was not quenched, exiting: %s: %s', exc_info[0].__name__, exc_info[1])
                raise SystemExit(1)
        finally:
            # be careful about cyclic references
            del exc_info
