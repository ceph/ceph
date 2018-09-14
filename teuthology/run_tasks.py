import logging
import os
import sys
import types

from copy import deepcopy

from .config import config as teuth_config
from .exceptions import ConnectionLostError
from .job_status import set_status
from .misc import get_http_log_path
from .sentry import get_client as get_sentry_client
from .timer import Timer

log = logging.getLogger(__name__)


def get_task(name):
    if '.' in name:
        module_name, task_name = name.split('.')
    else:
        module_name, task_name = (name, 'task')

    # First look for the tasks's module inside teuthology
    module = _import('teuthology.task', module_name, task_name)
    # If it is not found, try ceph-qa-suite (if it is in sys.path)
    if not module:
        module = _import('tasks', module_name, task_name)
    # If it is still not found, fail
    if not module:
        raise ImportError("Could not find task '{}'".format(name))
    try:
        # Attempt to locate the task object inside the module
        task = getattr(module, task_name)
        # If we get another module, we need to go deeper
        if isinstance(task, types.ModuleType):
            task = getattr(task, task_name)
    except AttributeError:
        log.error("No subtask of '{}' named '{}' was found".format(
            module_name,
            task_name,
        ))
        raise
    return task


def _import(from_package, module_name, task_name):
    full_module_name = '.'.join([from_package, module_name])
    try:
        module = __import__(
            full_module_name,
            globals(),
            locals(),
            [task_name],
            0,
        )
    except ImportError:
        return None
    return module


def run_one_task(taskname, stack, timer, **kwargs):
    if 'log_message' in kwargs:
        log.info(kwargs['log_message'])
        del(kwargs['log_message'])
    else:
        log.info("Running task {}...".format(taskname))
    timer.mark('%s enter' % taskname)
    taskname = taskname.replace('-', '_')
    task = get_task(taskname)
    manager = task(**kwargs)
    if hasattr(manager, '__enter__'):
        stack.append((taskname, manager))
        manager.__enter__()


def run_tasks(tasks, ctx):
    archive_path = ctx.config.get('archive_path')
    sleep_before_teardown = ctx.config.get('sleep_before_teardown')
    sleep_task = { "sleep": { "duration": sleep_before_teardown } }
    if archive_path:
        timer = Timer(
            path=os.path.join(archive_path, 'timing.yaml'),
            sync=True,
        )
    else:
        timer = Timer()
    stack = []
    try:
        for taskdict in tasks:
            try:
                ((taskname, config),) = taskdict.iteritems()
            except (ValueError, AttributeError):
                raise RuntimeError('Invalid task definition: %s' % taskdict)
            run_one_task(taskname, stack, timer, ctx=ctx, config=config)
        if sleep_before_teardown:
            ((taskname, config),) = sleep_task.iteritems()
            run_one_task(taskname, stack, timer, ctx=ctx, config=config,
                log_message='Running sleep task because --sleep-before-teardown was given...')
    except BaseException as e:
        if isinstance(e, ConnectionLostError):
            # Prevent connection issues being flagged as failures
            set_status(ctx.summary, 'dead')
        else:
            # the status may have been set to dead, leave it as-is if so
            if not ctx.summary.get('status', '') == 'dead':
                set_status(ctx.summary, 'fail')
        if 'failure_reason' not in ctx.summary:
            ctx.summary['failure_reason'] = str(e)
        log.exception('Saw exception from tasks.')

        sentry = get_sentry_client()
        if sentry:
            config = deepcopy(ctx.config)

            tags = {
                'task': taskname,
                'owner': ctx.owner,
            }
            if 'teuthology_branch' in config:
                tags['teuthology_branch'] = config['teuthology_branch']
            if 'branch' in config:
                tags['branch'] = config['branch']

            # Remove ssh keys from reported config
            if 'targets' in config:
                targets = config['targets']
                for host in targets.keys():
                    targets[host] = '<redacted>'

            job_id = ctx.config.get('job_id')
            archive_path = ctx.config.get('archive_path')
            extra = dict(config=config,
                         )
            if job_id:
                extra['logs'] = get_http_log_path(archive_path, job_id)

            exc_id = sentry.get_ident(sentry.captureException(
                tags=tags,
                extra=extra,
            ))
            event_url = "{server}/?q={id}".format(
                server=teuth_config.sentry_server.strip('/'), id=exc_id)
            log.exception(" Sentry event: %s" % event_url)
            ctx.summary['sentry_event'] = event_url

        if ctx.config.get('interactive-on-error'):
            ctx.config['interactive-on-error'] = False
            from .task import interactive
            log.warning('Saw failure during task execution, going into interactive mode...')
            interactive.task(ctx=ctx, config=None)
        if sleep_before_teardown:
            ((taskname, config),) = sleep_task.iteritems()
            run_one_task(taskname, stack, timer, ctx=ctx, config=config,
                log_message='Running sleep task because --sleep-before-teardown was given...')
        # Throughout teuthology, (x,) = y has been used to assign values
        # from yaml files where only one entry of type y is correct.  This
        # causes failures with 'too many values to unpack.'  We want to
        # fail as before, but with easier to understand error indicators.
        if type(e) == ValueError:
            if e.message == 'too many values to unpack':
                emsg = 'Possible configuration error in yaml file'
                log.error(emsg)
                ctx.summary['failure_info'] = emsg
    finally:
        try:
            exc_info = sys.exc_info()
            while stack:
                taskname, manager = stack.pop()
                log.debug('Unwinding manager %s', taskname)
                timer.mark('%s exit' % taskname)
                try:
                    suppress = manager.__exit__(*exc_info)
                except Exception as e:
                    if isinstance(e, ConnectionLostError):
                        # Prevent connection issues being flagged as failures
                        set_status(ctx.summary, 'dead')
                    else:
                        set_status(ctx.summary, 'fail')
                    if 'failure_reason' not in ctx.summary:
                        ctx.summary['failure_reason'] = str(e)
                    log.exception('Manager failed: %s', taskname)

                    if exc_info == (None, None, None):
                        # if first failure is in an __exit__, we don't
                        # have exc_info set yet
                        exc_info = sys.exc_info()

                    if ctx.config.get('interactive-on-error'):
                        from .task import interactive
                        log.warning(
                            'Saw failure during task cleanup, going into interactive mode...')
                        interactive.task(ctx=ctx, config=None)
                else:
                    if suppress:
                        sys.exc_clear()
                        exc_info = (None, None, None)

            if exc_info != (None, None, None):
                log.debug('Exception was not quenched, exiting: %s: %s',
                          exc_info[0].__name__, exc_info[1])
                raise SystemExit(1)
        finally:
            # be careful about cyclic references
            del exc_info
        timer.mark("tasks complete")
