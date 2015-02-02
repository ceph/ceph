import sys
import logging
from .sentry import get_client as get_sentry_client
from .job_status import set_status
from .misc import get_http_log_path
from .config import config as teuth_config
from .exceptions import ConnectionLostError
from copy import deepcopy

log = logging.getLogger(__name__)


def import_task(name):
    internal_pkg = __import__('teuthology.task', globals(), locals(), [name],
                              0)
    if hasattr(internal_pkg, name):
        return getattr(internal_pkg, name)
    else:
        external_pkg = __import__('tasks', globals(), locals(),
                                  [name], 0)
    if hasattr(external_pkg, name):
        return getattr(external_pkg, name)
    raise ImportError("Could not find task '%s'" % name)


def run_one_task(taskname, **kwargs):
    submod = taskname
    subtask = 'task'
    if '.' in taskname:
        (submod, subtask) = taskname.rsplit('.', 1)

    # Teuthology configs may refer to modules like ceph_deploy as ceph-deploy
    submod = submod.replace('-', '_')

    task = import_task(submod)
    try:
        fn = getattr(task, subtask)
    except AttributeError:
        log.error("No subtask of %s named %s was found", task, subtask)
        raise
    return fn(**kwargs)


def run_tasks(tasks, ctx):
    stack = []
    try:
        for taskdict in tasks:
            try:
                ((taskname, config),) = taskdict.iteritems()
            except (ValueError, AttributeError):
                raise RuntimeError('Invalid task definition: %s' % taskdict)
            log.info('Running task %s...', taskname)
            manager = run_one_task(taskname, ctx=ctx, config=config)
            if hasattr(manager, '__enter__'):
                manager.__enter__()
                stack.append((taskname, manager))
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

            # Remove ssh keys from reported config
            if 'targets' in config:
                targets = config['targets']
                for host in targets.keys():
                    targets[host] = '<redacted>'

            job_id = ctx.config.get('job_id')
            archive_path = ctx.config.get('archive_path')
            extra = dict(config=config,
                         branch=ctx.config.get('branch'),
                         )
            if job_id:
                extra['logs'] = get_http_log_path(archive_path, job_id)

            exc_id = sentry.get_ident(sentry.captureException(
                tags=tags,
                extra=extra,
            ))
            event_url = "{server}/search?q={id}".format(
                server=teuth_config.sentry_server.strip('/'), id=exc_id)
            log.exception(" Sentry event: %s" % event_url)
            ctx.summary['sentry_event'] = event_url

        if ctx.config.get('interactive-on-error'):
            ctx.config['interactive-on-error'] = False
            from .task import interactive
            log.warning('Saw failure during task execution, going into interactive mode...')
            interactive.task(ctx=ctx, config=None)
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
