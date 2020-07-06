import jinja2
import logging
import os
import sys
import time
import types

from copy import deepcopy
from humanfriendly import format_timespan

from teuthology.config import config as teuth_config
from teuthology.exceptions import ConnectionLostError
from teuthology.job_status import set_status, get_status
from teuthology.misc import get_http_log_path, get_results_url
from teuthology.sentry import get_client as get_sentry_client
from teuthology.timer import Timer

log = logging.getLogger(__name__)


def get_task(name):
    # todo: support of submodules
    if '.' in name:
        module_name, task_name = name.split('.')
    else:
        module_name, task_name = (name, 'task')

    # First look for the tasks's module inside teuthology
    module = _import('teuthology.task', module_name, task_name)
    # If it is not found, try qa/ directory (if it is in sys.path)
    if not module:
        module = _import('tasks', module_name, task_name, fail_on_import_error=True)
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


def _import(from_package, module_name, task_name, fail_on_import_error=False):
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
        if fail_on_import_error:
            raise
        else:
            return None
    return module


def run_one_task(taskname, **kwargs):
    taskname = taskname.replace('-', '_')
    task = get_task(taskname)
    return task(**kwargs)


def run_tasks(tasks, ctx):
    archive_path = ctx.config.get('archive_path')
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
                ((taskname, config),) = taskdict.items()
            except (ValueError, AttributeError):
                raise RuntimeError('Invalid task definition: %s' % taskdict)
            log.info('Running task %s...', taskname)
            timer.mark('%s enter' % taskname)
            manager = run_one_task(taskname, ctx=ctx, config=config)
            if hasattr(manager, '__enter__'):
                stack.append((taskname, manager))
                manager.__enter__()
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
            from teuthology.task import interactive
            log.warning('Saw failure during task execution, going into interactive mode...')
            interactive.task(ctx=ctx, config=None)
        # Throughout teuthology, (x,) = y has been used to assign values
        # from yaml files where only one entry of type y is correct.  This
        # causes failures with 'too many values to unpack.'  We want to
        # fail as before, but with easier to understand error indicators.
        if isinstance(e, ValueError):
            if str(e) == 'too many values to unpack':
                emsg = 'Possible configuration error in yaml file'
                log.error(emsg)
                ctx.summary['failure_info'] = emsg
    finally:
        try:
            exc_info = sys.exc_info()
            sleep_before_teardown = ctx.config.get('sleep_before_teardown')
            if sleep_before_teardown:
                log.info(
                    'Sleeping for {} seconds before unwinding because'
                    ' --sleep-before-teardown was given...'
                    .format(sleep_before_teardown))
                notify_sleep_before_teardown(ctx, stack, sleep_before_teardown)
                time.sleep(sleep_before_teardown)
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
                        from tuethology.task import interactive
                        log.warning(
                            'Saw failure during task cleanup, going into interactive mode...')
                        interactive.task(ctx=ctx, config=None)
                else:
                    if suppress:
                        exc_info = (None, None, None)

            if exc_info != (None, None, None):
                log.debug('Exception was not quenched, exiting: %s: %s',
                          exc_info[0].__name__, exc_info[1])
                raise SystemExit(1)
        finally:
            # be careful about cyclic references
            del exc_info
        timer.mark("tasks complete")

def build_email_body(ctx, stack, sleep_time_sec):
    email_template_path = os.path.dirname(__file__) + \
            '/templates/email-sleep-before-teardown.jinja2'

    with open(email_template_path) as f:
        template_text = f.read()

    email_template = jinja2.Template(template_text)
    archive_path = ctx.config.get('archive_path')
    job_id = ctx.config.get('job_id')
    status = get_status(ctx.summary)
    stack_path = '/'.join(task for task, _ in stack)
    suite_name=ctx.config.get('suite')
    sleep_date=time.time()
    sleep_date_str=time.strftime('%Y-%m-%d %H:%M:%S',
                                 time.gmtime(sleep_date))

    body = email_template.render(
        sleep_time=format_timespan(sleep_time_sec),
        sleep_time_sec=sleep_time_sec,
        sleep_date=sleep_date_str,
        owner=ctx.owner,
        run_name=ctx.name,
        job_id=ctx.config.get('job_id'),
        job_info=get_results_url(ctx.name),
        job_logs=get_http_log_path(archive_path, job_id),
        suite_name=suite_name,
        status=status,
        task_stack=stack_path,
    )
    subject = (
        'teuthology job {run}/{job} has fallen asleep at {date}'
        .format(run=ctx.name, job=job_id, date=sleep_date_str)
    )
    return (subject.strip(), body.strip())

def notify_sleep_before_teardown(ctx, stack, sleep_time):
    email = ctx.config.get('email', None)
    if not email:
        # we have no email configured, return silently
        return
    (subject, body) = build_email_body(ctx, stack, sleep_time)
    log.info('Sending no to {to}: {body}'.format(to=email, body=body))
    import smtplib
    from email.mime.text import MIMEText
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = teuth_config.results_sending_email or 'teuthology'
    msg['To'] = email
    log.debug('sending email %s', msg.as_string())
    smtp = smtplib.SMTP('localhost')
    smtp.sendmail(msg['From'], [msg['To']], msg.as_string())
    smtp.quit()

