import os
import yaml
import StringIO
import contextlib
import sys
import logging
from traceback import format_tb

import teuthology
from . import report
from .misc import get_distro
from .misc import get_user
from .misc import read_config
from .nuke import nuke
from .run_tasks import run_tasks
from .results import email_results


def set_up_logging(ctx):
    if ctx.verbose:
        teuthology.log.setLevel(logging.DEBUG)

    if ctx.archive is not None:
        os.mkdir(ctx.archive)

        teuthology.setup_log_file(
            logging.getLogger(),
            os.path.join(ctx.archive, 'teuthology.log'))

    install_except_hook()


def install_except_hook():
    def log_exception(exception_class, exception, traceback):
        logging.critical(''.join(format_tb(traceback)))
        if not exception.message:
            logging.critical(exception_class.__name__)
            return
        logging.critical('{0}: {1}'.format(
            exception_class.__name__, exception))

    sys.excepthook = log_exception


def write_initial_metadata(ctx):
    if ctx.archive is not None:
        with file(os.path.join(ctx.archive, 'pid'), 'w') as f:
            f.write('%d' % os.getpid())

        with file(os.path.join(ctx.archive, 'owner'), 'w') as f:
            f.write(ctx.owner + '\n')

        with file(os.path.join(ctx.archive, 'orig.config.yaml'), 'w') as f:
            yaml.safe_dump(ctx.config, f, default_flow_style=False)

        info = {
            'name': ctx.name,
            'description': ctx.description,
            'owner': ctx.owner,
            'pid': os.getpid(),
        }
        if 'job_id' in ctx.config:
            info['job_id'] = ctx.config['job_id']

        with file(os.path.join(ctx.archive, 'info.yaml'), 'w') as f:
            yaml.safe_dump(info, f, default_flow_style=False)


def main(ctx):
    set_up_logging(ctx)
    log = logging.getLogger(__name__)

    if ctx.owner is None:
        ctx.owner = get_user()

    # Older versions of teuthology stored job_id as an int. Convert it to a str
    # if necessary.
    job_id = ctx.config.get('job_id')
    if job_id is not None:
        job_id = str(job_id)
        ctx.config['job_id'] = job_id

    write_initial_metadata(ctx)
    report.try_push_job_info(ctx.config, dict(status='running'))

    if 'targets' in ctx.config and 'roles' in ctx.config:
        targets = len(ctx.config['targets'])
        roles = len(ctx.config['roles'])
        assert targets >= roles, \
            '%d targets are needed for all roles but found %d listed.' % (
                roles, targets)

    machine_type = ctx.machine_type
    if machine_type is None:
        fallback_default = ctx.config.get('machine_type', 'plana')
        machine_type = ctx.config.get('machine-type', fallback_default)

    if ctx.block:
        assert ctx.lock, \
            'the --block option is only supported with the --lock option'

    read_config(ctx)

    log.debug('\n  '.join(['Config:', ] + yaml.safe_dump(
        ctx.config, default_flow_style=False).splitlines()))

    ctx.summary = dict(success=True)

    ctx.summary['owner'] = ctx.owner

    if ctx.description is not None:
        ctx.summary['description'] = ctx.description

    for task in ctx.config['tasks']:
        msg = ('kernel installation shouldn be a base-level item, not part ' +
               'of the tasks list')
        assert 'kernel' not in task, msg

    init_tasks = []
    if ctx.lock:
        msg = ('You cannot specify targets in a config file when using the ' +
               '--lock option')
        assert 'targets' not in ctx.config, msg
        init_tasks.append({'internal.lock_machines': (
            len(ctx.config['roles']), machine_type)})

    init_tasks.extend([
        {'internal.save_config': None},
        {'internal.check_lock': None},
        {'internal.connect': None},
        {'internal.serialize_remote_roles': None},
        {'internal.check_conflict': None},
    ])
    if not ctx.config.get('use_existing_cluster', False):
        init_tasks.extend([
            {'internal.check_ceph_data': None},
            {'internal.vm_setup': None},
        ])
    if 'kernel' in ctx.config:
        sha1 = ctx.config['kernel'].get('sha1')
        distro = get_distro(ctx)
        if (distro == 'ubuntu') or (sha1 == 'distro'):
            init_tasks.append({'kernel': ctx.config['kernel']})
    init_tasks.extend([
        {'internal.base': None},
        {'internal.archive': None},
        {'internal.coredump': None},
        {'internal.sudo': None},
        {'internal.syslog': None},
        {'internal.timer': None},
    ])

    ctx.config['tasks'][:0] = init_tasks

    try:
        run_tasks(tasks=ctx.config['tasks'], ctx=ctx)
    finally:
        if not ctx.summary.get('success') and ctx.config.get('nuke-on-error'):
            # only unlock if we locked them in the first place
            nuke(ctx, ctx.lock)
        if ctx.archive is not None:
            with file(os.path.join(ctx.archive, 'summary.yaml'), 'w') as f:
                yaml.safe_dump(ctx.summary, f, default_flow_style=False)
        with contextlib.closing(StringIO.StringIO()) as f:
            yaml.safe_dump(ctx.summary, f)
            log.info('Summary data:\n%s' % f.getvalue())
        with contextlib.closing(StringIO.StringIO()) as f:
            if ('email-on-error' in ctx.config
                    and not ctx.summary.get('success', False)):
                yaml.safe_dump(ctx.summary, f)
                yaml.safe_dump(ctx.config, f)
                emsg = f.getvalue()
                subject = "Teuthology error -- %s" % ctx.summary[
                    'failure_reason']
                email_results(subject, "Teuthology", ctx.config[
                              'email-on-error'], emsg)

        report.try_push_job_info(ctx.config, ctx.summary)

        if ctx.summary.get('success', True):
            log.info('pass')
        else:
            log.info('FAIL')
            sys.exit(1)
