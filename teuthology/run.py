import argparse
import os
import yaml
import StringIO
import contextlib
import sys
from traceback import format_tb


def config_file(string):
    config = {}
    try:
        with file(string) as f:
            g = yaml.safe_load_all(f)
            for new in g:
                config.update(new)
    except IOError as e:
        raise argparse.ArgumentTypeError(str(e))
    return config


class MergeConfig(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        config = getattr(namespace, self.dest)
        from teuthology.misc import deep_merge
        for new in values:
            deep_merge(config, new)


def set_up_logging(ctx):
    import logging

    loglevel = logging.INFO
    if ctx.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(level=loglevel)
    if ctx.archive is not None:
        os.mkdir(ctx.archive)

        handler = logging.FileHandler(
            filename=os.path.join(ctx.archive, 'teuthology.log'),
        )
        formatter = logging.Formatter(
            fmt='%(asctime)s.%(msecs)03d %(levelname)s:%(name)s:%(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S',
        )
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)

    install_except_hook()


def install_except_hook():
    def log_exception(exception_class, exception, traceback):
        import logging

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
    from gevent import monkey
    monkey.patch_all(dns=False)
    from .orchestra import monkey
    monkey.patch_all()

    # WARNING: Do not import any modules that import logging before this next
    # block. That would cause connections to hang because the monkey patching
    # hadn't been done.
    import logging
    set_up_logging(ctx)
    log = logging.getLogger(__name__)

    # Now it is safe to import other teuthology modules.
    from . import report

    if ctx.owner is None:
        from teuthology.misc import get_user
        ctx.owner = get_user()

    # Older versions of teuthology stored job_id as an int. Convert it to a str
    # if necessary.
    job_id = ctx.config.get('job_id')
    if job_id is not None:
        job_id = str(job_id)
        ctx.config['job_id'] = job_id

    write_initial_metadata(ctx)
    report.try_push_job_info(ctx.config)

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

    from teuthology.misc import read_config
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
        {'internal.check_conflict': None},
        {'internal.check_ceph_data': None},
        {'internal.vm_setup': None},
    ])
    if 'kernel' in ctx.config:
        from teuthology.misc import get_distro
        distro = get_distro(ctx)
        if distro == 'ubuntu':
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

    from teuthology.run_tasks import run_tasks
    try:
        run_tasks(tasks=ctx.config['tasks'], ctx=ctx)
    finally:
        if not ctx.summary.get('success') and ctx.config.get('nuke-on-error'):
            from teuthology.nuke import nuke
            # only unlock if we locked them in the first place
            nuke(ctx, log, ctx.lock)
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
                from teuthology.suite import email_results
                email_results(subject, "Teuthology", ctx.config[
                              'email-on-error'], emsg)

        report.try_push_job_info(ctx.config, ctx.summary)

        if ctx.summary.get('success', True):
            log.info('pass')
        else:
            log.info('FAIL')
            import sys
            sys.exit(1)
