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
    except IOError, e:
        raise argparse.ArgumentTypeError(str(e))
    return config

class MergeConfig(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        config = getattr(namespace, self.dest)
        from teuthology.misc import deep_merge
        for new in values:
            deep_merge(config, new)

def parse_args():
    parser = argparse.ArgumentParser(description='Run ceph integration tests')
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=None,
        help='be more verbose',
        )
    parser.add_argument(
        'config',
        metavar='CONFFILE',
        nargs='+',
        type=config_file,
        action=MergeConfig,
        default={},
        help='config file to read',
        )
    parser.add_argument(
        '-a', '--archive',
        metavar='DIR',
        help='path to archive results in',
        )
    parser.add_argument(
        '--description',
        help='job description',
        )
    parser.add_argument(
        '--owner',
        help='job owner',
        )
    parser.add_argument(
        '--lock',
        action='store_true',
        default=False,
        help='lock machines for the duration of the run',
        )
    parser.add_argument(
        '--machine-type',
        default=None,
        help='Type of machine to lock/run tests on.',
        )
    parser.add_argument(
        '--os-type',
        default='ubuntu',
        help='Distro/OS of machine to run test on.',
        )
    parser.add_argument(
        '--block',
        action='store_true',
        default=False,
        help='block until locking machines succeeds (use with --lock)',
        )
    parser.add_argument(
        '--name',
        metavar='NAME',
        help='name for this teuthology run',
        )

    args = parser.parse_args()
    return args


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
        logging.critical('{0}: {1}'.format(exception_class.__name__, exception))

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


def main():
    from gevent import monkey
    monkey.patch_all(dns=False)
    from .orchestra import monkey
    monkey.patch_all()
    import logging

    from . import report

    ctx = parse_args()
    set_up_logging(ctx)
    log = logging.getLogger(__name__)

    if ctx.owner is None:
        from teuthology.misc import get_user
        ctx.owner = get_user()

    write_initial_metadata(ctx)
    report.try_push_job_info(ctx.config)

    if 'targets' in ctx.config and 'roles' in ctx.config:
        targets = len(ctx.config['targets'])
        roles = len(ctx.config['roles'])
        assert targets >= roles, \
            '%d targets are needed for all roles but found %d listed.' % (roles, targets)

    machine_type = ctx.machine_type
    if machine_type is None:
        fallback_default = ctx.config.get('machine_type', 'plana')
        machine_type = ctx.config.get('machine-type', fallback_default)

    if ctx.block:
        assert ctx.lock, \
            'the --block option is only supported with the --lock option'

    from teuthology.misc import read_config
    read_config(ctx)

    log.debug('\n  '.join(['Config:', ] + yaml.safe_dump(ctx.config, default_flow_style=False).splitlines()))

    ctx.summary = dict(success=True)

    ctx.summary['owner'] = ctx.owner

    if ctx.description is not None:
        ctx.summary['description'] = ctx.description

    for task in ctx.config['tasks']:
        assert 'kernel' not in task, \
            'kernel installation shouldn be a base-level item, not part of the tasks list'

    init_tasks = []
    if ctx.lock:
        assert 'targets' not in ctx.config, \
            'You cannot specify targets in a config file when using the --lock option'
        init_tasks.append({'internal.lock_machines': (len(ctx.config['roles']), machine_type)})

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
            if 'email-on-error' in ctx.config and not ctx.summary.get('success', False):
                yaml.safe_dump(ctx.summary, f)
                yaml.safe_dump(ctx.config, f)
                emsg = f.getvalue()
                subject = "Teuthology error -- %s" % ctx.summary['failure_reason']
                from teuthology.suite import email_results
                email_results(subject,"Teuthology",ctx.config['email-on-error'],emsg)

        report.try_push_job_info(ctx.config, ctx.summary)
        report.try_push_job_info(ctx.config)

        if ctx.summary.get('success', True):
            log.info('pass')
        else:
            log.info('FAIL')
            import sys
            sys.exit(1)


def schedule():
    parser = argparse.ArgumentParser(description='Schedule ceph integration tests')
    parser.add_argument(
        'config',
        metavar='CONFFILE',
        nargs='*',
        type=config_file,
        action=MergeConfig,
        default={},
        help='config file to read',
        )
    parser.add_argument(
        '--name',
        help='name of suite run the job is part of',
        )
    parser.add_argument(
        '--last-in-suite',
        action='store_true',
        default=False,
        help='mark the last job in a suite so suite post-processing can be run',
        )
    parser.add_argument(
        '--email',
        help='where to send the results of a suite (only applies to the last job in a suite)',
        )
    parser.add_argument(
        '--timeout',
        help='how many seconds to wait for jobs to finish before emailing results (only applies to the last job in a suite',
        type=int,
        )
    parser.add_argument(
        '--description',
        help='job description',
        )
    parser.add_argument(
        '--owner',
        help='job owner',
        )
    parser.add_argument(
        '--delete',
        metavar='JOBID',
        type=int,
        nargs='*',
        help='list of jobs to remove from the queue',
        )
    parser.add_argument(
        '-n', '--num',
        default=1,
        type=int,
        help='number of times to run/queue the job'
        )
    parser.add_argument(
        '-p', '--priority',
        default=1000,
        type=int,
        help='beanstalk priority (lower is sooner)'
        )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        help='be more verbose',
        )
    parser.add_argument(
        '-w', '--worker',
        default='plana',
        help='which worker to use (type of machine)',
        )
    parser.add_argument(
        '-s', '--show',
        metavar='JOBID',
        type=int,
        nargs='*',
        help='output the contents of specified jobs in the queue',
        )

    ctx = parser.parse_args()
    if not ctx.last_in_suite:
        assert not ctx.email, '--email is only applicable to the last job in a suite'
        assert not ctx.timeout, '--timeout is only applicable to the last job in a suite'

    from teuthology.misc import read_config, get_user
    if ctx.owner is None:
        ctx.owner = 'scheduled_{user}'.format(user=get_user())
    read_config(ctx)

    import teuthology.queue
    beanstalk = teuthology.queue.connect(ctx)

    tube=ctx.worker
    beanstalk.use(tube)

    if ctx.show:
        for job_id in ctx.show:
            job = beanstalk.peek(job_id)
            if job is None and ctx.verbose:
                print 'job {jid} is not in the queue'.format(jid=job_id)
            else:
                print '--- job {jid} priority {prio} ---\n'.format(
                    jid=job_id,
                    prio=job.stats()['pri']), job.body
        return

    if ctx.delete:
        for job_id in ctx.delete:
            job = beanstalk.peek(job_id)
            if job is None:
                print 'job {jid} is not in the queue'.format(jid=job_id)
            else:
                job.delete()
        return

    # strip out targets; the worker will allocate new ones when we run
    # the job with --lock.
    if ctx.config.get('targets'):
        del ctx.config['targets']

    job_config = dict(
            name=ctx.name,
            last_in_suite=ctx.last_in_suite,
            email=ctx.email,
            description=ctx.description,
            owner=ctx.owner,
            verbose=ctx.verbose,
            )
    # Merge job_config and ctx.config
    job_config.update(ctx.config)
    if ctx.timeout is not None:
        job_config['results_timeout'] = ctx.timeout

    job = yaml.safe_dump(job_config)
    num = ctx.num
    while num > 0:
        jid = beanstalk.put(
            job,
            ttr=60*60*24,
            priority=ctx.priority,
            )
        print 'Job scheduled with ID {jid}'.format(jid=jid)
        num -= 1
