import argparse
import os
import yaml

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
    def deep_merge(self, a, b):
        if a is None:
            return b
        if b is None:
            return a
        if isinstance(a, list):
            assert isinstance(b, list)
            a.extend(b)
            return a
        if isinstance(a, dict):
            assert isinstance(b, dict)
            for (k, v) in b.iteritems():
                if k in a:
                    a[k] = self.deep_merge(a[k], v)
                else:
                    a[k] = v
            return a
        return b

    def __call__(self, parser, namespace, values, option_string=None):
        config = getattr(namespace, self.dest)
        for new in values:
            self.deep_merge(config, new)

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
        '--archive',
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
        '--block',
        action='store_true',
        default=False,
        help='block until locking machines succeeds (use with --lock)',
        )
    parser.add_argument(
        '--keep-locked-on-error',
        action='store_true',
        default=False,
        help='unlock machines only if the test succeeds (use with --lock)',
        )

    args = parser.parse_args()
    return args

def main():
    from gevent import monkey; monkey.patch_all()
    from .orchestra import monkey; monkey.patch_all()

    import logging

    log = logging.getLogger(__name__)
    ctx = parse_args()

    loglevel = logging.INFO
    if ctx.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        )

    if ctx.block:
        assert ctx.lock, \
            'the --block option is only supported with the --lock option'
    if ctx.keep_locked_on_error:
        assert ctx.lock, \
            'the --keep_locked_on_error option is only supported with the --lock option'

    from teuthology.misc import read_config
    read_config(ctx)

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

        with file(os.path.join(ctx.archive, 'config.yaml'), 'w') as f:
            yaml.safe_dump(ctx.config, f, default_flow_style=False)

    log.debug('\n  '.join(['Config:', ] + yaml.safe_dump(ctx.config, default_flow_style=False).splitlines()))

    ctx.summary = dict(success=True)

    if ctx.owner is None:
        from teuthology.misc import get_user
        ctx.owner = get_user()
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
        init_tasks.append({'internal.lock_machines': len(ctx.config['roles'])})

    init_tasks.extend([
            {'internal.check_lock': None},
            {'internal.connect': None},
            {'internal.check_conflict': None},
            ])
    if 'kernel' in ctx.config:
        init_tasks.append({'kernel': ctx.config['kernel']})
    init_tasks.extend([
            {'internal.base': None},
            {'internal.archive': None},
            {'internal.coredump': None},
            {'internal.syslog': None},
            ])

    ctx.config['tasks'][:0] = init_tasks

    from teuthology.run_tasks import run_tasks
    try:
        run_tasks(tasks=ctx.config['tasks'], ctx=ctx)
    finally:
        if ctx.archive is not None:
            with file(os.path.join(ctx.archive, 'summary.yaml'), 'w') as f:
                yaml.safe_dump(ctx.summary, f, default_flow_style=False)

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
        '--email-on-success',
        action='store_true',
        default=False,
        help='email even if all tests pass',
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
        '-v', '--verbose',
        action='store_true',
        default=False,
        help='be more verbose',
        )

    ctx = parser.parse_args()
    if not ctx.last_in_suite:
        assert not ctx.email, '--email is only applicable to the last job in a suite'
        assert not ctx.timeout, '--timeout is only applicable to the last job in a suite'
        assert not ctx.email_on_success, '--email-on-success is only applicable to the last job in a suite'

    from teuthology.misc import read_config, get_user
    if ctx.owner is None:
        ctx.owner = 'scheduled_{user}'.format(user=get_user())
    read_config(ctx)

    import teuthology.queue
    beanstalk = teuthology.queue.connect(ctx)

    beanstalk.use('teuthology')

    if ctx.delete:
        for jobid in ctx.delete:
            job = beanstalk.peek(jobid)
            if job is None:
                print 'job {jid} is not in the queue'.format(jid=jobid)
            else:
                job.delete()
        return

    job_config = dict(
            config=ctx.config,
            name=ctx.name,
            last_in_suite=ctx.last_in_suite,
            email=ctx.email,
            email_on_success=ctx.email_on_success,
            description=ctx.description,
            owner=ctx.owner,
            verbose=ctx.verbose,
            )
    if ctx.timeout is not None:
        job_config['results_timeout'] = ctx.timeout

    job = yaml.safe_dump(job_config)
    jid = beanstalk.put(job, ttr=60*60*24)
    print 'Job scheduled with ID {jid}'.format(jid=jid)
