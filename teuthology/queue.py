import argparse
import logging
import os
import subprocess
import sys
import tempfile
import yaml

import beanstalkc

from teuthology import safepath

log = logging.getLogger(__name__)

def connect(ctx):
    host = ctx.teuthology_config['queue_host']
    port = ctx.teuthology_config['queue_port']
    return beanstalkc.Connection(host=host, port=port)

def worker():
    parser = argparse.ArgumentParser(description="""
Grab jobs from a beanstalk queue and run the teuthology tests they
describe. One job is run at a time.
""")
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=None,
        help='be more verbose',
        )
    parser.add_argument(
        '--archive-dir',
        metavar='DIR',
        help='path under which to archive results',
        required=True,
        )
    parser.add_argument(
        '-l', '--log-dir',
        help='path in which to store logs',
        required=True,
        )
    parser.add_argument(
        '-t', '--tube',
        help='which beanstalk tube to read jobs from',
        required=True,
        )

    ctx = parser.parse_args()

    loglevel = logging.INFO
    if ctx.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        filename=os.path.join(ctx.log_dir, 'worker.{pid}'.format(pid=os.getpid())),
        format='%(asctime)s.%(msecs)03d %(levelname)s:%(name)s:%(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        )

    if not os.path.isdir(ctx.archive_dir):
        sys.exit("{prog}: archive directory must exist: {path}".format(
                prog=os.path.basename(sys.argv[0]),
                path=ctx.archive_dir,
                ))

    from teuthology.misc import read_config
    read_config(ctx)

    beanstalk = connect(ctx)
    beanstalk.watch(ctx.tube)
    beanstalk.ignore('default')

    while True:
        job = beanstalk.reserve(timeout=60)
        if job is None:
            continue

        # bury the job so it won't be re-run if it fails
        job.bury()
        log.debug('Reserved job %d', job.jid)
        log.debug('Config is: %s', job.body)
        job_config = yaml.safe_load(job.body)
        safe_archive = safepath.munge(job_config['name'])
        teuthology_branch = job_config.get('config', {}).get('teuthology_branch', 'master')

        teuth_path = os.path.join(os.getenv("HOME"), 'teuthology-' + teuthology_branch, 'virtualenv', 'bin')
        if not os.path.isdir(teuth_path):
            raise Exception('Teuthology branch ' + teuthology_branch + ' not found at ' + teuth_path)
        if job_config.get('last_in_suite'):
            log.debug('Generating coverage for %s', job_config['name'])
            args = [
                os.path.join(teuth_path, 'teuthology-results'),
                '--timeout',
                str(job_config.get('results_timeout', 21600)),
                '--email',
                job_config['email'],
                '--archive-dir',
                os.path.join(ctx.archive_dir, safe_archive),
                '--name',
                job_config['name'],
                ]
            subprocess.Popen(args=args)
        else:
            log.debug('Creating archive dir...')
            safepath.makedirs(ctx.archive_dir, safe_archive)
            archive_path = os.path.join(ctx.archive_dir, safe_archive, str(job.jid))
            log.info('Running job %d', job.jid)
            run_job(job_config, archive_path, teuth_path)
        job.delete()

def run_job(job_config, archive_path, teuth_path):
    arg = [
        os.path.join(teuth_path, 'teuthology'),
        ]

    if job_config['verbose']:
        arg.append('-v')

    arg.extend([
            '--lock',
            '--block',
            '--owner', job_config['owner'],
            '--archive', archive_path,
            '--name', job_config['name'],
            ])
    if job_config['description'] is not None:
        arg.extend(['--description', job_config['description']])
    arg.append('--')

    with tempfile.NamedTemporaryFile(
        prefix='teuthology-worker.',
        suffix='.tmp',
        ) as tmp:
        yaml.safe_dump(data=job_config['config'], stream=tmp)
        tmp.flush()
        arg.append(tmp.name)
        p = subprocess.Popen(
            args=arg,
            close_fds=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            )
        child = logging.getLogger(__name__ + '.child')
        for line in p.stdout:
            child.info(': %s', line.rstrip('\n'))
        p.wait()
        if p.returncode != 0:
            log.error('Child exited with code %d', p.returncode)
        else:
            log.info('Success!')
