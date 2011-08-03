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
    beanstalk.watch('teuthology')
    beanstalk.ignore('default')

    while True:
        job = beanstalk.reserve(timeout=60)
        if job is None:
            continue

        # bury the job so it won't be re-run if it fails
        job.bury()
        run_job(job, ctx.archive_dir)

def run_job(job, archive_dir):
    log.info('Running job %d', job.jid)
    log.debug('Config is: %s', job.body)
    job_config = yaml.safe_load(job.body)

    safe_archive = safepath.munge(job_config['name'])
    safepath.makedirs(archive_dir, safe_archive)
    archive_path = os.path.join(archive_dir, safe_archive, str(job.jid))

    arg = [
        os.path.join(os.path.dirname(sys.argv[0]), 'teuthology'),
        ]

    if job_config['verbose']:
        arg.append('-v')

    arg.extend([
            '--lock',
            '--block',
            '--keep-locked-on-error',
            '--owner', job_config['owner'],
            '--archive', archive_path,
            ])
    if job_config['description'] is not None:
        arg.extend(['--description', job_config['description']])
    arg.append('--')

    tmp_fp, tmp_path = tempfile.mkstemp()
    try:
        os.write(tmp_fp, yaml.safe_dump(job_config['config']))
        arg.append(tmp_path)
        subprocess.check_call(
            args=arg,
            close_fds=True,
            )
    except subprocess.CalledProcessError as e:
        log.exception(e)
    else:
        log.info('Success!')
        job.delete()
    finally:
        os.close(tmp_fp)
        os.unlink(tmp_path)
