import logging
import os
import subprocess
import sys
import yaml
import tempfile

from datetime import datetime

from teuthology import setup_log_file, install_except_hook
from teuthology import beanstalk
from teuthology.config import config as teuth_config
from teuthology.repo_utils import fetch_qa_suite, fetch_teuthology
from teuthology.task.internal.lock_machines import lock_machines_helper
from teuthology.dispatcher import supervisor
from teuthology.worker import prep_job

log = logging.getLogger(__name__)
start_time = datetime.utcnow()
restart_file_path = '/tmp/teuthology-restart-dispatcher'
stop_file_path = '/tmp/teuthology-stop-dispatcher'


def sentinel(path):
    if not os.path.exists(path):
        return False
    file_mtime = datetime.utcfromtimestamp(os.path.getmtime(path))
    if file_mtime > start_time:
        return True
    else:
        return False


def restart():
    log.info('Restarting...')
    args = sys.argv[:]
    args.insert(0, sys.executable)
    os.execv(sys.executable, args)


def stop():
    log.info('Stopping...')
    sys.exit(0)


def load_config(archive_dir=None):
    teuth_config.load()
    if archive_dir is not None:
        if not os.path.isdir(archive_dir):
            sys.exit("{prog}: archive directory must exist: {path}".format(
                prog=os.path.basename(sys.argv[0]),
                path=archive_dir,
            ))
        else:
            teuth_config.archive_base = archive_dir


def main(args):
    # run dispatcher in job supervisor mode if --supervisor passed
    if args["--supervisor"]:
        return supervisor.main(args)

    verbose = args["--verbose"]
    tube = args["--tube"]
    log_dir = args["--log-dir"]
    archive_dir = args["--archive-dir"]

    # setup logging for disoatcher in {log_dir}
    loglevel = logging.INFO
    if verbose:
        loglevel = logging.DEBUG
    log.setLevel(loglevel)
    log_file_path = os.path.join(log_dir, 'dispatcher.{tube}.{pid}'.format(
        pid=os.getpid(), tube=tube))
    setup_log_file(log_file_path)
    install_except_hook()

    load_config(archive_dir=archive_dir)

    connection = beanstalk.connect()
    beanstalk.watch_tube(connection, tube)
    result_proc = None

    if teuth_config.teuthology_path is None:
        fetch_teuthology('master')
    fetch_qa_suite('master')

    keep_running = True
    while keep_running:
        # Check to see if we have a teuthology-results process hanging around
        # and if so, read its return code so that it can exit.
        if result_proc is not None and result_proc.poll() is not None:
            log.debug("teuthology-results exited with code: %s",
                      result_proc.returncode)
            result_proc = None

        if sentinel(restart_file_path):
            restart()
        elif sentinel(stop_file_path):
            stop()

        load_config()

        job = connection.reserve(timeout=60)
        if job is None:
            continue

        # bury the job so it won't be re-run if it fails
        job.bury()
        job_id = job.jid
        log.info('Reserved job %d', job_id)
        log.info('Config is: %s', job.body)
        job_config = yaml.safe_load(job.body)
        job_config['job_id'] = str(job_id)

        if job_config.get('stop_worker'):
            keep_running = False

        job_config, teuth_bin_path = prep_job(
            job_config,
            log_file_path,
            archive_dir,
        )

        # lock machines but do not reimage them
        if 'roles' in job_config:
            job_config = lock_machines(job_config)

        run_args = [
            os.path.join(teuth_bin_path, 'teuthology-dispatcher'),
            '--supervisor',
            '-v',
            '--bin-path', teuth_bin_path,
            '--archive-dir', archive_dir,
        ]

        with tempfile.NamedTemporaryFile(prefix='teuthology-dispatcher.',
                                         suffix='.tmp', mode='w+t') as tmp:
            yaml.safe_dump(data=job_config, stream=tmp)
            tmp.flush()
            run_args.extend(["--config-fd", str(tmp.fileno())])
            job_proc = subprocess.Popen(run_args, pass_fds=[tmp.fileno()])

        log.info('Job subprocess PID: %s', job_proc.pid)

        # This try/except block is to keep the worker from dying when
        # beanstalkc throws a SocketError
        try:
            job.delete()
        except Exception:
            log.exception("Saw exception while trying to delete job")


def lock_machines(job_config):
    fake_ctx = supervisor.create_fake_context(job_config, block=True)
    lock_machines_helper(fake_ctx, [len(job_config['roles']),
                         job_config['machine_type']], reimage=False)
    job_config = fake_ctx.config
    return job_config
