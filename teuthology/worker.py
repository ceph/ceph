import logging
import os
import subprocess
import sys
import tempfile
import time
import yaml

from datetime import datetime

from teuthology import setup_log_file, install_except_hook
from teuthology import beanstalk
from teuthology import report
from teuthology import safepath
from teuthology.config import config as teuth_config
from teuthology.config import set_config_attr
from teuthology.exceptions import BranchNotFoundError, SkipJob, MaxWhileTries
from teuthology.kill import kill_job
from teuthology.repo_utils import fetch_qa_suite, fetch_teuthology

log = logging.getLogger(__name__)
start_time = datetime.utcnow()
restart_file_path = '/tmp/teuthology-restart-workers'
stop_file_path = '/tmp/teuthology-stop-workers'


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


def load_config(ctx=None):
    teuth_config.load()
    if ctx is not None:
        if not os.path.isdir(ctx.archive_dir):
            sys.exit("{prog}: archive directory must exist: {path}".format(
                prog=os.path.basename(sys.argv[0]),
                path=ctx.archive_dir,
            ))
        else:
            teuth_config.archive_base = ctx.archive_dir


def main(ctx):
    loglevel = logging.INFO
    if ctx.verbose:
        loglevel = logging.DEBUG
    log.setLevel(loglevel)

    log_file_path = os.path.join(ctx.log_dir, 'worker.{tube}.{pid}'.format(
        pid=os.getpid(), tube=ctx.tube,))
    setup_log_file(log_file_path)

    install_except_hook()

    load_config(ctx=ctx)

    set_config_attr(ctx)

    connection = beanstalk.connect()
    beanstalk.watch_tube(connection, ctx.tube)
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

        try:
            job_config, teuth_bin_path = prep_job(
                job_config,
                log_file_path,
                ctx.archive_dir,
            )
            run_job(
                job_config,
                teuth_bin_path,
                ctx.archive_dir,
                ctx.verbose,
            )
        except SkipJob:
            continue

        # This try/except block is to keep the worker from dying when
        # beanstalkc throws a SocketError
        try:
            job.delete()
        except Exception:
            log.exception("Saw exception while trying to delete job")


def prep_job(job_config, log_file_path, archive_dir):
    job_id = job_config['job_id']
    safe_archive = safepath.munge(job_config['name'])
    job_config['worker_log'] = log_file_path
    archive_path_full = os.path.join(
        archive_dir, safe_archive, str(job_id))
    job_config['archive_path'] = archive_path_full

    # If the teuthology branch was not specified, default to master and
    # store that value.
    teuthology_branch = job_config.get('teuthology_branch', 'master')
    job_config['teuthology_branch'] = teuthology_branch

    try:
        if teuth_config.teuthology_path is not None:
            teuth_path = teuth_config.teuthology_path
        else:
            teuth_path = fetch_teuthology(branch=teuthology_branch)
        # For the teuthology tasks, we look for suite_branch, and if we
        # don't get that, we look for branch, and fall back to 'master'.
        # last-in-suite jobs don't have suite_branch or branch set.
        ceph_branch = job_config.get('branch', 'master')
        suite_branch = job_config.get('suite_branch', ceph_branch)
        suite_repo = job_config.get('suite_repo')
        if suite_repo:
            teuth_config.ceph_qa_suite_git_url = suite_repo
        job_config['suite_path'] = os.path.normpath(os.path.join(
            fetch_qa_suite(suite_branch),
            job_config.get('suite_relpath', ''),
        ))
    except BranchNotFoundError as exc:
        log.exception("Branch not found; marking job as dead")
        report.try_push_job_info(
            job_config,
            dict(status='dead', failure_reason=str(exc))
        )
        raise SkipJob()
    except MaxWhileTries as exc:
        log.exception("Failed to fetch or bootstrap; marking job as dead")
        report.try_push_job_info(
            job_config,
            dict(status='dead', failure_reason=str(exc))
        )
        raise SkipJob()

    teuth_bin_path = os.path.join(teuth_path, 'virtualenv', 'bin')
    if not os.path.isdir(teuth_bin_path):
        raise RuntimeError("teuthology branch %s at %s not bootstrapped!" %
                           (teuthology_branch, teuth_bin_path))
    return job_config, teuth_bin_path


def run_job(job_config, teuth_bin_path, archive_dir, verbose):
    safe_archive = safepath.munge(job_config['name'])
    if job_config.get('first_in_suite') or job_config.get('last_in_suite'):
        if teuth_config.results_server:
            try:
                report.try_delete_jobs(job_config['name'], job_config['job_id'])
            except Exception as e:
                log.warning("Unable to delete job %s, exception occurred: %s",
                            job_config['job_id'], e)
        suite_archive_dir = os.path.join(archive_dir, safe_archive)
        safepath.makedirs('/', suite_archive_dir)
        args = [
            os.path.join(teuth_bin_path, 'teuthology-results'),
            '--archive-dir', suite_archive_dir,
            '--name', job_config['name'],
        ]
        if job_config.get('first_in_suite'):
            log.info('Generating memo for %s', job_config['name'])
            if job_config.get('seed'):
                args.extend(['--seed', job_config['seed']])
            if job_config.get('subset'):
                args.extend(['--subset', job_config['subset']])
        else:
            log.info('Generating results for %s', job_config['name'])
            timeout = job_config.get('results_timeout',
                                     teuth_config.results_timeout)
            args.extend(['--timeout', str(timeout)])
            if job_config.get('email'):
                args.extend(['--email', job_config['email']])
        # Execute teuthology-results, passing 'preexec_fn=os.setpgrp' to
        # make sure that it will continue to run if this worker process
        # dies (e.g. because of a restart)
        result_proc = subprocess.Popen(args=args, preexec_fn=os.setpgrp)
        log.info("teuthology-results PID: %s", result_proc.pid)
        return

    log.info('Creating archive dir %s', job_config['archive_path'])
    safepath.makedirs('/', job_config['archive_path'])
    log.info('Running job %s', job_config['job_id'])

    suite_path = job_config['suite_path']
    arg = [
        os.path.join(teuth_bin_path, 'teuthology'),
    ]
    # The following is for compatibility with older schedulers, from before we
    # started merging the contents of job_config['config'] into job_config
    # itself.
    if 'config' in job_config:
        inner_config = job_config.pop('config')
        if not isinstance(inner_config, dict):
            log.warn("run_job: job_config['config'] isn't a dict, it's a %s",
                     str(type(inner_config)))
        else:
            job_config.update(inner_config)

    if verbose or job_config['verbose']:
        arg.append('-v')

    arg.extend([
        '--lock',
        '--block',
        '--owner', job_config['owner'],
        '--archive', job_config['archive_path'],
        '--name', job_config['name'],
    ])
    if job_config['description'] is not None:
        arg.extend(['--description', job_config['description']])
    arg.append('--')

    with tempfile.NamedTemporaryFile(prefix='teuthology-worker.',
                                     suffix='.tmp', mode='w+t') as tmp:
        yaml.safe_dump(data=job_config, stream=tmp)
        tmp.flush()
        arg.append(tmp.name)
        env = os.environ.copy()
        python_path = env.get('PYTHONPATH', '')
        python_path = ':'.join([suite_path, python_path]).strip(':')
        env['PYTHONPATH'] = python_path
        log.debug("Running: %s" % ' '.join(arg))
        p = subprocess.Popen(args=arg, env=env)
        log.info("Job archive: %s", job_config['archive_path'])
        log.info("Job PID: %s", str(p.pid))

        if teuth_config.results_server:
            log.info("Running with watchdog")
            try:
                run_with_watchdog(p, job_config)
            except Exception:
                log.exception("run_with_watchdog had an unhandled exception")
                raise
        else:
            log.info("Running without watchdog")
            # This sleep() is to give the child time to start up and create the
            # archive dir.
            time.sleep(5)
            symlink_worker_log(job_config['worker_log'],
                               job_config['archive_path'])
            p.wait()

        if p.returncode != 0:
            log.error('Child exited with code %d', p.returncode)
        else:
            log.info('Success!')


def run_with_watchdog(process, job_config):
    job_start_time = datetime.utcnow()

    # Only push the information that's relevant to the watchdog, to save db
    # load
    job_info = dict(
        name=job_config['name'],
        job_id=job_config['job_id'],
    )

    # Sleep once outside of the loop to avoid double-posting jobs
    time.sleep(teuth_config.watchdog_interval)
    symlink_worker_log(job_config['worker_log'], job_config['archive_path'])
    while process.poll() is None:
        # Kill jobs that have been running longer than the global max
        run_time = datetime.utcnow() - job_start_time
        total_seconds = run_time.days * 60 * 60 * 24 + run_time.seconds
        if total_seconds > teuth_config.max_job_time:
            log.warning("Job ran longer than {max}s. Killing...".format(
                max=teuth_config.max_job_time))
            kill_job(job_info['name'], job_info['job_id'],
                     teuth_config.archive_base, job_config['owner'])

        # calling this without a status just updates the jobs updated time
        report.try_push_job_info(job_info)
        time.sleep(teuth_config.watchdog_interval)

    # we no longer support testing theses old branches
    assert(job_config.get('teuthology_branch') not in ('argonaut', 'bobtail',
                                                       'cuttlefish', 'dumpling'))

    # Let's make sure that paddles knows the job is finished. We don't know
    # the status, but if it was a pass or fail it will have already been
    # reported to paddles. In that case paddles ignores the 'dead' status.
    # If the job was killed, paddles will use the 'dead' status.
    report.try_push_job_info(job_info, dict(status='dead'))


def symlink_worker_log(worker_log_path, archive_dir):
    try:
        log.debug("Worker log: %s", worker_log_path)
        os.symlink(worker_log_path, os.path.join(archive_dir, 'worker.log'))
    except Exception:
        log.exception("Failed to symlink worker log")
