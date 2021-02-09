import logging
import os
import subprocess
import time
import yaml

from datetime import datetime

import teuthology
from teuthology import report
from teuthology import safepath
from teuthology.config import config as teuth_config
from teuthology.exceptions import SkipJob
from teuthology import setup_log_file, install_except_hook
from teuthology.lock.ops import reimage_machines
from teuthology.misc import get_user, archive_logs, compress_logs
from teuthology.config import FakeNamespace
from teuthology.job_status import get_status
from teuthology.nuke import nuke
from teuthology.kill import kill_job
from teuthology.task.internal import add_remotes
from teuthology.misc import decanonicalize_hostname as shortname
from teuthology.lock import query

log = logging.getLogger(__name__)


def main(args):

    verbose = args["--verbose"]
    archive_dir = args["--archive-dir"]
    teuth_bin_path = args["--bin-path"]
    config_file_path = args["--job-config"]

    with open(config_file_path, 'r') as config_file:
        job_config = yaml.safe_load(config_file)

    loglevel = logging.INFO
    if verbose:
        loglevel = logging.DEBUG
    log.setLevel(loglevel)

    log_file_path = os.path.join(job_config['archive_path'],
                                 f"supervisor.{job_config['job_id']}.log")
    setup_log_file(log_file_path)
    install_except_hook()

    # reimage target machines before running the job
    if 'targets' in job_config:
        reimage(job_config)
        with open(config_file_path, 'w') as f:
            yaml.safe_dump(job_config, f, default_flow_style=False)

    try:
        run_job(
            job_config,
            teuth_bin_path,
            archive_dir,
            verbose
        )
    except SkipJob:
        return


def run_job(job_config, teuth_bin_path, archive_dir, verbose):
    safe_archive = safepath.munge(job_config['name'])
    if job_config.get('first_in_suite') or job_config.get('last_in_suite'):
        job_archive = os.path.join(archive_dir, safe_archive)
        args = [
            os.path.join(teuth_bin_path, 'teuthology-results'),
            '--archive-dir', job_archive,
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
        # Remove unnecessary logs for first and last jobs in run
        log.info('Deleting job\'s archive dir %s', job_config['archive_path'])
        for f in os.listdir(job_config['archive_path']):
            os.remove(os.path.join(job_config['archive_path'], f))
        os.rmdir(job_config['archive_path'])
        return

    log.info('Running job %s', job_config['job_id'])

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
        '--owner', job_config['owner'],
        '--archive', job_config['archive_path'],
        '--name', job_config['name'],
    ])
    if job_config['description'] is not None:
        arg.extend(['--description', job_config['description']])
    job_archive = os.path.join(job_config['archive_path'], 'orig.config.yaml')
    arg.extend(['--', job_archive])

    log.debug("Running: %s" % ' '.join(arg))
    p = subprocess.Popen(args=arg)
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
        p.wait()

    if p.returncode != 0:
        log.error('Child exited with code %d', p.returncode)
    else:
        log.info('Success!')
    if 'targets' in job_config:
        unlock_targets(job_config)


def reimage(job_config):
    # Reimage the targets specified in job config
    # and update their keys in config after reimaging
    ctx = create_fake_context(job_config)
    # change the status during the reimaging process
    report.try_push_job_info(ctx.config, dict(status='waiting'))
    targets = job_config['targets']
    try:
        reimaged = reimage_machines(ctx, targets, job_config['machine_type'])
    except Exception as e:
        log.exception('Reimaging error. Nuking machines...')
        # Reimage failures should map to the 'dead' status instead of 'fail'
        report.try_push_job_info(ctx.config, dict(status='dead', failure_reason='Error reimaging machines: ' + str(e)))
        nuke(ctx, True)
        raise
    ctx.config['targets'] = reimaged
    # change the status to running after the reimaging process
    report.try_push_job_info(ctx.config, dict(status='running'))


def unlock_targets(job_config):
    serializer = report.ResultsSerializer(teuth_config.archive_base)
    job_info = serializer.job_info(job_config['name'], job_config['job_id'])
    machine_status = query.get_statuses(job_info['targets'].keys())
    # only unlock/nuke targets if locked in the first place
    locked = [shortname(_['name'])
              for _ in machine_status if _['locked']]
    if not locked:
        return
    job_status = get_status(job_info)
    if job_status == 'pass' or \
            (job_config.get('unlock_on_failure', False) and not job_config.get('nuke-on-error', False)):
        log.info('Unlocking machines...')
        fake_ctx = create_fake_context(job_config)
        for machine in locked:
            teuthology.lock.ops.unlock_one(fake_ctx,
                                           machine, job_info['owner'],
                                           job_info['archive_path'])
    if job_status != 'pass' and job_config.get('nuke-on-error', False):
        log.info('Nuking machines...')
        fake_ctx = create_fake_context(job_config)
        nuke(fake_ctx, True)


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
    hit_max_timeout = False
    while process.poll() is None:
        # Kill jobs that have been running longer than the global max
        run_time = datetime.utcnow() - job_start_time
        total_seconds = run_time.days * 60 * 60 * 24 + run_time.seconds
        if total_seconds > teuth_config.max_job_time:
            hit_max_timeout = True
            log.warning("Job ran longer than {max}s. Killing...".format(
                max=teuth_config.max_job_time))
            try:
                # kill processes but do not unlock yet so we can save
                # the logs, coredumps, etc.
                kill_job(job_info['name'], job_info['job_id'],
                         teuth_config.archive_base, job_config['owner'],
                         save_logs=True)
            except Exception:
                log.exception('Failed to kill job')

            try:
                transfer_archives(job_info['name'], job_info['job_id'],
                                  teuth_config.archive_base, job_config)
            except Exception:
                log.exception('Could not save logs')

            try:
                # this time remove everything and unlock the machines
                kill_job(job_info['name'], job_info['job_id'],
                         teuth_config.archive_base, job_config['owner'])
            except Exception:
                log.exception('Failed to kill job and unlock machines')

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
    extra_info = dict(status='dead')
    if hit_max_timeout:
        extra_info['failure_reason'] = 'hit max job timeout'
    report.try_push_job_info(job_info, extra_info)


def create_fake_context(job_config, block=False):
    owner = job_config.get('owner', get_user())
    os_version = job_config.get('os_version', None)

    ctx_args = {
        'config': job_config,
        'block': block,
        'owner': owner,
        'archive': job_config['archive_path'],
        'machine_type': job_config['machine_type'],
        'os_type': job_config.get('os_type', 'ubuntu'),
        'os_version': os_version,
        'name': job_config['name'],
    }

    return FakeNamespace(ctx_args)


def transfer_archives(run_name, job_id, archive_base, job_config):
    serializer = report.ResultsSerializer(archive_base)
    job_info = serializer.job_info(run_name, job_id, simple=True)

    if 'archive' in job_info:
        ctx = create_fake_context(job_config)
        add_remotes(ctx, job_config)

        for log_type, log_path in job_info['archive'].items():
            if log_type == 'init':
                log_type = ''
            compress_logs(ctx, log_path)
            archive_logs(ctx, log_path, log_type)
    else:
        log.info('No archives to transfer.')
