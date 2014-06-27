#!/usr/bin/python
import os
import sys
import yaml
import psutil
import subprocess
import tempfile
import logging
import getpass

from . import beanstalk
from . import report
from .config import config

log = logging.getLogger(__name__)


def main(args):
    run_name = args['--run']
    job = args['--job']
    jobspec = args['--jobspec']
    archive_base = args['--archive']
    owner = args['--owner']
    machine_type = args['--machine_type']
    preserve_queue = args['--preserve-queue']

    if jobspec:
        split_spec = jobspec.split('/')
        run_name = split_spec[0]
        job = [split_spec[1]]

    if job:
        for job_id in job:
            kill_job(run_name, job_id, archive_base, owner, machine_type)
    else:
        kill_run(run_name, archive_base, owner, machine_type,
                 preserve_queue=preserve_queue)


def kill_run(run_name, archive_base=None, owner=None, machine_type=None,
             preserve_queue=False):
    run_info = {}
    serializer = report.ResultsSerializer(archive_base)
    if archive_base:
        run_archive_dir = os.path.join(archive_base, run_name)
        if os.path.isdir(run_archive_dir):
            run_info = find_run_info(serializer, run_name)
            machine_type = run_info['machine_type']
            owner = run_info['owner']
        elif machine_type is None:
            raise RuntimeError("The run is still entirely enqueued; " +
                               "you must also pass --machine-type")

    if not preserve_queue:
        remove_beanstalk_jobs(run_name, machine_type)
        remove_paddles_jobs(run_name)
    kill_processes(run_name, run_info.get('pids'))
    if owner is not None:
        targets = find_targets(run_name, owner)
        nuke_targets(targets, owner)


def kill_job(run_name, job_id, archive_base=None, owner=None,
             machine_type=None):
    serializer = report.ResultsSerializer(archive_base)
    job_info = serializer.job_info(run_name, job_id)
    owner = job_info['owner']
    kill_processes(run_name, [job_info.get('pid')])
    targets = dict(targets=job_info.get('targets', {}))
    nuke_targets(targets, owner)


def find_run_info(serializer, run_name):
    log.info("Assembling run information...")
    run_info_fields = [
        'machine_type',
        'owner',
    ]

    pids = []
    run_info = {}
    job_info = {}
    for (job_id, job_dir) in serializer.jobs_for_run(run_name).iteritems():
        if not os.path.isdir(job_dir):
            continue
        job_info = serializer.job_info(run_name, job_id)
        for key in job_info.keys():
            if key in run_info_fields and key not in run_info:
                run_info[key] = job_info[key]
        if 'pid' in job_info:
            pids.append(job_info['pid'])
    run_info['pids'] = pids
    return run_info


def remove_paddles_jobs(run_name):
    jobs = report.ResultsReporter().get_jobs(run_name, fields=['status'])
    job_ids = [job['job_id'] for job in jobs if job['status'] == 'queued']
    if job_ids:
        log.info("Deleting jobs from paddles: %s", str(job_ids))
        report.try_delete_jobs(run_name, job_ids)


def remove_beanstalk_jobs(run_name, tube_name):
    qhost = config.queue_host
    qport = config.queue_port
    if qhost is None or qport is None:
        raise RuntimeError(
            'Beanstalk queue information not found in {conf_path}'.format(
                conf_path=config.teuthology_yaml))
    log.info("Checking Beanstalk Queue...")
    beanstalk_conn = beanstalk.connect()
    beanstalk.watch_tube(beanstalk_conn, tube_name)

    curjobs = beanstalk_conn.stats_tube(tube_name)['current-jobs-ready']
    if curjobs != 0:
        x = 1
        while x != curjobs:
            x += 1
            job = beanstalk_conn.reserve(timeout=20)
            if job is None:
                continue
            job_config = yaml.safe_load(job.body)
            if run_name == job_config['name']:
                job_id = job.stats()['id']
                msg = "Deleting job from queue. ID: " + \
                    "{id} Name: {name} Desc: {desc}".format(
                        id=str(job_id),
                        name=job_config['name'],
                        desc=job_config['description'],
                    )
                log.info(msg)
                job.delete()
    else:
        print "No jobs in Beanstalk Queue"
    beanstalk_conn.close()


def kill_processes(run_name, pids=None):
    if pids:
        to_kill = set(pids).intersection(psutil.pids())
    else:
        to_kill = find_pids(run_name)

    if len(to_kill) == 0:
        log.info("No teuthology processes running")
    else:
        log.info("Killing Pids: " + str(to_kill))
        for pid in to_kill:
            args = ['kill', str(pid)]
            # Don't attempt to use sudo if it's not necessary
            proc_user = psutil.Process(int(pid)).username()
            if proc_user != getpass.getuser():
                args.insert(0, 'sudo')
            subprocess.call(args)


def process_matches_run(pid, run_name):
    try:
        p = psutil.Process(pid)
        cmd = p.cmdline()
        if run_name in cmd and sys.argv[0] not in cmd:
            return True
    except psutil.NoSuchProcess:
        pass
    return False


def find_pids(run_name):
    run_pids = []
    for pid in psutil.pids():
        if process_matches_run(pid, run_name):
            run_pids.append(pid)
    return run_pids


def find_targets(run_name, owner):
    lock_args = [
        'teuthology-lock',
        '--list-targets',
        '--desc-pattern',
        '/' + run_name + '/',
        '--status',
        'up',
        '--owner',
        owner
    ]
    proc = subprocess.Popen(lock_args, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    out_obj = yaml.safe_load(stdout)
    if not out_obj or 'targets' not in out_obj:
        return {}

    return out_obj


def nuke_targets(targets_dict, owner):
    targets = targets_dict.get('targets')
    if not targets:
        log.info("No locked machines. Not nuking anything")

    to_nuke = []
    for target in targets:
        to_nuke.append(target.split('@')[1].split('.')[0])

    target_file = tempfile.NamedTemporaryFile(delete=False)
    target_file.write(yaml.safe_dump(targets_dict))
    target_file.close()

    log.info("Nuking machines: " + str(to_nuke))
    nuke_args = [
        'teuthology-nuke',
        '-t',
        target_file.name,
        '--unlock',
        '-r',
        '--owner',
        owner
    ]
    proc = subprocess.Popen(
        nuke_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    for line in iter(proc.stdout.readline, ''):
        line = line.replace('\r', '').replace('\n', '')
        log.info(line)
        sys.stdout.flush()

    os.unlink(target_file.name)
