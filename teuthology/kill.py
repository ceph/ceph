#!/usr/bin/python
import os
import sys
import beanstalkc
import yaml
import psutil
import subprocess
import tempfile
import logging

from .config import config

log = logging.getLogger(__name__)


def main(args):
    suite_name = args['--suite']
    archive_base = args['--archive']
    owner = args['--owner']
    machine_type = args['--machine_type']

    kill_suite(suite_name, archive_base, owner, machine_type)


def kill_suite(suite_name, archive_base=None, owner=None, machine_type=None):
    if archive_base:
        suite_archive_dir = os.path.join(archive_base, suite_name)
        job_info = find_suite_info(suite_archive_dir)
        machine_type = job_info['machine_type']
        owner = job_info['owner']

    remove_beanstalk_jobs(suite_name, machine_type)
    kill_processes(suite_name)
    nuke_machines(suite_name, owner)


def find_suite_info(suite_archive_dir):
    job_info = {}
    for job_dir in os.listdir(suite_archive_dir):
        if os.path.isdir(job_dir):
            job_info = find_job_info(job_dir)
            if job_info:
                break
    return job_info


def find_job_info(job_archive_dir):
    info_file = os.path.join(job_archive_dir, 'info.yaml')
    if os.path.isfile(info_file):
        job_info = yaml.safe_load(open(info_file, 'r'))
        return job_info
    return {}


def remove_beanstalk_jobs(suite_name, tube_name):
    qhost = config.queue_host
    qport = config.queue_port
    if qhost is None or qport is None:
        raise RuntimeError(
            'Beanstalk queue information not found in {conf_path}'.format(
                conf_path=config.teuthology_yaml))
    log.info("Checking Beanstalk Queue...")
    beanstalk = beanstalkc.Connection(host=qhost, port=qport)
    beanstalk.watch(tube_name)
    beanstalk.ignore('default')

    curjobs = beanstalk.stats_tube(tube_name)['current-jobs-ready']
    if curjobs != 0:
        x = 1
        while x != curjobs:
            x += 1
            job = beanstalk.reserve(timeout=20)
            job_config = yaml.safe_load(job.body)
            if suite_name == job_config['name']:
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
    beanstalk.close()


def kill_processes(suite_name):
    suite_pids = []
    for pid in psutil.get_pid_list():
        try:
            p = psutil.Process(pid)
        except psutil.NoSuchProcess:
            continue
        if suite_name in p.cmdline and sys.argv[0] not in p.cmdline:
                suite_pids.append(str(pid))

    if len(suite_pids) == 0:
        log.info("No teuthology processes running")
    else:
        log.info("Killing Pids: " + str(suite_pids))
        for pid in suite_pids:
            subprocess.call(['sudo', 'kill', pid])


def find_targets(suite_name, owner):
    lock_args = [
        'teuthology-lock',
        '--list-targets',
        '--desc-pattern',
        '/' + suite_name + '/',
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


def nuke_machines(suite_name, owner):
    targets_dict = find_targets(suite_name, owner)
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
