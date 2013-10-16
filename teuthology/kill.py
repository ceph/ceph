#!/usr/bin/python
import os
import sys
import beanstalkc
import yaml
import psutil
import subprocess
import tempfile

from .config import config


def main(args):
    suite_name = args['--suite']
    archive_base = args['--archive']
    owner = args['--owner']
    machine_type = args['--machine_type']

    kill_suite(suite_name, archive_base, owner, machine_type)
    #if args.suite:
    #    for suite_name in args.suite:
    #        kill_suite(args.archive, suite_name)


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
    print "Checking Beanstalk Queue..."
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
                print msg
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
        print "No teuthology processes running"
    else:
        print 'Killing Pids: ' + str(suite_pids)
        for pid in suite_pids:
            subprocess.call(['sudo', 'kill', pid])


def nuke_machines(suite_name, owner):
    #lock_args = argparse.Namespace(
    #    list_targets=True,
    #    desc_pattern='/%s/' % suite_name,
    #    status='up',
    #    owner=owner,
    #)
    proc = subprocess.Popen(
        ['teuthology-lock',
            '--list-targets',
            '--desc-pattern',
            '/' + suite_name + '/',
            '--status',
            'up',
            '--owner',
            owner],
        stdout=subprocess.PIPE)
    tout, terr = proc.communicate()

    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp.write(tout)
    tmp.close()

    targets = yaml.safe_load(tout)['targets']
    nuking = []
    for target in targets:
        nuking.append(target.split('@')[1].split('.')[0])
    if 'ubuntu' not in tout:
        print 'No locked machines. Not nuking anything'
    else:
        print 'Nuking machines: ' + str(nuking)
        nukeargs = [
            '/var/lib/teuthworker/teuthology-master/virtualenv/bin/teuthology-nuke',  # noqa
            '-t', tmp.name, '--unlock', '-r', '--owner', owner]
        nuke = subprocess.Popen(
            nukeargs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in iter(nuke.stdout.readline, ''):
            line = line.replace('\r', '').replace('\n', '')
            print line
            sys.stdout.flush()

    os.unlink(tmp.name)
