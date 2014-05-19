import beanstalkc
import yaml
import logging
import sys
from collections import OrderedDict

from .config import config
from . import report

log = logging.getLogger(__name__)


def connect():
    host = config.queue_host
    port = config.queue_port
    if host is None or port is None:
        raise RuntimeError(
            'Beanstalk queue information not found in {conf_path}'.format(
                conf_path=config.teuthology_yaml))
    return beanstalkc.Connection(host=host, port=port)


def watch_tube(connection, tube_name):
    connection.watch(tube_name)
    connection.ignore('default')


def walk_jobs(connection, tube_name, callback, pattern=None):
    """
    def callback(jobs_dict)
    """
    log.info("Checking Beanstalk Queue...")
    job_count = connection.stats_tube(tube_name)['current-jobs-ready']
    if job_count == 0:
        log.info('No jobs in Beanstalk Queue')
        return

    # Try to figure out a sane timeout based on how many jobs are in the queue
    timeout = job_count / 2000.0 * 60
    matching_jobs = OrderedDict()
    for i in range(1, job_count + 1):
        print_progress(i, job_count, "Loading")
        job = connection.reserve(timeout=timeout)
        if job is None or job.body is None:
            continue
        job_config = yaml.safe_load(job.body)
        job_name = job_config['name']
        job_id = job.stats()['id']
        if pattern is not None and pattern not in job_name:
            continue
        matching_jobs[job_id] = [job, job_config]
    end_progress()
    callback(matching_jobs)


def print_progress(index, total, message=None):
    msg = "{m} ".format(m=message) if message else ''
    sys.stderr.write("{msg}{i}/{total}\r".format(
        msg=msg, i=index, total=total))
    sys.stderr.flush()


def end_progress():
    sys.stderr.write('\n')
    sys.stderr.flush()


def _print_matching_jobs(show_desc=False):
    def print_matching_jobs(jobs_dict):
        i = 0
        job_count = len(jobs_dict)
        for job_id, (job, job_config) in jobs_dict.iteritems():
            i += 1
            job_name = job_config['name']
            job_desc = job_config['description']
            job_id = job.stats()['id']
            print 'Job: {i}/{count} {job_name}/{job_id}'.format(
                i=i,
                count=job_count,
                job_id=job_id,
                job_name=job_name,
                )
            if job_desc and show_desc:
                for desc in job_desc.split():
                    print '\t {desc}'.format(desc=desc)
    return print_matching_jobs


def delete_matching_jobs(jobs_dict):
    for job_id, (job, job_config) in jobs_dict.iteritems():
        job_name = job_config['name']
        job_id = job.stats()['id']
        print 'Deleting {job_id}/{job_name}'.format(
            job_id=job_id,
            job_name=job_name,
            )
        job.delete()
        report.try_delete_jobs(job_name, job_id)


def print_matching_runs(jobs_dict):
    runs = set()
    for job_id, (job, job_config) in jobs_dict.iteritems():
        runs.add(job_config['name'])
    for run in runs:
        print run


def main(args):
    machine_type = args['--machine_type']
    delete = args['--delete']
    runs = args['--runs']
    show_desc = args['--description']
    try:
        connection = connect()
        watch_tube(connection, machine_type)
        if delete:
            walk_jobs(connection, machine_type,
                      delete_matching_jobs)
        elif runs:
            walk_jobs(connection, machine_type,
                      print_matching_runs)
        else:
            walk_jobs(connection, machine_type,
                      _print_matching_jobs(show_desc))
    except KeyboardInterrupt:
        log.info("Interrupted.")
    finally:
        connection.close()
