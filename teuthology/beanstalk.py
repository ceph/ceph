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


def walk_jobs(connection, tube_name, processor, pattern=None):
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
        processor.add_job(job_id, job_config, job)
    end_progress()
    processor.complete()


def print_progress(index, total, message=None):
    msg = "{m} ".format(m=message) if message else ''
    sys.stderr.write("{msg}{i}/{total}\r".format(
        msg=msg, i=index, total=total))
    sys.stderr.flush()


def end_progress():
    sys.stderr.write('\n')
    sys.stderr.flush()


class JobProcessor(object):
    def __init__(self):
        self.jobs = OrderedDict()

    def add_job(self, job_id, job_config, job_obj=None):
        job_id = str(job_id)

        job_dict = dict(
            index=(len(self.jobs) + 1),
            job_config=job_config,
        )
        if job_obj:
            job_dict['job_obj'] = job_obj
        self.jobs[job_id] = job_dict

        self.process_job(job_id)

    def process_job(self, job_id):
        pass

    def complete(self):
        pass


class JobPrinter(JobProcessor):
    def __init__(self, show_desc=False):
        super(JobPrinter, self).__init__()
        self.show_desc = show_desc

    def process_job(self, job_id):
        job_config = self.jobs[job_id]['job_config']
        job_index = self.jobs[job_id]['index']
        job_name = job_config['name']
        job_desc = job_config['description']
        print 'Job: {i:>4} {job_name}/{job_id}'.format(
            i=job_index,
            job_id=job_id,
            job_name=job_name,
            )
        if job_desc and self.show_desc:
            for desc in job_desc.split():
                print '\t {desc}'.format(desc=desc)


class RunPrinter(JobProcessor):
    def __init__(self):
        super(RunPrinter, self).__init__()
        self.runs = list()

    def process_job(self, job_id):
        run = self.jobs[job_id]['job_config']['name']
        if run not in self.runs:
            self.runs.append(run)
            print run


class JobDeleter(JobProcessor):
    def process_job(self, job_id):
        job_config = self.jobs[job_id]['job_config']
        job_name = job_config['name']
        print 'Deleting {job_id}/{job_name}'.format(
            job_id=job_id,
            job_name=job_name,
            )
        job_obj = self.jobs[job_id].get('job_obj')
        if job_obj:
            job_obj.delete()
        report.try_delete_jobs(job_name, job_id)


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
                      JobDeleter())
        elif runs:
            walk_jobs(connection, machine_type,
                      RunPrinter())
        else:
            walk_jobs(connection, machine_type,
                      JobPrinter(show_desc=show_desc))
    except KeyboardInterrupt:
        log.info("Interrupted.")
    finally:
        connection.close()
