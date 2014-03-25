import beanstalkc
import yaml
import logging

from .config import config

log = logging.getLogger(__name__)


def beanstalk_connect(machine_type):
    qhost = config.queue_host
    qport = config.queue_port
    if qhost is None or qport is None:
        raise RuntimeError(
            'Beanstalk queue information not found in {conf_path}'.format(
                conf_path=config.teuthology_yaml))
    log.info("Checking Beanstalk Queue...")
    beanstalk = beanstalkc.Connection(host=qhost, port=qport)
    beanstalk.watch(machine_type)
    beanstalk.ignore('default')
    return beanstalk


def walk_jobs(beanstalk, machine_type, show_desc=False, delete=None):
    job_count = beanstalk.stats_tube(machine_type)['current-jobs-ready']
    if job_count == 0:
        log.info('No jobs in Beanstalk Queue')
        return
    x = 1
    while x < job_count:
        x += 1
        job = beanstalk.reserve(timeout=20)
        if job is not None and job.body is not None:
            job_config = yaml.safe_load(job.body)
            job_name = job_config['name']
            job_id = job.stats()['id']
            job_description = job_config['description']
            if delete:
                if delete in job_name:
                    m = 'Deleting {job_id}/{job_name}'.format(
                        job_id=job_id,
                        job_name=job_name,
                        )
                    print m
                    job.delete()
                else:
                    m = "Searching queue... Checked {x}/{count} Jobs\r".format(
                        x=x, count=job_count)
                    print m,
            else:
                m = 'Job: {x}/{count} {job_name}/{job_id}'.format(
                    x=x,
                    count=job_count,
                    job_id=job_id,
                    job_name=job_name,
                    )
                print m
                if job_description and show_desc:
                    for desc in job_description.split():
                        print '\t {desc}'.format(desc=desc)
    log.info("Finished checking Beanstalk Queue.")


def main(args):
    machine_type = args['--machine_type']
    delete = args['--delete']
    show_desc = args['--description']
    try:
        beanstalk = beanstalk_connect(machine_type)
        walk_jobs(beanstalk, machine_type, show_desc=show_desc, delete=delete)
    except KeyboardInterrupt:
        log.info("Interrupted.")
    finally:
        beanstalk.close()
