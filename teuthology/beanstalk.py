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

def walk_jobs(beanstalk, machine_type, delete=None):
    curjobs = beanstalk.stats_tube(machine_type)['current-jobs-ready']
    if curjobs != 0:
        x=1
        while x != curjobs:
            x += 1
            job = beanstalk.reserve(timeout=20)
            if job is not None:
                if job.body is not None:
                    job_config = yaml.safe_load(job.body)
                    job_name=job_config['name']
                    job_id = job.stats()['id']
                    job_description = job_config['description']
                    if delete:
                        if delete in job_name:
                            print 'Deleted Job: {job_id} from queue. Name: {job_name}'.format(
                            job_id = job_id,
                            job_name = job_name,
                            )
                            job.delete()
                        else:
                            print "Searching queue... Checked " + str(x) + "/" + str(curjobs)," Jobs\r",
                    else:
                        print 'Job: {x}/{curjobs} ID: {job_id} Name: {job_name}'.format(
                        x=x,
                        curjobs=curjobs,
                        job_id=job_id,
                        job_name=job_name,
                        )
                        if job_description:
                            for desc in job_description.split():
                                print '\t {desc}'.format(desc=desc)
        log.info("Finished checking Beanstalk Queue.")
    else:
        log.info('No jobs in Beanstalk Queue')

def main(args):
    machine_type =  args['--machine_type']
    delete = args['--delete']
    beanstalk = beanstalk_connect(machine_type)
    walk_jobs(beanstalk, machine_type, delete)
    beanstalk.close()
