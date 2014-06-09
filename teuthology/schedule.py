import yaml

import teuthology.beanstalk
from teuthology.misc import get_user
from teuthology.misc import read_config
from teuthology import report


def main(ctx):
    if ctx.owner is None:
        ctx.owner = 'scheduled_{user}'.format(user=get_user())
    read_config(ctx)

    beanstalk = teuthology.beanstalk.connect()

    tube = ctx.worker
    beanstalk.use(tube)

    # strip out targets; the worker will allocate new ones when we run
    # the job with --lock.
    if ctx.config.get('targets'):
        del ctx.config['targets']

    job_config = dict(
        name=ctx.name,
        last_in_suite=ctx.last_in_suite,
        email=ctx.email,
        description=ctx.description,
        owner=ctx.owner,
        verbose=ctx.verbose,
        machine_type=ctx.worker,
    )
    # Merge job_config and ctx.config
    job_config.update(ctx.config)
    if ctx.timeout is not None:
        job_config['results_timeout'] = ctx.timeout

    job = yaml.safe_dump(job_config)
    num = ctx.num
    while num > 0:
        jid = beanstalk.put(
            job,
            ttr=60 * 60 * 24,
            priority=ctx.priority,
        )
        print 'Job scheduled with name {name} and ID {jid}'.format(
            name=ctx.name, jid=jid)
        job_config['job_id'] = str(jid)
        report.try_push_job_info(job_config, dict(status='queued'))
        num -= 1
