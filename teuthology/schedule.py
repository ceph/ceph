import yaml

import teuthology.queue
from teuthology.misc import get_user
from teuthology.misc import read_config


def main(ctx):
    if ctx.owner is None:
        ctx.owner = 'scheduled_{user}'.format(user=get_user())
    read_config(ctx)

    beanstalk = teuthology.queue.connect(ctx)

    tube = ctx.worker
    beanstalk.use(tube)

    if ctx.show:
        for job_id in ctx.show:
            job = beanstalk.peek(job_id)
            if job is None and ctx.verbose:
                print 'job {jid} is not in the queue'.format(jid=job_id)
            else:
                print '--- job {jid} priority {prio} ---\n'.format(
                    jid=job_id,
                    prio=job.stats()['pri']), job.body
        return

    if ctx.delete:
        for job_id in ctx.delete:
            job = beanstalk.peek(job_id)
            if job is None:
                print 'job {jid} is not in the queue'.format(jid=job_id)
            else:
                job.delete()
        return

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
        num -= 1
