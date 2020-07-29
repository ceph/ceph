import os
import yaml

import teuthology.beanstalk
from teuthology.misc import get_user, merge_configs
from teuthology import report


def main(args):
    if not args['--first-in-suite']:
        first_job_args = ['subset', 'seed']
        for arg in first_job_args:
            opt = '--{arg}'.format(arg=arg)
            msg_fmt = '{opt} is only applicable to the first job in a suite'
            if args[opt]:
                raise ValueError(msg_fmt.format(opt=opt))

    if not args['--last-in-suite']:
        last_job_args = ['email', 'timeout']
        for arg in last_job_args:
            opt = '--{arg}'.format(arg=arg)
            msg_fmt = '{opt} is only applicable to the last job in a suite'
            if args[opt]:
                raise ValueError(msg_fmt.format(opt=opt))

    if args['--first-in-suite'] or args['--last-in-suite']:
        report_status = False
    else:
        report_status = True

    name = args['--name']
    if not name or name.isdigit():
        raise ValueError("Please use a more descriptive value for --name")
    job_config = build_config(args)
    backend = args['--queue-backend']
    if args['--dry-run']:
        print('---\n' + yaml.safe_dump(job_config))
    elif backend == 'beanstalk':
        schedule_job(job_config, args['--num'], report_status)
    elif backend.startswith('@'):
        dump_job_to_file(backend.lstrip('@'), job_config, args['--num'])
    else:
        raise ValueError("Provided schedule backend '%s' is not supported. "
                         "Try 'beanstalk' or '@path-to-a-file" % backend)


def build_config(args):
    """
    Given a dict of arguments, build a job config
    """
    config_paths = args.get('<conf_file>', list())
    conf_dict = merge_configs(config_paths)
    # strip out targets; the worker will allocate new ones when we run
    # the job with --lock.
    if 'targets' in conf_dict:
        del conf_dict['targets']
    args['config'] = conf_dict

    owner = args['--owner']
    if owner is None:
        owner = 'scheduled_{user}'.format(user=get_user())

    job_config = dict(
        name=args['--name'],
        first_in_suite=args['--first-in-suite'],
        last_in_suite=args['--last-in-suite'],
        email=args['--email'],
        description=args['--description'],
        owner=owner,
        verbose=args['--verbose'],
        machine_type=args['--worker'],
        tube=args['--worker'],
        priority=int(args['--priority']),
    )
    # Update the dict we just created, and not the other way around, to let
    # settings in the yaml override what's passed on the command line. This is
    # primarily to accommodate jobs with multiple machine types.
    job_config.update(conf_dict)
    for arg,conf in {'--timeout':'results_timeout',
                     '--seed': 'seed',
                     '--subset': 'subset'}.items():
        val = args.get(arg, None)
        if val is not None:
            job_config[conf] = val

    return job_config


def schedule_job(job_config, num=1, report_status=True):
    """
    Schedule a job.

    :param job_config: The complete job dict
    :param num:      The number of times to schedule the job
    """
    num = int(num)
    job = yaml.safe_dump(job_config)
    tube = job_config.pop('tube')
    beanstalk = teuthology.beanstalk.connect()
    beanstalk.use(tube)
    while num > 0:
        jid = beanstalk.put(
            job,
            ttr=60 * 60 * 24,
            priority=job_config['priority'],
        )
        print('Job scheduled with name {name} and ID {jid}'.format(
            name=job_config['name'], jid=jid))
        job_config['job_id'] = str(jid)
        if report_status:
            report.try_push_job_info(job_config, dict(status='queued'))
        num -= 1


def dump_job_to_file(path, job_config, num=1):
    """
    Schedule a job.

    :param job_config: The complete job dict
    :param num:      The number of times to schedule the job
    :param path:     The file path where the job config to append
    """
    num = int(num)
    count_file_path = path + '.count'

    jid = 0
    if os.path.exists(count_file_path):
        with open(count_file_path, 'r') as f:
            jid=int(f.read() or '0')

    with open(path, 'a') as f:
        while num > 0:
            jid += 1
            job_config['job_id'] = str(jid)
            job = yaml.safe_dump(job_config)
            print('Job scheduled with name {name} and ID {jid}'.format(
                name=job_config['name'], jid=jid))
            f.write('---\n' + job)
            num -= 1
    with open(count_file_path, 'w') as f:
        f.write(str(jid))

