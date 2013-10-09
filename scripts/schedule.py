import argparse

import teuthology.run
import teuthology.schedule


def main():
    teuthology.schedule.main(parse_args())


def parse_args():
    parser = argparse.ArgumentParser(
        description='Schedule ceph integration tests')
    parser.add_argument(
        'config',
        metavar='CONFFILE',
        nargs='*',
        type=teuthology.run.config_file,
        action=teuthology.run.MergeConfig,
        default={},
        help='config file to read',
    )
    parser.add_argument(
        '--name',
        help='name of suite run the job is part of',
    )
    parser.add_argument(
        '--last-in-suite',
        action='store_true',
        default=False,
        help='mark the last job in a suite so suite post-processing can be ' +
        'run',
    )
    parser.add_argument(
        '--email',
        help='where to send the results of a suite (only applies to the ' +
        'last job in a suite)',
    )
    parser.add_argument(
        '--timeout',
        help='how many seconds to wait for jobs to finish before emailing ' +
        'results (only applies to the last job in a suite',
        type=int,
    )
    parser.add_argument(
        '--description',
        help='job description',
    )
    parser.add_argument(
        '--owner',
        help='job owner',
    )
    parser.add_argument(
        '--delete',
        metavar='JOBID',
        type=int,
        nargs='*',
        help='list of jobs to remove from the queue',
    )
    parser.add_argument(
        '-n', '--num',
        default=1,
        type=int,
        help='number of times to run/queue the job'
    )
    parser.add_argument(
        '-p', '--priority',
        default=1000,
        type=int,
        help='beanstalk priority (lower is sooner)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        help='be more verbose',
    )
    parser.add_argument(
        '-w', '--worker',
        default='plana',
        help='which worker to use (type of machine)',
    )
    parser.add_argument(
        '-s', '--show',
        metavar='JOBID',
        type=int,
        nargs='*',
        help='output the contents of specified jobs in the queue',
    )

    args = parser.parse_args()

    if not args.last_in_suite:
        msg = '--email is only applicable to the last job in a suite'
        assert not args.email, msg
        msg = '--timeout is only applicable to the last job in a suite'
        assert not args.timeout, msg

    return args
