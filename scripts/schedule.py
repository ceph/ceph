import docopt

import teuthology.misc
import teuthology.schedule
import sys

doc = """
usage: teuthology-schedule -h
       teuthology-schedule [options] --name <name> [--] [<conf_file> ...]

Schedule ceph integration tests

positional arguments:
  <conf_file>                          Config file to read

optional arguments:
  -h, --help                           Show this help message and exit
  -v, --verbose                        Be more verbose
  -b <backend>, --queue-backend <backend>
                                       Queue backend name, use prefix '@'
                                       to append job config to the given
                                       file path as yaml.
                                       [default: beanstalk]
  -n <name>, --name <name>             Name of suite run the job is part of
  -d <desc>, --description <desc>      Job description
  -o <owner>, --owner <owner>          Job owner
  -w <worker>, --worker <worker>       Which worker to use (type of machine)
                                       [default: plana]
  -p <priority>, --priority <priority> Job priority (lower is sooner)
                                       [default: 1000]
  -N <num>, --num <num>                Number of times to run/queue the job
                                       [default: 1]

  --first-in-suite                     Mark the first job in a suite so suite
                                       can note down the rerun-related info
                                       [default: False]
  --last-in-suite                      Mark the last job in a suite so suite
                                       post-processing can be run
                                       [default: False]
  --email <email>                      Where to send the results of a suite.
                                       Only applies to the last job in a suite.
  --timeout <timeout>                  How many seconds to wait for jobs to
                                       finish before emailing results. Only
                                       applies to the last job in a suite.
  --seed <seed>                        The random seed for rerunning the suite.
                                       Only applies to the last job in a suite.
  --subset <subset>                    The subset option passed to teuthology-suite.
                                       Only applies to the last job in a suite.
  --dry-run                            Instead of scheduling, just output the
                                       job config.

"""


def main(argv=sys.argv[1:]):
    args = docopt.docopt(doc, argv=argv)
    teuthology.schedule.main(args)
