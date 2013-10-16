import docopt

import teuthology.kill

doc = """
usage: teuthology-kill [-h] -a ARCHIVE -s SUITE
       teuthology-kill [-h] -o OWNER -m MACHINE_TYPE -s SUITE

Kill running teuthology jobs:
1. Removes any queued jobs from the beanstalk queue
2. Kills any running jobs
3. Nukes any machines involved

optional arguments:
  -h, --help            show this help message and exit
  -a ARCHIVE, --archive ARCHIVE
                        The base archive directory
  -s, --suite SUITE     The name(s) of the suite(s) to kill
  -o, --owner OWNER     The owner of the job(s)
  -m, --machine_type MACHINE_TYPE
                        The type of machine the job(s) are running on
"""


def main():
    args = docopt.docopt(doc)
    teuthology.kill.main(args)
