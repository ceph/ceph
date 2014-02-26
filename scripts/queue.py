import docopt

import teuthology.config
import teuthology.beanstalk

doc = """
usage: teuthology-queue [-h] -m MACHINE_TYPE
       teuthology-queue [-h] -m MACHINE_TYPE -d JOB

List Jobs in queue:
  If -d then jobs with JOB in the job name are deleted from the queue.

Arguments:
  -m, --machine_type MACHINE_TYPE
                        Which machine type queue to work on.

optional arguments:
  -h, --help            show this help message and exit

  -d, --delete JOB      Delete Jobs JOB in their name.

""".format(archive_base=teuthology.config.config.archive_base)


def main():
    args = docopt.docopt(doc)
    teuthology.beanstalk.main(args)
