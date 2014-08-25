import docopt

import teuthology.config
import teuthology.beanstalk

doc = """
usage: teuthology-queue -h
       teuthology-queue [-d|-f] -m MACHINE_TYPE
       teuthology-queue [-r] -m MACHINE_TYPE
       teuthology-queue -m MACHINE_TYPE -D PATTERN

List Jobs in queue.
If -D is passed, then jobs with PATTERN in the job name are deleted from the
queue.

Arguments:
  -m, --machine_type MACHINE_TYPE [default: multi]
                        Which machine type queue to work on.

optional arguments:
  -h, --help            Show this help message and exit
  -D, --delete PATTERN  Delete Jobs with PATTERN in their name
  -d, --description     Show job descriptions
  -r, --runs            Only show run names
  -f, --full            Print the entire job config. Use with caution.
""".format(archive_base=teuthology.config.config.archive_base)


def main():
    args = docopt.docopt(doc)
    teuthology.beanstalk.main(args)
