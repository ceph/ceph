import docopt

import teuthology.config
import teuthology.kill

doc = """
usage: teuthology-kill [-h] [-a ARCHIVE] -r RUN
       teuthology-kill [-h] [-a ARCHIVE] -m MACHINE_TYPE -r RUN
       teuthology-kill [-h] [-a ARCHIVE] -r RUN -j JOB ...
       teuthology-kill [-h] -o OWNER -m MACHINE_TYPE -r RUN

Kill running teuthology jobs:
1. Removes any queued jobs from the beanstalk queue
2. Kills any running jobs
3. Nukes any machines involved

optional arguments:
  -h, --help            show this help message and exit
  -a ARCHIVE, --archive ARCHIVE
                        The base archive directory
                        [default: {archive_base}]
  -r, --run RUN         The name(s) of the run(s) to kill
  -j, --job JOB         The job_id of the job to kill
  -o, --owner OWNER     The owner of the job(s)
  -m, --machine_type MACHINE_TYPE
                        The type of machine the job(s) are running on.
                        This is required if killing a job that is still
                        entirely in the queue.
""".format(archive_base=teuthology.config.config.archive_base)


def main():
    args = docopt.docopt(doc)
    teuthology.kill.main(args)
