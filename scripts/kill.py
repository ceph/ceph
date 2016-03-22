import docopt

import teuthology.config
import teuthology.kill

doc = """
usage: teuthology-kill -h
       teuthology-kill [-a ARCHIVE] [-p] -r RUN
       teuthology-kill [-a ARCHIVE] [-p] -m MACHINE_TYPE -r RUN
       teuthology-kill [-a ARCHIVE] [-o OWNER] -r RUN -j JOB ...
       teuthology-kill [-a ARCHIVE] [-o OWNER] -J JOBSPEC
       teuthology-kill [-p] -o OWNER -m MACHINE_TYPE -r RUN

Kill running teuthology jobs:
1. Removes any queued jobs from the beanstalk queue
2. Kills any running jobs
3. Nukes any machines involved

NOTE: Must be run on the same machine that is executing the teuthology job
processes.

optional arguments:
  -h, --help            show this help message and exit
  -a ARCHIVE, --archive ARCHIVE
                        The base archive directory
                        [default: {archive_base}]
  -p, --preserve-queue  Preserve the queue - do not delete queued jobs
  -r, --run RUN         The name(s) of the run(s) to kill
  -j, --job JOB         The job_id of the job to kill
  -J, --jobspec JOBSPEC
                        The 'jobspec' of the job to kill. A jobspec consists of
                        both the name of the run and the job_id, separated by a
                        '/'. e.g. 'my-test-run/1234'
  -o, --owner OWNER     The owner of the job(s)
  -m, --machine-type MACHINE_TYPE
                        The type of machine the job(s) are running on.
                        This is required if killing a job that is still
                        entirely in the queue.
""".format(archive_base=teuthology.config.config.archive_base)


def main():
    args = docopt.docopt(doc)
    teuthology.kill.main(args)
