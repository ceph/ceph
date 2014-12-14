import os
import yaml
import errno
import re

from .job_status import get_status


def main(args):
    return ls(args["<archive_dir>"], args["--verbose"])


def ls(archive_dir, verbose):
    for j in get_jobs(archive_dir):
        job_dir = os.path.join(archive_dir, j)
        summary = {}
        try:
            with file(os.path.join(job_dir, 'summary.yaml')) as f:
                g = yaml.safe_load_all(f)
                for new in g:
                    summary.update(new)
        except IOError as e:
            if e.errno == errno.ENOENT:
                print_debug_info(j, job_dir, archive_dir)
                continue
            else:
                raise

        print "{job} {status} {owner} {desc} {duration}s".format(
            job=j,
            owner=summary.get('owner', '-'),
            desc=summary.get('description', '-'),
            status=get_status(summary),
            duration=int(summary.get('duration', 0)),
        )
        if verbose and 'failure_reason' in summary:
            print '    {reason}'.format(reason=summary['failure_reason'])


def get_jobs(archive_dir):
    dir_contents = os.listdir(archive_dir)

    def is_job_dir(parent, subdir):
        if (os.path.isdir(os.path.join(parent, subdir)) and re.match('\d+$',
                                                                     subdir)):
            return True
        return False

    jobs = [job for job in dir_contents if is_job_dir(archive_dir, job)]
    return sorted(jobs)


def print_debug_info(job, job_dir, archive_dir):
    print '%s      ' % job,

    try:
        pidfile = os.path.join(job_dir, 'pid')
        found = False
        if os.path.isfile(pidfile):
            pid = open(pidfile, 'r').read()
            if os.path.isdir("/proc/%s" % pid):
                cmdline = open('/proc/%s/cmdline' % pid,
                               'r').read()
                if cmdline.find(archive_dir) >= 0:
                    print '(pid %s)' % pid,
                    found = True
        if not found:
            print '(no process or summary.yaml)',
        # tail
        tail = os.popen(
            'tail -1 %s/%s/teuthology.log' % (archive_dir, job)
        ).read().rstrip()
        print tail,
    except IOError:
        pass
    print ''
