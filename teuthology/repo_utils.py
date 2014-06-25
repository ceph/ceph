import logging
import os
import shutil
import subprocess
import time

log = logging.getLogger(__name__)


def checkout_repo(repo_url, dest_path, branch):
    # if os.path.isdir(path):
    #     p = subprocess.Popen('git status', shell=True, cwd=path)
    #     if p.wait() == 128:
    #         log.info("Repo at %s appears corrupt; removing",
    #                  branch)
    #         shutil.rmtree(path)

    if not os.path.isdir(dest_path):
        log.info("Cloning %s %s from upstream", repo_url, branch)
        log.info(
            subprocess.check_output(('git', 'clone', '--branch', branch,
                                     repo_url, dest_path),
                                    cwd=os.path.dirname(dest_path))
        )
    elif time.time() - os.stat('/etc/passwd').st_mtime > 60:
        # only do this at most once per minute
        log.info("Fetching %s from upstream", branch)
        log.info(
            subprocess.check_output(('git', 'fetch', '-p', 'origin'),
                                    cwd=dest_path)
        )
        log.info(
            subprocess.check_output(('touch', dest_path))
        )
    else:
        log.info("%s was just updated; assuming it is current", branch)

    # This try/except block will notice if the requested branch doesn't
    # exist, whether it was cloned or fetched.
    try:
        subprocess.check_output(
            ('git', 'reset', '--hard', 'origin/%s' % branch),
            cwd=dest_path,
        )
    except subprocess.CalledProcessError:
        shutil.rmtree(dest_path)
        raise BranchNotFoundError(branch)


class BranchNotFoundError(ValueError):
    def __init__(self, branch):
        self.branch = branch

    def __str__(self):
        return "teuthology branch not found: '{0}'".format(self.branch)
