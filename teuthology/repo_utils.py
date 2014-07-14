import logging
import os
import shutil
import subprocess
import time

log = logging.getLogger(__name__)


def enforce_repo_state(repo_url, dest_path, branch, remove_on_error=True):
    """
    Use git to either clone or update a given repo, forcing it to switch to the
    specified branch.

    :param repo_url:  The full URL to the repo (not including the branch)
    :param dest_path: The full path to the destination directory
    :param branch:    The branch.
    :param remove:    Whether or not to remove dest_dir when an error occurs
    :raises:          BranchNotFoundError if the branch is not found;
                      RuntimeError for other errors
    """
    validate_branch(branch)
    try:
        if not os.path.isdir(dest_path):
            clone_repo(repo_url, dest_path, branch)
        elif time.time() - os.stat('/etc/passwd').st_mtime > 60:
            # only do this at most once per minute
            fetch_branch(dest_path, branch)
            out = subprocess.check_output(('touch', dest_path))
            if out:
                log.info(out)
        else:
            log.info("%s was just updated; assuming it is current", branch)

        reset_repo(repo_url, dest_path, branch)
    except BranchNotFoundError:
        if remove_on_error:
            shutil.rmtree(dest_path, ignore_errors=True)
        raise


def clone_repo(repo_url, dest_path, branch):
    """
    Clone a repo into a path

    :param repo_url:  The full URL to the repo (not including the branch)
    :param dest_path: The full path to the destination directory
    :param branch:    The branch.
    :raises:          BranchNotFoundError if the branch is not found;
                      RuntimeError for other errors
    """
    validate_branch(branch)
    log.info("Cloning %s %s from upstream", repo_url, branch)
    proc = subprocess.Popen(
        ('git', 'clone', '--branch', branch, repo_url, dest_path),
        cwd=os.path.dirname(dest_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    if proc.wait() != 0:
        not_found_str = "Remote branch %s not found" % branch
        out = proc.stdout.read()
        log.error(out)
        if not_found_str in out:
            raise BranchNotFoundError(branch, repo_url)
        else:
            raise RuntimeError("git clone failed!")


def fetch_branch(dest_path, branch):
    """
    Call "git fetch -p origin <branch>"

    :param dest_path: The full path to the destination directory
    :param branch:    The branch.
    :raises:          BranchNotFoundError if the branch is not found;
                      RuntimeError for other errors
    """
    validate_branch(branch)
    log.info("Fetching %s from upstream", branch)
    proc = subprocess.Popen(
        ('git', 'fetch', '-p', 'origin', branch),
        cwd=dest_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    if proc.wait() != 0:
        not_found_str = "fatal: Couldn't find remote ref %s" % branch
        out = proc.stdout.read()
        log.error(out)
        if not_found_str in out:
            raise BranchNotFoundError(branch)
        else:
            raise RuntimeError("git fetch failed!")


def reset_repo(repo_url, dest_path, branch):
    """

    :param repo_url:  The full URL to the repo (not including the branch)
    :param dest_path: The full path to the destination directory
    :param branch:    The branch.
    :raises:          BranchNotFoundError if the branch is not found;
                      RuntimeError for other errors
    """
    validate_branch(branch)
    # This try/except block will notice if the requested branch doesn't
    # exist, whether it was cloned or fetched.
    try:
        subprocess.check_output(
            ('git', 'reset', '--hard', 'origin/%s' % branch),
            cwd=dest_path,
        )
    except subprocess.CalledProcessError:
        raise BranchNotFoundError(branch, repo_url)


class BranchNotFoundError(ValueError):
    def __init__(self, branch, repo=None):
        self.branch = branch
        self.repo = repo

    def __str__(self):
        if self.repo:
            repo_str = " in repo: %s" % self.repo
        else:
            repo_str = ""
        return "Branch '{branch}' not found{repo_str}!".format(
            branch=self.branch, repo_str=repo_str)


def validate_branch(branch):
    if ' ' in branch:
        raise ValueError("Illegal branch name: '%s'" % branch)
