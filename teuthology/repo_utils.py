import logging
import os
import re
import shutil
import subprocess
import time

from teuthology import misc
from teuthology.util.flock import FileLock
from teuthology.config import config
from teuthology.contextutil import MaxWhileTries, safe_while
from teuthology.exceptions import BootstrapError, BranchNotFoundError, CommitNotFoundError, GitError

log = logging.getLogger(__name__)


# Repos must not have been fetched in the last X seconds to get fetched again.
# Similar for teuthology's bootstrap
FRESHNESS_INTERVAL = 60


def touch_file(path):
    out = subprocess.check_output(('touch', path))
    if out:
        log.info(out)


def is_fresh(path):
    """
    Has this file been modified in the last FRESHNESS_INTERVAL seconds?

    Returns False if the file does not exist
    """
    if not os.path.exists(path):
        return False
    elif time.time() - os.stat(path).st_mtime < FRESHNESS_INTERVAL:
        return True
    return False


def build_git_url(project, project_owner='ceph'):
    """
    Return the git URL to clone the project
    """
    if project == 'ceph-qa-suite':
        base = config.get_ceph_qa_suite_git_url()
    elif project == 'ceph-cm-ansible':
        base = config.get_ceph_cm_ansible_git_url()
    elif project == 'ceph':
        base = config.get_ceph_git_url()
    else:
        base = 'https://github.com/{project_owner}/{project}'
    url_templ = re.sub('\.git$', '', base)
    return url_templ.format(project_owner=project_owner, project=project)


def ls_remote(url, ref):
    """
    Return the current sha1 for a given repository and ref

    :returns: The sha1 if found; else None
    """
    sha1 = None
    cmd = "git ls-remote {} {}".format(url, ref)
    result = subprocess.check_output(
        cmd, shell=True).split()
    if result:
        sha1 = result[0].decode()
    log.debug("{} -> {}".format(cmd, sha1))
    return sha1


def enforce_repo_state(repo_url, dest_path, branch, commit=None, remove_on_error=True):
    """
    Use git to either clone or update a given repo, forcing it to switch to the
    specified branch.

    :param repo_url:        The full URL to the repo (not including the branch)
    :param dest_path:       The full path to the destination directory
    :param branch:          The branch.
    :param commit:          The sha1 to checkout. Defaults to None, which uses HEAD of the branch.
    :param remove_on_error: Whether or not to remove dest_dir when an error occurs
    :raises:                BranchNotFoundError if the branch is not found;
                            CommitNotFoundError if the commit is not found;
                            GitError for other errors
    """
    validate_branch(branch)
    sentinel = os.path.join(dest_path, '.fetched')
    # sentinel to track whether the repo has checked out the intended
    # version, in addition to being cloned
    repo_reset = os.path.join(dest_path, '.fetched_and_reset')
    try:
        if not os.path.isdir(dest_path):
            clone_repo(repo_url, dest_path, branch, shallow=commit is None)
        elif not commit and not is_fresh(sentinel):
            set_remote(dest_path, repo_url)
            fetch_branch(dest_path, branch)
            touch_file(sentinel)
        else:
            log.info("%s was just updated or references a specific commit; assuming it is current", dest_path)

        if commit and os.path.exists(repo_reset):
            return

        reset_repo(repo_url, dest_path, branch, commit)
        touch_file(repo_reset)
        # remove_pyc_files(dest_path)
    except (BranchNotFoundError, CommitNotFoundError):
        if remove_on_error:
            shutil.rmtree(dest_path, ignore_errors=True)
        raise


def clone_repo(repo_url, dest_path, branch, shallow=True):
    """
    Clone a repo into a path

    :param repo_url:  The full URL to the repo (not including the branch)
    :param dest_path: The full path to the destination directory
    :param branch:    The branch.
    :param shallow:   Whether to perform a shallow clone (--depth 1)
    :raises:          BranchNotFoundError if the branch is not found;
                      GitError for other errors
    """
    validate_branch(branch)
    log.info("Cloning %s %s from upstream", repo_url, branch)
    if branch.startswith('refs/'):
        clone_repo_ref(repo_url, dest_path, branch)
        return
    args = ['git', 'clone', '--single-branch']
    if shallow:
        args.extend(['--depth', '1'])
    args.extend(['--branch', branch, repo_url, dest_path])
    proc = subprocess.Popen(
        args,
        cwd=os.path.dirname(dest_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    not_found_str = "Remote branch %s not found" % branch
    out = proc.stdout.read().decode()
    result = proc.wait()
    # Newer git versions will bail if the branch is not found, but older ones
    # will not. Fortunately they both output similar text.
    if not_found_str in out:
        log.error(out)
        if result == 0:
            # Old git left a repo with the wrong branch. Remove it.
            shutil.rmtree(dest_path, ignore_errors=True)
        raise BranchNotFoundError(branch, repo_url)
    elif result != 0:
        # Unknown error
        raise GitError("git clone failed!")


def rsstrip(s, suffix):
    return s[:-len(suffix)] if s.endswith(suffix) else s


def lsstrip(s, prefix):
    return s[len(prefix):] if s.startswith(prefix) else s


def remote_ref_from_ref(ref, remote='origin'):
    if ref.startswith('refs/pull/'):
        return 'refs/remotes/' + remote + lsstrip(ref, 'refs')
    elif ref.startswith('refs/heads/'):
        return 'refs/remotes/' + remote + lsstrip(ref, 'refs/heads')
    raise GitError("Unsupported ref '%s'" % ref)


def local_branch_from_ref(ref):
    if ref.startswith('refs/pull/'):
        s = lsstrip(ref, 'refs/pull/')
        s = rsstrip(s, '/merge')
        s = rsstrip(s, '/head')
        return "PR#%s" % s
    elif ref.startswith('refs/heads/'):
        return lsstrip(ref, 'refs/heads/')
    raise GitError("Unsupported ref '%s', try 'refs/heads/' or 'refs/pull/'" % ref)


def fetch_refspec(ref):
    if '/' in ref:
        remote_ref = remote_ref_from_ref(ref)
        return "+%s:%s" % (ref, remote_ref)
    else:
        # looks like a branch name
        return ref


def clone_repo_ref(repo_url, dest_path, ref):
    branch_name = local_branch_from_ref(ref)
    remote_ref = remote_ref_from_ref(ref)
    misc.sh('git init %s' % dest_path)
    misc.sh('git remote add origin %s' % repo_url, cwd=dest_path)
    #misc.sh('git fetch --depth 1 origin %s' % fetch_refspec(ref),
    #                                                        cwd=dest_path)
    fetch_branch(dest_path, ref)
    misc.sh('git checkout -b %s %s' % (branch_name, remote_ref),
                                                            cwd=dest_path)


def set_remote(repo_path, repo_url):
    """
    Call "git remote set-url origin <repo_url>"

    :param repo_url:  The full URL to the repo (not including the branch)
    :param repo_path: The full path to the repository
    :raises:          GitError if the operation fails
    """
    log.debug("Setting repo remote to %s", repo_url)
    proc = subprocess.Popen(
        ('git', 'remote', 'set-url', 'origin', repo_url),
        cwd=repo_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    if proc.wait() != 0:
        out = proc.stdout.read()
        log.error(out)
        raise GitError("git remote set-url failed!")


def fetch(repo_path):
    """
    Call "git fetch -p origin"

    :param repo_path: The full path to the repository
    :raises:          GitError if the operation fails
    """
    log.info("Fetching from upstream into %s", repo_path)
    proc = subprocess.Popen(
        ('git', 'fetch', '-p', 'origin'),
        cwd=repo_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    if proc.wait() != 0:
        out = proc.stdout.read().decode()
        log.error(out)
        raise GitError("git fetch failed!")


def fetch_branch(repo_path, branch, shallow=True):
    """
    Call "git fetch -p origin <branch>"

    :param repo_path: The full path to the repository on-disk
    :param branch:    The branch.
    :param shallow:   Whether to perform a shallow fetch (--depth 1)
    :raises:          BranchNotFoundError if the branch is not found;
                      GitError for other errors
    """
    validate_branch(branch)
    log.info("Fetching %s from origin", branch)
    args = ['git', 'fetch']
    if shallow:
        args.extend(['--depth', '1'])
    args.extend(['-p', 'origin', fetch_refspec(branch)])
    proc = subprocess.Popen(
        args,
        cwd=repo_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    if proc.wait() != 0:
        not_found_str = "fatal: couldn't find remote ref %s" % branch
        out = proc.stdout.read().decode()
        log.error(out)
        if not_found_str in out.lower():
            raise BranchNotFoundError(branch)
        else:
            raise GitError("git fetch failed!")


def reset_repo(repo_url, dest_path, branch, commit=None):
    """

    :param repo_url:  The full URL to the repo (not including the branch)
    :param dest_path: The full path to the destination directory
    :param branch:    The branch.
    :param commit:    The sha1 to checkout. Defaults to None, which uses HEAD of the branch.
    :raises:          BranchNotFoundError if the branch is not found;
                      CommitNotFoundError if the commit is not found;
                      GitError for other errors
    """
    validate_branch(branch)
    if '/' in branch:
        reset_branch = lsstrip(remote_ref_from_ref(branch), 'refs/remotes/')
    else:
        reset_branch = 'origin/%s' % branch
    reset_ref = commit or reset_branch
    log.info('Resetting repo at %s to %s', dest_path, reset_ref)
    # This try/except block will notice if the requested branch doesn't
    # exist, whether it was cloned or fetched.
    try:
        subprocess.check_output(
            ('git', 'reset', '--hard', reset_ref),
            cwd=dest_path,
        )
    except subprocess.CalledProcessError:
        if commit:
            raise CommitNotFoundError(commit, repo_url)
        raise BranchNotFoundError(branch, repo_url)


def remove_pyc_files(dest_path):
    subprocess.check_call(
        ['find', dest_path, '-name', '*.pyc', '-exec', 'rm', '{}', ';']
    )


def validate_branch(branch):
    if ' ' in branch:
        raise ValueError("Illegal branch name: '%s'" % branch)


def fetch_repo(url, branch, commit=None, bootstrap=None, lock=True):
    """
    Make sure we have a given project's repo checked out and up-to-date with
    the current branch requested

    :param url:        The URL to the repo
    :param bootstrap:  An optional callback function to execute. Gets passed a
                       dest_dir argument: the path to the repo on-disk.
    :param branch:     The branch we want
    :param commit:     The sha1 to checkout. Defaults to None, which uses HEAD of the branch.
    :returns:          The destination path
    """
    src_base_path = config.src_base_path
    if not os.path.exists(src_base_path):
        os.mkdir(src_base_path)
    ref_dir = ref_to_dirname(commit or branch)
    dirname = '%s_%s' % (url_to_dirname(url), ref_dir)
    dest_path = os.path.join(src_base_path, dirname)
    # only let one worker create/update the checkout at a time
    lock_path = dest_path.rstrip('/') + '.lock'
    with FileLock(lock_path, noop=not lock):
        with safe_while(sleep=10, tries=60) as proceed:
            try:
                while proceed():
                    try:
                        enforce_repo_state(url, dest_path, branch, commit)
                        if bootstrap:
                            sentinel = os.path.join(dest_path, '.bootstrapped')
                            if commit and os.path.exists(sentinel) or is_fresh(sentinel):
                                log.info(
                                    "Skipping bootstrap as it was already done in the last %ss",
                                    FRESHNESS_INTERVAL,
                                )
                                break
                            bootstrap(dest_path)
                            touch_file(sentinel)
                        break
                    except GitError:
                        log.exception("Git error encountered; retrying")
                    except BootstrapError:
                        log.exception("Bootstrap error encountered; retrying")
            except MaxWhileTries:
                shutil.rmtree(dest_path, ignore_errors=True)
                raise
    return dest_path


def ref_to_dirname(branch):
    if '/' in branch:
        return local_branch_from_ref(branch)
    else:
        return branch


def url_to_dirname(url):
    """
    Given a URL, returns a string that's safe to use as a directory name.
    Examples:

        git@git.ceph.com/ceph-qa-suite.git -> git.ceph.com_ceph-qa-suite
        git://git.ceph.com/ceph-qa-suite.git -> git.ceph.com_ceph-qa-suite
        https://github.com/ceph/ceph -> github.com_ceph_ceph
        https://github.com/liewegas/ceph.git -> github.com_liewegas_ceph
        file:///my/dir/has/ceph.git -> my_dir_has_ceph
    """
    # Strip protocol from left-hand side
    string = re.match('(?:.*://|.*@)(.*)', url).groups()[0]
    # Strip '.git' from the right-hand side
    string = string.rstrip('.git')
    # Replace certain characters with underscores
    string = re.sub('[:/]', '_', string)
    # Remove duplicate underscores
    string = re.sub('_+', '_', string)
    # Remove leading or trailing underscore
    string = string.strip('_')
    return string


def fetch_qa_suite(branch, commit=None, lock=True):
    """
    Make sure ceph-qa-suite is checked out.

    :param branch: The branch to fetch
    :param commit: The sha1 to checkout. Defaults to None, which uses HEAD of the branch.
    :returns:      The destination path
    """
    return fetch_repo(config.get_ceph_qa_suite_git_url(),
                      branch, commit, lock=lock)


def fetch_teuthology(branch, commit=None, lock=True):
    """
    Make sure we have the correct teuthology branch checked out and up-to-date

    :param branch: The branch we want
    :param commit: The sha1 to checkout. Defaults to None, which uses HEAD of the branch.
    :returns:      The destination path
    """
    url = config.ceph_git_base_url + 'teuthology.git'
    return fetch_repo(url, branch, commit, bootstrap_teuthology, lock)


def bootstrap_teuthology(dest_path):
        log.info("Bootstrapping %s", dest_path)
        # This magic makes the bootstrap script not attempt to clobber an
        # existing virtualenv. But the branch's bootstrap needs to actually
        # check for the NO_CLOBBER variable.
        env = os.environ.copy()
        env['NO_CLOBBER'] = '1'
        cmd = './bootstrap'
        boot_proc = subprocess.Popen(cmd, shell=True, cwd=dest_path, env=env,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT)
        out, err = boot_proc.communicate()
        returncode = boot_proc.wait()
        log.info("Bootstrap exited with status %s", returncode)
        if returncode != 0:
            for line in out.split():
                log.warn(line.strip())
            venv_path = os.path.join(dest_path, 'virtualenv')
            log.info("Removing %s", venv_path)
            shutil.rmtree(venv_path, ignore_errors=True)
            raise BootstrapError("Bootstrap failed!")
