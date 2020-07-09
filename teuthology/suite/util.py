import copy
import logging
import os
import requests
import smtplib
import socket
import subprocess
import sys

from email.mime.text import MIMEText

import teuthology.lock.query
import teuthology.lock.util
from teuthology import repo_utils

from teuthology.config import config
from teuthology.exceptions import BranchNotFoundError, ScheduleFailError
from teuthology.misc import deep_merge
from teuthology.repo_utils import fetch_qa_suite, fetch_teuthology
from teuthology.orchestra.opsys import OS
from teuthology.packaging import get_builder_project
from teuthology.repo_utils import build_git_url
from teuthology.suite.build_matrix import combine_path
from teuthology.task.install import get_flavor

log = logging.getLogger(__name__)


def fetch_repos(branch, test_name):
    """
    Fetch the suite repo (and also the teuthology repo) so that we can use it
    to build jobs. Repos are stored in ~/src/.

    The reason the teuthology repo is also fetched is that currently we use
    subprocess to call teuthology-schedule to schedule jobs so we need to make
    sure it is up-to-date. For that reason we always fetch the master branch
    for test scheduling, regardless of what teuthology branch is requested for
    testing.

    :returns: The path to the suite repo on disk
    """
    try:
        # When a user is scheduling a test run from their own copy of
        # teuthology, let's not wreak havoc on it.
        if config.automated_scheduling:
            # We use teuthology's master branch in all cases right now
            if config.teuthology_path is None:
                fetch_teuthology('master')
        suite_repo_path = fetch_qa_suite(branch)
    except BranchNotFoundError as exc:
        schedule_fail(message=str(exc), name=test_name)
    return suite_repo_path


def schedule_fail(message, name=''):
    """
    If an email address has been specified anywhere, send an alert there. Then
    raise a ScheduleFailError.
    """
    email = config.results_email
    if email:
        subject = "Failed to schedule {name}".format(name=name)
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = config.results_sending_email
        msg['To'] = email
        try:
            smtp = smtplib.SMTP('localhost')
            smtp.sendmail(msg['From'], [msg['To']], msg.as_string())
            smtp.quit()
        except socket.error:
            log.exception("Failed to connect to mail server!")
    raise ScheduleFailError(message, name)


def get_worker(machine_type):
    """
    Map a given machine_type to a beanstalkd worker. If machine_type mentions
    multiple machine types - e.g. 'plana,mira', then this returns 'multi'.
    Otherwise it returns what was passed.
    """
    if ',' in machine_type:
        return 'multi'
    else:
        return machine_type


def get_gitbuilder_hash(project=None, branch=None, flavor=None,
                        machine_type=None, distro=None,
                        distro_version=None):
    """
    Find the hash representing the head of the project's repository via
    querying a gitbuilder repo.

    Will return None in the case of a 404 or any other HTTP error.
    """
    # Alternate method for github-hosted projects - left here for informational
    # purposes
    # resp = requests.get(
    #     'https://api.github.com/repos/ceph/ceph/git/refs/heads/master')
    # hash = .json()['object']['sha']
    (arch, release, _os) = get_distro_defaults(distro, machine_type)
    if distro is None:
        distro = _os.name
    bp = get_builder_project()(
        project,
        dict(
            branch=branch,
            flavor=flavor,
            os_type=distro,
            os_version=distro_version,
            arch=arch,
        ),
    )
    return bp.sha1


def get_distro_defaults(distro, machine_type):
    """
    Given a distro (e.g. 'ubuntu') and machine type, return:
        (arch, release, pkg_type)

    This is used to default to:
        ('x86_64', 'trusty', 'deb') when passed 'ubuntu' and 'plana'
        ('armv7l', 'saucy', 'deb') when passed 'ubuntu' and 'saya'
        ('x86_64', 'wheezy', 'deb') when passed 'debian'
        ('x86_64', 'fedora20', 'rpm') when passed 'fedora'
    And ('x86_64', 'centos7', 'rpm') when passed anything else
    """
    arch = 'x86_64'
    if distro in (None, 'None'):
        os_type = 'centos'
        os_version = '7'
    elif distro in ('rhel', 'centos'):
        os_type = 'centos'
        os_version = '7'
    elif distro == 'ubuntu':
        os_type = distro
        if machine_type == 'saya':
            os_version = '13.10'
            arch = 'armv7l'
        else:
            os_version = '16.04'
    elif distro == 'debian':
        os_type = distro
        os_version = '7'
    elif distro == 'fedora':
        os_type = distro
        os_version = '20'
    elif distro == 'opensuse':
        os_type = distro
        os_version = '15.1'
    else:
        raise ValueError("Invalid distro value passed: %s", distro)
    _os = OS(name=os_type, version=os_version)
    release = get_builder_project()._get_distro(
        _os.name,
        _os.version,
        _os.codename,
    )
    template = "Defaults for machine_type {mtype} distro {distro}: " \
        "arch={arch}, release={release}, pkg_type={pkg}"
    log.debug(template.format(
        mtype=machine_type,
        distro=_os.name,
        arch=arch,
        release=release,
        pkg=_os.package_type)
    )
    return (
        arch,
        release,
        _os,
    )


def git_ls_remote(project_or_url, branch, project_owner='ceph'):
    """
    Find the latest sha1 for a given project's branch.

    :param project_or_url: Either a project name or a full URL
    :param branch:         The branch to query
    :param project_owner:  The GitHub project owner. Only used when a project
                           name is passed; not when a URL is passed
    :returns: The sha1 if found; else None
    """
    if '://' in project_or_url:
        url = project_or_url
    else:
        url = build_git_url(project_or_url, project_owner)
    return repo_utils.ls_remote(url, branch)


def git_validate_sha1(project, sha1, project_owner='ceph'):
    '''
    Use http to validate that project contains sha1
    I can't find a way to do this with git, period, so
    we have specific urls to HEAD for github and git.ceph.com/gitweb
    for now
    '''
    url = build_git_url(project, project_owner)

    if '/github.com/' in url:
        url = '/'.join((url, 'commit', sha1))
    elif '/git.ceph.com/' in url:
        # kinda specific to knowing git.ceph.com is gitweb
        url = ('http://git.ceph.com/?p=%s.git;a=blob_plain;f=.gitignore;hb=%s'
               % (project, sha1))
    else:
        raise RuntimeError(
            'git_validate_sha1: how do I check %s for a sha1?' % url
        )

    resp = requests.head(url)
    if resp.ok:
        return sha1
    return None


def git_branch_exists(project_or_url, branch, project_owner='ceph'):
    """
    Query the git repository to check the existence of a project's branch

    :param project_or_url: Either a project name or a full URL
    :param branch:         The branch to query
    :param project_owner:  The GitHub project owner. Only used when a project
                           name is passed; not when a URL is passed
    """
    return git_ls_remote(project_or_url, branch, project_owner) is not None


def get_branch_info(project, branch, project_owner='ceph'):
    """
    NOTE: This is currently not being used because of GitHub's API rate
    limiting. We use github_branch_exists() instead.

    Use the GitHub API to query a project's branch. Returns:
        {u'object': {u'sha': <a_sha_string>,
                    u'type': <string>,
                    u'url': <url_to_commit>},
        u'ref': u'refs/heads/<branch>',
        u'url': <url_to_branch>}

    We mainly use this to check if a branch exists.
    """
    url_templ = 'https://api.github.com/repos/{project_owner}/{project}/git/refs/heads/{branch}'  # noqa
    url = url_templ.format(project_owner=project_owner, project=project,
                           branch=branch)
    resp = requests.get(url)
    if resp.ok:
        return resp.json()


def package_version_for_hash(hash, kernel_flavor='basic', distro='rhel',
                             distro_version='8.0', machine_type='smithi'):
    """
    Does what it says on the tin. Uses gitbuilder repos.

    :returns: a string.
    """
    (arch, release, _os) = get_distro_defaults(distro, machine_type)
    if distro in (None, 'None'):
        distro = _os.name
    bp = get_builder_project()(
        'ceph',
        dict(
            flavor=kernel_flavor,
            os_type=distro,
            os_version=distro_version,
            arch=arch,
            sha1=hash,
        ),
    )
    return bp.version


def get_arch(machine_type):
    """
    Based on a given machine_type, return its architecture by querying the lock
    server.

    :returns: A string or None
    """
    result = teuthology.lock.query.list_locks(machine_type=machine_type, count=1)
    if not result:
        log.warn("No machines found with machine_type %s!", machine_type)
    else:
        return result[0]['arch']


def strip_fragment_path(original_path):
    """
    Given a path, remove the text before '/suites/'.  Part of the fix for
    http://tracker.ceph.com/issues/15470
    """
    scan_after = '/suites/'
    scan_start = original_path.find(scan_after)
    if scan_start > 0:
        return original_path[scan_start + len(scan_after):]
    return original_path


def get_install_task_flavor(job_config):
    """
    Pokes through the install task's configuration (including its overrides) to
    figure out which flavor it will want to install.

    Only looks at the first instance of the install task in job_config.
    """
    project, = job_config.get('project', 'ceph'),
    tasks = job_config.get('tasks', dict())
    overrides = job_config.get('overrides', dict())
    install_overrides = overrides.get('install', dict())
    project_overrides = install_overrides.get(project, dict())
    first_install_config = dict()
    for task in tasks:
        if list(task.keys())[0] == 'install':
            first_install_config = list(task.values())[0] or dict()
            break
    first_install_config = copy.deepcopy(first_install_config)
    deep_merge(first_install_config, install_overrides)
    deep_merge(first_install_config, project_overrides)
    return get_flavor(first_install_config)


def get_package_versions(sha1, os_type, os_version, flavor,
                         package_versions=None):
    """
    Will retrieve the package versions for the given sha1, os_type/version,
    and flavor from gitbuilder.

    Optionally, a package_versions dict can be provided
    from previous calls to this function to avoid calling gitbuilder for
    information we've already retrieved.

    The package_versions dict will be in the following format::

        {
            "sha1": {
                "ubuntu": {
                    "14.04": {
                        "basic": "version",
                    }
                    "15.04": {
                        "notcmalloc": "version",
                    }
                }
                "rhel": {
                    "basic": "version",
                    }
            },
            "another-sha1": {
                "ubuntu": {
                    "basic": "version",
                    }
            }
        }

    :param sha1:             The sha1 hash of the ceph version.
    :param os_type:          The distro we want to get packages for, given
                             the ceph sha1. Ex. 'ubuntu', 'rhel', etc.
    :param os_version:       The distro's version, e.g. '14.04', '7.0'
    :param flavor:           Package flavor ('testing', 'notcmalloc', etc.)
    :param package_versions: Use this optionally to use cached results of
                             previous calls to gitbuilder.
    :returns:                A dict of package versions. Will return versions
                             for all hashes/distros/vers, not just for the given
                             hash/distro/ver.
    """
    if package_versions is None:
        package_versions = dict()

    os_type = str(os_type)

    os_types = package_versions.get(sha1, dict())
    os_versions = os_types.get(os_type, dict())
    flavors = os_versions.get(os_version, dict())
    if flavor not in flavors:
        package_version = package_version_for_hash(
            sha1,
            flavor,
            distro=os_type,
            distro_version=os_version,
        )
        flavors[flavor] = package_version
        os_versions[os_version] = flavors
        os_types[os_type] = os_versions
        package_versions[sha1] = os_types

    return package_versions


def has_packages_for_distro(sha1, os_type, os_version, flavor,
                            package_versions=None):
    """
    Checks to see if gitbuilder has packages for the given sha1, os_type and
    kernel_flavor.

    See above for package_versions description.

    :param sha1:             The sha1 hash of the ceph version.
    :param os_type:          The distro we want to get packages for, given
                             the ceph sha1. Ex. 'ubuntu', 'rhel', etc.
    :param kernel_flavor:    The kernel flavor
    :param package_versions: Use this optionally to use cached results of
                             previous calls to gitbuilder.
    :returns:                True, if packages are found. False otherwise.
    """
    os_type = str(os_type)
    if package_versions is None:
        package_versions = get_package_versions(
            sha1, os_type, os_version, flavor)

    flavors = package_versions.get(sha1, dict()).get(
            os_type, dict()).get(
            os_version, dict())
    # we want to return a boolean here, not the actual package versions
    return bool(flavors.get(flavor, None))


def teuthology_schedule(args, verbose, dry_run, log_prefix=''):
    """
    Run teuthology-schedule to schedule individual jobs.

    If --dry-run has been passed but --verbose has been passed just once, don't
    actually run the command - only print what would be executed.

    If --dry-run has been passed and --verbose has been passed multiple times,
    do both.
    """
    exec_path = os.path.join(
        os.path.dirname(sys.argv[0]),
        'teuthology-schedule')
    args.insert(0, exec_path)
    if dry_run:
        # Quote any individual args so that individual commands can be copied
        # and pasted in order to execute them individually.
        printable_args = []
        for item in args:
            if ' ' in item:
                printable_args.append("'%s'" % item)
            else:
                printable_args.append(item)
        log.info('{0}{1}'.format(
            log_prefix,
            ' '.join(printable_args),
        ))
    if not dry_run or (dry_run and verbose > 1):
        subprocess.check_call(args=args)


def find_git_parent(project, sha1):

    base_url = config.githelper_base_url
    if not base_url:
        log.warning('githelper_base_url not set, --newest disabled')
        return None

    def refresh(project):
        url = '%s/%s.git/refresh' % (base_url, project)
        resp = requests.get(url)
        if not resp.ok:
            log.error('git refresh failed for %s: %s',
                      project, resp.content.decode())

    def get_sha1s(project, committish, count):
        url = '/'.join((base_url, '%s.git' % project,
                       'history/?committish=%s&count=%d' % (committish, count)))
        resp = requests.get(url)
        resp.raise_for_status()
        sha1s = resp.json()['sha1s']
        if len(sha1s) != count:
            log.debug('got response: %s', resp.json())
            log.error('can''t find %d parents of %s in %s: %s',
                       int(count), sha1, project, resp.json()['error'])
        return sha1s

    # XXX don't do this every time?..
    refresh(project)
    # we want the one just before sha1; list two, return the second
    sha1s = get_sha1s(project, sha1, 2)
    if len(sha1s) == 2:
        return sha1s[1]
    else:
        return None


def filter_configs(configs, suite_name=None,
                            filter_in=None,
                            filter_out=None,
                            filter_all=None,
                            filter_fragments=True):
    """
    Returns a generator for pairs of description and fragment paths.

    Usage:

        configs = build_matrix(path, subset, seed)
        for description, fragments in filter_configs(configs):
            pass
    """
    for item in configs:
        fragment_paths = item[1]
        description = combine_path(suite_name, item[0]) \
                                        if suite_name else item[0]
        base_frag_paths = [strip_fragment_path(x)
                                        for x in fragment_paths]
        def matches(f):
            if f in description:
                return True
            if filter_fragments and \
                    any(f in path for path in base_frag_paths):
                return True
            return False
        if filter_all:
            if not all(matches(f) for f in filter_all):
                continue
        if filter_in:
            if not any(matches(f) for f in filter_in):
                continue
        if filter_out:
            if any(matches(f) for f in filter_out):
                continue
        yield([description, fragment_paths])
