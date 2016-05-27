# this file is responsible for submitting tests into the queue
# by generating combinations of facets found in
# https://github.com/ceph/ceph-qa-suite.git

import copy
from datetime import datetime
import logging
import os
import requests
import pwd
import re
import subprocess
import smtplib
import socket
import sys
import time
import yaml
from email.mime.text import MIMEText
from tempfile import NamedTemporaryFile

import teuthology
import matrix
from . import lock
from .config import config, JobConfig, YamlConfig
from .exceptions import (BranchNotFoundError, CommitNotFoundError,
                         ScheduleFailError)
from .misc import deep_merge, get_results_url
from .orchestra.opsys import OS
from .packaging import GitbuilderProject
from .repo_utils import fetch_qa_suite, fetch_teuthology
from .report import ResultsReporter
from .results import UNFINISHED_STATUSES
from .task.install import get_flavor

log = logging.getLogger(__name__)


def process_args(args):
    conf = YamlConfig()
    rename_args = {
        'ceph': 'ceph_branch',
        'sha1': 'ceph_sha1',
        'kernel': 'kernel_branch',
        # FIXME: ceph flavor and kernel flavor are separate things
        'flavor': 'kernel_flavor',
        '<config_yaml>': 'base_yaml_paths',
        'filter': 'filter_in',
    }
    for (key, value) in args.iteritems():
        # Translate --foo-bar to foo_bar
        key = key.lstrip('--').replace('-', '_')
        # Rename the key if necessary
        key = rename_args.get(key) or key
        if key == 'suite':
            value = value.replace('/', ':')
        elif key in ('limit', 'priority', 'num'):
            value = int(value)
        conf[key] = value
    return conf


def main(args):
    fn = process_args(args)
    if fn.verbose:
        teuthology.log.setLevel(logging.DEBUG)

    if not fn.machine_type or fn.machine_type == 'None':
        schedule_fail("Must specify a machine_type")
    elif 'multi' in fn.machine_type:
        schedule_fail("'multi' is not a valid machine_type. " +
                      "Maybe you want 'plana,mira,burnupi' or similar")

    if fn.email:
        config.results_email = fn.email
    if args['--archive-upload']:
        config.archive_upload = args['--archive-upload']
        log.info('Will upload archives to ' + args['--archive-upload'])

    subset = None
    if args['--subset']:
        # take input string '2/3' and turn into (2, 3)
        subset = tuple(map(int, args['--subset'].split('/')))
        log.info('Passed subset=%s/%s' % (str(subset[0]), str(subset[1])))

    run = Run(fn)
    job_config = run.base_config
    name = run.name

    job_config.name = name
    job_config.priority = fn.priority
    if config.results_email:
        job_config.email = config.results_email
    if fn.owner:
        job_config.owner = fn.owner

    if fn.dry_run:
        log.debug("Base job config:\n%s" % job_config)

    with NamedTemporaryFile(prefix='schedule_suite_',
                            delete=False) as base_yaml:
        base_yaml.write(str(job_config))
        base_yaml_path = base_yaml.name
    run.base_yaml_paths.insert(0, base_yaml_path)
    run.prepare_and_schedule()
    os.remove(base_yaml_path)
    if not fn.dry_run and args['--wait']:
        return wait(name, config.max_job_time,
                    args['--archive-upload-url'])


class Run(object):
    WAIT_MAX_JOB_TIME = 30 * 60
    WAIT_PAUSE = 5 * 60
    __slots__ = (
        'args', 'name', 'base_config', 'suite_repo_path', 'base_yaml_paths',
        'base_args',
    )

    def __init__(self, args):
        """
        args must be a config.YamlConfig object
        """
        self.args = args
        self.name = self.make_run_name()
        self.base_config = self.create_initial_config()

        if self.args.suite_dir:
            self.suite_repo_path = self.args.suite_dir
        else:
            self.suite_repo_path = fetch_repos(self.base_config.suite_branch,
                                               test_name=self.name)

        # Interpret any relative paths as being relative to ceph-qa-suite
        # (absolute paths are unchanged by this)
        self.base_yaml_paths = [os.path.join(self.suite_repo_path, b) for b in
                                self.args.base_yaml_paths]

    def make_run_name(self):
        """
        Generate a run name. A run name looks like:
            teuthology-2014-06-23_19:00:37-rados-dumpling-testing-basic-plana
        """
        user = self.args.user or pwd.getpwuid(os.getuid()).pw_name
        # We assume timestamp is a datetime.datetime object
        timestamp = self.args.timestamp or \
            datetime.now().strftime('%Y-%m-%d_%H:%M:%S')

        worker = get_worker(self.args.machine_type)
        return '-'.join(
            [
                user, str(timestamp), self.args.suite, self.args.ceph_branch,
                self.args.kernel_branch or '-', self.args.kernel_flavor, worker
            ]
        )

    def create_initial_config(self):
        """
        Put together the config file used as the basis for each job in the run.
        Grabs hashes for the latest ceph, kernel and teuthology versions in the
        branches specified and specifies them so we know exactly what we're
        testing.

        :returns: A JobConfig object
        """
        kernel_dict = self.choose_kernel()
        ceph_hash = self.choose_ceph_hash()
        # We don't store ceph_version because we don't use it yet outside of
        # logging.
        self.choose_ceph_version(ceph_hash)
        teuthology_branch = self.choose_teuthology_branch()
        suite_branch = self.choose_suite_branch()
        suite_hash = self.choose_suite_hash(suite_branch)

        config_input = dict(
            suite=self.args.suite,
            suite_branch=suite_branch,
            suite_hash=suite_hash,
            ceph_branch=self.args.ceph_branch,
            ceph_hash=ceph_hash,
            teuthology_branch=teuthology_branch,
            machine_type=self.args.machine_type,
            distro=self.args.distro,
            archive_upload=config.archive_upload,
            archive_upload_key=config.archive_upload_key,
        )
        conf_dict = substitute_placeholders(dict_templ, config_input)
        conf_dict.update(kernel_dict)
        job_config = JobConfig.from_dict(conf_dict)
        return job_config

    def choose_kernel(self):
        # Put together a stanza specifying the kernel hash
        if self.args.kernel_branch == 'distro':
            kernel_hash = 'distro'
        # Skip the stanza if no -k given
        elif self.args.kernel_branch is None:
            kernel_hash = None
        else:
            kernel_hash = get_gitbuilder_hash(
                'kernel', self.args.kernel_branch, self.args.kernel_flavor,
                self.args.machine_type, self.args.distro,
            )
            if not kernel_hash:
                schedule_fail(
                    message="Kernel branch '{branch}' not found".format(
                        branch=self.args.kernel_branch), name=self.name
                )
        if kernel_hash:
            log.info("kernel sha1: {hash}".format(hash=kernel_hash))
            kernel_dict = dict(kernel=dict(kdb=True, sha1=kernel_hash))
            if kernel_hash is not 'distro':
                kernel_dict['kernel']['flavor'] = self.args.kernel_flavor
        else:
            kernel_dict = dict()
        return kernel_dict

    def choose_ceph_hash(self):
        """
        Get the ceph hash: if --sha1/-S is supplied, use it if it is valid, and
        just keep the ceph_branch around.  Otherwise use the current git branch
        tip.
        """

        if self.args.ceph_sha1:
            ceph_hash = git_validate_sha1('ceph', self.args.ceph_sha1)
            if not ceph_hash:
                exc = CommitNotFoundError(self.args.ceph_sha1, 'ceph.git')
                schedule_fail(message=str(exc), name=self.name)
            log.info("ceph sha1 explicitly supplied")

        elif self.args.ceph_branch:
            ceph_hash = git_ls_remote('ceph', self.args.ceph_branch)
            if not ceph_hash:
                exc = BranchNotFoundError(self.args.ceph_branch, 'ceph.git')
                schedule_fail(message=str(exc), name=self.name)

        log.info("ceph sha1: {hash}".format(hash=ceph_hash))
        return ceph_hash

    def choose_ceph_version(self, ceph_hash):
        if config.suite_verify_ceph_hash:
            # Get the ceph package version
            ceph_version = package_version_for_hash(
                ceph_hash, self.args.kernel_flavor, self.args.distro,
                self.args.machine_type,
            )
            if not ceph_version:
                schedule_fail(
                    "Packages for ceph hash '{ver}' not found".format(
                        ver=ceph_hash), self.name)
            log.info("ceph version: {ver}".format(ver=ceph_version))
            return ceph_version
        else:
            log.info('skipping ceph package verification')

    def choose_teuthology_branch(self):
        teuthology_branch = self.args.teuthology_branch
        if teuthology_branch and teuthology_branch != 'master':
            if not git_branch_exists('teuthology', teuthology_branch):
                exc = BranchNotFoundError(teuthology_branch, 'teuthology.git')
                schedule_fail(message=str(exc), name=self.name)
        elif not teuthology_branch:
            # Decide what branch of teuthology to use
            if git_branch_exists('teuthology', self.args.ceph_branch):
                teuthology_branch = self.args.ceph_branch
            else:
                log.info(
                    "branch {0} not in teuthology.git; will use master for"
                    " teuthology".format(self.args.ceph_branch))
                teuthology_branch = 'master'
        log.info("teuthology branch: %s", teuthology_branch)
        return teuthology_branch

    def choose_suite_branch(self):
        suite_branch = self.args.suite_branch
        ceph_branch = self.args.ceph_branch
        if suite_branch and suite_branch != 'master':
            if not git_branch_exists('ceph-qa-suite', suite_branch):
                exc = BranchNotFoundError(suite_branch, 'ceph-qa-suite.git')
                schedule_fail(message=str(exc), name=self.name)
        elif not suite_branch:
            # Decide what branch of ceph-qa-suite to use
            if git_branch_exists('ceph-qa-suite', ceph_branch):
                suite_branch = ceph_branch
            else:
                log.info(
                    "branch {0} not in ceph-qa-suite.git; will use master for"
                    " ceph-qa-suite".format(ceph_branch))
                suite_branch = 'master'
        return suite_branch

    def choose_suite_hash(self, suite_branch):
        suite_hash = git_ls_remote('ceph-qa-suite', suite_branch)
        if not suite_hash:
            exc = BranchNotFoundError(suite_branch, 'ceph-qa-suite.git')
            schedule_fail(message=str(exc), name=self.name)
        log.info("ceph-qa-suite branch: %s %s", suite_branch, suite_hash)

    def build_base_args(self):
        base_args = [
            '--name', self.name,
            '--num', str(self.args.num),
            '--worker', get_worker(self.args.machine_type),
        ]
        if self.args.dry_run:
            base_args.append('--dry-run')
        if self.args.priority is not None:
            base_args.extend(['--priority', str(self.args.priority)])
        if self.args.verbose:
            base_args.append('-v')
        if self.args.owner:
            base_args.extend(['--owner', self.args.owner])
        return base_args

    def prepare_and_schedule(self):
        """
        Puts together some "base arguments" with which to execute
        teuthology-schedule for each job, then passes them and other parameters
        to schedule_suite(). Finally, schedules a "last-in-suite" job that
        sends an email to the specified address (if one is configured).
        """
        self.base_args = self.build_base_args()

        # Make sure the yaml paths are actually valid
        for yaml_path in self.base_yaml_paths:
            full_yaml_path = os.path.join(self.suite_repo_path, yaml_path)
            if not os.path.exists(full_yaml_path):
                raise IOError("File not found: " + full_yaml_path)

        num_jobs = self.schedule_suite()

        if self.base_config.email and num_jobs:
            arg = copy.deepcopy(self.base_args)
            arg.append('--last-in-suite')
            arg.extend(['--email', self.base_config.email])
            if self.args.timeout:
                arg.extend(['--timeout', self.args.timeout])
            teuthology_schedule(
                args=arg,
                dry_run=self.args.dry_run,
                verbose=self.args.verbose,
                log_prefix="Results email: ",
            )
            results_url = get_results_url(self.base_config.name)
            if results_url:
                log.info("Test results viewable at %s", results_url)

    def schedule_suite(self):
        """
        Schedule the suite-run. Returns the number of jobs scheduled.
        """
        name = self.name
        arch = get_arch(self.base_config.machine_type)
        suite_name = self.base_config.suite
        suite_path = os.path.join(
            self.suite_repo_path, 'suites',
            self.base_config.suite.replace(':', '/'))
        log.debug('Suite %s in %s' % (suite_name, suite_path))
        configs = [
            (combine_path(suite_name, item[0]), item[1]) for item in
            build_matrix(suite_path, subset=self.args.subset)
        ]
        log.info('Suite %s in %s generated %d jobs (not yet filtered)' % (
            suite_name, suite_path, len(configs)))

        # used as a local cache for package versions from gitbuilder
        package_versions = dict()
        jobs_to_schedule = []
        jobs_missing_packages = []
        for description, fragment_paths in configs:
            base_frag_paths = [strip_fragment_path(x) for x in fragment_paths]
            limit = self.args.limit
            if limit > 0 and len(jobs_to_schedule) >= limit:
                log.info(
                    'Stopped after {limit} jobs due to --limit={limit}'.format(
                        limit=limit))
                break
            # Break apart the filter parameter (one string) into comma
            # separated components to be used in searches.
            filter_in = self.args.filter_in
            if filter_in:
                filter_list = [x.strip() for x in filter_in.split(',')]
                if not any([x in description for x in filter_list]):
                    all_filt = []
                    for filt_samp in filter_list:
                        all_filt.extend(
                            [x.find(filt_samp) < 0 for x in base_frag_paths]
                        )
                    if all(all_filt):
                        continue
            filter_out = self.args.filter_out
            if filter_out:
                filter_list = [x.strip() for x in filter_out.split(',')]
                if any([x in description for x in filter_list]):
                    continue
                all_filt_val = False
                for filt_samp in filter_list:
                    flist = [filt_samp in x for x in base_frag_paths]
                    if any(flist):
                        all_filt_val = True
                        continue
                if all_filt_val:
                    continue

            raw_yaml = '\n'.join([file(a, 'r').read() for a in fragment_paths])

            parsed_yaml = yaml.load(raw_yaml)
            os_type = parsed_yaml.get('os_type') or self.base_config.os_type
            exclude_arch = parsed_yaml.get('exclude_arch')
            exclude_os_type = parsed_yaml.get('exclude_os_type')

            if exclude_arch and exclude_arch == arch:
                log.info('Skipping due to excluded_arch: %s facets %s',
                         exclude_arch, description)
                continue
            if exclude_os_type and exclude_os_type == os_type:
                log.info('Skipping due to excluded_os_type: %s facets %s',
                         exclude_os_type, description)
                continue

            arg = copy.deepcopy(self.base_args)
            arg.extend([
                '--description', description,
                '--',
            ])
            arg.extend(self.base_yaml_paths)
            arg.extend(fragment_paths)

            job = dict(
                yaml=parsed_yaml,
                desc=description,
                sha1=self.base_config.sha1,
                args=arg
            )

            if config.suite_verify_ceph_hash:
                full_job_config = dict()
                deep_merge(full_job_config, self.base_config.to_dict())
                deep_merge(full_job_config, parsed_yaml)
                flavor = get_install_task_flavor(full_job_config)
                sha1 = self.base_config.sha1
                # Get package versions for this sha1, os_type and flavor. If
                # we've already retrieved them in a previous loop, they'll be
                # present in package_versions and gitbuilder will not be asked
                # again for them.
                package_versions = get_package_versions(
                    sha1,
                    os_type,
                    flavor,
                    package_versions
                )
                if not has_packages_for_distro(sha1, os_type, flavor,
                                               package_versions):
                    m = "Packages for os_type '{os}', flavor {flavor} and " + \
                        "ceph hash '{ver}' not found"
                    log.error(m.format(os=os_type, flavor=flavor, ver=sha1))
                    jobs_missing_packages.append(job)

            jobs_to_schedule.append(job)

        for job in jobs_to_schedule:
            log.info(
                'Scheduling %s', job['desc']
            )

            log_prefix = ''
            if job in jobs_missing_packages:
                log_prefix = "Missing Packages: "
                if (not self.args.dry_run and not
                        config.suite_allow_missing_packages):
                    schedule_fail(
                        "At least one job needs packages that don't exist for "
                        "hash {sha1}.".format(sha1=self.base_config.sha1),
                        name,
                    )
            teuthology_schedule(
                args=job['args'],
                dry_run=self.args.dry_run,
                verbose=self.args.verbose,
                log_prefix=log_prefix,
            )
            throttle = self.args.throttle
            if not self.args.dry_run and throttle:
                log.info("pause between jobs : --throttle " + str(throttle))
                time.sleep(int(throttle))

        count = len(jobs_to_schedule)
        missing_count = len(jobs_missing_packages)
        log.info(
            'Suite %s in %s scheduled %d jobs.' %
            (suite_name, suite_path, count)
        )
        log.info('%d/%d jobs were filtered out.',
                 (len(configs) - count),
                 len(configs))
        if missing_count:
            log.warn('Scheduled %d/%d jobs that are missing packages!',
                     missing_count, count)
        return count


class Job(object):
    pass


class WaitException(Exception):
    pass


def wait(name, max_job_time, upload_url):
    stale_job = max_job_time + Run.WAIT_MAX_JOB_TIME
    reporter = ResultsReporter()
    past_unfinished_jobs = []
    progress = time.time()
    log.info("waiting for the suite to complete")
    log.debug("the list of unfinished jobs will be displayed "
              "every " + str(Run.WAIT_PAUSE / 60) + " minutes")
    exit_code = 0
    while True:
        jobs = reporter.get_jobs(name, fields=['job_id', 'status'])
        unfinished_jobs = []
        for job in jobs:
            if job['status'] in UNFINISHED_STATUSES:
                unfinished_jobs.append(job)
            elif job['status'] != 'pass':
                exit_code = 1
        if len(unfinished_jobs) == 0:
            log.info("wait is done")
            break
        if (len(past_unfinished_jobs) == len(unfinished_jobs) and
                time.time() - progress > stale_job):
            raise WaitException(
                "no progress since " + str(config.max_job_time) +
                " + " + str(Run.WAIT_PAUSE) + " seconds")
        if len(past_unfinished_jobs) != len(unfinished_jobs):
            past_unfinished_jobs = unfinished_jobs
            progress = time.time()
        time.sleep(Run.WAIT_PAUSE)
        job_ids = [job['job_id'] for job in unfinished_jobs]
        log.debug('wait for jobs ' + str(job_ids))
    jobs = reporter.get_jobs(name, fields=['job_id', 'status',
                                           'description', 'log_href'])
    # dead, fail, pass : show fail/dead jobs first
    jobs = sorted(jobs, lambda a, b: cmp(a['status'], b['status']))
    for job in jobs:
        if upload_url:
            url = os.path.join(upload_url, name, job['job_id'])
        else:
            url = job['log_href']
        log.info(job['status'] + " " + url + " " + job['description'])
    return exit_code


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


def get_gitbuilder_hash(project='ceph', branch='master', flavor='basic',
                        machine_type='plana', distro='ubuntu'):
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
    gp = GitbuilderProject(
        project,
        dict(
            branch=branch,
            flavor=flavor,
            os_type=distro,
            arch=arch,
        ),
    )
    return gp.sha1


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
            os_version = '14.04'
    elif distro == 'debian':
        os_type = distro
        os_version = '7'
    elif distro == 'fedora':
        os_type = distro
        os_version = '20'
    else:
        raise ValueError("Invalid distro value passed: %s", distro)
    _os = OS(name=os_type, version=os_version)
    release = GitbuilderProject._get_distro(
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


def package_version_for_hash(hash, kernel_flavor='basic',
                             distro='rhel', machine_type='plana'):
    """
    Does what it says on the tin. Uses gitbuilder repos.

    :returns: a string.
    """
    (arch, release, _os) = get_distro_defaults(distro, machine_type)
    if distro in (None, 'None'):
        distro = _os.name
    gp = GitbuilderProject(
        'ceph',
        dict(
            flavor=kernel_flavor,
            os_type=distro,
            arch=arch,
            sha1=hash,
        ),
    )
    return gp.version


def git_ls_remote(project, branch, project_owner='ceph'):
    """
    Find the latest sha1 for a given project's branch.

    :returns: The sha1 if found; else None
    """
    url = build_git_url(project, project_owner)
    cmd = "git ls-remote {} {}".format(url, branch)
    result = subprocess.check_output(
        cmd, shell=True).split()
    sha1 = result[0] if result else None
    log.debug("{} -> {}".format(cmd, sha1))
    return sha1


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


def build_git_url(project, project_owner='ceph'):
    """
    Return the git URL to clone the project
    """
    if project == 'ceph-qa-suite':
        base = config.get_ceph_qa_suite_git_url()
    elif project == 'ceph':
        base = config.get_ceph_git_url()
    else:
        base = 'https://github.com/{project_owner}/{project}'
    url_templ = re.sub('\.git$', '', base)
    return url_templ.format(project_owner=project_owner, project=project)


def git_branch_exists(project, branch, project_owner='ceph'):
    """
    Query the git repository to check the existence of a project's branch
    """
    return git_ls_remote(project, branch, project_owner) is not None


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
        if task.keys()[0] == 'install':
            first_install_config = task.values()[0] or dict()
            break
    first_install_config = copy.deepcopy(first_install_config)
    deep_merge(first_install_config, install_overrides)
    deep_merge(first_install_config, project_overrides)
    return get_flavor(first_install_config)


def get_package_versions(sha1, os_type, kernel_flavor, package_versions=None):
    """
    Will retrieve the package versions for the given sha1, os_type and
    kernel_flavor from gitbuilder.

    Optionally, a package_versions dict can be provided
    from previous calls to this function to avoid calling gitbuilder for
    information we've already retrieved.

    The package_versions dict will be in the following format::

        {
            "sha1": {
                "ubuntu": {
                    "basic": "version",
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
    :param kernel_flavor:    The kernel flavor
    :param package_versions: Use this optionally to use cached results of
                             previous calls to gitbuilder.
    :returns:                A dict of package versions. Will return versions
                             for all hashs and distros, not just for the given
                             hash and distro.
    """
    if not package_versions:
        package_versions = dict()

    os_type = str(os_type)

    os_types = package_versions.get(sha1, dict())
    package_versions_for_flavor = os_types.get(os_type, dict())
    if kernel_flavor not in package_versions_for_flavor:
        package_version = package_version_for_hash(
            sha1,
            kernel_flavor,
            distro=os_type
        )
        package_versions_for_flavor[kernel_flavor] = package_version
        os_types[os_type] = package_versions_for_flavor
        package_versions[sha1] = os_types

    return package_versions


def has_packages_for_distro(sha1, os_type, kernel_flavor,
                            package_versions=None):
    """
    Checks to see if gitbuilder has packages for the given sha1, os_type and
    kernel_flavor.

    Optionally, a package_versions dict can be provided
    from previous calls to this function to avoid calling gitbuilder for
    information we've already retrieved.

    The package_versions dict will be in the following format::

        {
            "sha1": {
                "ubuntu": {
                    "basic": "version",
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
    :param kernel_flavor:    The kernel flavor
    :param package_versions: Use this optionally to use cached results of
                             previous calls to gitbuilder.
    :returns:                True, if packages are found. False otherwise.
    """
    os_type = str(os_type)
    if not package_versions:
        package_versions = get_package_versions(sha1, os_type, kernel_flavor)

    package_versions_for_hash = package_versions.get(sha1, dict()).get(
        os_type, dict())
    # we want to return a boolean here, not the actual package versions
    return bool(package_versions_for_hash.get(kernel_flavor, None))


def combine_path(left, right):
    """
    os.path.join(a, b) doesn't like it when b is None
    """
    if right:
        return os.path.join(left, right)
    return left


def generate_combinations(path, mat, generate_from, generate_to):
    """
    Return a list of items describe by path

    The input is just a path.  The output is an array of (description,
    [file list]) tuples.

    For a normal file we generate a new item for the result list.

    For a directory, we (recursively) generate a new item for each
    file/dir.

    For a directory with a magic '+' file, we generate a single item
    that concatenates all files/subdirs.

    For a directory with a magic '%' file, we generate a result set
    for each item in the directory, and then do a product to generate
    a result list with all combinations.

    The final description (after recursion) for each item will look
    like a relative path.  If there was a % product, that path
    component will appear as a file with braces listing the selection
    of chosen subitems.
    """
    ret = []
    for i in range(generate_from, generate_to):
        output = mat.index(i)
        ret.append((
            matrix.generate_desc(combine_path, output),
            matrix.generate_paths(path, output, combine_path)))
    return ret


def build_matrix(path, subset=None):
    """
    Return a list of items descibed by path such that if the list of
    items is chunked into mincyclicity pieces, each piece is still a
    good subset of the suite.

    A good subset of a product ensures that each facet member appears
    at least once.  A good subset of a sum ensures that the subset of
    each sub collection reflected in the subset is a good subset.

    A mincyclicity of 0 does not attempt to enforce the good subset
    property.

    The input is just a path.  The output is an array of (description,
    [file list]) tuples.

    For a normal file we generate a new item for the result list.

    For a directory, we (recursively) generate a new item for each
    file/dir.

    For a directory with a magic '+' file, we generate a single item
    that concatenates all files/subdirs (A Sum).

    For a directory with a magic '%' file, we generate a result set
    for each item in the directory, and then do a product to generate
    a result list with all combinations (A Product).

    The final description (after recursion) for each item will look
    like a relative path.  If there was a % product, that path
    component will appear as a file with braces listing the selection
    of chosen subitems.

    :param path:        The path to search for yaml fragments
    :param subset:	(index, outof)
    """
    mat, first, matlimit = _get_matrix(path, subset)
    return generate_combinations(path, mat, first, matlimit)


def _get_matrix(path, subset=None):
    mat = None
    first = None
    matlimit = None
    if subset:
        (index, outof) = subset
        mat = _build_matrix(path, mincyclicity=outof)
        first = (mat.size() / outof) * index
        if index == outof or index == outof - 1:
            matlimit = mat.size()
        else:
            matlimit = (mat.size() / outof) * (index + 1)
    else:
        first = 0
        mat = _build_matrix(path)
        matlimit = mat.size()
    return mat, first, matlimit


def _build_matrix(path, mincyclicity=0, item=''):
    if not os.path.exists(path):
        raise IOError('%s does not exist' % path)
    if os.path.isfile(path):
        if path.endswith('.yaml'):
            return matrix.Base(item)
        return None
    if os.path.isdir(path):
        if path.endswith('.disable'):
            return None
        files = sorted(os.listdir(path))
        if len(files) == 0:
            return None
        if '+' in files:
            # concatenate items
            files.remove('+')
            submats = []
            for fn in sorted(files):
                submat = _build_matrix(
                    os.path.join(path, fn),
                    mincyclicity,
                    fn)
                if submat is not None:
                    submats.append(submat)
            return matrix.Concat(item, submats)
        elif '%' in files:
            # convolve items
            files.remove('%')
            submats = []
            for fn in sorted(files):
                submat = _build_matrix(
                    os.path.join(path, fn),
                    mincyclicity=0,
                    item=fn)
                if submat is not None:
                    submats.append(submat)
            mat = matrix.Product(item, submats)
            if mat and mat.cyclicity() < mincyclicity:
                mat = matrix.Cycle(
                    (mincyclicity + mat.cyclicity() - 1) / mat.cyclicity(), mat
                )
            return mat
        else:
            # list items
            submats = []
            for fn in sorted(files):
                submat = _build_matrix(
                    os.path.join(path, fn),
                    mincyclicity,
                    fn)
                if submat is None:
                    continue
                if submat.cyclicity() < mincyclicity:
                    submat = matrix.Cycle(
                        ((mincyclicity + submat.cyclicity() - 1) /
                         submat.cyclicity()),
                        submat)
                submats.append(submat)
            return matrix.Sum(item, submats)
    assert False, "Invalid path %s seen in _build_matrix" % path
    return None


def get_arch(machine_type):
    """
    Based on a given machine_type, return its architecture by querying the lock
    server.

    :returns: A string or None
    """
    result = lock.list_locks(machine_type=machine_type, count=1)
    if not result:
        log.warn("No machines found with machine_type %s!", machine_type)
    else:
        return result[0]['arch']


class Placeholder(object):
    """
    A placeholder for use with substitute_placeholders. Simply has a 'name'
    attribute.
    """
    def __init__(self, name):
        self.name = name


def substitute_placeholders(input_dict, values_dict):
    """
    Replace any Placeholder instances with values named in values_dict. In the
    case of None values, the key is omitted from the result.

    Searches through nested dicts.

    :param input_dict:  A dict which may contain one or more Placeholder
                        instances as values.
    :param values_dict: A dict, with keys matching the 'name' attributes of all
                        of the Placeholder instances in the input_dict, and
                        values to be substituted.
    :returns:           The modified input_dict
    """
    input_dict = copy.deepcopy(input_dict)

    def _substitute(input_dict, values_dict):
        for key, value in input_dict.items():
            if isinstance(value, dict):
                _substitute(value, values_dict)
            elif isinstance(value, Placeholder):
                if values_dict[value.name] is None:
                    del input_dict[key]
                    continue
                # If there is a Placeholder without a corresponding entry in
                # values_dict, we will hit a KeyError - we want this.
                input_dict[key] = values_dict[value.name]
        return input_dict

    return _substitute(input_dict, values_dict)


# Template for the config that becomes the base for each generated job config
dict_templ = {
    'branch': Placeholder('ceph_branch'),
    'sha1': Placeholder('ceph_hash'),
    'teuthology_branch': Placeholder('teuthology_branch'),
    'archive_upload': Placeholder('archive_upload'),
    'archive_upload_key': Placeholder('archive_upload_key'),
    'machine_type': Placeholder('machine_type'),
    'nuke-on-error': True,
    'os_type': Placeholder('distro'),
    'overrides': {
        'admin_socket': {
            'branch': Placeholder('ceph_branch'),
        },
        'ceph': {
            'conf': {
                'mon': {
                    'debug mon': 20,
                    'debug ms': 1,
                    'debug paxos': 20},
                'osd': {
                    'debug filestore': 20,
                    'debug journal': 20,
                    'debug ms': 1,
                    'debug osd': 25
                }
            },
            'log-whitelist': ['slow request'],
            'sha1': Placeholder('ceph_hash'),
        },
        'ceph-deploy': {
            'branch': {
                'dev-commit': Placeholder('ceph_hash'),
            },
            'conf': {
                'client': {
                    'log file': '/var/log/ceph/ceph-$name.$pid.log'
                },
                'mon': {
                    'debug mon': 1,
                    'debug ms': 20,
                    'debug paxos': 20,
                    'osd default pool size': 2
                }
            }
        },
        'install': {
            'ceph': {
                'sha1': Placeholder('ceph_hash'),
            }
        },
        'workunit': {
            'sha1': Placeholder('ceph_hash'),
        }
    },
    'suite': Placeholder('suite'),
    'suite_branch': Placeholder('suite_branch'),
    'suite_sha1': Placeholder('suite_hash'),
    'tasks': [],
}
