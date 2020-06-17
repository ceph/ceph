import copy
import logging
import os
import pwd
import re
import time
import yaml

from humanfriendly import format_timespan

from datetime import datetime
from tempfile import NamedTemporaryFile

from teuthology.config import config, JobConfig
from teuthology.exceptions import (
    BranchNotFoundError, CommitNotFoundError, VersionNotFoundError
)
from teuthology.misc import deep_merge, get_results_url
from teuthology.orchestra.opsys import OS
from teuthology.repo_utils import build_git_url

from teuthology.suite import util
from teuthology.suite.build_matrix import build_matrix
from teuthology.suite.placeholder import substitute_placeholders, dict_templ

log = logging.getLogger(__name__)


class Run(object):
    WAIT_MAX_JOB_TIME = 30 * 60
    WAIT_PAUSE = 5 * 60
    __slots__ = (
        'args', 'name', 'base_config', 'suite_repo_path', 'base_yaml_paths',
        'base_args', 'package_versions', 'kernel_dict', 'config_input',
    )

    def __init__(self, args):
        """
        args must be a config.YamlConfig object
        """
        self.args = args
        self.name = self.make_run_name()

        if self.args.ceph_repo:
            config.ceph_git_url = self.args.ceph_repo
        if self.args.suite_repo:
            config.ceph_qa_suite_git_url = self.args.suite_repo

        self.base_config = self.create_initial_config()
        # caches package versions to minimize requests to gbs
        self.package_versions = dict()

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

        worker = util.get_worker(self.args.machine_type)
        return '-'.join(
            [
                user, str(timestamp), self.args.suite, self.args.ceph_branch,
                self.args.kernel_branch or '-', self.args.kernel_flavor, worker
            ]
        ).replace('/', ':')

    def create_initial_config(self):
        """
        Put together the config file used as the basis for each job in the run.
        Grabs hashes for the latest ceph, kernel and teuthology versions in the
        branches specified and specifies them so we know exactly what we're
        testing.

        :returns: A JobConfig object
        """
        self.kernel_dict = self.choose_kernel()
        ceph_hash = self.choose_ceph_hash()
        # We don't store ceph_version because we don't use it yet outside of
        # logging.
        self.choose_ceph_version(ceph_hash)
        suite_branch = self.choose_suite_branch()
        suite_hash = self.choose_suite_hash(suite_branch)
        if self.args.suite_dir:
            self.suite_repo_path = self.args.suite_dir
        else:
            self.suite_repo_path = util.fetch_repos(
                suite_branch, test_name=self.name)
        teuthology_branch = self.choose_teuthology_branch()


        if self.args.distro_version:
            self.args.distro_version, _ = \
                OS.version_codename(self.args.distro, self.args.distro_version)
        self.config_input = dict(
            suite=self.args.suite,
            suite_branch=suite_branch,
            suite_hash=suite_hash,
            ceph_branch=self.args.ceph_branch,
            ceph_hash=ceph_hash,
            ceph_repo=config.get_ceph_git_url(),
            teuthology_branch=teuthology_branch,
            machine_type=self.args.machine_type,
            distro=self.args.distro,
            distro_version=self.args.distro_version,
            archive_upload=config.archive_upload,
            archive_upload_key=config.archive_upload_key,
            suite_repo=config.get_ceph_qa_suite_git_url(),
            suite_relpath=self.args.suite_relpath,
        )
        return self.build_base_config()

    def choose_kernel(self):
        # Put together a stanza specifying the kernel hash
        if self.args.kernel_branch == 'distro':
            kernel_hash = 'distro'
        # Skip the stanza if '-k none' is given
        elif self.args.kernel_branch is None or \
             self.args.kernel_branch.lower() == 'none':
            kernel_hash = None
        else:
            kernel_hash = util.get_gitbuilder_hash(
                'kernel', self.args.kernel_branch, self.args.kernel_flavor,
                self.args.machine_type, self.args.distro,
                self.args.distro_version,
            )
            if not kernel_hash:
                util.schedule_fail(
                    "Kernel branch '{branch}' not found".format(
                     branch=self.args.kernel_branch)
                )
        if kernel_hash:
            log.info("kernel sha1: {hash}".format(hash=kernel_hash))
            kernel_dict = dict(kernel=dict(kdb=True, sha1=kernel_hash))
            if kernel_hash != 'distro':
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
        repo_name = self.ceph_repo_name

        if self.args.ceph_sha1:
            ceph_hash = self.args.ceph_sha1
            if self.args.validate_sha1:
                ceph_hash = util.git_validate_sha1(repo_name, ceph_hash)
            if not ceph_hash:
                exc = CommitNotFoundError(
                    self.args.ceph_sha1,
                    '%s.git' % repo_name
                )
                util.schedule_fail(message=str(exc), name=self.name)
            log.info("ceph sha1 explicitly supplied")

        elif self.args.ceph_branch:
            ceph_hash = util.git_ls_remote(repo_name, self.args.ceph_branch)
            if not ceph_hash:
                exc = BranchNotFoundError(
                    self.args.ceph_branch,
                    '%s.git' % repo_name
                )
                util.schedule_fail(message=str(exc), name=self.name)

        log.info("ceph sha1: {hash}".format(hash=ceph_hash))
        return ceph_hash

    def choose_ceph_version(self, ceph_hash):
        if config.suite_verify_ceph_hash and not self.args.newest:
            # don't bother if newest; we'll search for an older one
            # Get the ceph package version
            try:
                ceph_version = util.package_version_for_hash(
                    ceph_hash, self.args.kernel_flavor, self.args.distro,
                    self.args.distro_version, self.args.machine_type,
                )
            except Exception as exc:
                util.schedule_fail(str(exc), self.name)
            log.info("ceph version: {ver}".format(ver=ceph_version))
            return ceph_version
        else:
            log.info('skipping ceph package verification')

    def choose_teuthology_branch(self):
        """Select teuthology branch, check if it is present in repo and
        return the branch name value.

        The branch name value is determined in the following order:

        Use ``--teuthology-branch`` argument value if supplied.

        Use ``TEUTH_BRANCH`` environment variable value if declared.

        If file ``qa/.teuthology_branch`` can be found in the suite repo
        supplied with ``--suite-repo`` or ``--suite-dir`` and contains
        non-empty string then use it as the branch name.

        Use ``teuthology_branch`` value if it is set in the one
        of the teuthology config files ``$HOME/teuthology.yaml``
        or ``/etc/teuthology.yaml`` correspondingly.

        Use ``master``.

        Generate exception if the branch is not present in the repo.
        """
        teuthology_branch = self.args.teuthology_branch
        if not teuthology_branch:
            teuthology_branch = os.environ.get('TEUTH_BRANCH', None)
        if not teuthology_branch:
            branch_file_path = self.suite_repo_path + '/qa/.teuthology_branch'
            log.debug('Check file %s exists', branch_file_path)
            if os.path.exists(branch_file_path):
                log.debug('Found teuthology branch config file %s',
                                                        branch_file_path)
                with open(branch_file_path) as f:
                    teuthology_branch = f.read().strip()
                    if teuthology_branch:
                        log.debug(
                            'The teuthology branch is overridden with %s',
                                                                teuthology_branch)
                    else:
                        log.warning(
                            'The teuthology branch config is empty, skipping')
        if not teuthology_branch:
            teuthology_branch = config.get('teuthology_branch', 'master')

        teuthology_hash = util.git_ls_remote(
            'teuthology',
            teuthology_branch
        )
        if not teuthology_hash:
            exc = BranchNotFoundError(teuthology_branch, build_git_url('teuthology'))
            util.schedule_fail(message=str(exc), name=self.name)
        log.info("teuthology branch: %s %s", teuthology_branch, teuthology_hash)
        return teuthology_branch

    @property
    def ceph_repo_name(self):
        if self.args.ceph_repo:
            return self._repo_name(self.args.ceph_repo)
        else:
            return 'ceph'

    @property
    def suite_repo_name(self):
        if self.args.suite_repo:
            return self._repo_name(self.args.suite_repo)
        else:
            return 'ceph-qa-suite'

    @staticmethod
    def _repo_name(url):
        return re.sub('\.git$', '', url.split('/')[-1])

    def choose_suite_branch(self):
        suite_repo_name = self.suite_repo_name
        suite_repo_project_or_url = self.args.suite_repo or 'ceph-qa-suite'
        suite_branch = self.args.suite_branch
        ceph_branch = self.args.ceph_branch
        if suite_branch and suite_branch != 'master':
            if not util.git_branch_exists(
                suite_repo_project_or_url,
                suite_branch
            ):
                exc = BranchNotFoundError(suite_branch, suite_repo_name)
                util.schedule_fail(message=str(exc), name=self.name)
        elif not suite_branch:
            # Decide what branch of the suite repo to use
            if util.git_branch_exists(suite_repo_project_or_url, ceph_branch):
                suite_branch = ceph_branch
            else:
                log.info(
                    "branch {0} not in {1}; will use master for"
                    " ceph-qa-suite".format(
                        ceph_branch,
                        suite_repo_name
                    ))
                suite_branch = 'master'
        return suite_branch

    def choose_suite_hash(self, suite_branch):
        suite_repo_name = self.suite_repo_name
        suite_repo_project_or_url = self.args.suite_repo or 'ceph-qa-suite'
        suite_hash = util.git_ls_remote(
            suite_repo_project_or_url,
            suite_branch
        )
        if not suite_hash:
            exc = BranchNotFoundError(suite_branch, suite_repo_name)
            util.schedule_fail(message=str(exc), name=self.name)
        log.info("%s branch: %s %s", suite_repo_name, suite_branch, suite_hash)
        return suite_hash

    def build_base_config(self):
        conf_dict = substitute_placeholders(dict_templ, self.config_input)
        conf_dict.update(self.kernel_dict)
        job_config = JobConfig.from_dict(conf_dict)
        job_config.name = self.name
        job_config.priority = self.args.priority
        if self.args.email:
            job_config.email = self.args.email
        if self.args.owner:
            job_config.owner = self.args.owner
        if self.args.sleep_before_teardown:
            job_config.sleep_before_teardown = int(self.args.sleep_before_teardown)
        return job_config

    def build_base_args(self):
        base_args = [
            '--name', self.name,
            '--worker', util.get_worker(self.args.machine_type),
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


    def write_rerun_memo(self):
        args = copy.deepcopy(self.base_args)
        args.append('--first-in-suite')
        if self.args.subset:
            subset = '/'.join(str(i) for i in self.args.subset)
            args.extend(['--subset', subset])
        args.extend(['--seed', str(self.args.seed)])
        util.teuthology_schedule(
            args=args,
            dry_run=self.args.dry_run,
            verbose=self.args.verbose,
            log_prefix="Memo: ")


    def write_result(self):
        arg = copy.deepcopy(self.base_args)
        arg.append('--last-in-suite')
        if self.base_config.email:
            arg.extend(['--email', self.base_config.email])
        if self.args.timeout:
            arg.extend(['--timeout', self.args.timeout])
        util.teuthology_schedule(
            args=arg,
            dry_run=self.args.dry_run,
            verbose=self.args.verbose,
            log_prefix="Results: ")
        results_url = get_results_url(self.base_config.name)
        if results_url:
            log.info("Test results viewable at %s", results_url)


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

        if num_jobs:
            self.write_result()

    def collect_jobs(self, arch, configs, newest=False, limit=0):
        jobs_to_schedule = []
        jobs_missing_packages = []
        for description, fragment_paths in configs:
            if limit > 0 and len(jobs_to_schedule) >= limit:
                log.info(
                    'Stopped after {limit} jobs due to --limit={limit}'.format(
                        limit=limit))
                break

            raw_yaml = '\n'.join([open(a, 'r').read() for a in fragment_paths])

            parsed_yaml = yaml.safe_load(raw_yaml)
            os_type = parsed_yaml.get('os_type') or self.base_config.os_type
            os_version = parsed_yaml.get('os_version') or self.base_config.os_version
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
                '--num', str(self.args.num),
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

            sha1 = self.base_config.sha1
            if config.suite_verify_ceph_hash:
                full_job_config = copy.deepcopy(self.base_config.to_dict())
                deep_merge(full_job_config, parsed_yaml)
                flavor = util.get_install_task_flavor(full_job_config)
                # Get package versions for this sha1, os_type and flavor. If
                # we've already retrieved them in a previous loop, they'll be
                # present in package_versions and gitbuilder will not be asked
                # again for them.
                try:
                    self.package_versions = util.get_package_versions(
                        sha1,
                        os_type,
                        os_version,
                        flavor,
                        self.package_versions
                    )
                except VersionNotFoundError:
                    pass
                if not util.has_packages_for_distro(
                    sha1, os_type, os_version, flavor, self.package_versions
                ):
                    m = "Packages for os_type '{os}', flavor {flavor} and " + \
                        "ceph hash '{ver}' not found"
                    log.error(m.format(os=os_type, flavor=flavor, ver=sha1))
                    jobs_missing_packages.append(job)
                    # optimization: one missing package causes backtrack in newest mode;
                    # no point in continuing the search
                    if newest:
                        return jobs_missing_packages, None

            jobs_to_schedule.append(job)
        return jobs_missing_packages, jobs_to_schedule

    def schedule_jobs(self, jobs_missing_packages, jobs_to_schedule, name):
        for job in jobs_to_schedule:
            log.info(
                'Scheduling %s', job['desc']
            )

            log_prefix = ''
            if job in jobs_missing_packages:
                log_prefix = "Missing Packages: "
                if (
                    not self.args.dry_run and
                    not config.suite_allow_missing_packages
                ):
                    util.schedule_fail(
                        "At least one job needs packages that don't exist for "
                        "hash {sha1}.".format(sha1=self.base_config.sha1),
                        name,
                    )
            util.teuthology_schedule(
                args=job['args'],
                dry_run=self.args.dry_run,
                verbose=self.args.verbose,
                log_prefix=log_prefix,
            )
            throttle = self.args.throttle
            if not self.args.dry_run and throttle:
                log.info("pause between jobs : --throttle " + str(throttle))
                time.sleep(int(throttle))


    def schedule_suite(self):
        """
        Schedule the suite-run. Returns the number of jobs scheduled.
        """
        name = self.name
        arch = util.get_arch(self.base_config.machine_type)
        suite_name = self.base_config.suite
        suite_path = os.path.normpath(os.path.join(
            self.suite_repo_path,
            self.args.suite_relpath,
            'suites',
            self.base_config.suite.replace(':', '/'),
        ))
        log.debug('Suite %s in %s' % (suite_name, suite_path))
        configs = build_matrix(suite_path,
                               subset=self.args.subset,
                               seed=self.args.seed)
        log.info('Suite %s in %s generated %d jobs (not yet filtered)' % (
            suite_name, suite_path, len(configs)))

        if self.args.dry_run:
            log.debug("Base job config:\n%s" % self.base_config)

        # create, but do not write, the temp file here, so it can be
        # added to the args in collect_jobs, but not filled until
        # any backtracking is done
        base_yaml_path = NamedTemporaryFile(
            prefix='schedule_suite_', delete=False
        ).name
        self.base_yaml_paths.insert(0, base_yaml_path)

        # compute job limit in respect of --sleep-before-teardown
        job_limit = self.args.limit or 0
        sleep_before_teardown = int(self.args.sleep_before_teardown or 0)
        if sleep_before_teardown:
            if job_limit == 0:
                log.warning('The --sleep-before-teardown option was provided: '
                            'only 1 job will be scheduled. '
                            'Use --limit to run more jobs')
                # give user a moment to read this warning
                time.sleep(5)
                job_limit = 1
            elif self.args.non_interactive:
                log.warning(
                    'The --sleep-before-teardown option is active. '
                    'There will be a maximum {} jobs running '
                    'which will fall asleep for {}'
                    .format(job_limit, format_timespan(sleep_before_teardown)))
            elif job_limit > 4:
                are_you_insane=(
                    'There are {total} configs and {maximum} job limit is used. '
                    'Do you really want to lock all machines needed for '
                    'this run for {that_long}? (y/N):'
                    .format(
                        that_long=format_timespan(sleep_before_teardown),
                        total=len(configs),
                        maximum=job_limit))
                while True:
                    insane=(input(are_you_insane) or 'n').lower()
                    if insane == 'y':
                        break
                    elif insane == 'n':
                        exit(0)

        # if newest, do this until there are no missing packages
        # if not, do it once
        backtrack = 0
        limit = self.args.newest
        while backtrack <= limit:
            jobs_missing_packages, jobs_to_schedule = \
                self.collect_jobs(arch,
                    util.filter_configs(configs,
                        filter_in=self.args.filter_in,
                        filter_out=self.args.filter_out,
                        filter_all=self.args.filter_all,
                        filter_fragments=self.args.filter_fragments,
                        suite_name=suite_name),
                                  self.args.newest, job_limit)
            if jobs_missing_packages and self.args.newest:
                new_sha1 = \
                    util.find_git_parent('ceph', self.base_config.sha1)
                if new_sha1 is None:
                    util.schedule_fail('Backtrack for --newest failed', name)
                 # rebuild the base config to resubstitute sha1
                self.config_input['ceph_hash'] = new_sha1
                self.base_config = self.build_base_config()
                backtrack += 1
                continue
            if backtrack:
                log.info("--newest supplied, backtracked %d commits to %s" %
                         (backtrack, self.base_config.sha1))
            break
        else:
            if self.args.newest:
                util.schedule_fail(
                    'Exceeded %d backtracks; raise --newest value' % limit,
                    name,
                )

        if self.args.dry_run:
            log.debug("Base job config:\n%s" % self.base_config)

        with open(base_yaml_path, 'w+b') as base_yaml:
            base_yaml.write(str(self.base_config).encode())

        if jobs_to_schedule:
            self.write_rerun_memo()

        self.schedule_jobs(jobs_missing_packages, jobs_to_schedule, name)

        os.remove(base_yaml_path)

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
