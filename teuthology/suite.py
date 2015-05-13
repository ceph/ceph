# this file is responsible for submitting tests into the queue
# by generating combinations of facets found in
# https://github.com/ceph/ceph-qa-suite.git

import copy
from datetime import datetime
import logging
import os
import requests
import pwd
import subprocess
import smtplib
import socket
import sys
import yaml
import math
from email.mime.text import MIMEText
from tempfile import NamedTemporaryFile

import teuthology
import matrix
from . import lock
from .config import config, JobConfig
from .exceptions import BranchNotFoundError, ScheduleFailError
from .repo_utils import fetch_qa_suite, fetch_teuthology

log = logging.getLogger(__name__)


def main(args):
    verbose = args['--verbose']
    if verbose:
        teuthology.log.setLevel(logging.DEBUG)
    dry_run = args['--dry-run']

    base_yaml_paths = args['<config_yaml>']
    suite = args['--suite'].replace('/', ':')
    ceph_branch = args['--ceph']
    kernel_branch = args['--kernel']
    kernel_flavor = args['--flavor']
    teuthology_branch = args['--teuthology-branch']
    machine_type = args['--machine-type']
    if not machine_type or machine_type == 'None':
        schedule_fail("Must specify a machine_type")
    elif 'multi' in machine_type:
        schedule_fail("'multi' is not a valid machine_type. " +
                      "Maybe you want 'plana,mira,burnupi' or similar")
    distro = args['--distro']
    suite_branch = args['--suite-branch']
    suite_dir = args['--suite-dir']

    limit = int(args['--limit'])
    priority = int(args['--priority'])
    num = int(args['--num'])
    owner = args['--owner']
    email = args['--email']
    if email:
        config.results_email = email
    timeout = args['--timeout']
    filter_in = args['--filter']
    filter_out = args['--filter-out']

    subset = None
    if args['--subset']:
        # take input string '2/3' and turn into (2, 3)
        subset = tuple(map(int, args['--subset'].split('/')))

    name = make_run_name(suite, ceph_branch, kernel_branch, kernel_flavor,
                         machine_type)

    job_config = create_initial_config(suite, suite_branch, ceph_branch,
                                       teuthology_branch, kernel_branch,
                                       kernel_flavor, distro, machine_type,
                                       name)

    if suite_dir:
        suite_repo_path = suite_dir
    else:
        suite_repo_path = fetch_repos(job_config.suite_branch, test_name=name)

    job_config.name = name
    job_config.priority = priority
    if config.results_email:
        job_config.email = config.results_email
    if owner:
        job_config.owner = owner

    if dry_run:
        log.debug("Base job config:\n%s" % job_config)

    # Interpret any relative paths as being relative to ceph-qa-suite (absolute
    # paths are unchanged by this)
    base_yaml_paths = [os.path.join(suite_repo_path, b) for b in
                       base_yaml_paths]

    with NamedTemporaryFile(prefix='schedule_suite_',
                            delete=False) as base_yaml:
        base_yaml.write(str(job_config))
        base_yaml_path = base_yaml.name
    base_yaml_paths.insert(0, base_yaml_path)
    prepare_and_schedule(job_config=job_config,
                         suite_repo_path=suite_repo_path,
                         base_yaml_paths=base_yaml_paths,
                         limit=limit,
                         num=num,
                         timeout=timeout,
                         dry_run=dry_run,
                         verbose=verbose,
                         filter_in=filter_in,
                         filter_out=filter_out,
                         subset=subset,
                         )
    os.remove(base_yaml_path)


def make_run_name(suite, ceph_branch, kernel_branch, kernel_flavor,
                  machine_type, user=None, timestamp=None):
    """
    Generate a run name based on the parameters. A run name looks like:
        teuthology-2014-06-23_19:00:37-rados-dumpling-testing-basic-plana
    """
    if not user:
        user = pwd.getpwuid(os.getuid()).pw_name
    # We assume timestamp is a datetime.datetime object
    if not timestamp:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')

    worker = get_worker(machine_type)
    return '-'.join(
        [user, str(timestamp), suite, ceph_branch,
         kernel_branch or '-', kernel_flavor, worker]
    )


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
            fetch_teuthology('master')
        suite_repo_path = fetch_qa_suite(branch)
    except BranchNotFoundError as exc:
        schedule_fail(message=str(exc), name=test_name)
    return suite_repo_path


def create_initial_config(suite, suite_branch, ceph_branch, teuthology_branch,
                          kernel_branch, kernel_flavor, distro, machine_type,
                          name=None):
    """
    Put together the config file used as the basis for each job in the run.
    Grabs hashes for the latest ceph, kernel and teuthology versions in the
    branches specified and specifies them so we know exactly what we're
    testing.

    :returns: A JobConfig object
    """
    # Put together a stanza specifying the kernel hash
    if kernel_branch == 'distro':
        kernel_hash = 'distro'
    # Skip the stanza if no -k given
    elif kernel_branch is None:
        kernel_hash = None
    else:
        kernel_hash = get_hash('kernel', kernel_branch, kernel_flavor,
                               machine_type, distro)
        if not kernel_hash:
            schedule_fail(message="Kernel branch '{branch}' not found".format(
                branch=kernel_branch), name=name)
    if kernel_hash:
        log.info("kernel sha1: {hash}".format(hash=kernel_hash))
        kernel_dict = dict(kernel=dict(kdb=True, sha1=kernel_hash))
    else:
        kernel_dict = dict()

    # Get the ceph hash
    ceph_hash = get_hash('ceph', ceph_branch, kernel_flavor, machine_type,
                         distro)
    if not ceph_hash:
        exc = BranchNotFoundError(ceph_branch, 'ceph.git')
        schedule_fail(message=str(exc), name=name)
    log.info("ceph sha1: {hash}".format(hash=ceph_hash))

    # Get the ceph package version
    ceph_version = package_version_for_hash(ceph_hash, kernel_flavor,
                                            distro, machine_type)
    if not ceph_version:
        schedule_fail("Packages for ceph hash '{ver}' not found".format(
            ver=ceph_hash), name)
    log.info("ceph version: {ver}".format(ver=ceph_version))

    if teuthology_branch and teuthology_branch != 'master':
        if not get_branch_info('teuthology', teuthology_branch):
            exc = BranchNotFoundError(teuthology_branch, 'teuthology.git')
            schedule_fail(message=str(exc), name=name)
    elif not teuthology_branch:
        # Decide what branch of teuthology to use
        if get_branch_info('teuthology', ceph_branch):
            teuthology_branch = ceph_branch
        else:
            log.info("branch {0} not in teuthology.git; will use master for"
                     " teuthology".format(ceph_branch))
            teuthology_branch = 'master'
    log.info("teuthology branch: %s", teuthology_branch)

    if suite_branch and suite_branch != 'master':
        if not get_branch_info('ceph-qa-suite', suite_branch):
            exc = BranchNotFoundError(suite_branch, 'ceph-qa-suite.git')
            schedule_fail(message=str(exc), name=name)
    elif not suite_branch:
        # Decide what branch of ceph-qa-suite to use
        if get_branch_info('ceph-qa-suite', ceph_branch):
            suite_branch = ceph_branch
        else:
            log.info("branch {0} not in ceph-qa-suite.git; will use master for"
                     " ceph-qa-suite".format(ceph_branch))
            suite_branch = 'master'
    log.info("ceph-qa-suite branch: %s", suite_branch)

    config_input = dict(
        suite=suite,
        suite_branch=suite_branch,
        ceph_branch=ceph_branch,
        ceph_hash=ceph_hash,
        teuthology_branch=teuthology_branch,
        machine_type=machine_type,
        distro=distro,
    )
    conf_dict = substitute_placeholders(dict_templ, config_input)
    conf_dict.update(kernel_dict)
    job_config = JobConfig.from_dict(conf_dict)
    return job_config


def prepare_and_schedule(job_config, suite_repo_path, base_yaml_paths, limit,
                         num, timeout, dry_run, verbose,
                         filter_in,
                         filter_out,
                         subset):
    """
    Puts together some "base arguments" with which to execute
    teuthology-schedule for each job, then passes them and other parameters to
    schedule_suite(). Finally, schedules a "last-in-suite" job that sends an
    email to the specified address (if one is configured).
    """
    arch = get_arch(job_config.machine_type)

    base_args = [
        os.path.join(os.path.dirname(sys.argv[0]), 'teuthology-schedule'),
        '--name', job_config.name,
        '--num', str(num),
        '--worker', get_worker(job_config.machine_type),
    ]
    if job_config.priority:
        base_args.extend(['--priority', str(job_config.priority)])
    if verbose:
        base_args.append('-v')
    if job_config.owner:
        base_args.extend(['--owner', job_config.owner])

    suite_path = os.path.join(suite_repo_path, 'suites',
                              job_config.suite.replace(':', '/'))

    # Make sure the yaml paths are actually valid
    for yaml_path in base_yaml_paths:
        full_yaml_path = os.path.join(suite_repo_path, yaml_path)
        if not os.path.exists(full_yaml_path):
            raise IOError("File not found: " + full_yaml_path)

    num_jobs = schedule_suite(
        job_config=job_config,
        path=suite_path,
        base_yamls=base_yaml_paths,
        base_args=base_args,
        arch=arch,
        limit=limit,
        dry_run=dry_run,
        filter_in=filter_in,
        filter_out=filter_out,
        subset=subset
    )

    if job_config.email and num_jobs:
        arg = copy.deepcopy(base_args)
        arg.append('--last-in-suite')
        arg.extend(['--email', job_config.email])
        if timeout:
            arg.extend(['--timeout', timeout])
        if dry_run:
            log.info('dry-run: %s' % ' '.join(arg))
        else:
            subprocess.check_call(
                args=arg,
            )


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


def get_hash(project='ceph', branch='master', flavor='basic',
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
    (arch, release, pkg_type) = get_distro_defaults(distro, machine_type)
    base_url = get_gitbuilder_url(project, release, pkg_type, arch, flavor)
    url = os.path.join(base_url, 'ref', branch, 'sha1')
    resp = requests.get(url)
    if not resp.ok:
        return None
    return str(resp.text.strip())


def get_distro_defaults(distro, machine_type):
    """
    Given a distro (e.g. 'ubuntu') and machine type, return:
        (arch, release, pkg_type)

    This is used to default to:
        ('x86_64', 'precise', 'deb') when passed 'ubuntu' and 'plana'
    ('armv7l', 'saucy', 'deb') when passed 'ubuntu' and 'saya'
    ('x86_64', 'wheezy', 'deb') when passed 'debian'
    ('x86_64', 'fedora20', 'rpm') when passed 'fedora'
    ('x86_64', 'centos6', 'rpm') when passed 'centos'
    And ('x86_64', 'rhel7_0', 'rpm') when passed anything else
    """
    arch = 'x86_64'
    if distro == 'ubuntu':
        pkg_type = 'deb'
        if machine_type == 'saya':
            release = 'saucy'
            arch = 'armv7l'
        else:
            release = 'precise'
    elif distro == 'debian':
        release = 'wheezy'
        pkg_type = 'deb'
    elif distro == 'centos':
        release = 'centos6'
        pkg_type = 'rpm'
    elif distro == 'fedora':
        release = 'fedora20'
        pkg_type = 'rpm'
    else:
        release = 'rhel7_0'
        pkg_type = 'rpm'
    log.debug(
        "Defaults for machine_type %s: arch=%s, release=%s, pkg_type=%s)",
        machine_type, arch, release, pkg_type)
    return (
        arch,
        release,
        pkg_type,
    )


def get_gitbuilder_url(project, distro, pkg_type, arch, kernel_flavor):
    """
    Return a base URL like:
        http://gitbuilder.ceph.com/ceph-deb-squeeze-x86_64-basic/

    :param project:       'ceph' or 'kernel'
    :param distro:        A distro-ish string like 'trusty' or 'fedora20'
    :param pkg_type:      Probably 'rpm' or 'deb'
    :param arch:          A string like 'x86_64'
    :param kernel_flavor: A string like 'basic'
    """
    templ = 'http://{host}/{proj}-{pkg}-{distro}-{arch}-{flav}/'
    return templ.format(proj=project, pkg=pkg_type, distro=distro, arch=arch,
                        flav=kernel_flavor, host=config.gitbuilder_host)


def package_version_for_hash(hash, kernel_flavor='basic',
                             distro='rhel', machine_type='plana'):
    """
    Does what it says on the tin. Uses gitbuilder repos.

    :returns: a string.
    """
    (arch, release, pkg_type) = get_distro_defaults(distro, machine_type)
    base_url = get_gitbuilder_url('ceph', release, pkg_type, arch,
                                  kernel_flavor)
    url = os.path.join(base_url, 'sha1', hash, 'version')
    log.debug("Looking for packages at {url}".format(url=url))
    resp = requests.get(url)
    if resp.ok:
        return resp.text.strip()


def get_branch_info(project, branch, project_owner='ceph'):
    """
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


def schedule_suite(job_config,
                   path,
                   base_yamls,
                   base_args,
                   arch,
                   limit=0,
                   dry_run=True,
                   filter_in=None,
                   filter_out=None,
                   subset=None
                   ):
    """
    schedule one suite.
    returns number of jobs scheduled
    """
    suite_name = job_config.suite
    log.debug('Suite %s in %s' % (suite_name, path))
    configs = [(combine_path(suite_name, item[0]), item[1]) for item in
               build_matrix(path, subset=subset)]
    log.info('Suite %s in %s generated %d jobs (not yet filtered)' % (
        suite_name, path, len(configs)))

    # used as a local cache for package versions from gitbuilder
    package_versions = dict()
    jobs_to_schedule = []
    jobs_missing_packages = []
    for description, fragment_paths in configs:
        if limit > 0 and len(jobs_to_schedule) >= limit:
            log.info(
                'Stopped after {limit} jobs due to --limit={limit}'.format(
                    limit=limit))
            break
        # Break apart the filter parameter (one string) into comma separated
        # components to be used in searches.
        if filter_in:
            filter_list = [x.strip() for x in filter_in.split(',')]
            if not any([x in description for x in filter_list]):
                all_filt = []
                for filt_samp in filter_list:
                    all_filt.extend([x.find(filt_samp) < 0 for x in fragment_paths])
                if all(all_filt):
                    continue
        if filter_out:
            filter_list = [x.strip() for x in filter_out.split(',')]
            if any([x in description for x in filter_list]):
                continue
            all_filt_val = False
            for filt_samp in filter_list:
                flist = [filt_samp in x for x in fragment_paths]
                if any(flist):
                    all_filt_val = True
                    continue
            if all_filt_val:
                continue

        raw_yaml = '\n'.join([file(a, 'r').read() for a in fragment_paths])

        parsed_yaml = yaml.load(raw_yaml)
        os_type = parsed_yaml.get('os_type') or job_config.os_type
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

        arg = copy.deepcopy(base_args)
        arg.extend([
            '--description', description,
            '--',
        ])
        arg.extend(base_yamls)
        arg.extend(fragment_paths)

        job = dict(
            yaml=parsed_yaml,
            desc=description,
            args=arg
        )

        if dry_run:
            sha1 = job_config.sha1
            # Get package versions for this sha1 and os_type. If we've already
            # retrieved them in a previous loop, they'll be present in
            # package_versions and gitbuilder will not be asked again for them.
            package_versions = get_package_versions(
                sha1,
                os_type,
                package_versions
            )

            if not has_packages_for_distro(sha1, os_type, package_versions):
                m = "Packages for os_type '{os}' and ceph hash '{ver}' not found"
                log.info(m.format(os=os_type, ver=sha1))
                jobs_missing_packages.append(job)

        jobs_to_schedule.append(job)

    for job in jobs_to_schedule:
        log.info(
            'Scheduling %s', job['desc']
        )

        if dry_run:
            # Quote any individual args so that individual commands can be
            # copied and pasted in order to execute them individually.
            printable_args = []
            for item in job['args']:
                if ' ' in item:
                    printable_args.append("'%s'" % item)
                else:
                    printable_args.append(item)
            prefix = "dry-run:"
            if job in jobs_missing_packages:
                prefix = "dry-run (missing packages):"
            log.info('%s %s' % (prefix, ' '.join(printable_args)))
        else:
            subprocess.check_call(
                args=job['args'],
            )

    count = len(jobs_to_schedule)
    missing_count = len(jobs_missing_packages)
    log.info('Suite %s in %s scheduled %d jobs.' % (suite_name, path, count))
    log.info('Suite %s in %s -- %d jobs were filtered out.' %
             (suite_name, path, len(configs) - count))
    if dry_run:
        log.info('Suite %s in %s scheduled %d jobs with missing packages.' %
                 (suite_name, path, missing_count))
    return count


def get_package_versions(sha1, os_type, package_versions=None):
    """
    Will retrieve the package versions for the given sha1 and os_type
    from gitbuilder.

    Optionally, a package_versions dict can be provided
    from previous calls to this function to avoid calling gitbuilder for
    information we've already retrieved.

    The package_versions dict will be in the following format::

        {
            "sha1": {
                "ubuntu": "version",
                "rhel": "version",
            },
            "another-sha1": {
                "ubuntu": "version",
            }
        }

    :param sha1:             The sha1 hash of the ceph version.
    :param os_type:          The distro we want to get packages for, given
                             the ceph sha1. Ex. 'ubuntu', 'rhel', etc.
    :param package_versions: Use this optionally to use cached results of
                             previous calls to gitbuilder.
    :returns:                A dict of package versions. Will return versions
                             for all hashs and distros, not just for the given
                             hash and distro.
    """
    if package_versions is None:
        package_versions = dict()

    os_type = str(os_type)

    package_versions_for_hash = package_versions.get(sha1, dict())
    if os_type not in package_versions_for_hash:
        package_version = package_version_for_hash(
            sha1,
            distro=os_type
        )
        package_versions_for_hash[os_type] = package_version
        package_versions[sha1] = package_versions_for_hash

    return package_versions


def has_packages_for_distro(sha1, os_type, package_versions=None):
    """
    Checks to see if gitbuilder has packages for the given sha1 and os_type.

    Optionally, a package_versions dict can be provided
    from previous calls to this function to avoid calling gitbuilder for
    information we've already retrieved.

    The package_versions dict will be in the following format::

        {
            "sha1": {
                "ubuntu": "version",
                "rhel": "version",
            },
            "another-sha1": {
                "ubuntu": "version",
            }
        }

    :param sha1:             The sha1 hash of the ceph version.
    :param os_type:          The distro we want to get packages for, given
                             the ceph sha1. Ex. 'ubuntu', 'rhel', etc.
    :param package_versions: Use this optionally to use cached results of
                             previous calls to gitbuilder.
    :returns:                True, if packages are found. False otherwise.
    """
    os_type = str(os_type)
    if package_versions is None:
        package_versions = get_package_versions(sha1, os_type)

    package_versions_for_hash = package_versions.get(sha1, dict())
    # we want to return a boolean here, not the actual package versions
    return bool(package_versions_for_hash.get(os_type, None))


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


def build_matrix(path, _isfile=os.path.isfile,
                 _isdir=os.path.isdir,
                 _listdir=os.listdir,
                 subset=None):
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
    :param _isfile:     Custom os.path.isfile(); for testing only
    :param _isdir:      Custom os.path.isdir(); for testing only
    :param _listdir:	Custom os.listdir(); for testing only
    :param subset:	(index, outof)
    """
    mat = None
    first = None
    matlimit = None
    if subset:
        (index, outof) = subset
        mat = _build_matrix(path, _isfile, _isdir, _listdir, mincyclicity=outof)
        first = (mat.size() / outof) * index
        if index == outof or index == outof - 1:
            matlimit = mat.size()
        else:
            matlimit = (mat.size() / outof) * (index + 1)
    else:
        first = 0
        mat = _build_matrix(path, _isfile, _isdir, _listdir)
        matlimit = mat.size()
    return generate_combinations(path, mat, first, matlimit)

def _build_matrix(path, _isfile=os.path.isfile,
                  _isdir=os.path.isdir, _listdir=os.listdir, mincyclicity=0, item=''):
    if _isfile(path):
        if path.endswith('.yaml'):
            return matrix.Base(item)
        return None
    if _isdir(path):
        if path.endswith('.disable'):
            return None
        files = sorted(_listdir(path))
        if '+' in files:
            # concatenate items
            files.remove('+')
            submats = []
            for fn in sorted(files):
                submat = _build_matrix(
                    os.path.join(path, fn),
                    _isfile,
                    _isdir,
                    _listdir,
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
                    _isfile,
                    _isdir,
                    _listdir,
                    mincyclicity=0,
                    item=fn)
                if submat is not None:
                    submats.append(submat)
            return matrix.Product(item, submats)
        else:
            # list items
            submats = []
            for fn in sorted(files):
                submat = _build_matrix(
                    os.path.join(path, fn),
                    _isfile,
                    _isdir,
                    _listdir,
                    mincyclicity,
                    fn)
                if submat is None:
                    continue
                if submat.cyclicity() < mincyclicity:
                    submat = matrix.Cycle(
                        int(math.ceil(
                            mincyclicity / submat.cyclicity())),
                        submat)
                submats.append(submat)
            return matrix.Sum(item, submats)
    assert False, "Invalid path seen in _build_matrix"
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
                    'debug osd': 20
                }
            },
            'log-whitelist': ['slow request'],
            'sha1': Placeholder('ceph_hash'),
        },
        'ceph-deploy': {
            'branch': {
                'dev': Placeholder('ceph_branch'),
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
    'tasks': [
        {'chef': None},
        {'clock.check': None}
    ],
}
