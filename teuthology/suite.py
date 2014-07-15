# this file is responsible for submitting tests into the queue
# by generating combinations of facets found in
# https://github.com/ceph/ceph-qa-suite.git

import copy
from datetime import datetime
import itertools
import logging
import os
import requests
import pwd
import subprocess
import smtplib
import sys
import yaml
from email.mime.text import MIMEText
from tempfile import NamedTemporaryFile

import teuthology
from . import lock
from .config import config, JobConfig
from .repo_utils import enforce_repo_state, BranchNotFoundError

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

    name = make_run_name(suite, ceph_branch, kernel_branch, kernel_flavor,
                         machine_type)

    job_config = create_initial_config(suite, suite_branch, ceph_branch,
                                       teuthology_branch, kernel_branch,
                                       kernel_flavor, distro, machine_type)

    if suite_dir:
        suite_repo_path = suite_dir
    else:
        suite_repo_path = fetch_suite_repo(job_config.suite_branch,
                                           test_name=name)

    job_config.name = name
    job_config.priority = priority
    if config.results_email:
        job_config.email = config.results_email
    if owner:
        job_config.owner = owner

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
         kernel_branch, kernel_flavor, worker]
    )


def fetch_suite_repo(branch, test_name):
    """
    Fetch the suite repo (and also the teuthology repo) so that we can use it
    to build jobs. Repos are stored in ~/src/.

    The reason the teuthology repo is also fetched is that currently we use
    subprocess to call teuthology-schedule to schedule jobs so we need to make
    sure it is up-to-date. For that reason we always fetch the master branch
    for test scheduling, regardless of what teuthology branch is requested for
    testing.

    :returns: The path to the repo on disk
    """
    src_base_path = config.src_base_path
    if not os.path.exists(src_base_path):
        os.mkdir(src_base_path)
    suite_repo_path = os.path.join(src_base_path,
                                   'ceph-qa-suite_' + branch)
    try:
        # When a user is scheduling a test run from their own copy of
        # teuthology, let's not wreak havoc on it.
        if config.automated_scheduling:
            enforce_repo_state(
                repo_url=os.path.join(config.ceph_git_base_url,
                                      'teuthology.git'),
                dest_path=os.path.join(src_base_path, 'teuthology'),
                branch='master',
                remove_on_error=False,
            )
        enforce_repo_state(
            repo_url=os.path.join(config.ceph_git_base_url,
                                  'ceph-qa-suite.git'),
            dest_path=suite_repo_path,
            branch=branch,
        )
    except BranchNotFoundError as exc:
        schedule_fail(message=str(exc), name=test_name)
    return suite_repo_path


def create_initial_config(suite, suite_branch, ceph_branch, teuthology_branch,
                          kernel_branch, kernel_flavor, distro, machine_type):
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
    # Skip the stanza if the branch passed is '-'
    elif kernel_branch == '-':
        kernel_hash = None
    else:
        kernel_hash = get_hash('kernel', kernel_branch, kernel_flavor,
                               machine_type)
        if not kernel_hash:
            schedule_fail(message="Kernel branch '{branch}' not found".format(
                branch=kernel_branch))
    if kernel_hash:
        log.info("kernel sha1: {hash}".format(hash=kernel_hash))
        kernel_dict = dict(kernel=dict(kdb=True, sha1=kernel_hash))
    else:
        kernel_dict = dict()

    # Get the ceph hash
    ceph_hash = get_hash('ceph', ceph_branch, kernel_flavor, machine_type)
    if not ceph_hash:
        exc = BranchNotFoundError(ceph_branch, 'ceph.git')
        schedule_fail(message=str(exc))
    log.info("ceph sha1: {hash}".format(hash=ceph_hash))

    # Get the ceph package version
    ceph_version = package_version_for_hash(ceph_hash, kernel_flavor,
                                            machine_type)
    if not ceph_version:
        schedule_fail("Packages for ceph version '{ver}' not found".format(
            ver=ceph_version))
    log.info("ceph version: {ver}".format(ver=ceph_version))

    # Decide what branch of s3-tests to use
    if get_branch_info('s3-tests', ceph_branch):
        s3_branch = ceph_branch
    else:
        log.info("branch {0} not in s3-tests.git; will use master for"
                 " s3-tests".format(ceph_branch))
        s3_branch = 'master'
    log.info("s3-tests branch: %s", s3_branch)

    if teuthology_branch:
        if not get_branch_info('teuthology', teuthology_branch):
            exc = BranchNotFoundError(teuthology_branch, 'teuthology.git')
            raise schedule_fail(message=str(exc))
    else:
        # Decide what branch of teuthology to use
        if get_branch_info('teuthology', ceph_branch):
            teuthology_branch = ceph_branch
        else:
            log.info("branch {0} not in teuthology.git; will use master for"
                     " teuthology".format(ceph_branch))
            teuthology_branch = 'master'
    log.info("teuthology branch: %s", teuthology_branch)

    if not suite_branch:
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
        s3_branch=s3_branch,
    )
    conf_dict = substitute_placeholders(dict_templ, config_input)
    conf_dict.update(kernel_dict)
    job_config = JobConfig.from_dict(conf_dict)
    return job_config


def prepare_and_schedule(job_config, suite_repo_path, base_yaml_paths, limit,
                         num, timeout, dry_run, verbose):
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

    num_jobs = schedule_suite(
        job_config=job_config,
        path=suite_path,
        base_yamls=base_yaml_paths,
        base_args=base_args,
        arch=arch,
        limit=limit,
        dry_run=dry_run,
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
        smtp = smtplib.SMTP('localhost')
        smtp.sendmail(msg['From'], [msg['To']], msg.as_string())
        smtp.quit()
    raise ScheduleFailError(message, name)


class ScheduleFailError(RuntimeError):
    def __init__(self, message, name=None):
        self.message = message
        self.name = name

    def __str__(self):
        return "Job scheduling {name} failed: {msg}".format(
            name=self.name,
            msg=self.message,
        ).replace('  ', ' ')


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
             distro='ubuntu', machine_type='plana'):
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

    This is mainly used to default to:
        ('x86_64', 'precise', 'deb') when passed 'ubuntu' and 'plana'
    And ('armv7l', 'saucy', 'deb') when passed 'ubuntu' and 'saya'
    And ('x86_64', 'centos6', 'rpm') when passed anything non-ubuntu
    """
    if distro == 'ubuntu':
        if machine_type == 'saya':
            arch = 'armv7l'
            release = 'saucy'
            pkg_type = 'deb'
        else:
            arch = 'x86_64'
            release = 'precise'
            pkg_type = 'deb'
    else:
        arch = 'x86_64'
        release = 'centos6'
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
    templ = 'http://gitbuilder.ceph.com/{proj}-{pkg}-{distro}-{arch}-{flav}/'
    return templ.format(proj=project, pkg=pkg_type, distro=distro, arch=arch,
                        flav=kernel_flavor)


def package_version_for_hash(hash, kernel_flavor='basic',
                             distro='ubuntu', machine_type='plana'):
    """
    Does what it says on the tin. Uses gitbuilder repos.

    :returns: a string.
    """
    (arch, release, pkg_type) = get_distro_defaults(distro, machine_type)
    base_url = get_gitbuilder_url('ceph', release, pkg_type, arch,
                                  kernel_flavor)
    url = os.path.join(base_url, 'sha1', hash, 'version')
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
                   ):
    """
    schedule one suite.
    returns number of jobs scheduled
    """
    machine_type = job_config.machine_type
    suite_name = job_config.suite
    count = 0
    log.debug('Suite %s in %s' % (suite_name, path))
    configs = [(combine_path(suite_name, item[0]), item[1]) for item in
               build_matrix(path)]
    job_count = len(configs)
    log.info('Suite %s in %s generated %d jobs' % (
        suite_name, path, len(configs)))

    for description, fragment_paths in configs:
        if limit > 0 and count >= limit:
            log.info(
                'Stopped after {limit} jobs due to --limit={limit}'.format(
                    limit=limit))
            break
        raw_yaml = '\n'.join([file(a, 'r').read() for a in fragment_paths])

        parsed_yaml = yaml.load(raw_yaml)
        os_type = parsed_yaml.get('os_type')
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
        # We should not run multiple tests (changing distros) unless the
        # machine is a VPS.
        # Re-imaging baremetal is not yet supported.
        if machine_type != 'vps' and os_type and os_type != 'ubuntu':
            log.info(
                'Skipping due to non-ubuntu on baremetal facets %s',
                description)
            continue

        log.info(
            'Scheduling %s', description
        )

        arg = copy.deepcopy(base_args)
        arg.extend([
            '--description', description,
            '--',
        ])
        arg.extend(base_yamls)
        arg.extend(fragment_paths)

        if dry_run:
            # Quote any individual args so that individual commands can be
            # copied and pasted in order to execute them individually.
            printable_args = []
            for item in arg:
                if ' ' in item:
                    printable_args.append("'%s'" % item)
                else:
                    printable_args.append(item)
            log.info('dry-run: %s' % ' '.join(printable_args))
        else:
            subprocess.check_call(
                args=arg,
            )
        count += 1
    return job_count


def combine_path(left, right):
    """
    os.path.join(a, b) doesn't like it when b is None
    """
    if right:
        return os.path.join(left, right)
    return left


def build_matrix(path):
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
    if os.path.isfile(path):
        if path.endswith('.yaml'):
            return [(None, [path])]
    if os.path.isdir(path):
        files = sorted(os.listdir(path))
        if '+' in files:
            # concatenate items
            files.remove('+')
            raw = []
            for fn in files:
                raw.extend(build_matrix(os.path.join(path, fn)))
            out = [(
                '{' + ' '.join(files) + '}',
                [a[1][0] for a in raw]
            )]
            return out
        elif '%' in files:
            # convolve items
            files.remove('%')
            sublists = []
            for fn in files:
                raw = build_matrix(os.path.join(path, fn))
                sublists.append([(combine_path(fn, item[0]), item[1])
                                for item in raw])
            out = []
            if sublists:
                for sublist in itertools.product(*sublists):
                    name = '{' + ' '.join([item[0] for item in sublist]) + '}'
                    val = []
                    for item in sublist:
                        val.extend(item[1])
                    out.append((name, val))
            return out
        else:
            # list items
            out = []
            for fn in files:
                raw = build_matrix(os.path.join(path, fn))
                out.extend([(combine_path(fn, item[0]), item[1])
                           for item in raw])
            return out
    return []


def get_arch(machine_type):
    """
    Based on a given machine_type, return its architecture by querying the lock
    server. Sound expensive? It is!

    :returns: A string or None
    """
    locks = lock.list_locks()
    for machine in locks:
        if machine['type'] == machine_type:
            arch = machine['arch']
            return arch
    return None


class Placeholder(object):
    """
    A placeholder for use with substitute_placeholders. Simply has a 'name'
    attribute.
    """
    def __init__(self, name):
        self.name = name


def substitute_placeholders(input_dict, values_dict):
    """
    Replace any Placeholder instances with values named in values_dict.
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
        for (key, value) in input_dict.iteritems():
            if isinstance(value, dict):
                _substitute(value, values_dict)
            elif isinstance(value, Placeholder):
                # If there is a Placeholder without a corresponding entry in
                # values_dict, we will hit a KeyError - we want this.
                input_dict[key] = values_dict[value.name]
        return input_dict

    return _substitute(input_dict, values_dict)


# Template for the config that becomes the base for each generated job config
dict_templ = {
    'branch': Placeholder('ceph_branch'),
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
        's3tests': {
            'branch': Placeholder('s3_branch'),
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
