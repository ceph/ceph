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
from teuthology import lock as lock
from teuthology.config import config

log = logging.getLogger(__name__)


def main(args):
    verbose = args['--verbose']
    if verbose:
        teuthology.log.setLevel(logging.DEBUG)
    dry_run = args['--dry-run']

    base_yaml_paths = args['<config_yaml>']
    base = os.path.expanduser(args['--base'])
    if not os.path.exists(base):
        schedule_fail("Base directory not found: {dir}".format(dir=base))
    suite = args['--suite']
    nice_suite = suite.replace('/', ':')
    ceph_branch = args['--ceph']
    kernel_branch = args['--kernel']
    kernel_flavor = args['--flavor']
    teuthology_branch = args['--teuthology-branch']
    machine_type = args['--machine-type']
    distro = args['--distro']

    limit = int(args['--limit'])
    priority = int(args['--priority'])
    num = int(args['--num'])
    owner = args['--owner']
    email = args['--email']
    if email:
        config.email_specified = True
        config.results_email = email
    timeout = args['--timeout']

    name = make_name(nice_suite, ceph_branch, kernel_branch, kernel_flavor,
                     machine_type)
    config_string = create_initial_config(nice_suite, ceph_branch,
                                          teuthology_branch, kernel_branch,
                                          kernel_flavor, distro, machine_type)

    with NamedTemporaryFile(prefix='schedule_suite_') as base_yaml:
        base_yaml.write(config_string)
        base_yaml_paths.insert(0, base_yaml.name)
        prepare_and_schedule(owner=owner,
                             name=name,
                             suite=suite,
                             machine_type=machine_type,
                             base=base,
                             base_yaml_paths=base_yaml_paths,
                             email=email,
                             priority=priority,
                             limit=limit,
                             num=num,
                             timeout=timeout,
                             dry_run=dry_run,
                             verbose=verbose,
                             )


def make_name(suite, ceph_branch, kernel_branch, kernel_flavor, machine_type,
              user=None, timestamp=None):
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


def create_initial_config(nice_suite, ceph_branch, teuthology_branch,
                          kernel_branch, kernel_flavor, distro, machine_type):
    # Put together a stanza specifying the kernel hash
    if kernel_branch == 'distro':
        kernel_hash = 'distro'
    else:
        kernel_hash = get_hash('kernel', kernel_branch, kernel_flavor,
                               machine_type)
        if not kernel_hash:
            schedule_fail(message="Kernel branch '{branch} not found".format(
                branch=kernel_branch))
    if kernel_hash:
        log.info("kernel sha1: {hash}".format(hash=kernel_hash))
        kernel_dict = dict(kernel=dict(kdb=True, sha1=kernel_hash))

    # Get the ceph hash
    ceph_hash = get_hash('ceph', ceph_branch, kernel_flavor, machine_type)
    if not ceph_hash:
        schedule_fail("Ceph branch '{branch}' not found".format(
            branch=ceph_branch))
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
                 "s3-tests".format(ceph_branch))
        s3_branch = 'master'
    log.info("s3-tests branch: %s", s3_branch)

    if not teuthology_branch:
        # Decide what branch of teuthology to use
        if get_branch_info('teuthology', ceph_branch):
            teuthology_branch = ceph_branch
        else:
            log.info("branch {0} not in teuthology.git; will use master for"
                     "teuthology".format(ceph_branch))
            teuthology_branch = 'master'
    log.info("teuthology branch: %s", teuthology_branch)

    config_input = dict(
        nice_suite=nice_suite,
        ceph_branch=ceph_branch,
        ceph_hash=ceph_hash,
        teuthology_branch=teuthology_branch,
        machine_type=machine_type,
        kernel_stanza=yaml.dump(kernel_dict, default_flow_style=False).strip(),
        distro=distro,
        s3_branch=s3_branch,
    )
    return config_template.format(**config_input)


def prepare_and_schedule(owner, name, suite, machine_type, base,
                         base_yaml_paths, email, priority, limit, num, timeout,
                         dry_run, verbose):
    arch = get_arch(machine_type)

    base_args = [
        os.path.join(os.path.dirname(sys.argv[0]), 'teuthology-schedule'),
        '--name', name,
        '--num', str(num),
        '--worker', get_worker(machine_type),
    ]
    if priority:
        base_args.extend(['--priority', str(priority)])
    if verbose:
        base_args.append('-v')
    if owner:
        base_args.extend(['--owner', owner])

    suite_path = os.path.join(base, suite)

    num_jobs = schedule_suite(
        name=suite,
        path=suite_path,
        base_yamls=base_yaml_paths,
        base_args=base_args,
        arch=arch,
        machine_type=machine_type,
        limit=limit,
        dry_run=dry_run,
        )

    if email and num_jobs:
        arg = copy.deepcopy(base_args)
        arg.append('--last-in-suite')
        if email:
            arg.extend(['--email', email])
        if timeout:
            arg.extend(['--timeout', timeout])
        if dry_run:
            log.info('dry-run: %s' % ' '.join(arg))
        else:
            subprocess.check_call(
                args=arg,
            )


def schedule_fail(message, name=None):
    email = config.results_email
    if not email:
        return
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
        return "Job scheduling {name} failed: '{msg}'".format(
            name=self.name,
            msg=self.message,
        ).replace('  ', ' ')


def get_worker(machine_type):
    if ',' in machine_type:
        return 'multi'
    else:
        return machine_type


def get_hash(project='ceph', branch='master', flavor='basic',
             distro='ubuntu', machine_type='plana'):
    # Alternate method for ceph
    #resp = requests.get(
    #    'https://api.github.com/repos/ceph/ceph/git/refs/heads/master')
    #hash = .json()['object']['sha']
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
    (arch, release, pkg_type) = get_distro_defaults(distro, machine_type)
    base_url = get_gitbuilder_url('ceph', release, pkg_type, arch,
                                  kernel_flavor)
    url = os.path.join(base_url, 'sha1', hash, 'version')
    resp = requests.get(url)
    if not resp.ok:
        return None
    return resp.text.strip()


def get_branch_info(project, branch, project_owner='ceph'):
    url_templ = 'https://api.github.com/repos/{project_owner}/{project}/git/refs/heads/{branch}'  # noqa
    url = url_templ.format(project_owner=project_owner, project=project,
                           branch=branch)
    resp = requests.get(url)
    if resp.ok:
        return resp.json()


def schedule_suite(name,
                   path,
                   base_yamls,
                   base_args,
                   arch,
                   machine_type,
                   limit=0,
                   dry_run=True,
                   ):
    """
    schedule one suite.
    returns number of jobs scheduled
    """
    count = 0
    log.debug('Suite %s in %s' % (name, path))
    configs = [(combine_path(name, item[0]), item[1]) for item in
               build_matrix(path)]
    job_count = len(configs)
    log.info('Suite %s in %s generated %d jobs' % (
        name, path, len(configs)))

    for description, config in configs:
        if limit > 0 and count >= limit:
            log.info(
                'Stopped after {limit} jobs due to --limit={limit}'.format(
                    limit=limit))
            break
        raw_yaml = '\n'.join([file(a, 'r').read() for a in config])

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
        arg.extend(config)

        if dry_run:
            log.info('dry-run: %s' % ' '.join(arg))
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
    for each tiem in the directory, and then do a product to generate
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
    locks = lock.list_locks()
    for machine in locks:
        if machine['type'] == machine_type:
            arch = machine['arch']
            return arch
    return None

# yaml template for the config that becomes the base for each generated job
# config
config_template = """
teuthology_branch: {teuthology_branch}
{kernel_stanza}
nuke-on-error: true
machine_type: {machine_type}
os_type: {distro}
branch: {ceph_branch}
suite: {nice_suite}
tasks:
- chef:
- clock.check:
overrides:
  workunit:
    sha1: {ceph_hash}
  s3tests:
    branch: {s3_branch}
  install:
    ceph:
      sha1: {ceph_hash}
  ceph:
    sha1: {ceph_hash}
    conf:
      mon:
        debug ms: 1
        debug mon: 20
        debug paxos: 20
      osd:
        debug ms: 1
        debug osd: 20
        debug filestore: 20
        debug journal: 20
    log-whitelist:
    - slow request
  ceph-deploy:
    branch:
      dev: {ceph_branch}
    conf:
      mon:
        osd default pool size: 2
        debug mon: 1
        debug paxos: 20
        debug ms: 20
      client:
        log file: /var/log/ceph/ceph-$name.$pid.log
  admin_socket:
    branch: {ceph_branch}
""".strip()
