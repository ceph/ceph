# this file is responsible for submitting tests into the queue
# by generating combinations of facets found in
# https://github.com/ceph/ceph-qa-suite.git

import copy
import itertools
import logging
import os
import requests
import subprocess
import sys
import yaml

import teuthology
from teuthology import lock as lock

log = logging.getLogger(__name__)


def main(args):
    verbose = args['--verbose']
    limit = int(args['--limit'])
    dry_run = args['--dry-run']
    name = args['--name']
    priority = int(args['--priority'])
    num = int(args['--num'])
    machine_type = args['--machine-type']
    owner = args['--owner']
    base = args['--base']
    suite = args['--suite']
    email = args['--email']
    timeout = args['--timeout']
    base_yaml_paths = args['<config_yaml>']

    if verbose:
        teuthology.log.setLevel(logging.DEBUG)

    arch = get_arch(base_yaml_paths)

    if ',' in machine_type:
        worker = 'multi'
    else:
        worker = machine_type

    base_args = [
        os.path.join(os.path.dirname(sys.argv[0]), 'teuthology-schedule'),
        '--name', name,
        '--num', str(num),
        '--worker', worker,
    ]
    if priority:
        base_args.extend(['--priority', str(priority)])
    if verbose:
        base_args.append('-v')
    if owner:
        base_args.extend(['--owner', owner])

    suite_path = os.path.join(base, suite)

    num_jobs = 0
    schedule_suite(name=suite,
                   path=suite_path,
                   base_yamls=base_yaml_paths,
                   base_args=base_args,
                   arch=arch,
                   machine_type=machine_type,
                   limit=limit,
                   dry_run=dry_run,
                   )

    if num_jobs:
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


def get_gitbuilder_url(distro, pkg_type, arch, kernel_flavor):
    """
    Return a base URL like:
        http://gitbuilder.ceph.com/ceph-deb-squeeze-x86_64-basic/

    :param distro:        A distro-ish string like 'trusty' or 'fedora20'
    :param pkg_type:      Probably 'rpm' or 'deb'
    :param arch:          A string like 'x86_64'
    :param kernel_flavor: A string like 'basic'
    """
    templ = 'http://gitbuilder.ceph.com/ceph-{pkg}-{distro}-{arch}-{flav}/'
    return templ.format(pkg=pkg_type, distro=distro, arch=arch,
                        flav=kernel_flavor)


def get_hash(branch='master'):
    # Alternate method:
    #resp = requests.get(
    #    'https://api.github.com/repos/ceph/ceph/git/refs/heads/master')
    #hash = .json()['object']['sha']
    base_url = get_gitbuilder_url('precise', 'deb', 'x86_64', 'basic')
    url = os.path.join(base_url, 'ref', branch, 'sha1')
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.text.strip()


def package_version_for_hash(hash, distro, pkg_type, arch='x86_64',
                             kernel_flavor='basic'):
    base_url = get_gitbuilder_url(distro, pkg_type, arch, kernel_flavor)
    url = os.path.join(base_url, 'sha1', hash, 'version')
    resp = requests.get(url)
    if not resp.ok:
        return None
    return resp.text.strip()


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


def get_arch(config):
    for yamlfile in config:
        y = yaml.safe_load(file(yamlfile))
        machine_type = y.get('machine_type')
        if machine_type:
            locks = lock.list_locks()
            for machine in locks:
                if machine['type'] == machine_type:
                    arch = machine['arch']
                    return arch
    return None
