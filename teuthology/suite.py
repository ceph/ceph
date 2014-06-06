# this file is responsible for submitting tests into the queue
# by generating combinations of facets found in
# https://github.com/ceph/ceph-qa-suite.git

import copy
import itertools
import logging
import os
import subprocess
import sys
import yaml

import teuthology
from teuthology import lock as lock

log = logging.getLogger(__name__)


def main(args):
    if args.verbose:
        teuthology.log.setLevel(logging.DEBUG)

    base_arg = [
        os.path.join(os.path.dirname(sys.argv[0]), 'teuthology-schedule'),
        '--name', args.name,
        '--num', str(args.num),
        '--worker', args.worker,
    ]
    if args.priority:
        base_arg.extend(['--priority', str(args.priority)])
    if args.verbose:
        base_arg.append('-v')
    if args.owner:
        base_arg.extend(['--owner', args.owner])

    collections = [
        (os.path.join(args.base, collection), collection)
        for collection in args.collections
    ]

    count = 1
    num_jobs = 0
    for collection, collection_name in sorted(collections):
        log.debug('Collection %s in %s' % (collection_name, collection))
        configs = [(combine_path(collection_name, item[0]), item[1])
                   for item in build_matrix(collection)]
        log.info('Collection %s in %s generated %d jobs' %
                 (collection_name, collection, len(configs)))
        num_jobs += len(configs)

        arch = get_arch(args.config)
        machine_type = get_machine_type(args.config)
        for description, config in configs:
            if args.limit > 0:
                if count > args.limit:
                    log.info('Stopped after {limit} jobs due to --limit={limit}'.format(
                    limit=args.limit))

                    break
            raw_yaml = '\n'.join([file(a, 'r').read() for a in config])

            parsed_yaml = yaml.load(raw_yaml)
            os_type = parsed_yaml.get('os_type')
            exclude_arch = parsed_yaml.get('exclude_arch')
            exclude_os_type = parsed_yaml.get('exclude_os_type')

            if exclude_arch:
                if exclude_arch == arch:
                    log.info('Skipping due to excluded_arch: %s facets %s',
                             exclude_arch, description)
                    continue
            if exclude_os_type:
                if exclude_os_type == os_type:
                    log.info('Skipping due to excluded_os_type: %s facets %s',
                             exclude_os_type, description)
                    continue
            # We should not run multiple tests (changing distros) unless the
            # machine is a VPS.
            # Re-imaging baremetal is not yet supported.
            if machine_type != 'vps':
                if os_type and os_type != 'ubuntu':
                    log.info(
                        'Skipping due to non-ubuntu on baremetal facets %s',
                        description)
                    continue

            log.info(
                'Scheduling %s', description
            )

            arg = copy.deepcopy(base_arg)
            arg.extend([
                '--description', description,
                '--',
            ])
            arg.extend(args.config)
            arg.extend(config)

            if args.dry_run:
                log.info('dry-run: %s' % ' '.join(arg))
            else:
                subprocess.check_call(
                    args=arg,
                )
            count += 1

    if num_jobs:
        arg = copy.deepcopy(base_arg)
        arg.append('--last-in-suite')
        if args.email:
            arg.extend(['--email', args.email])
        if args.timeout:
            arg.extend(['--timeout', args.timeout])
        if args.dry_run:
            log.info('dry-run: %s' % ' '.join(arg))
        else:
            subprocess.check_call(
                args=arg,
            )


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


def get_os_type(configs):
    for config in configs:
        yamlfile = config[2]
        y = yaml.safe_load(file(yamlfile))
        if not y:
            y = {}
        os_type = y.get('os_type')
        if os_type:
            return os_type
    return None


def get_exclude_arch(configs):
    for config in configs:
        yamlfile = config[2]
        y = yaml.safe_load(file(yamlfile))
        if not y:
            y = {}
        exclude_arch = y.get('exclude_arch')
        if exclude_arch:
            return exclude_arch
    return None


def get_exclude_os_type(configs):
    for config in configs:
        yamlfile = config[2]
        y = yaml.safe_load(file(yamlfile))
        if not y:
            y = {}
        exclude_os_type = y.get('exclude_os_type')
        if exclude_os_type:
            return exclude_os_type
    return None


def get_machine_type(config):
    for yamlfile in config:
        y = yaml.safe_load(file(yamlfile))
        if not y:
            y = {}
        machine_type = y.get('machine_type')
        if machine_type:
            return machine_type
    return None
