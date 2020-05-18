# -*- coding: utf-8 -*-

import csv
import json
from prettytable import PrettyTable, FRAME, ALL
import os
import sys
import yaml

import random
from distutils.util import strtobool

from teuthology.exceptions import ParseError
from teuthology.suite.build_matrix import \
        build_matrix, generate_combinations, _get_matrix
from teuthology.suite import util

def main(args):
    try:
        describe_tests(args)
    except ParseError:
        sys.exit(1)


def describe_tests(args):
    suite_dir = os.path.abspath(args["<suite_dir>"])
    output_format = args['--format']

    conf=dict()
    rename_args = {
        'filter': 'filter_in',
    }
    for (key, value) in args.items():
        key = key.lstrip('--').replace('-', '_')
        key = rename_args.get(key) or key
        if key in ('filter_all', 'filter_in', 'filter_out', 'fields'):
            if not value:
                value = []
            else:
                value = [_ for _ in
                            (x.strip() for x in value.split(',')) if _]
        elif key in ('limit'):
            value = int(value)
        elif key in ('seed'):
            value = int(value)
            if value < 0:
                value = None
        elif key == 'subset' and value is not None:
            # take input string '2/3' and turn into (2, 3)
            value = tuple(map(int, value.split('/')))
        elif key in ('show_facet'):
            value = strtobool(value)
        conf[key] = value

    if args['--combinations']:
        headers, rows = get_combinations(suite_dir,
                                         limit=conf['limit'],
                                         seed=conf['seed'],
                                         subset=conf['subset'],
                                         fields=conf['fields'],
                                         filter_in=conf['filter_in'],
                                         filter_out=conf['filter_out'],
                                         filter_all=conf['filter_all'],
                                         filter_fragments=conf['filter_fragments'],
                                         include_facet=conf['show_facet'])
        hrule = ALL
    elif args['--summary']:
        output_summary(suite_dir,
                       limit=conf['limit'],
                       seed=conf['seed'],
                       subset=conf['subset'],
                       show_desc=conf['print_description'],
                       show_frag=conf['print_fragments'],
                       filter_in=conf['filter_in'],
                       filter_out=conf['filter_out'],
                       filter_all=conf['filter_all'],
                       filter_fragments=conf['filter_fragments'])
        exit(0)
    else:
        headers, rows = describe_suite(suite_dir, conf['fields'], conf['show_facet'],
                                       output_format)
        hrule = FRAME

    output_results(headers, rows, output_format, hrule)


def output_results(headers, rows, output_format, hrule):
    """
    Write the headers and rows given in the specified output format to
    stdout.
    """
    if output_format == 'json':
        objects = [{k: v for k, v in zip(headers, row) if v}
                   for row in rows]
        print(json.dumps(dict(headers=headers, data=objects)))
    elif output_format == 'csv':
        writer = csv.writer(sys.stdout)
        writer.writerows([headers] + rows)
    else:
        table = PrettyTable(headers)
        table.align = 'l'
        table.vrules = ALL
        table.hrules = hrule
        for row in rows:
            table.add_row(row)
        print(table)


def output_summary(path, limit=0,
                         seed=None,
                         subset=None,
                         show_desc=True,
                         show_frag=False,
                         show_matrix=False,
                         filter_in=None,
                         filter_out=None,
                         filter_all=None,
                         filter_fragments=True):
    """
    Prints number of all facets for a given suite for inspection,
    taking into accout such options like --subset, --filter,
    --filter-out and --filter-all. Optionally dumps matrix objects,
    yaml files which is used for generating combinations.
    """

    random.seed(seed)
    mat, first, matlimit = _get_matrix(path, subset)
    configs = generate_combinations(path, mat, first, matlimit)
    count = 0
    suite = os.path.basename(path)
    config_list = util.filter_configs(configs,
                                      suite_name=suite,
                                      filter_in=filter_in,
                                      filter_out=filter_out,
                                      filter_all=filter_all,
                                      filter_fragments=filter_fragments)
    for c in config_list:
        if limit and count >= limit:
            break
        count += 1
        if show_desc or show_frag:
            print("{}".format(c[0]))
            if show_frag:
                for path in c[1]:
                    print("    {}".format(util.strip_fragment_path(path)))
    if show_matrix:
       print(mat.tostr(1))
    print("# {}/{} {}".format(count, len(configs), path))

def get_combinations(suite_dir,
                     limit=0,
                     seed=None,
                     subset=None,
                     fields=[],
                     filter_in=None,
                     filter_out=None,
                     filter_all=None,
                     filter_fragments=False,
                     include_facet=True):
    """
    Describes the combinations of a suite, optionally limiting
    or filtering output based on the given parameters. Includes
    columns for the subsuite and facets when include_facet is True.

    Returns a tuple of (headers, rows) where both elements are lists
    of strings.
    """
    suite = os.path.basename(suite_dir)
    configs = build_matrix(suite_dir, subset, seed)

    num_listed = 0
    rows = []

    facet_headers = set()
    dirs = {}
    max_dir_depth = 0

    configs = util.filter_configs(configs,
                                  suite_name=suite,
                                  filter_in=filter_in,
                                  filter_out=filter_out,
                                  filter_all=filter_all,
                                  filter_fragments=filter_fragments)
    for _, fragment_paths in configs:
        if limit > 0 and num_listed >= limit:
            break

        fragment_fields = [extract_info(path, fields)
                           for path in fragment_paths]

        # merge fields from multiple fragments by joining their values with \n
        metadata = {}
        for fragment_meta in fragment_fields:
            for field, value in fragment_meta.items():
                if value == '':
                    continue
                if field in metadata:
                    metadata[field] += '\n' + str(value)
                else:
                    metadata[field] = str(value)

        if include_facet:
            # map final dir (facet) -> filename without the .yaml suffix
            for path in fragment_paths:
                facet_dir = os.path.dirname(path)
                facet = os.path.basename(facet_dir)
                metadata[facet] = os.path.basename(path)[:-5]
                facet_headers.add(facet)
                facet_dirs = facet_dir.split('/')[:-1]
                for i, dir_ in enumerate(facet_dirs):
                    if i not in dirs:
                        dirs[i] = set()
                    dirs[i].add(dir_)
                    metadata['_dir_' + str(i)] = os.path.basename(dir_)
                    max_dir_depth = max(max_dir_depth, i)

        rows.append(metadata)
        num_listed += 1

    subsuite_headers = []
    if include_facet:
        first_subsuite_depth = max_dir_depth
        for i in range(max_dir_depth):
            if len(dirs[i]) > 1:
                first_subsuite_depth = i
                break

        subsuite_headers = ['subsuite depth ' + str(i)
                            for i in
                            range(0, max_dir_depth - first_subsuite_depth + 1)]

        for row in rows:
            for i in range(first_subsuite_depth, max_dir_depth + 1):
                row[subsuite_headers[i - first_subsuite_depth]] = \
                    row.get('_dir_' + str(i), '')

    headers = subsuite_headers + sorted(facet_headers) + fields
    return headers, sorted([[row.get(field, '') for field in headers]
                            for row in rows])


def describe_suite(suite_dir, fields, include_facet, output_format):
    """
    Describe a suite listing each subdirectory and file once as a
    separate row.

    Returns a tuple of (headers, rows) where both elements are lists
    of strings.

    """
    rows = tree_with_info(suite_dir, fields, include_facet, '', [],
                          output_format=output_format)

    headers = ['path']
    if include_facet:
        headers.append('facet')
    return headers + fields, rows


def extract_info(file_name, fields):
    """
    Read a yaml file and return a dictionary mapping the fields to the
    values of those fields in the file.

    The returned dictionary will always contain all the provided
    fields, mapping any non-existent ones to ''.

    Assumes fields are set in a format of:

    {'meta': [{'field' : value, 'field2' : value2}]

    or in yaml:

    meta:
    - field: value
      field2: value2

    If 'meta' is present but not in this format, prints an error
    message and raises ParseError.
    """
    empty_result = {f: '' for f in fields}
    if os.path.isdir(file_name) or not file_name.endswith('.yaml'):
        return empty_result

    with open(file_name, 'r') as f:
        parsed = yaml.safe_load(f)

    if not isinstance(parsed, dict):
        return empty_result

    meta = parsed.get('meta', [{}])
    if not (isinstance(meta, list) and
            len(meta) == 1 and
            isinstance(meta[0], dict)):
        print('Error in meta format in %s' % file_name)
        print('Meta must be a list containing exactly one dict.')
        print('Meta is: %s' % meta)
        raise ParseError()

    return {field: meta[0].get(field, '') for field in fields}


def path_relative_to_suites(path):
    """
    Attempt to trim the ceph-qa-suite root directory from the beginning
    of a path.
    """
    try:
        root = os.path.join('ceph-qa-suite', 'suites')
        return path[path.index(root) + len(root):]
    except ValueError:
        return path


def tree_with_info(cur_dir, fields, include_facet, prefix, rows,
                   output_format='plain'):
    """
    Gather fields from all files and directories in cur_dir.
    Returns a list of strings for each path containing:

    1) the path relative to ceph-qa-suite/suites (or the basename with
        a /usr/bin/tree-like prefix if output_format is plain)
    2) the facet containing the path (if include_facet is True)
    3) the values of the provided fields in the path ('' is used for
       missing values) in the same order as the provided fields
    """
    files = sorted(os.listdir(cur_dir))
    has_yamls = any([x.endswith('.yaml') for x in files])
    facet = os.path.basename(cur_dir) if has_yamls else ''
    for i, f in enumerate(files):
        # skip any hidden files
        if f.startswith('.'):
            continue
        path = os.path.join(cur_dir, f)
        if i == len(files) - 1:
            file_pad = '└── '
            dir_pad = '    '
        else:
            file_pad = '├── '
            dir_pad = '│   '
        info = extract_info(path, fields)
        tree_node = prefix + file_pad + f
        if output_format != 'plain':
            tree_node = path_relative_to_suites(path)
        meta = [info[f] for f in fields]
        row = [tree_node]
        if include_facet:
            row.append(facet)
        rows.append(row + meta)
        if os.path.isdir(path):
            tree_with_info(path, fields, include_facet,
                           prefix + dir_pad, rows, output_format)
    return rows
