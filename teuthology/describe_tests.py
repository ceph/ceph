# -*- coding: utf-8 -*-

from prettytable import PrettyTable, FRAME, ALL
import os
import yaml

from teuthology.exceptions import ParseError
from teuthology.suite import build_matrix, combine_path

def main(args):
    suite_dir = os.path.abspath(args["<suite_dir>"])
    fields = args["--fields"].split(',')
    include_facet = args['--show-facet'] == 'yes'

    if args['--combinations']:
        limit = int(args['--limit'])
        filter_in = None
        if args['--filter']:
            filter_in = [f.strip() for f in args['--filter'].split(',')]
        filter_out = None
        if args['--filter-out']:
            filter_out = [f.strip() for f in args['--filter-out'].split(',')]
        subset = None
        if args['--subset']:
            subset = map(int, args['--subset'].split('/'))
        describe_combinations(suite_dir, fields, subset,
                              limit, filter_in, filter_out,
                              include_facet)
    else:
        describe_suite(suite_dir, fields, include_facet)

def get_combinations(suite_dir, fields, subset,
                     limit, filter_in, filter_out,
                     include_facet, _isdir=os.path.isdir, _open=open,
                     _isfile=os.path.isfile, _listdir=os.listdir):
    configs = [(combine_path(suite_dir, item[0]), item[1]) for item in
               build_matrix(suite_dir, _isfile, _isdir, _listdir, subset)]

    num_listed = 0
    rows = []

    facet_headers = set()

    for _, fragment_paths in configs:
        if limit > 0 and num_listed >= limit:
            break
        if filter_in and not any([f in path for f in filter_in
                                  for path in fragment_paths]):
            continue
        if filter_out and any([f in path for f in filter_out
                               for path in fragment_paths]):
            continue

        fragment_fields = [extract_info(path, fields, _isdir, _open)
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
                facet = os.path.basename(os.path.dirname(path))
                metadata[facet] = os.path.basename(path)[:-5]
                facet_headers.add(facet)

        rows.append(metadata)
        num_listed += 1

    headers = sorted(facet_headers) + fields
    return headers, [[row.get(field, '') for field in headers] for row in rows]

def describe_combinations(suite_dir, fields, subset,
                          limit, filter_in, filter_out,
                          include_facet):
    headers, rows = get_combinations(suite_dir, fields, subset,
                                     limit, filter_in, filter_out,
                                     include_facet)

    table = PrettyTable(headers)
    table.align = 'l'
    table.vrules = ALL
    table.hrules = ALL

    for row in rows:
        table.add_row(row)

    print(table)

def describe_suite(suite_dir, fields, include_facet):
    try:
        rows = tree_with_info(suite_dir, fields, include_facet, '', [])
    except ParseError:
        return 1

    headers = ['path']
    if include_facet:
        headers.append('facet')

    table = PrettyTable(headers + fields)
    table.align = 'l'
    table.vrules = ALL
    table.hrules = FRAME

    for row in rows:
        table.add_row(row)

    print(suite_dir)
    print(table)

def extract_info(file_name, fields, _isdir=os.path.isdir, _open=open):
    empty_result = {f: '' for f in fields}
    if _isdir(file_name) or not file_name.endswith('.yaml'):
        return empty_result

    with _open(file_name, 'r') as f:
        parsed = yaml.load(f)

    if not isinstance(parsed, dict):
        return empty_result

    description = parsed.get('description', [{}])
    if not (isinstance(description, list) and
            len(description) == 1 and
            isinstance(description[0], dict)):
        print 'Error in description format in', file_name
        print 'Description must be a list containing exactly one dict.'
        print 'Description is:', description
        raise ParseError()

    return {field: description[0].get(field, '') for field in fields}

def tree_with_info(cur_dir, fields, include_facet, prefix, rows,
                   _listdir=os.listdir, _isdir=os.path.isdir,
                   _open=open):
    files = sorted(_listdir(cur_dir))
    has_yamls = any([x.endswith('.yaml') for x in files])
    facet = os.path.basename(cur_dir) if has_yamls else ''
    for i, f in enumerate(files):
        path = os.path.join(cur_dir, f)
        if i == len(files) - 1:
            file_pad = '└── '
            dir_pad = '    '
        else:
            file_pad = '├── '
            dir_pad = '│   '
        info = extract_info(path, fields, _isdir, _open)
        tree_node = prefix + file_pad + f
        meta = [info[f] for f in fields]
        row = [tree_node]
        if include_facet:
            row.append(facet)
        rows.append(row + meta)
        if _isdir(path):
            tree_with_info(path, fields, include_facet,
                           prefix + dir_pad, rows,
                           _listdir, _isdir, _open)
    return rows
