# -*- coding: utf-8 -*-
from prettytable import PrettyTable, FRAME, ALL
import os
import yaml

from teuthology.exceptions import ParseError

def main(args):
    suite_dir = os.path.abspath(args["<suite_dir>"])
    fields = args["--fields"].split(',')
    include_facet = args['--show-facet'] == 'yes'

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
