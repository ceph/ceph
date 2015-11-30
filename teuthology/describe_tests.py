# -*- coding: utf-8 -*-
from prettytable import PrettyTable, FRAME, ALL
import os

def main(args):
    suite_dir = os.path.abspath(args["<suite_dir>"])
    filters = args["--prefix"].split(',')
    include_facet = args['--show-facet'] == 'yes'

    print(suite_dir)
    rows = tree_with_info(suite_dir, filters, include_facet, '', [])

    headers = ['path']
    if include_facet:
        headers.append('facet')

    table = PrettyTable(headers + filters)
    table.align = 'l'
    table.vrules = ALL
    table.hrules = FRAME

    for row in rows:
        table.add_row(row)
    print(table)

def extract_info(file_name, filters, _isdir=os.path.isdir, _open=open):
    result = {f: '' for f in filters}
    if _isdir(file_name):
        return result
    with _open(file_name, 'r') as f:
        for line in f:
            for filt in filters:
                prefix = '# ' + filt + ':'
                if line.startswith(prefix):
                    if result[filt]:
                        result[filt] += '\n'
                    result[filt] += line[len(prefix):].rstrip('\n').strip()
    return result

def tree_with_info(cur_dir, filters, include_facet, prefix, rows,
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
        info = extract_info(path, filters, _isdir, _open)
        tree_node = prefix + file_pad + f
        meta = [info[f] for f in filters]
        row = [tree_node]
        if include_facet:
            row.append(facet)
        rows.append(row + meta)
        if _isdir(path):
            tree_with_info(path, filters, include_facet,
                           prefix + dir_pad, rows,
                           _listdir, _isdir, _open)
    return rows
