# -*- coding: utf-8 -*-
import os

def main(args):
    suite_dir = os.path.abspath(args["<suite_dir>"])
    filters = args["--prefix"].split(',')
    print suite_dir
    return tree(suite_dir, filters, '')

def extract_info(file_name, filters):
    result = []
    if os.path.isdir(file_name):
        return result
    with file(file_name, 'r') as f:
        for line in f:
            for filt in filters:
                prefix = '# ' + filt + ':'
                if line.startswith(prefix):
                    result.append(line[len(prefix):].rstrip('\n'))
    return result

def tree(cur_dir, filters, prefix):
    files = sorted(os.listdir(cur_dir))
    for i, f in enumerate(files):
        path = os.path.join(cur_dir, f)
        if i == len(files) - 1:
            file_pad = '└── '
            dir_pad = '    '
        else:
            file_pad = '├── '
            dir_pad = '│   '
        info = extract_info(path, filters)
        print prefix + file_pad + f + '    ' + '  |  '.join(info)
        if os.path.isdir(path):
            tree(path, filters, prefix + dir_pad)
