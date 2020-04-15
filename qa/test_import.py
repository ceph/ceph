# try to import all .py files from a given directory

import argparse
import glob
import os
import importlib
import importlib.util


def _module_name(path):
    task = os.path.splitext(path)[0]
    parts = task.split(os.path.sep)
    package = parts[0]
    name = ''.join('.' + c for c in parts[1:])
    return package, name

def _import_file(path):
    package, mod_name = _module_name(path)
    line = f'Importing {package}{mod_name} from {path}'
    print(f'{line:<80}', end='')
    mod_spec = importlib.util.find_spec(mod_name, package)
    mod = mod_spec.loader.load_module()
    if mod is None:
        result = 'FAIL'
    else:
        result = 'DONE'
    print(f'{result:>6}')
    mod_spec.loader.exec_module(mod)

def _parser():
    parser = argparse.ArgumentParser(
        description='Try to import a file',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('path', nargs='+', help='Glob to select files')
    return parser


if __name__ == '__main__':
    parser = _parser()
    args = parser.parse_args()
    for g in args.path:
        for p in glob.glob(g, recursive=True):
            _import_file(p)
