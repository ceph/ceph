# try to import all .py files from a given directory

import glob
import os
import importlib
import importlib.util
import pytest

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
    mod = mod_spec.loader.load_module(f'{package}{mod_name}')
    if mod is None:
        result = 'FAIL'
    else:
        result = 'DONE'
    print(f'{result:>6}')
    mod_spec.loader.exec_module(mod)
    return result

def get_paths():
    for g in ['tasks/**/*.py']:
        for p in glob.glob(g, recursive=True):
            yield p

@pytest.mark.parametrize("path", list(sorted(get_paths())))
def test_import(path):
    assert _import_file(path) == 'DONE'

