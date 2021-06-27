#!/usr/bin/python3
# -*- mode:python -*-
# vim: ts=4 sw=4 smarttab expandtab

__all__ = ['ConsoleOptions', 'MgrModuleInterpreter']

import readline
import sys
from code import InteractiveConsole
from collections import namedtuple
from pathlib import Path

from ceph_argparse import json_command


ConsoleOptions = namedtuple('ConsoleOptions',
                            ['name', 'conffile', 'prefix', 'timeout'])


class MgrModuleInteractiveConsole(InteractiveConsole):
    def __init__(self, rados, opt, filename="<console>"):
        super().__init__(filename)
        self.cmd_prefix = opt.prefix
        self.timeout = opt.timeout
        self.cluster = rados.Rados(name=opt.name,
                                   conffile=opt.conffile)
        self.cluster.connect(timeout=opt.timeout)

    def _do_runsource(self, source):
        ret, buf, s = json_command(self.cluster,
                                   prefix=self.cmd_prefix,
                                   target=('mon-mgr',),
                                   inbuf=source.encode(),
                                   timeout=self.timeout)
        if ret == 0:
            # TODO: better way to encode the outputs
            sys.stdout.write(buf.decode())
            sys.stderr.write(s)
        else:
            # needs more
            self.write("the input is not complete")

    def runsource(self, source, filename='<input>', symbol='single'):
        try:
            # just validate the syntax
            code = self.compile(source, filename, symbol)
        except (OverflowError, SyntaxError, ValueError):
            # Case 1
            self.showsyntaxerror(filename)
            return False

        if code is None:
            # Case 2
            return True

        # Case 3
        self._do_runsource(source)
        return False

    def runcode(self, code):
        # code object cannot be pickled
        raise NotImplementedError()


def show_env():
    prog = Path(__file__).resolve()
    ceph_dir = prog.parents[2]
    python_path = ':'.join([f'{ceph_dir}/src/pybind',
                            f'{ceph_dir}/build/lib/cython_modules/lib.3',
                            f'{ceph_dir}/src/python-common',
                            '$PYTHONPATH'])
    ld_library_path = ':'.join([f'{ceph_dir}/build/lib',
                                '$LD_LIBRARY_PATH'])
    return f'''
    $ export PYTHONPATH={python_path}
    $ export LD_LIBRARY_PATH={ld_library_path}'''.strip('\n')


def main():
    import argparse
    try:
        import rados
    except ImportError:
        print(f'''Unable to import rados python binding.
Please set the environment variables first:
{show_env()}''',
              file=sys.stderr)
        exit(1)

    prog = Path(__file__).name
    epilog = f'''Usage:
    {prog} -c "print(mgr.release_name)"'''
    parser = argparse.ArgumentParser(epilog=epilog)
    parser.add_argument('--name', action='store',
                        default='client.admin',
                        help='user name for connecting to cluster')
    parser.add_argument('--conffile', action='store',
                        default=rados.Rados.DEFAULT_CONF_FILES,
                        help='path to ceph.conf')
    parser.add_argument('--prefix', action='store',
                        default='mgr self-test eval',
                        help='command prefix for eval the source')
    parser.add_argument('--timeout', action='store',
                        type=int,
                        default=10,
                        help='timeout in seconds')
    parser.add_argument('--show-env', action='store_true',
                        help='show instructions to set environment variables')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-c', action='store',
                       help='optional statement',
                       dest='command')
    group.add_argument('script', nargs='?', type=argparse.FileType('r'))
    args = parser.parse_args()
    options = ConsoleOptions(name=args.name,
                             conffile=args.conffile,
                             prefix=args.prefix,
                             timeout=args.timeout)
    console = MgrModuleInteractiveConsole(rados, options)
    if args.show_env:
        print(show_env())
    elif args.command:
        console.runsource(args.command)
    elif args.script:
        console.runsource(args.script.read())
    else:
        sys.ps1 = f'[{args.prefix}] >>> '
        console.interact()


if __name__ == '__main__':
    main()
