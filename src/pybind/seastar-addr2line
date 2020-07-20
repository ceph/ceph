#!/usr/bin/env python3
#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Copyright (C) 2017 ScyllaDB

import argparse
import collections
import re
import sys
import subprocess

class Addr2Line:
    def __init__(self, binary):
        self._binary = binary

        # Print warning if binary has no debug info according to `file`.
        # Note: no message is printed for system errors as they will be
        # printed also by addr2line later on.
        output = subprocess.check_output(["file", self._binary])
        s = output.decode("utf-8")
        if s.find('ELF') >= 0 and s.find('debug_info', len(self._binary)) < 0:
            print('{}'.format(s))

        self._addr2line = subprocess.Popen(["addr2line", "-Cfpia", "-e", self._binary], stdin=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)

        # If a library doesn't exist in a particular path, addr2line
        # will just exit.  We need to be robust against that.  We
        # can't just wait on self._addr2line since there is no
        # guarantee on what timeout is sufficient.
        self._addr2line.stdin.write('\n')
        self._addr2line.stdin.flush()
        res = self._addr2line.stdout.readline()
        self._missing = res == ''

    def _read_resolved_address(self):
        res = self._addr2line.stdout.readline()
        # remove the address
        res = res.split(': ', 1)[1]
        dummy = '0x0000000000000000: ?? ??:0\n'
        line = ''
        while line != dummy:
            res += line
            line = self._addr2line.stdout.readline()
        return res

    def __call__(self, address):
        if self._missing:
            return " ".join([self._binary, address, '\n'])
        # print two lines to force addr2line to output a dummy
        # line which we can look for in _read_address
        self._addr2line.stdin.write(address + '\n\n')
        self._addr2line.stdin.flush()
        return self._read_resolved_address()

class BacktraceResolver(object):
    object_address_re = re.compile('^(.*?)\W(((/[^/]+)+)\+)?(0x[0-9a-f]+)\W*$')

    def __init__(self, executable, before_lines, context_re, verbose):
        self._executable = executable
        self._current_backtrace = []
        self._prefix = None
        self._before_lines = before_lines
        self._before_lines_queue = collections.deque(maxlen=before_lines)
        self._i = 0
        self._known_backtraces = {}
        if context_re is not None:
            self._context_re = re.compile(context_re)
        else:
            self._context_re = None
        self._verbose = verbose
        self._known_modules = {self._executable: Addr2Line(self._executable)}

    def _get_resolver_for_module(self, module):
        if not module in self._known_modules:
            self._known_modules[module] = Addr2Line(module)
        return self._known_modules[module]

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self._print_current_backtrace()

    def _print_resolved_address(self, module, address):
        resolved_address = self._get_resolver_for_module(module)(address)
        if self._verbose:
            resolved_address = '{{{}}} {}: {}'.format(module, address, resolved_address)
        sys.stdout.write(resolved_address)

    def _backtrace_context_matches(self):
        if self._context_re is None:
            return True

        if any(map(lambda x: self._context_re.search(x) is not None, self._before_lines_queue)):
            return True

        if (not self._prefix is None) and self._context_re.search(self._prefix):
            return True

        return False

    def _print_current_backtrace(self):
        if len(self._current_backtrace) == 0:
            return

        if not self._backtrace_context_matches():
            self._current_backtrace = []
            return

        for line in self._before_lines_queue:
            sys.stdout.write(line)

        if not self._prefix is None:
            print(self._prefix)
            self._prefix = None

        backtrace = "".join(map(str, self._current_backtrace))
        if backtrace in self._known_backtraces:
            print("[Backtrace #{}] Already seen, not resolving again.".format(self._known_backtraces[backtrace]))
            print("") # To separate traces with an empty line
            self._current_backtrace = []
            return

        self._known_backtraces[backtrace] = self._i

        print("[Backtrace #{}]".format(self._i))

        for module, addr in self._current_backtrace:
            self._print_resolved_address(module, addr)

        print("") # To separate traces with an empty line

        self._current_backtrace = []
        self._i += 1

    def __call__(self, line):
        match = re.match(self.object_address_re, line)

        if match:
            prefix, _, object_path, _, addr = match.groups()

            if len(self._current_backtrace) == 0:
                self._prefix = prefix;

            if object_path:
                self._current_backtrace.append((object_path, addr))
            else:
                self._current_backtrace.append((self._executable, addr))
        else:
            self._print_current_backtrace()
            if self._before_lines > 0:
                self._before_lines_queue.append(line)
            elif self._before_lines < 0:
                sys.stdout.write(line) # line already has a trailing newline
            else:
                pass # when == 0 no non-backtrace lines are printed


class StdinBacktraceIterator(object):
    """
    Read stdin char-by-char and stop when when user pressed Ctrl+D or the
    Enter twice. Altough reading char-by-char is slow this won't be a
    problem here as backtraces shouldn't be huge.
    """
    def __iter__(self):
        linefeeds = 0
        lines = []
        line = []

        while True:
            char = sys.stdin.read(1)

            if char == '\n':
                linefeeds += 1

                if len(line) > 0:
                    lines.append(''.join(line))
                    line = []
            else:
                line.append(char)
                linefeeds = 0

            if char == '' or linefeeds > 1:
                break

        return iter(lines)


description='Massage and pass addresses to the real addr2line for symbol lookup.'
epilog='''
There are three operational modes:
  1) If -f is specified input will be read from FILE
  2) If -f is omitted and there are ADDRESS args they will be read as input
  3) If -f is omitted and there are no ADDRESS args input will be read from stdin
'''

cmdline_parser = argparse.ArgumentParser(
    description=description,
    epilog=epilog,
    formatter_class=argparse.RawDescriptionHelpFormatter,
)

cmdline_parser.add_argument(
        '-e',
        '--executable',
        type=str,
        required=True,
        metavar='EXECUTABLE',
        dest='executable',
        help='The executable where the addresses originate from')

cmdline_parser.add_argument(
        '-f',
        '--file',
        type=str,
        required=False,
        metavar='FILE',
        dest='file',
        help='The file containing the addresses (one per line)')

cmdline_parser.add_argument(
        '-b',
        '--before',
        type=int,
        metavar='BEFORE',
        default=1,
        help='Non-backtrace lines to print before resolved backtraces for context.'
        ' Set to 0 to print only resolved backtraces.'
        ' Set to -1 to print all non-backtrace lines. Default is 1.')

cmdline_parser.add_argument(
        '-m',
        '--match',
        type=str,
        metavar='MATCH',
        help='Only resolve backtraces whose non-backtrace lines match the regular-expression.'
        ' The amount of non-backtrace lines considered can be set with --before.'
        ' By default no matching is performed.')

cmdline_parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        default=False,
        help='Make resolved backtraces verbose, prepend to each line the module'
        ' it originates from, as well as the address being resolved')

cmdline_parser.add_argument(
        'addresses',
        type=str,
        metavar='ADDRESS',
        nargs='*',
        help='Addresses to parse')

args = cmdline_parser.parse_args()

if args.addresses and args.file:
    print("Cannot use both -f and ADDRESS")
    cmdline_parser.print_help()


if args.file:
    lines = open(args.file, 'r')
elif args.addresses:
    lines = args.addresses
else:
    if sys.stdin.isatty():
        lines = StdinBacktraceIterator()
    else:
        lines = sys.stdin

with BacktraceResolver(args.executable, args.before, args.match, args.verbose) as resolve:
    for line in lines:
        resolve(line)
