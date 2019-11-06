#!/usr/bin/env bash

# needs to be executed from the src directory.
# generates a report at src/mypy_report.txt

set -e

python3 -m venv .mypy_venv

. .mypy_venv/bin/activate

! pip install $(find -name requirements.txt -not -path './frontend/*' -printf '-r%p ')
pip install mypy

MYPY_INI="$PWD"/mypy.ini

export MYPYPATH="$PWD/pybind/rados:$PWD/pybind/rbd:$PWD/pybind/cephfs"

echo -n > mypy_report.txt
pushd pybind
mypy --config-file="$MYPY_INI" *.py | awk '{print "pybind/" $0}' >> ../mypy_report.txt
popd

pushd pybind/mgr
mypy --config-file="$MYPY_INI" $(find * -name '*.py' | grep -v -e venv -e tox -e env -e gyp -e node_modules) | awk '{print "pybind/mgr/" $0}' >> ../../mypy_report.txt
popd

pushd ceph-volume/ceph_volume
mypy --config-file="$MYPY_INI" $(find * -name '*.py' | grep -v -e venv -e tox -e env -e gyp -e node_modules -e tests) | awk '{print "ceph-volume/ceph_volume/" $0}' >> ../../mypy_report.txt
popd

SORT_MYPY=$(cat <<-EOF
#!/bin/python3
import re
from collections import namedtuple

class Line(namedtuple('Line', 'prefix no rest')):
    @classmethod
    def parse(cls, l):
        if not l:
            return cls('', 0, '')
        if re.search('Found [0-9]+ errors in [0-9]+ files', l):
            return cls('', 0, '')     
        p, *rest = l.split(':', 2)
        if len(rest) == 1:
            return cls(p, 0, rest[0])
        elif len(rest) == 2:
            try:
                return cls(p, int(rest[0]), rest[1])
            except ValueError:
                return cls(p, 0, rest[0] + ':' + rest[1])
        assert False, rest
 
class Group(object):
    def __init__(self, line):
        self.line = line
        self.lines = []

    def matches(self, other):
        return Line.parse(self.line).prefix == Line.parse(other).prefix

    def __bool__(self):
        return bool(self.lines) or ': note: In' not in self.line

    def __str__(self):
        return '\n'.join([self.line] + self.lines)

    def key(self):
        l1 = Line.parse(self.line)
        if l1.no:
            return l1.prefix, int(l1.no)
        if not self.lines:
            return l1.prefix, None
        return l1.prefix, Line.parse(self.lines[0]).no

def parse(text):
    groups = []

    def group():
        try:
            return groups[-1]
        except IndexError:
            groups.append(Group(''))
            return groups[-1]
    
    for l in text:
        l = l.strip()
        if ': note: In' in l or not group().matches(l):
            groups.append(Group(l))
        elif not l:
            pass
        else:
            group().lines.append(l)

    return (g for g in groups if g)

def render(groups):
    groups = sorted(groups, key=Group.key)
    return '\n'.join(map(str, groups))

with open('mypy_report.txt') as f:
    new = render(parse(f))
with open('mypy_report.txt', 'w') as f:
    f.write(new)
EOF
)

python <(echo "$SORT_MYPY")
