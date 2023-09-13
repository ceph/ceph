import os
import sys

project = u'Ceph'
copyright = u'2010-2014, Inktank Storage, Inc. and contributors. Licensed under Creative Commons Attribution Share Alike 3.0 (CC-BY-SA-3.0)'
version = 'dev'
release = 'dev'

exclude_patterns = ['**/.#*', '**/*~']


def _get_description(fname, base):
    with open(fname) as f:
        one = None
        while True:
            line = f.readline().rstrip('\n')
            if not line:
                continue
            if line.startswith(':') and line.endswith(':'):
                continue
            if line.startswith('.. '):
                continue
            one = line
            break
        two = f.readline().rstrip('\n')
        three = f.readline().rstrip('\n')
        assert one == three
        assert all(c=='=' for c in one)
        name, description = two.split('--', 1)
        assert name.strip() == base
        return description.strip()


def _get_manpages():
    src_dir = os.path.dirname(__file__)
    top_srcdir = os.path.dirname(src_dir)
    man_dir = os.path.join(top_srcdir, 'doc', 'man')
    sections = os.listdir(man_dir)
    for section in sections:
        section_dir = os.path.join(man_dir, section)
        if not os.path.isdir(section_dir):
            continue
        for filename in os.listdir(section_dir):
            base, ext = os.path.splitext(filename)
            if ext != '.rst':
                continue
            if base == 'index':
                continue
            path = os.path.join(section_dir, filename)
            try:
                description = _get_description(path, base)
            except UnicodeDecodeError as e:
                print(f"unable to decode {path}", file=sys.stderr)
                raise e
            yield (
                os.path.join(section, base),
                base,
                description,
                '',
                section,
                )

man_pages = list(_get_manpages())
# sphinx warns if no toc is found, so feed it with a random file
# which is also rendered in this run.
master_doc = '8/ceph'
