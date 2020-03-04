#!/usr/bin/env python3
from collections import OrderedDict
import argparse
import logging
import os
import os.path
import urllib.parse
import urllib
import requests
import string
import subprocess
import sys

log = logging.basicConfig()

CEPH_REPO_TEMPLATE = """
[ceph]
name=Ceph packages for $$basearch
baseurl=$url/$$basearch
enabled=0
priority=2

[ceph-noarch]
name=Ceph noarch packages
baseurl=$url/noarch
enabled=0
priority=2
"""


def _search_uri(distro='centos/8', sha1='latest', arch='x86_64', flavor='default'):
    req_obj = OrderedDict()
    req_obj['status'] = 'ready'
    req_obj['project'] = 'ceph'
    req_obj['flavor'] = flavor
    req_obj['distros'] = '/'.join((distro, arch))
    req_obj['sha1'] = sha1
    req_str = urllib.parse.urlencode(req_obj)
    uri = urllib.parse.urljoin(
        'http://shaman.ceph.com/api/', 'search',
    ) + '?%s' % req_str
    return uri


def find_git_parents(project, sha1, count=10):

    base_url = 'http://git.ceph.com:8080'

    def refresh(project):
        url = '%s/%s.git/refresh' % (base_url, project)
        resp = requests.get(url)
        if not resp.ok:
            log.error('git refresh failed for %s: %s', project, resp.content)

    def get_sha1s(project, committish, count):
        url = '/'.join((base_url, '%s.git' % project,
                       'history/?committish=%s&count=%d' % (committish, count)))
        resp = requests.get(url)
        resp.raise_for_status()
        sha1s = resp.json()['sha1s']
        if len(sha1s) != count:
            log.error('can''t find %d parents of %s in %s: %s',
                      int(count), sha1, project, resp.json()['error'])
        return sha1s

    refresh(project)
    sha1s = get_sha1s(project, sha1, count)
    return sha1s


def find_local_git_parents(sha1='HEAD', count=10):
    args = [
        'git',
        'rev-list',
        '--first-parent',
        '--max-count=%d' % count,
        sha1
    ]
    out = subprocess.check_output(
        args=args
    ).decode()
    return out.split('\n')


def get_build_and_repo_url(distro, sha1, arch, flavor):
    uri = _search_uri(distro, sha1, arch, flavor)
    resp = requests.get(uri)
    resp.raise_for_status()

    if not len(resp.json()):
        print('no repo found', file=sys.stderr)
        return 1
    repo_url = resp.json()[0]['url']
    sha1 = resp.json()[0]['sha1']
    return sha1, repo_url


def repo_filename(name):
    return '/etc/yum.repos.d/%s.repo' % name


def repo_present(name):
    return os.path.exists(repo_filename(name))


def create_repo(name, url):
    contents = string.Template(CEPH_REPO_TEMPLATE)
    contents = contents.substitute(
        {
            'name': name,
            'url': url,
        }
    )
    with open(repo_filename(name), 'w') as f:
        f.write(contents)


def remove_repo(name):
    os.remove(repo_filename(name))


def main():
    parser = argparse.ArgumentParser()
    defhelp = 'default: %(default)s'
    parser.add_argument('-d', '--distro', default='centos/8', help=defhelp)
    parser.add_argument('-s', '--sha1', default='latest', help=defhelp)
    parser.add_argument('-a', '--arch', default='x86_64', help=defhelp)
    parser.add_argument('-f', '--flavor', default='default', help=defhelp)
    parser.add_argument('-l', '--list', action='store_true', help=defhelp)
    args = parser.parse_args(sys.argv[1:])

    repo_sha1, repo_url = get_build_and_repo_url(
        distro=args.distro,
        sha1=args.sha1,
        arch=args.arch,
        flavor=args.flavor
    )

    local_sha1 = subprocess.check_output(
        args=['git', 'rev-parse', 'HEAD']).decode().strip()
    count = 30
    local_parent_sha1s = find_local_git_parents(local_sha1, count)
    found = False
    if repo_sha1 == local_sha1:
        print('found build for local sha1 at %s' % repo_url)
        found = True

    # could instead query shaman for all sha1s of this distro/arch/flavor
    # and then search locally, but 1) that takes a while because 2)
    # shaman returns them all, and 3) apparently a sha1/distro/arch/flavor
    # doesn't have to be unique?...so that's puzzling.

    else:
        for search_sha1 in local_parent_sha1s:
            repo_sha1, repo_url = get_build_and_repo_url(
                distro=args.distro,
                sha1=search_sha1,
                arch=args.arch,
                flavor=args.flavor
            )
            if repo_sha1 == search_sha1:
                print('local sha1 is %d commits ahead of most recent parent build at %s' % (
                    local_parent_sha1s.index(repo_sha1) + 1,
                    repo_url,
                ))
                found = True
                break

    if not found:
        print('local sha1 not found within last %d builds?' % count)
        return 1

    if repo_present('ceph'):
        remove_repo('ceph')

    create_repo('ceph', repo_url)

    cephpkgs = subprocess.check_output(args=[
        'dnf',
        '--disablerepo=*',
        '--enablerepo=ceph',
        'repoquery'
    ]).decode().split('\n')
    alldeps = set()
    for p in cephpkgs:
        rawdeps = subprocess.check_output(args=[
            'dnf',
            '--enablerepo=ceph',
            'deplist',
            p
        ]).decode().split('\n')

        # the line with 'provider:' has the full package name
        deps = [dep for dep in rawdeps if 'provider:' in dep]

        # remove ' provider: '
        deps = [dep[dep.find(': ') + 2:] for dep in deps]

        # don't include ceph packages themselves, we already have those taken care of
        deps = [dep for dep in deps if dep not in cephpkgs]

        alldeps.update(deps)

    alldeps = list(alldeps)
    if args.list:
        print('\n'.join(sorted(alldeps)))
        return 0

    print(subprocess.check_output(
        args=['dnf', 'install', '-y'] + alldeps
    )).decode()

    remove_repo('ceph')
    return 0


sys.exit(main())
