#!/usr/bin/python3
#
# make a combined "manifest-list" container out of two arch-specific containers
# searches for latest tags on HOST/{AMD,ARM}64_REPO, makes sure they refer
# to the same Ceph SHA1, and creates a manifest-list ("fat") image on
# MANIFEST_HOST/MANIFEST_REPO with the 'standard' set of tags.
#
# uses scratch local manifest LOCALMANIFEST, will be destroyed if present

from datetime import datetime
import functools
import json
import os
import re
import subprocess
import sys

# optional env vars (will default if not set)

OPTIONAL_VARS = (
    'HOST',
    'AMD64_REPO',
    'ARM64_REPO',
    'MANIFEST_HOST',
    'MANIFEST_REPO',
)

# Manifest image.  Will be destroyed if already present.
LOCALMANIFEST = 'localhost/m'


def dump_vars(names, vardict):
    for name in names:
        print(f'{name}: {vardict[name]}', file=sys.stderr)


def run_command(args):
    print(f'running {args}', file=sys.stderr)
    if not isinstance(args, list):
        args = args.split()
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=True)
        return True, result.stdout, result.stderr

    except subprocess.CalledProcessError as e:
        print(f"Command '{e.cmd}' returned {e.returncode}")
        print("Error output:")
        print(e.stderr)
        return False, result.stdout, result.stderr


def get_command_output(args):
    success, stdout, stderr = run_command(args)
    return (stdout if success else None)


def run_command_show_failure(args):
    success, stdout, stderr = run_command(args)
    if not success:
        print(f'{args} failed:', file=sys.stderr)
        print(f'stdout:\n{stdout}')
        print(f'stderr:\n{stderr}')
    return success


@functools.lru_cache
def get_latest_tag(path):
    latest_tag = json.loads(
        get_command_output(f'skopeo list-tags docker://{path}')
    )['Tags'][-1]
    return latest_tag


@functools.lru_cache
def get_image_inspect(path):
    info = json.loads(
        get_command_output(f'skopeo inspect docker://{path}')
    )
    return info


def get_sha1(info):
    return info['Labels']['GIT_COMMIT']


def main():
    host = os.environ.get('HOST', 'quay.io')
    amd64_repo = os.environ.get('AMD64_REPO', 'ceph/ceph-amd64')
    arm64_repo = os.environ.get('ARM64_REPO', 'ceph/ceph-arm64')
    manifest_host = os.environ.get('MANIFEST_HOST', host)
    manifest_repo = os.environ.get('MANIFEST_REPO', 'ceph/ceph')
    dump_vars(
        ('host',
         'amd64_repo',
         'arm64_repo',
         'manifest_host',
         'manifest_repo',
         ),
        locals())

    repopaths = (
        f'{host}/{amd64_repo}',
        f'{host}/{arm64_repo}',
    )
    tags = [get_latest_tag(p) for p in repopaths]
    print(f'latest tags: amd64:{tags[0]} arm64:{tags[1]}')

    # check that version of latest tag matches
    version_re = \
        r'v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<micro>\d+)-(?P<date>\d+)'
    versions = list()
    for tag in tags:
        mo = re.match(version_re, tag)
        ver = f'{mo.group("major")}.{mo.group("minor")}.{mo.group("micro")}'
        versions.append(ver)
    if versions[0] != versions[1]:
        print(
            f'version mismatch: amd64:{versions[0]} arm64:{versions[1]}',
            file=sys.stderr,
        )
        return(1)

    major, minor, micro = mo.group(1), mo.group(2), mo.group(3)
    print(f'Ceph version: {major}.{minor}.{micro}', file=sys.stderr)

    # check that ceph sha1 of two arch images matches
    paths_with_tags = [f'{p}:{t}' for (p, t) in zip(repopaths, tags)]
    info = [get_image_inspect(p) for p in paths_with_tags]
    sha1s = [get_sha1(i) for i in info]
    if sha1s[0] != sha1s[1]:
        print(
            f'sha1 mismatch: amd64: {sha1s[0]} arm64: {sha1s[1]}',
            file=sys.stderr,
        )
        builddate = [i['Created'] for i in info]
        print(
            f'Build dates: amd64: {builddate[0]} arm64: {builddate[1]}',
            file=sys.stderr,
        )
        return(1)

    # create manifest list image with the standard list of tags
    # ignore failure on manifest rm
    run_command(f'podman manifest rm localhost/m')
    run_command_show_failure(f'podman manifest create localhost/m')
    for p in paths_with_tags:
        run_command_show_failure(f'podman manifest add m {p}')
    base = f'{manifest_host}/{manifest_repo}'
    for t in (
            f'v{major}',
            f'v{major}.{minor}',
            f'v{major}.{minor}.{micro}',
            f'v{major}.{minor}.{micro}-{datetime.today().strftime("%Y%m%d")}',
        ):
        run_command_show_failure(
          f'podman manifest push localhost/m {base}:{t}')


if (__name__ == '__main__'):
    sys.exit(main())
