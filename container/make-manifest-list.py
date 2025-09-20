#!/usr/bin/python3
#
# in default mode:
# make a combined "manifest-list" container out of two arch-specific containers
# searches for latest tags on HOST/{AMD,ARM}64_REPO, makes sure they refer
# to the same Ceph SHA1, and creates a manifest-list ("fat") image on
# MANIFEST_HOST/MANIFEST_REPO with the 'standard' set of tags:
# v<major>
# v<major>.<minor>
# v<major>.<minor>.<micro>
# v<major>.<minor>.<micro>-<YYYYMMDD>
#
# uses scratch local manifest LOCALMANIFEST, defined here; will be destroyed if present
#
# in promote mode (by adding the --promote argument):
# instead of building the manifest-list container, copy it
# (and all of its tags) from the prerelease repo to the release repo
#
# Assumes valid logins to the necessary hosts/repos with permission to write images
#
# Environment variables to set:
# ARCH_SPECIFIC_HOST (default 'quay.ceph.io'): host of prerelease repos
# AMD64_REPO (default 'ceph/prerelease-amd64') prerelease amd64 repo
# ARM64_REPO (default 'ceph/prerelease-arm64') prerelease arm64 repo
# MANIFEST_HOST (default 'quay.ceph.io') prerelease manifest-list host
# MANIFEST_REPO (default 'ceph/prerelease') prerelease manifest-list repo
# RELEASE_MANIFEST_HOST (default 'quay.io') release host
# RELEASE_MANIFEST_REPO (default 'ceph/ceph') release repo


import argparse
from datetime import datetime
import functools
import json
import os
import re
import subprocess
import sys

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
        return False, e.output, e.stderr


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
def get_tags(path):
    cmdout = get_command_output(f'skopeo list-tags docker://{path}')
    return json.loads(cmdout)['Tags']


def get_latest_tag(path):
    try:
        latest_tag = get_tags(path)[-1]
    except IndexError:
        return None
    return latest_tag


@functools.lru_cache
def get_image_inspect(path):
    info = json.loads(
        get_command_output(f'skopeo inspect docker://{path}')
    )
    return info


def get_sha1(info):
    labels = info.get('Labels', None)
    if not labels:
        return None
    return labels.get('CEPH_SHA1', None)


@functools.lru_cache
def get_all_matching_digest_tags(path, tag):

    matching_tags = list()
    digest = get_image_inspect(f'{path}:{tag}')['Digest']

    for t in get_tags(path):
        this_digest = get_image_inspect(f'{path}:{t}')['Digest']
        if this_digest == digest:
            matching_tags.append(t)

    return matching_tags


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('-n', '--dry-run', action='store_true', help='do all local manipulations but do not push final containers to MANIFEST_HOST, or in --promote, calculate but do not copy images to release host')
    ap.add_argument('-P', '--promote', action='store_true', help='promote newest prerelease manifest container to released (move from MANIFEST_HOST to RELEASE_MANIFEST_HOST')
    args = ap.parse_args()
    return args

def build_prerelease(sysargs):
    global args

    arch_specific_host = os.environ.get('ARCH_SPECIFIC_HOST', 'quay.ceph.io')
    amd64_repo = os.environ.get('AMD64_REPO', 'ceph/prerelease-amd64')
    arm64_repo = os.environ.get('ARM64_REPO', 'ceph/prerelease-arm64')
    manifest_host = os.environ.get('MANIFEST_HOST', 'quay.ceph.io')
    manifest_repo = os.environ.get('MANIFEST_REPO', 'ceph/prerelease')

    dump_vars(
        ('arch_specific_host',
         'amd64_repo',
         'arm64_repo',
         'manifest_host',
         'manifest_repo',
         ),
        locals())
    repopaths = (
        f'{arch_specific_host}/{amd64_repo}',
        f'{arch_specific_host}/{arm64_repo}',
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
    run_command(f'podman manifest rm {LOCALMANIFEST}')
    run_command_show_failure(f'podman manifest create {LOCALMANIFEST}')
    for p in paths_with_tags:
        run_command_show_failure(f'podman manifest add m {p}')
    base = f'{manifest_host}/{manifest_repo}'
    for t in (
            f'v{major}',
            f'v{major}.{minor}',
            f'v{major}.{minor}.{micro}',
            f'v{major}.{minor}.{micro}-{datetime.today().strftime("%Y%m%d")}',
        ):
        if sysargs.dry_run:
            print(f'skipping podman manifest push {LOCALMANIFEST} {base}:{t}')
        else:
            run_command_show_failure(
              f'podman manifest push {LOCALMANIFEST} {base}:{t}')

def promote(sysargs):
    manifest_host = os.environ.get('MANIFEST_HOST', 'quay.ceph.io')
    manifest_repo = os.environ.get('MANIFEST_REPO', 'ceph/prerelease')
    release_manifest_host = os.environ.get('RELEASE_MANIFEST_HOST', 'quay.io')
    release_manifest_repo = os.environ.get('RELEASE_MANIFEST_REPO', 'ceph/ceph')
    dump_vars(
        ('manifest_host',
         'manifest_repo',
         'release_manifest_host',
         'release_manifest_repo',
         ),
        locals())

    manifest_path = f'{manifest_host}/{manifest_repo}'
    release_path = f'{release_manifest_host}/{release_manifest_repo}'
    latest_tag = get_latest_tag(manifest_path)
    all_tags = get_all_matching_digest_tags(manifest_path, latest_tag)

    copypaths = list()
    for t in all_tags:
        from_path = f'{manifest_path}:{t}'
        to_path = f'{release_path}:{t}'
        copypaths.append((from_path, to_path))

    if sysargs.dry_run:
        for f, t in copypaths:
            print(f'dry-run: Would copy: {f} -> {t}')
        return(0)

    for f, t in copypaths:
        print(f'Will copy: {f} -> {t}')

    for f, t in copypaths:
        run_command_show_failure(f'skopeo copy --multi-arch=all docker://{f} docker://{t}')


def main():
    args = parse_args()

    if args.promote:
        promote(args)
    else:
        build_prerelease(args)


if (__name__ == '__main__'):
    sys.exit(main())
