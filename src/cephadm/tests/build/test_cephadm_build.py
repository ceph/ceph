# tests for building cephadm into a zipapp using build.py
#
# these should not be run automatically as they require the use of podman,
# which should not be assumed to exist on a typical test node

import os
import pathlib
import pytest
import subprocess
import sys


CONTAINERS = {
    'centos-8': {
        'name': 'cephadm-build-test:centos8-py36',
        'base_image': 'quay.io/centos/centos:stream8',
        'script': 'dnf install -y python36',
    },
    'centos-9': {
        'name': 'cephadm-build-test:centos9-py3',
        'base_image': 'quay.io/centos/centos:stream9',
        'script': 'dnf install -y python3',
    },
    'ubuntu-20.04': {
        'name': 'cephadm-build-test:ubuntu-20-04-py3',
        'base_image': 'docker.io/library/ubuntu:20.04',
        'script': 'apt update && apt install -y python3-venv',
    },
    'ubuntu-22.04': {
        'name': 'cephadm-build-test:ubuntu-22-04-py3',
        'base_image': 'docker.io/library/ubuntu:22.04',
        'script': 'apt update && apt install -y python3-venv',
    },
}

BUILD_PY = 'src/cephadm/build.py'


def _print(*args):
    """Print with a highlight prefix."""
    print('----->', *args)
    sys.stdout.flush()


def container_cmd(image, cmd, ceph_dir, out_dir):
    return [
        'podman',
        'run',
        '--rm',
        f'--volume={ceph_dir}:/ceph:ro',
        f'--volume={out_dir}:/out',
        image,
    ] + list(cmd)


def run_container_cmd(image, cmd, ceph_dir, out_dir):
    full_cmd = container_cmd(image, cmd, ceph_dir, out_dir)
    _print("CMD", full_cmd)
    return subprocess.run(full_cmd)


def build_container(src_image, dst_image, build_script, workdir):
    cfile = pathlib.Path(workdir) / 'Dockerfile'
    with open(cfile, 'w') as fh:
        fh.write(f'FROM {src_image}\n')
        fh.write(f'RUN {build_script}\n')
    cmd = ['podman', 'build', '-t', str(dst_image), '-f', str(cfile)]
    _print("BUILD CMD", cmd)
    subprocess.run(cmd, check=True)


def build_in(alias, ceph_dir, out_dir, args):
    ctr = CONTAINERS[alias]
    build_container(ctr['base_image'], ctr['name'], ctr['script'], out_dir)
    cmd = ['/ceph/' + BUILD_PY] + list(args or []) + ['/out/cephadm']
    run_container_cmd(ctr['name'], cmd, ceph_dir, out_dir)


@pytest.fixture
def source_dir():
    return pathlib.Path(__file__).parents[4].absolute()


@pytest.mark.parametrize(
    'env',
    [
        'centos-8',
        'centos-9',
        'ubuntu-20.04',
        'ubuntu-22.04',
    ],
)
def test_cephadm_build(env, source_dir, tmp_path):
    build_in(env, source_dir, tmp_path, [])
    assert (tmp_path / 'cephadm').is_file()
    # TODO: verify contents of zip
