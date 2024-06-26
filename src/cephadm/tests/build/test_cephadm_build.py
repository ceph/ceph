# tests for building cephadm into a zipapp using build.py
#
# these should not be run automatically as they require the use of podman,
# which should not be assumed to exist on a typical test node

import json
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
    'centos-8-plusdeps': {
        'name': 'cephadm-build-test:centos8-py36-deps',
        'base_image': 'quay.io/centos/centos:stream8',
        'script': 'dnf install -y python36 python3-jinja2 python3-pyyaml',
    },
    'centos-9-plusdeps': {
        'name': 'cephadm-build-test:centos9-py3-deps',
        'base_image': 'quay.io/centos/centos:stream9',
        'script': 'dnf install -y python3 python3-jinja2 python3-pyyaml',
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
    return run_container_cmd(ctr['name'], cmd, ceph_dir, out_dir)


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
    binary = tmp_path / 'cephadm'
    assert binary.is_file()
    res = subprocess.run(
        [sys.executable, str(binary), 'version'],
        stdout=subprocess.PIPE,
    )
    out = res.stdout.decode('utf8')
    assert 'version' in out
    assert 'UNKNOWN' in out
    assert res.returncode != 0
    res = subprocess.run(
        [sys.executable, str(binary), 'version', '--verbose'],
        stdout=subprocess.PIPE,
    )
    data = json.loads(res.stdout)
    assert isinstance(data, dict)
    assert 'bundled_packages' in data
    assert all(v['package_source'] == 'pip' for v in data['bundled_packages'])
    assert all(
        v['name'] in ('Jinja2', 'MarkupSafe', 'PyYAML')
        for v in data['bundled_packages']
    )
    assert all('requirements_entry' in v for v in data['bundled_packages'])
    assert 'zip_root_entries' in data
    zre = data['zip_root_entries']
    assert any(e.startswith('Jinja2') for e in zre)
    assert any(e.startswith('MarkupSafe') for e in zre)
    assert any(e.startswith('jinja2') for e in zre)
    assert any(e.startswith('markupsafe') for e in zre)
    assert any(e.startswith('cephadmlib') for e in zre)
    assert any(e.startswith('_cephadmmeta') for e in zre)


@pytest.mark.parametrize(
    'env',
    [
        'centos-8-plusdeps',
        'centos-9-plusdeps',
        'centos-9',
    ],
)
def test_cephadm_build_from_rpms(env, source_dir, tmp_path):
    res = build_in(
        env,
        source_dir,
        tmp_path,
        ['-Brpm', '-SCEPH_GIT_VER=0', '-SCEPH_GIT_NICE_VER=foobar'],
    )
    if 'plusdeps' not in env:
        assert res.returncode != 0
        return
    binary = tmp_path / 'cephadm'
    if 'centos-8' in env and sys.version_info[:2] >= (3, 10):
        # The version of markupsafe in centos 8 is incompatible with
        # python>=3.10 due to changes in the stdlib therefore we can't execute
        # the cephadm binary, so we quit the test early.
        return
    assert binary.is_file()
    res = subprocess.run(
        [sys.executable, str(binary), 'version'],
        stdout=subprocess.PIPE,
    )
    out = res.stdout.decode('utf8')
    assert 'version' in out
    assert 'foobar' in out
    assert res.returncode == 0
    res = subprocess.run(
        [sys.executable, str(binary), 'version', '--verbose'],
        stdout=subprocess.PIPE,
    )
    data = json.loads(res.stdout)
    assert isinstance(data, dict)
    assert 'bundled_packages' in data
    assert all(v['package_source'] == 'rpm' for v in data['bundled_packages'])
    assert all(
        v['name'] in ('Jinja2', 'MarkupSafe', 'PyYAML')
        for v in data['bundled_packages']
    )
    assert all('requirements_entry' in v for v in data['bundled_packages'])
    assert 'zip_root_entries' in data
    zre = data['zip_root_entries']
    assert any(e.startswith('Jinja2') for e in zre)
    assert any(e.startswith('MarkupSafe') for e in zre)
    assert any(e.startswith('jinja2') for e in zre)
    assert any(e.startswith('markupsafe') for e in zre)
    assert any(e.startswith('cephadmlib') for e in zre)
    assert any(e.startswith('_cephadmmeta') for e in zre)
