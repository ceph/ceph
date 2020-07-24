import pytest

from cephadm.registry import Registry


def test_parse_www_authendicate():
    r = Registry('docker.io').parse_www_authenticate('Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:ceph/ceph:pull"')
    assert r == ('https://auth.docker.io/token', {'scope': 'repository:ceph/ceph:pull', 'service': 'registry.docker.io'})


@pytest.mark.skip('todo: mock network requests')
def test_docker_io():
    tags = Registry('docker.io').get_tags('ceph/ceph')
    assert len(tags) > 1
