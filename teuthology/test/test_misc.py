import argparse
from ..orchestra import cluster
from .. import misc
from ..config import config

import pytest


class FakeRemote(object):
    pass


def test_get_clients_simple():
    ctx = argparse.Namespace()
    remote = FakeRemote()
    ctx.cluster = cluster.Cluster(
        remotes=[
            (remote, ['client.0', 'client.1'])
            ],
        )
    g = misc.get_clients(ctx=ctx, roles=['client.1'])
    got = next(g)
    assert len(got) == 2
    assert got[0] == ('1')
    assert got[1] is remote
    with pytest.raises(StopIteration):
        next(g)


def test_get_http_log_path():
    # Fake configuration
    archive_server = "http://example.com/server_root"
    config.archive_server = archive_server
    archive_dir = "/var/www/archives"

    path = misc.get_http_log_path(archive_dir)
    assert path == "http://example.com/server_root/archives/"

    job_id = '12345'
    path = misc.get_http_log_path(archive_dir, job_id)
    assert path == "http://example.com/server_root/archives/12345/"

    # Inktank configuration
    archive_server = "http://qa-proxy.ceph.com/teuthology/"
    config.archive_server = archive_server
    archive_dir = "/var/lib/teuthworker/archive/teuthology-2013-09-12_11:49:50-ceph-deploy-master-testing-basic-vps"
    job_id = 31087
    path = misc.get_http_log_path(archive_dir, job_id)
    assert path == "http://qa-proxy.ceph.com/teuthology/teuthology-2013-09-12_11:49:50-ceph-deploy-master-testing-basic-vps/31087/"

    path = misc.get_http_log_path(archive_dir)
    assert path == "http://qa-proxy.ceph.com/teuthology/teuthology-2013-09-12_11:49:50-ceph-deploy-master-testing-basic-vps/"
