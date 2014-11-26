import argparse

from mock import patch
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


class TestHostnames(object):
    def setup(self):
        self.old_lab_domain = config.lab_domain

    def teardown(self):
        config.lab_domain = self.old_lab_domain

    def test_canonicalize_hostname(self):
        host_base = 'box1'
        result = misc.canonicalize_hostname(host_base)
        assert result == 'ubuntu@box1.front.sepia.ceph.com'

    def test_decanonicalize_hostname(self):
        host = 'ubuntu@box1.front.sepia.ceph.com'
        result = misc.decanonicalize_hostname(host)
        assert result == 'box1'

    def test_canonicalize_hostname_nouser(self):
        host_base = 'box1'
        result = misc.canonicalize_hostname(host_base, user=None)
        assert result == 'box1.front.sepia.ceph.com'

    def test_decanonicalize_hostname_nouser(self):
        host = 'box1.front.sepia.ceph.com'
        result = misc.decanonicalize_hostname(host)
        assert result == 'box1'

    def test_canonicalize_hostname_otherlab(self):
        config.lab_domain = 'example.com'
        host_base = 'box1'
        result = misc.canonicalize_hostname(host_base)
        assert result == 'ubuntu@box1.example.com'

    def test_decanonicalize_hostname_otherlab(self):
        config.lab_domain = 'example.com'
        host = 'ubuntu@box1.example.com'
        result = misc.decanonicalize_hostname(host)
        assert result == 'box1'


class TestMergeConfigs(object):
    """ Tests merge_config and deep_merge in teuthology.misc """

    @patch("os.path.exists")
    @patch("yaml.safe_load")
    @patch("__builtin__.file")
    def test_merge_configs(self, m_file, m_safe_load, m_exists):
        """ Only tests with one yaml file being passed, mainly just to test
            the loop logic.  The actual merge will be tested in subsequent
            tests.
        """
        expected = {"a": "b", "b": "c"}
        m_exists.return_value = True
        m_safe_load.return_value = expected
        result = misc.merge_configs(["path/to/config1"])
        assert result == expected
        m_file.assert_called_once_with("path/to/config1")

    def test_merge_configs_empty(self):
        assert misc.merge_configs([]) == {}

    def test_deep_merge(self):
        a = {"a": "b"}
        b = {"b": "c"}
        result = misc.deep_merge(a, b)
        assert result == {"a": "b", "b": "c"}

    def test_overwrite_deep_merge(self):
        a = {"a": "b"}
        b = {"a": "overwritten", "b": "c"}
        result = misc.deep_merge(a, b)
        assert result == {"a": "overwritten", "b": "c"}

    def test_list_deep_merge(self):
        a = [1, 2]
        b = [3, 4]
        result = misc.deep_merge(a, b)
        assert result == [1, 2, 3, 4]

    def test_missing_list_deep_merge(self):
        a = [1, 2]
        b = "not a list"
        with pytest.raises(AssertionError):
            misc.deep_merge(a, b)

    def test_missing_a_deep_merge(self):
        result = misc.deep_merge(None, [1, 2])
        assert result == [1, 2]

    def test_missing_b_deep_merge(self):
        result = misc.deep_merge([1, 2], None)
        assert result == [1, 2]

    def test_invalid_b_deep_merge(self):
        with pytest.raises(AssertionError):
            misc.deep_merge({"a": "b"}, "invalid")
