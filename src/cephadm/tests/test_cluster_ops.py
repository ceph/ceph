# type: ignore
# flake8: noqa: F811
"""Tests for cluster shutdown, start, and status operations."""

import json
import mock
import os

from .fixtures import (
    cephadm_fs,  # noqa: F401  # type: ignore[unused-import]
    with_cephadm_ctx,
)

from cephadmlib import cluster_ops
from cephadmlib import constants


SAMPLE_FSID = '00000000-0000-0000-0000-0000deadbeef'


class TestClusterStateFile:
    """Tests for cluster state file operations."""

    def test_get_cluster_state_file(self):
        """Test that state file path is correctly generated."""
        path = cluster_ops.get_cluster_state_file(SAMPLE_FSID)
        assert path == f'/var/lib/ceph/{SAMPLE_FSID}/cluster-shutdown-state.json'

    def test_save_and_load_cluster_state(self, cephadm_fs):
        """Test saving and loading cluster state."""
        state = {
            'timestamp': '2024-01-01T00:00:00Z',
            'fsid': SAMPLE_FSID,
            'admin_host': 'node-1',
            'shutdown_order': ['node-2', 'node-1'],
            'hosts': {'node-1': ['mon', 'mgr'], 'node-2': ['osd']},
            'flags_set': ['noout'],
        }

        # Create the directory
        os.makedirs(f'/var/lib/ceph/{SAMPLE_FSID}', exist_ok=True)

        # Save state
        cluster_ops.save_cluster_state(SAMPLE_FSID, state)

        # Verify file exists
        state_file = cluster_ops.get_cluster_state_file(SAMPLE_FSID)
        assert os.path.exists(state_file)

        # Load and verify state
        loaded_state = cluster_ops.load_cluster_state(SAMPLE_FSID)
        assert loaded_state == state

    def test_load_cluster_state_not_found(self, cephadm_fs):
        """Test loading state when file doesn't exist."""
        state = cluster_ops.load_cluster_state(SAMPLE_FSID)
        assert state is None

    def test_remove_cluster_state(self, cephadm_fs):
        """Test removing cluster state file."""
        # Create state file
        os.makedirs(f'/var/lib/ceph/{SAMPLE_FSID}', exist_ok=True)
        state_file = cluster_ops.get_cluster_state_file(SAMPLE_FSID)
        with open(state_file, 'w') as f:
            json.dump({'test': 'data'}, f)

        assert os.path.exists(state_file)

        # Remove state
        cluster_ops.remove_cluster_state(SAMPLE_FSID)

        assert not os.path.exists(state_file)


class TestCheckClusterHealth:
    """Tests for cluster health check."""

    def test_check_cluster_health_ok(self):
        """Test health check when cluster is healthy."""
        with with_cephadm_ctx(['--image=quay.io/ceph/ceph:latest', 'shell']) as ctx:
            ctx.fsid = SAMPLE_FSID

            # Mock run_ceph_command to return healthy status
            with mock.patch.object(cluster_ops, 'run_ceph_command') as mock_cmd:
                # Single call to ceph status --format json
                mock_cmd.return_value = {
                    'health': {'status': 'HEALTH_OK'},
                    'pgmap': {
                        'num_pgs': 100,
                        'pgs_by_state': [{'state_name': 'active+clean', 'count': 100}]
                    }
                }

                is_healthy, msg = cluster_ops.check_cluster_health(ctx)

                assert is_healthy is True
                assert 'healthy' in msg.lower()

    def test_check_cluster_health_not_ok(self):
        """Test health check when cluster is not healthy."""
        with with_cephadm_ctx(['--image=quay.io/ceph/ceph:latest', 'shell']) as ctx:
            ctx.fsid = SAMPLE_FSID

            with mock.patch.object(cluster_ops, 'run_ceph_command') as mock_cmd:
                mock_cmd.return_value = {
                    'health': {'status': 'HEALTH_WARN'},
                    'pgmap': {'num_pgs': 100, 'pgs_by_state': []}
                }

                is_healthy, msg = cluster_ops.check_cluster_health(ctx)

                assert is_healthy is False
                assert 'HEALTH_WARN' in msg

    def test_check_cluster_health_pgs_not_clean(self):
        """Test health check when PGs are not all active+clean."""
        with with_cephadm_ctx(['--image=quay.io/ceph/ceph:latest', 'shell']) as ctx:
            ctx.fsid = SAMPLE_FSID

            with mock.patch.object(cluster_ops, 'run_ceph_command') as mock_cmd:
                mock_cmd.return_value = {
                    'health': {'status': 'HEALTH_OK'},
                    'pgmap': {
                        'num_pgs': 100,
                        'pgs_by_state': [
                            {'state_name': 'active+clean', 'count': 90},
                            {'state_name': 'active+recovering', 'count': 10}
                        ]
                    }
                }

                is_healthy, msg = cluster_ops.check_cluster_health(ctx)

                assert is_healthy is False
                assert '90/100' in msg


class TestDaemonOrdering:
    """Tests for daemon and host ordering."""

    def test_daemon_shutdown_order(self):
        """Test that shutdown order has monitoring/mgmt first, core last."""
        order = cluster_ops.DAEMON_SHUTDOWN_ORDER
        # Monitoring types should come before core types
        assert order.index('prometheus') < order.index('mon')
        assert order.index('grafana') < order.index('osd')
        # Gateways should come before core types
        assert order.index('nvmeof') < order.index('mon')
        # Core types should be at the end
        assert order.index('mon') > order.index('rgw')
        assert order.index('mgr') == len(order) - 1

    def test_daemons_to_stop_first(self):
        """Test that DAEMONS_TO_STOP_FIRST contains expected daemon types."""
        expected = ['nvmeof', 'iscsi', 'nfs', 'smb', 'rgw', 'mds']
        assert cluster_ops.DAEMONS_TO_STOP_FIRST == expected

    def test_shutdown_osd_flags(self):
        """Test that SHUTDOWN_OSD_FLAGS contains expected flags."""
        assert 'noout' in cluster_ops.SHUTDOWN_OSD_FLAGS
        assert isinstance(cluster_ops.SHUTDOWN_OSD_FLAGS, list)

    def test_default_parallel_hosts(self):
        """Test that DEFAULT_PARALLEL_HOSTS is set to a reasonable value."""
        assert cluster_ops.DEFAULT_PARALLEL_HOSTS == 5
        assert isinstance(cluster_ops.DEFAULT_PARALLEL_HOSTS, int)
        assert cluster_ops.DEFAULT_PARALLEL_HOSTS > 0

    def test_get_daemon_type_priority(self):
        """Test daemon type priority for shutdown ordering."""
        # In shutdown order: rgw < osd < mon (rgw stops first, mon last)
        rgw_priority = cluster_ops.get_daemon_type_priority(['rgw'])
        osd_priority = cluster_ops.get_daemon_type_priority(['osd'])
        mon_priority = cluster_ops.get_daemon_type_priority(['mon'])

        assert rgw_priority < osd_priority
        assert osd_priority < mon_priority

    def test_order_hosts_for_shutdown(self):
        """Test host ordering for shutdown (admin host last)."""
        host_daemon_map = {
            'node-1': ['mon', 'mgr', 'osd'],  # admin
            'node-2': ['mon', 'osd', 'rgw'],
            'node-3': ['osd'],
        }
        admin_host = 'node-1'

        ordered = cluster_ops.order_hosts_for_shutdown(host_daemon_map, admin_host)

        # Admin host should be last
        assert ordered[-1] == admin_host
        # All hosts should be included
        assert set(ordered) == set(host_daemon_map.keys())


class TestRemoteSystemctl:
    """Tests for remote systemctl operations."""

    def test_remote_systemctl_local(self):
        """Test systemctl on local host."""
        with with_cephadm_ctx(['shell'], mock_cephadm_call_fn=False) as ctx:
            ctx.fsid = SAMPLE_FSID

            with mock.patch('cephadmlib.cluster_ops.call', return_value=('', '', 0)) as mock_call:
                out, err, code = cluster_ops.remote_systemctl(
                    ctx, 'node-1', 'stop', 'ceph.target', is_local=True
                )

                assert code == 0
                # Should use local systemctl command
                call_args = mock_call.call_args[0]
                cmd = call_args[1]
                assert cmd == ['systemctl', 'stop', 'ceph.target']

    def test_remote_systemctl_remote(self):
        """Test systemctl on remote host via SSH."""
        with with_cephadm_ctx(['shell'], mock_cephadm_call_fn=False) as ctx:
            ctx.fsid = SAMPLE_FSID

            with mock.patch('cephadmlib.cluster_ops.call', return_value=('', '', 0)) as mock_call:
                out, err, code = cluster_ops.remote_systemctl(
                    ctx, 'node-2', 'stop', 'ceph.target', is_local=False, ssh_key='/tmp/key'
                )

                assert code == 0
                call_args = mock_call.call_args[0]
                cmd = call_args[1]
                # Should use ssh with key
                assert 'ssh' in cmd
                assert '-i' in cmd
                assert '/tmp/key' in cmd
                assert 'root@node-2' in cmd


class TestBuildHostDaemonMap:
    """Tests for building host to daemon type mapping."""

    def test_build_host_daemon_map(self):
        """Test building host daemon map from daemon list."""
        daemons = [
            {'hostname': 'node-1', 'daemon_type': 'mon', 'daemon_id': 'node-1'},
            {'hostname': 'node-1', 'daemon_type': 'mgr', 'daemon_id': 'node-1.abc'},
            {'hostname': 'node-1', 'daemon_type': 'osd', 'daemon_id': '0'},
            {'hostname': 'node-2', 'daemon_type': 'mon', 'daemon_id': 'node-2'},
            {'hostname': 'node-2', 'daemon_type': 'osd', 'daemon_id': '1'},
            {'hostname': 'node-2', 'daemon_type': 'rgw', 'daemon_id': 'rgw.zone'},
        ]

        result = cluster_ops.build_host_daemon_map(daemons)

        assert 'node-1' in result
        assert 'node-2' in result
        assert set(result['node-1']) == {'mon', 'mgr', 'osd'}
        assert set(result['node-2']) == {'mon', 'osd', 'rgw'}


class TestGetAdminHost:
    """Tests for identifying admin host."""

    def test_get_admin_host(self):
        """Test finding admin host from host list."""
        hosts = [
            {'hostname': 'node-1', 'labels': [constants.ADMIN_LABEL, 'mon']},
            {'hostname': 'node-2', 'labels': ['mon']},
            {'hostname': 'node-3', 'labels': ['osd']},
        ]

        admin = cluster_ops.get_admin_host(hosts)
        assert admin == 'node-1'

    def test_get_admin_host_not_found(self):
        """Test when no admin host exists."""
        hosts = [
            {'hostname': 'node-1', 'labels': ['mon']},
            {'hostname': 'node-2', 'labels': ['osd']},
        ]

        admin = cluster_ops.get_admin_host(hosts)
        assert admin is None


class TestSSHKeyHandling:
    """Tests for SSH key retrieval and caching."""

    def test_get_cephadm_ssh_key_cached(self, cephadm_fs):
        """Test getting SSH key when it's already cached."""
        with with_cephadm_ctx(['shell']) as ctx:
            ctx.fsid = SAMPLE_FSID

            # Create cached key
            key_dir = f'/var/lib/ceph/{SAMPLE_FSID}'
            os.makedirs(key_dir, exist_ok=True)
            key_path = os.path.join(key_dir, '.ssh_key')
            with open(key_path, 'w') as f:
                f.write('-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----')

            result = cluster_ops.get_cephadm_ssh_key(ctx)
            assert result == key_path

    def test_get_cephadm_ssh_key_from_store(self, cephadm_fs):
        """Test retrieving SSH key from config-key store."""
        with with_cephadm_ctx(['shell']) as ctx:
            ctx.fsid = SAMPLE_FSID

            # Ensure directory exists but no cached key
            key_dir = f'/var/lib/ceph/{SAMPLE_FSID}'
            os.makedirs(key_dir, exist_ok=True)

            test_key = '-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----'

            with mock.patch.object(cluster_ops, 'run_ceph_command') as mock_cmd:
                mock_cmd.return_value = test_key

                result = cluster_ops.get_cephadm_ssh_key(ctx)

                assert result is not None
                assert os.path.exists(result)
                with open(result, 'r') as f:
                    assert f.read() == test_key

    def test_remove_cached_ssh_key(self, cephadm_fs):
        """Test removing cached SSH key."""
        # Create cached key
        key_dir = f'/var/lib/ceph/{SAMPLE_FSID}'
        os.makedirs(key_dir, exist_ok=True)
        key_path = os.path.join(key_dir, '.ssh_key')
        with open(key_path, 'w') as f:
            f.write('test key')

        assert os.path.exists(key_path)

        cluster_ops.remove_cached_ssh_key(SAMPLE_FSID)

        assert not os.path.exists(key_path)
