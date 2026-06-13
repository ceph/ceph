import json

import pytest

import smb


class _FakeToolExecer:
    """Mock tool executor for testing RGW operations."""

    def tool_exec(self, cmd: list[str]) -> tuple[int, str, str]:
        """Mock tool_exec that returns success for RGW commands."""
        # Check if this is a radosgw-admin bucket stats command
        if 'radosgw-admin' in cmd and 'bucket' in cmd and 'stats' in cmd:
            # Return mock bucket stats with owner field
            bucket_stats = json.dumps(
                {'owner': 'testuser', 'bucket': 'my-bucket', 'usage': {}}
            )
            return (0, bucket_stats, '')
        # Check if this is a radosgw-admin user info command
        if 'radosgw-admin' in cmd and 'user' in cmd and 'info' in cmd:
            # Return mock user info with credentials
            user_info = json.dumps(
                {
                    'user_id': 'testuser',
                    'keys': [
                        {
                            'access_key': 'AUTO_FETCHED_ACCESS_KEY',
                            'secret_key': 'AUTO_FETCHED_SECRET_KEY',
                        }
                    ],
                }
            )
            return (0, user_info, '')
        # Default response for other commands
        return (0, '{}', '')


def _cluster(**kwargs):
    """Helper to create cluster with default clustering setting."""
    if 'clustering' not in kwargs:
        kwargs['clustering'] = smb.enums.SMBClustering.NEVER
    return smb.resources.Cluster(**kwargs)


@pytest.fixture
def thandler():
    """Fixture for RGW tests with RGW-aware tool executor."""
    ext_store = smb.config_store.MemConfigStore()
    return smb.handler.ClusterConfigHandler(
        internal_store=smb.config_store.MemConfigStore(),
        public_store=ext_store,
        priv_store=ext_store,
        tool_execer=_FakeToolExecer(),
    )


def test_internal_apply_cluster_and_rgw_share(thandler):
    """Test creating a cluster with an RGW share."""
    cluster = _cluster(
        cluster_id='rgwtest',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    share = smb.resources.Share(
        cluster_id='rgwtest',
        share_id='rgwshare1',
        name='RGW Share One',
        rgw=smb.resources.RGWStorage(
            bucket='my-bucket',
            user_id='testuser',
            access_key_id='AKIAIOSFODNN7EXAMPLE',
            secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        ),
    )
    rg = thandler.apply([cluster, share])
    assert rg.success, rg.to_simplified()
    assert ('clusters', 'rgwtest') in thandler.internal_store.data
    assert ('shares', 'rgwtest.rgwshare1') in thandler.internal_store.data

    shares = thandler.share_ids()
    assert len(shares) == 1
    assert ('rgwtest', 'rgwshare1') in shares


def test_apply_rgw_share_with_credentials(thandler):
    """Test applying an RGW share with full credentials."""
    cluster = _cluster(
        cluster_id='rgwcluster',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    share = smb.resources.Share(
        cluster_id='rgwcluster',
        share_id='s3share',
        name='S3 Share',
        rgw=smb.resources.RGWStorage(
            bucket='test-bucket',
            user_id='rgwuser',
            access_key_id='AKIAIOSFODNN7EXAMPLE',
            secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        ),
    )
    rg = thandler.apply([cluster, share])
    assert rg.success, rg.to_simplified()

    # Verify RGW storage configuration was stored
    share_dict = thandler.internal_store.data[
        ('shares', 'rgwcluster.s3share')
    ]
    assert share_dict['rgw']['bucket'] == 'test-bucket'
    assert share_dict['rgw']['user_id'] == 'rgwuser'
    assert share_dict['rgw']['access_key_id'] == 'AKIAIOSFODNN7EXAMPLE'
    assert (
        share_dict['rgw']['secret_access_key']
        == 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    )


def test_rgw_share_auto_fetch_credentials(thandler):
    """Test RGW share with auto-fetched credentials (bucket only provided)."""
    cluster = _cluster(
        cluster_id='autofetch',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    share = smb.resources.Share(
        cluster_id='autofetch',
        share_id='autoshare',
        name='Auto Fetch Share',
        rgw=smb.resources.RGWStorage(
            bucket='my-bucket',
        ),
    )
    rg = thandler.apply([cluster, share])
    assert rg.success, rg.to_simplified()

    # Verify credentials were auto-fetched and stored
    share_dict = thandler.internal_store.data[
        ('shares', 'autofetch.autoshare')
    ]
    assert share_dict['rgw']['bucket'] == 'my-bucket'
    assert share_dict['rgw']['user_id'] == 'testuser'
    assert share_dict['rgw']['access_key_id'] == 'AUTO_FETCHED_ACCESS_KEY'
    assert share_dict['rgw']['secret_access_key'] == 'AUTO_FETCHED_SECRET_KEY'


def test_rgw_share_remove(thandler):
    """Test removing an RGW share."""
    # First create a cluster and RGW share
    cluster = _cluster(
        cluster_id='rgwtest',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    share = smb.resources.Share(
        cluster_id='rgwtest',
        share_id='rgwshare1',
        name='RGW Share One',
        rgw=smb.resources.RGWStorage(
            bucket='my-bucket',
            user_id='testuser',
            access_key_id='AKIAIOSFODNN7EXAMPLE',
            secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        ),
    )
    rg = thandler.apply([cluster, share])
    assert rg.success, rg.to_simplified()

    # Now remove the share
    rmshare = smb.resources.RemovedShare(
        cluster_id='rgwtest',
        share_id='rgwshare1',
    )
    rg = thandler.apply([rmshare])
    assert rg.success, rg.to_simplified()

    shares = thandler.share_ids()
    assert len(shares) == 0
    assert ('shares', 'rgwtest.rgwshare1') not in thandler.internal_store.data


def test_rgw_share_minimal_config(thandler):
    """Test RGW share with minimal configuration (bucket only)."""
    cluster = _cluster(
        cluster_id='minimalrgw',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    share = smb.resources.Share(
        cluster_id='minimalrgw',
        share_id='minimal',
        name='Minimal RGW Share',
        rgw=smb.resources.RGWStorage(
            bucket='my-bucket',
        ),
    )
    rg = thandler.apply([cluster, share])
    assert rg.success, rg.to_simplified()

    # Verify credentials were auto-fetched
    share_dict = thandler.internal_store.data[
        ('shares', 'minimalrgw.minimal')
    ]
    assert share_dict['rgw']['bucket'] == 'my-bucket'
    assert share_dict['rgw']['user_id'] == 'testuser'
    assert share_dict['rgw']['access_key_id'] == 'AUTO_FETCHED_ACCESS_KEY'
    assert share_dict['rgw']['secret_access_key'] == 'AUTO_FETCHED_SECRET_KEY'


def test_multiple_rgw_shares_same_cluster(thandler):
    """Test creating multiple RGW shares in the same cluster."""
    cluster = _cluster(
        cluster_id='multirgw',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    share1 = smb.resources.Share(
        cluster_id='multirgw',
        share_id='share1',
        name='RGW Share 1',
        rgw=smb.resources.RGWStorage(
            bucket='bucket1',
            user_id='user1',
        ),
    )
    share2 = smb.resources.Share(
        cluster_id='multirgw',
        share_id='share2',
        name='RGW Share 2',
        rgw=smb.resources.RGWStorage(
            bucket='bucket2',
            user_id='user2',
        ),
    )
    rg = thandler.apply([cluster, share1, share2])
    assert rg.success, rg.to_simplified()

    shares = thandler.share_ids()
    assert len(shares) == 2
    assert ('multirgw', 'share1') in shares
    assert ('multirgw', 'share2') in shares

    # Verify both shares are stored correctly with auto-fetched credentials
    share1_dict = thandler.internal_store.data[('shares', 'multirgw.share1')]
    share2_dict = thandler.internal_store.data[('shares', 'multirgw.share2')]
    assert share1_dict['rgw']['bucket'] == 'bucket1'
    assert share2_dict['rgw']['bucket'] == 'bucket2'
    # Verify credentials were auto-fetched
    assert share1_dict['rgw']['access_key_id'] == 'AUTO_FETCHED_ACCESS_KEY'
    assert share2_dict['rgw']['access_key_id'] == 'AUTO_FETCHED_ACCESS_KEY'
