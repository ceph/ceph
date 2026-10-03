import json

import pytest

import smb
import smb.external


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
        mon_cmd_issuer=None,
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
        ),
    )
    rg = thandler.apply([cluster, share])
    assert rg.success, rg.to_simplified()
    assert ('clusters', 'rgwtest') in thandler.internal_store.data
    assert ('shares', 'rgwtest.rgwshare1') in thandler.internal_store.data
    # Check that credential was auto-created
    assert ('rgw_creds', 'testuser') in thandler.internal_store.data

    shares = thandler.share_ids()
    assert len(shares) == 1
    assert ('rgwtest', 'rgwshare1') in shares

    # Verify share uses credential_ref
    share_data = thandler.internal_store.data[('shares', 'rgwtest.rgwshare1')]
    assert share_data['rgw']['credential_ref'] == 'testuser'


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
        ),
    )
    rg = thandler.apply([cluster, share])
    assert rg.success, rg.to_simplified()

    # Verify credential was auto-created with fetched credentials
    assert ('rgw_creds', 'rgwuser') in thandler.internal_store.data
    cred_dict = thandler.internal_store.data[('rgw_creds', 'rgwuser')]
    assert cred_dict['user_id'] == 'rgwuser'
    assert cred_dict['access_key_id'] == 'AUTO_FETCHED_ACCESS_KEY'
    assert cred_dict['secret_access_key'] == 'AUTO_FETCHED_SECRET_KEY'

    # Verify share uses credential_ref
    share_dict = thandler.internal_store.data[
        ('shares', 'rgwcluster.s3share')
    ]
    assert share_dict['rgw']['bucket'] == 'test-bucket'
    assert share_dict['rgw']['credential_ref'] == 'rgwuser'


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

    # Verify credential was auto-created with fetched credentials
    assert ('rgw_creds', 'testuser') in thandler.internal_store.data
    cred_dict = thandler.internal_store.data[('rgw_creds', 'testuser')]
    assert cred_dict['user_id'] == 'testuser'
    assert cred_dict['access_key_id'] == 'AUTO_FETCHED_ACCESS_KEY'
    assert cred_dict['secret_access_key'] == 'AUTO_FETCHED_SECRET_KEY'

    # Verify share uses credential_ref
    share_dict = thandler.internal_store.data[
        ('shares', 'autofetch.autoshare')
    ]
    assert share_dict['rgw']['bucket'] == 'my-bucket'
    assert share_dict['rgw']['credential_ref'] == 'testuser'


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

    # Verify credential was auto-created with fetched credentials
    assert ('rgw_creds', 'testuser') in thandler.internal_store.data
    cred_dict = thandler.internal_store.data[('rgw_creds', 'testuser')]
    assert cred_dict['user_id'] == 'testuser'
    assert cred_dict['access_key_id'] == 'AUTO_FETCHED_ACCESS_KEY'
    assert cred_dict['secret_access_key'] == 'AUTO_FETCHED_SECRET_KEY'

    # Verify share uses credential_ref
    share_dict = thandler.internal_store.data[
        ('shares', 'minimalrgw.minimal')
    ]
    assert share_dict['rgw']['bucket'] == 'my-bucket'
    assert share_dict['rgw']['credential_ref'] == 'testuser'


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

    # Verify credentials were auto-created for both users
    assert ('rgw_creds', 'user1') in thandler.internal_store.data
    assert ('rgw_creds', 'user2') in thandler.internal_store.data

    # Verify both shares use credential_ref
    share1_dict = thandler.internal_store.data[('shares', 'multirgw.share1')]
    share2_dict = thandler.internal_store.data[('shares', 'multirgw.share2')]
    assert share1_dict['rgw']['bucket'] == 'bucket1'
    assert share1_dict['rgw']['credential_ref'] == 'user1'
    assert share2_dict['rgw']['bucket'] == 'bucket2'
    assert share2_dict['rgw']['credential_ref'] == 'user2'


def test_rgw_credential_reuse_same_user(thandler):
    """Test that multiple shares with same user_id reuse the same credential."""
    cluster = _cluster(
        cluster_id='reusetest',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    share1 = smb.resources.Share(
        cluster_id='reusetest',
        share_id='share1',
        name='RGW Share 1',
        rgw=smb.resources.RGWStorage(
            bucket='bucket1',
            user_id='shareduser',
        ),
    )
    share2 = smb.resources.Share(
        cluster_id='reusetest',
        share_id='share2',
        name='RGW Share 2',
        rgw=smb.resources.RGWStorage(
            bucket='bucket2',
            user_id='shareduser',
        ),
    )
    rg = thandler.apply([cluster, share1, share2])
    assert rg.success, rg.to_simplified()

    # Verify only ONE credential was created for the shared user
    rgw_creds = [
        k for k in thandler.internal_store.data.keys() if k[0] == 'rgw_creds'
    ]
    assert len(rgw_creds) == 1
    assert ('rgw_creds', 'shareduser') in thandler.internal_store.data

    # Verify both shares reference the same credential
    share1_dict = thandler.internal_store.data[('shares', 'reusetest.share1')]
    share2_dict = thandler.internal_store.data[('shares', 'reusetest.share2')]
    assert share1_dict['rgw']['credential_ref'] == 'shareduser'
    assert share2_dict['rgw']['credential_ref'] == 'shareduser'


# ---- priv-store credential isolation tests ----
# These tests use SEPARATE public and private stores so each can be
# independently inspected.  The real credential values must
# only ever appear in the private store (via a config:merge stub), never in
# the public RADOS config.


@pytest.fixture
def split_handler():
    """Handler with separate public and private stores."""
    pub = smb.config_store.MemConfigStore()
    priv = smb.config_store.MemConfigStore()
    h = smb.handler.ClusterConfigHandler(
        internal_store=smb.config_store.MemConfigStore(),
        public_store=pub,
        priv_store=priv,
        tool_execer=_FakeToolExecer(),
    )
    return h, pub, priv


def _rgw_cluster_and_share(cluster_id, share_id, share_name, **rgw_kwargs):
    cluster = _cluster(
        cluster_id=cluster_id,
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    share = smb.resources.Share(
        cluster_id=cluster_id,
        share_id=share_id,
        name=share_name,
        rgw=smb.resources.RGWStorage(**rgw_kwargs),
    )
    return cluster, share


def test_rgw_credentials_absent_from_public_config(split_handler):
    """Credential values must be empty strings in the public RADOS config."""
    h, pub, priv = split_handler
    cluster, share = _rgw_cluster_and_share(
        'c1',
        's1',
        'mybucket',
        bucket='mybucket',
        user_id='testuser',
    )
    rg = h.apply([cluster, share])
    assert rg.success, rg.to_simplified()

    # Verify credential was auto-created
    assert ('rgw_creds', 'testuser') in h.internal_store.data

    pub_cfg = pub['c1', 'config.smb'].get()
    opts = pub_cfg['shares']['mybucket']['options']
    assert opts['ceph_rgw:access_key'] == ''
    assert opts['ceph_rgw:secret_access_key'] == ''


def test_rgw_credential_stub_written_to_priv_store(split_handler):
    """Private store must hold config:merge stub with real credential values."""
    h, pub, priv = split_handler
    cluster, share = _rgw_cluster_and_share(
        'c1',
        's1',
        'mybucket',
        bucket='mybucket',
        user_id='testuser',
    )
    rg = h.apply([cluster, share])
    assert rg.success, rg.to_simplified()

    # Verify credential was auto-created with fetched credentials
    assert ('rgw_creds', 'testuser') in h.internal_store.data
    cred_dict = h.internal_store.data[('rgw_creds', 'testuser')]
    assert cred_dict['access_key_id'] == 'AUTO_FETCHED_ACCESS_KEY'
    assert cred_dict['secret_access_key'] == 'AUTO_FETCHED_SECRET_KEY'

    stub = priv['c1', 'config.smb.rgw'].get()
    assert stub['samba-container-config'] == 'v0'
    assert 'config:merge' in stub
    opts = stub['config:merge']['shares']['mybucket']['options']
    assert opts['ceph_rgw:access_key'] == 'AUTO_FETCHED_ACCESS_KEY'
    assert opts['ceph_rgw:secret_access_key'] == 'AUTO_FETCHED_SECRET_KEY'


def test_rgw_credential_stub_survives_last_share_removal(split_handler):
    """Deleting the last RGW share in a cluster must not prune the
    credential stub from the private store.
    """
    h, pub, priv = split_handler
    cluster, share = _rgw_cluster_and_share(
        'c1',
        's1',
        'mybucket',
        bucket='mybucket',
        user_id='testuser',
    )
    rg = h.apply([cluster, share])
    assert rg.success, rg.to_simplified()
    assert priv['c1', 'config.smb.rgw'].exists()

    rmshare = smb.resources.RemovedShare(cluster_id='c1', share_id='s1')
    rg = h.apply([rmshare])
    assert rg.success, rg.to_simplified()

    stub_entry = priv['c1', 'config.smb.rgw']
    assert stub_entry.exists()
    assert stub_entry.get()['config:merge']['shares'] == {}


def test_no_priv_store_entry_for_non_rgw_cluster(split_handler):
    """A cluster with no RGW shares must not write a credential stub."""
    h, pub, priv = split_handler
    cluster = _cluster(
        cluster_id='cephfs1',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    share = smb.resources.Share(
        cluster_id='cephfs1',
        share_id='fsshare',
        name='FS Share',
        cephfs=smb.resources.CephFSStorage(
            volume='cephfs',
            path='/',
        ),
    )
    rg = h.apply([cluster, share])
    assert rg.success, rg.to_simplified()

    stub_entry = priv['cephfs1', 'config.smb.rgw']
    assert not stub_entry.exists()


def test_multi_share_stub_covers_all_rgw_shares(split_handler):
    """Credential stub must contain entries for every RGW share."""
    h, pub, priv = split_handler
    cluster = _cluster(
        cluster_id='multi',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    share1 = smb.resources.Share(
        cluster_id='multi',
        share_id='s1',
        name='bucket1',
        rgw=smb.resources.RGWStorage(
            bucket='bucket1',
            user_id='u1',
        ),
    )
    share2 = smb.resources.Share(
        cluster_id='multi',
        share_id='s2',
        name='bucket2',
        rgw=smb.resources.RGWStorage(
            bucket='bucket2',
            user_id='u2',
        ),
    )
    rg = h.apply([cluster, share1, share2])
    assert rg.success, rg.to_simplified()

    # Verify credentials were auto-created for both users with fetched credentials
    assert ('rgw_creds', 'u1') in h.internal_store.data
    assert ('rgw_creds', 'u2') in h.internal_store.data
    cred1 = h.internal_store.data[('rgw_creds', 'u1')]
    cred2 = h.internal_store.data[('rgw_creds', 'u2')]
    assert cred1['access_key_id'] == 'AUTO_FETCHED_ACCESS_KEY'
    assert cred1['secret_access_key'] == 'AUTO_FETCHED_SECRET_KEY'
    assert cred2['access_key_id'] == 'AUTO_FETCHED_ACCESS_KEY'
    assert cred2['secret_access_key'] == 'AUTO_FETCHED_SECRET_KEY'

    stub = priv['multi', 'config.smb.rgw'].get()
    merge_shares = stub['config:merge']['shares']
    b1opts = merge_shares['bucket1']['options']
    b2opts = merge_shares['bucket2']['options']
    assert b1opts['ceph_rgw:access_key'] == 'AUTO_FETCHED_ACCESS_KEY'
    assert b1opts['ceph_rgw:secret_access_key'] == 'AUTO_FETCHED_SECRET_KEY'
    assert b2opts['ceph_rgw:access_key'] == 'AUTO_FETCHED_ACCESS_KEY'
    assert b2opts['ceph_rgw:secret_access_key'] == 'AUTO_FETCHED_SECRET_KEY'


# ---- External Cluster RGW Share Tests ----


def test_external_cluster_rgw_only(thandler):
    """Test external cluster with RGW user only (no CephFS user)."""
    ext_cluster = smb.resources.ExternalCephCluster(
        external_ceph_cluster_id='exo-rgw',
        cluster=smb.resources.ExternalCephClusterValues(
            fsid='12345678-1234-1234-1234-123456789abc',
            mon_host='10.0.1.10:6789',
            rgw_user=smb.resources.CephUserKey(
                name='client.rgw.exo',
                key='AQExternalRGWKey==',
            ),
        ),
    )
    cluster = _cluster(
        cluster_id='exo-rgw',
        auth_mode=smb.enums.AuthMode.USER,
        external_ceph_cluster=smb.resources.ExternalCephClusterSource(
            ref='exo-rgw',
        ),
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    # Add RGW credential (required for external clusters)
    credential = smb.resources.RGWCredential(
        rgw_credential_id='exo-user',
        user_id='exo-user',
        access_key_id='EXTERNAL_ACCESS_KEY',
        secret_access_key='EXTERNAL_SECRET_KEY',
    )
    share = smb.resources.Share(
        cluster_id='exo-rgw',
        share_id='exo-bucket',
        name='External Bucket',
        rgw=smb.resources.RGWStorage(
            bucket='external-bucket',
            user_id='exo-user',
            credential_ref='exo-user',
        ),
    )

    rg = thandler.apply([ext_cluster, cluster, credential, share])
    assert rg.success, rg.to_simplified()

    # Verify external cluster was stored
    assert ('ext_ceph_clusters', 'exo-rgw') in thandler.internal_store.data
    ext_data = thandler.internal_store.data[('ext_ceph_clusters', 'exo-rgw')]
    assert (
        ext_data['cluster']['fsid'] == '12345678-1234-1234-1234-123456789abc'
    )
    assert ext_data['cluster']['rgw_user']['name'] == 'client.rgw.exo'
    assert 'cephfs_user' not in ext_data['cluster']

    # Verify share was created
    assert ('shares', 'exo-rgw.exo-bucket') in thandler.internal_store.data

    # Verify RGW credential was auto-created
    assert ('rgw_creds', 'exo-user') in thandler.internal_store.data


def test_external_cluster_mixed_users(thandler):
    """Test external cluster with both CephFS and RGW users."""
    ext_cluster = smb.resources.ExternalCephCluster(
        external_ceph_cluster_id='exo-mixed',
        cluster=smb.resources.ExternalCephClusterValues(
            fsid='12345678-1234-1234-1234-123456789abc',
            mon_host='10.0.1.10:6789',
            cephfs_user=smb.resources.CephUserKey(
                name='client.fs.exo',
                key='AQExternalFSKey==',
            ),
            rgw_user=smb.resources.CephUserKey(
                name='client.rgw.exo',
                key='AQExternalRGWKey==',
            ),
        ),
    )
    cluster = _cluster(
        cluster_id='exo-mixed',
        auth_mode=smb.enums.AuthMode.USER,
        external_ceph_cluster=smb.resources.ExternalCephClusterSource(
            ref='exo-mixed',
        ),
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    cephfs_share = smb.resources.Share(
        cluster_id='exo-mixed',
        share_id='fs-share',
        name='FS Share',
        cephfs=smb.resources.CephFSStorage(
            volume='cephfs',
            path='/data',
        ),
    )
    # Add RGW credential (required for external clusters)
    credential = smb.resources.RGWCredential(
        rgw_credential_id='mixed-user',
        user_id='mixed-user',
        access_key_id='EXTERNAL_ACCESS_KEY',
        secret_access_key='EXTERNAL_SECRET_KEY',
    )
    rgw_share = smb.resources.Share(
        cluster_id='exo-mixed',
        share_id='rgw-share',
        name='RGW Share',
        rgw=smb.resources.RGWStorage(
            bucket='mixed-bucket',
            user_id='mixed-user',
            credential_ref='mixed-user',
        ),
    )

    rg = thandler.apply(
        [ext_cluster, cluster, cephfs_share, credential, rgw_share]
    )
    assert rg.success, rg.to_simplified()

    # Verify external cluster has both users
    ext_data = thandler.internal_store.data[
        ('ext_ceph_clusters', 'exo-mixed')
    ]
    assert ext_data['cluster']['cephfs_user']['name'] == 'client.fs.exo'
    assert ext_data['cluster']['rgw_user']['name'] == 'client.rgw.exo'

    # Verify both shares were created
    assert ('shares', 'exo-mixed.fs-share') in thandler.internal_store.data
    assert ('shares', 'exo-mixed.rgw-share') in thandler.internal_store.data

    # Verify RGW credential was auto-created
    assert ('rgw_creds', 'mixed-user') in thandler.internal_store.data


def test_external_cluster_multiple_rgw_shares(thandler):
    """Test multiple RGW shares on external cluster."""
    ext_cluster = smb.resources.ExternalCephCluster(
        external_ceph_cluster_id='exo-multi',
        cluster=smb.resources.ExternalCephClusterValues(
            fsid='12345678-1234-1234-1234-123456789abc',
            mon_host='10.0.1.10:6789',
            rgw_user=smb.resources.CephUserKey(
                name='client.rgw.exo',
                key='AQExternalRGWKey==',
            ),
        ),
    )
    cluster = _cluster(
        cluster_id='exo-multi',
        auth_mode=smb.enums.AuthMode.USER,
        external_ceph_cluster=smb.resources.ExternalCephClusterSource(
            ref='exo-multi',
        ),
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    # Add RGW credentials (required for external clusters)
    credential1 = smb.resources.RGWCredential(
        rgw_credential_id='exo-user-1',
        user_id='exo-user-1',
        access_key_id='EXTERNAL_ACCESS_KEY_1',
        secret_access_key='EXTERNAL_SECRET_KEY_1',
    )
    credential2 = smb.resources.RGWCredential(
        rgw_credential_id='exo-user-2',
        user_id='exo-user-2',
        access_key_id='EXTERNAL_ACCESS_KEY_2',
        secret_access_key='EXTERNAL_SECRET_KEY_2',
    )
    share1 = smb.resources.Share(
        cluster_id='exo-multi',
        share_id='bucket1',
        name='Bucket 1',
        rgw=smb.resources.RGWStorage(
            bucket='exo-bucket-1',
            user_id='exo-user-1',
            credential_ref='exo-user-1',
        ),
    )
    share2 = smb.resources.Share(
        cluster_id='exo-multi',
        share_id='bucket2',
        name='Bucket 2',
        rgw=smb.resources.RGWStorage(
            bucket='exo-bucket-2',
            user_id='exo-user-2',
            credential_ref='exo-user-2',
        ),
    )

    rg = thandler.apply(
        [ext_cluster, cluster, credential1, credential2, share1, share2]
    )
    assert rg.success, rg.to_simplified()

    # Verify both shares were created
    shares = thandler.share_ids()
    assert len(shares) == 2
    assert ('exo-multi', 'bucket1') in shares
    assert ('exo-multi', 'bucket2') in shares

    # Verify credentials were auto-created for both users
    assert ('rgw_creds', 'exo-user-1') in thandler.internal_store.data
    assert ('rgw_creds', 'exo-user-2') in thandler.internal_store.data


def test_external_cluster_rgw_credential_isolation(split_handler):
    """Test RGW credentials on external cluster are properly isolated."""
    h, pub, priv = split_handler

    ext_cluster = smb.resources.ExternalCephCluster(
        external_ceph_cluster_id='exo-iso',
        cluster=smb.resources.ExternalCephClusterValues(
            fsid='12345678-1234-1234-1234-123456789abc',
            mon_host='10.0.1.10:6789',
            rgw_user=smb.resources.CephUserKey(
                name='client.rgw.exo',
                key='AQExternalRGWKey==',
            ),
        ),
    )
    cluster = _cluster(
        cluster_id='exo-iso',
        auth_mode=smb.enums.AuthMode.USER,
        external_ceph_cluster=smb.resources.ExternalCephClusterSource(
            ref='exo-iso',
        ),
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    # Add RGW credential (required for external clusters)
    credential = smb.resources.RGWCredential(
        rgw_credential_id='iso-user',
        user_id='iso-user',
        access_key_id='EXTERNAL_ACCESS_KEY',
        secret_access_key='EXTERNAL_SECRET_KEY',
    )
    share = smb.resources.Share(
        cluster_id='exo-iso',
        share_id='iso-bucket',
        name='Isolated Bucket',
        rgw=smb.resources.RGWStorage(
            bucket='iso-bucket',
            user_id='iso-user',
            credential_ref='iso-user',
        ),
    )

    rg = h.apply([ext_cluster, cluster, credential, share])
    assert rg.success, rg.to_simplified()

    # Verify credential was auto-created
    assert ('rgw_creds', 'iso-user') in h.internal_store.data

    # Verify public config has empty credential values
    pub_cfg = pub['exo-iso', 'config.smb'].get()
    opts = pub_cfg['shares']['Isolated Bucket']['options']
    assert opts['ceph_rgw:access_key'] == ''
    assert opts['ceph_rgw:secret_access_key'] == ''

    # Verify private store has real credential values
    stub = priv['exo-iso', 'config.smb.rgw'].get()
    merge_opts = stub['config:merge']['shares']['Isolated Bucket']['options']
    assert merge_opts['ceph_rgw:access_key'] == 'EXTERNAL_ACCESS_KEY'
    assert merge_opts['ceph_rgw:secret_access_key'] == 'EXTERNAL_SECRET_KEY'


def test_external_cluster_rgw_share_removal(thandler):
    """Test removing RGW share from external cluster."""
    ext_cluster = smb.resources.ExternalCephCluster(
        external_ceph_cluster_id='exo-rm',
        cluster=smb.resources.ExternalCephClusterValues(
            fsid='12345678-1234-1234-1234-123456789abc',
            mon_host='10.0.1.10:6789',
            rgw_user=smb.resources.CephUserKey(
                name='client.rgw.exo',
                key='AQExternalRGWKey==',
            ),
        ),
    )
    cluster = _cluster(
        cluster_id='exo-rm',
        auth_mode=smb.enums.AuthMode.USER,
        external_ceph_cluster=smb.resources.ExternalCephClusterSource(
            ref='exo-rm',
        ),
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    # Add RGW credential (required for external clusters)
    credential = smb.resources.RGWCredential(
        rgw_credential_id='rm-user',
        user_id='rm-user',
        access_key_id='EXTERNAL_ACCESS_KEY',
        secret_access_key='EXTERNAL_SECRET_KEY',
    )
    share = smb.resources.Share(
        cluster_id='exo-rm',
        share_id='rm-bucket',
        name='Remove Bucket',
        rgw=smb.resources.RGWStorage(
            bucket='rm-bucket',
            user_id='rm-user',
            credential_ref='rm-user',
        ),
    )

    # Create the share
    rg = thandler.apply([ext_cluster, cluster, credential, share])
    assert rg.success, rg.to_simplified()

    # Verify external cluster and share were created
    assert ('ext_ceph_clusters', 'exo-rm') in thandler.internal_store.data
    assert ('shares', 'exo-rm.rm-bucket') in thandler.internal_store.data

    # Remove the share
    rmshare = smb.resources.RemovedShare(
        cluster_id='exo-rm',
        share_id='rm-bucket',
    )
    rg = thandler.apply([rmshare])
    assert rg.success, rg.to_simplified()

    # Verify share was removed
    shares = thandler.share_ids()
    assert ('exo-rm', 'rm-bucket') not in shares
    assert ('shares', 'exo-rm.rm-bucket') not in thandler.internal_store.data


def test_external_cluster_no_user_validation(thandler):
    """Test that external cluster requires at least one user."""
    # Try to create external cluster values without any users (should fail during validation)
    with pytest.raises(
        ValueError, match='At least one of cephfs_user or rgw_user'
    ):
        smb.resources.ExternalCephClusterValues(
            fsid='12345678-1234-1234-1234-123456789abc',
            mon_host='10.0.1.10:6789',
        )


def test_rgw_share_acl_configuration(thandler):
    """Test that RGW shares include proper ACL configuration."""
    cluster = _cluster(
        cluster_id='rgwacl',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    share = smb.resources.Share(
        cluster_id='rgwacl',
        share_id='aclshare',
        name='ACL Test Share',
        rgw=smb.resources.RGWStorage(
            bucket='acl-bucket',
            user_id='acluser',
        ),
    )
    rg = thandler.apply([cluster, share])
    assert rg.success, rg.to_simplified()

    # Verify the share was created
    assert ('shares', 'rgwacl.aclshare') in thandler.internal_store.data

    # Sync to generate the configuration
    thandler._sync_clusters(['rgwacl'])

    # Verify ACL configuration in public store
    cfg = thandler.public_store['rgwacl', 'config.smb'].get()
    assert cfg
    assert 'shares' in cfg
    assert 'ACL Test Share' in cfg['shares']

    share_opts = cfg['shares']['ACL Test Share']['options']

    # Verify ACL-related VFS objects are present
    assert 'vfs objects' in share_opts
    assert share_opts['vfs objects'] == 'acl_xattr ceph_rgw'

    # Verify ACL xattr security name is configured
    assert 'acl_xattr:security_acl_name' in share_opts
    assert share_opts['acl_xattr:security_acl_name'] == 'user.NTACL'


# Tests for tenant-aware RGW operations
class _TenantAwareFakeToolExecer:
    """Mock tool executor that handles tenant-aware RGW operations."""

    def tool_exec(self, cmd: list[str]) -> tuple[int, str, str]:
        """Mock tool_exec supporting tenant-aware bucket and user operations."""
        # Check for bucket stats command
        if 'radosgw-admin' in cmd and 'bucket' in cmd and 'stats' in cmd:
            # Extract bucket name from command
            bucket_idx = (
                cmd.index('--bucket') + 1 if '--bucket' in cmd else -1
            )
            bucket_name = cmd[bucket_idx] if bucket_idx > 0 else 'my-bucket'

            # Handle tenant-aware bucket names (format: "tenant/bucket")
            if '/' in bucket_name:
                tenant, bucket = bucket_name.split('/', 1)
                owner = f"{tenant}$tenantuser"
            else:
                tenant = None
                bucket = bucket_name
                owner = 'testuser'

            bucket_stats = json.dumps(
                {'owner': owner, 'bucket': bucket, 'usage': {}}
            )
            return (0, bucket_stats, '')

        # Check for user info command
        if 'radosgw-admin' in cmd and 'user' in cmd and 'info' in cmd:
            # Extract user_id from command
            uid_idx = cmd.index('--uid') + 1 if '--uid' in cmd else -1
            user_id = cmd[uid_idx] if uid_idx > 0 else 'testuser'

            # For tenant-aware users, return credentials
            user_info = json.dumps(
                {
                    'user_id': user_id,
                    'keys': [
                        {
                            'access_key': f'TENANT_ACCESS_KEY_{user_id}',
                            'secret_key': f'TENANT_SECRET_KEY_{user_id}',
                        }
                    ],
                }
            )
            return (0, user_info, '')

        # Default response for other commands
        return (0, '{}', '')


def test_tenant_aware_rgw_share_creation(thandler):
    """Test creating RGW share with tenant-aware user_id (format: tenant$user)."""
    cluster = _cluster(
        cluster_id='tenantcluster',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    # Create share with tenant-aware user_id format
    share = smb.resources.Share(
        cluster_id='tenantcluster',
        share_id='tenantshare',
        name='Tenant Share',
        rgw=smb.resources.RGWStorage(
            bucket='tenant-bucket',
            user_id='mytenant$tenantuser',
        ),
    )

    # Use tenant-aware mock executor
    ext_store = smb.config_store.MemConfigStore()
    handler = smb.handler.ClusterConfigHandler(
        internal_store=smb.config_store.MemConfigStore(),
        public_store=ext_store,
        priv_store=ext_store,
        mon_cmd_issuer=None,
        tool_execer=_TenantAwareFakeToolExecer(),
    )

    rg = handler.apply([cluster, share])
    assert rg.success, rg.to_simplified()

    # Verify share was created with tenant-aware user_id
    assert (
        'shares',
        'tenantcluster.tenantshare',
    ) in handler.internal_store.data
    share_dict = handler.internal_store.data[
        ('shares', 'tenantcluster.tenantshare')
    ]
    # Verify credential_ref was set to full tenant$user format
    assert share_dict['rgw']['credential_ref'] == 'mytenant$tenantuser'
    assert share_dict['rgw']['bucket'] == 'tenant-bucket'

    # Verify credential was created with proper key
    assert ('rgw_creds', 'mytenant$tenantuser') in handler.internal_store.data
    cred_dict = handler.internal_store.data[
        ('rgw_creds', 'mytenant$tenantuser')
    ]
    assert cred_dict['user_id'] == 'mytenant$tenantuser'


def test_tenant_user_extraction():
    """Test that tenant is properly extracted from user_id format."""
    from smb.rgw import _split_tenant_user_id

    # Test tenant$user format
    tenant, user = _split_tenant_user_id('mytenant$myuser')
    assert tenant == 'mytenant'
    assert user == 'myuser'

    # Test plain user format (no tenant)
    tenant, user = _split_tenant_user_id('plainuser')
    assert tenant == ''
    assert user == 'plainuser'

    # Test empty string
    tenant, user = _split_tenant_user_id('')
    assert tenant == ''
    assert user == ''


def test_tenant_aware_bucket_validation():
    """Test bucket validation with tenant-aware user_id."""
    from smb.rgw import validate_rgw_bucket

    executor = _TenantAwareFakeToolExecer()

    # Validate bucket with tenant-aware user_id
    result = validate_rgw_bucket(
        executor, 'tenant-bucket', user_id='mytenant$tenantuser'
    )
    assert result is True

    # Validate bucket with plain user_id
    result = validate_rgw_bucket(executor, 'my-bucket', user_id='testuser')
    assert result is True


def test_fetch_rgw_credentials_with_tenant():
    """Test fetching credentials for tenant-aware user."""
    from smb.rgw import fetch_rgw_credentials

    executor = _TenantAwareFakeToolExecer()

    # Fetch credentials with tenant-aware user_id
    user_id, access_key, secret_key = fetch_rgw_credentials(
        executor, 'tenant-bucket', user_id='mytenant$tenantuser'
    )

    # Verify user_id is returned in same format as input (with tenant prefix)
    assert user_id == 'mytenant$tenantuser'
    assert access_key == 'TENANT_ACCESS_KEY_tenantuser'
    assert secret_key == 'TENANT_SECRET_KEY_tenantuser'

    # Fetch credentials with plain user_id
    user_id, access_key, secret_key = fetch_rgw_credentials(
        executor, 'my-bucket', user_id='testuser'
    )

    # Verify user_id is returned in same format as input (without tenant)
    assert user_id == 'testuser'
    assert access_key == 'TENANT_ACCESS_KEY_testuser'
    assert secret_key == 'TENANT_SECRET_KEY_testuser'


def test_reject_bucket_with_slash_in_name():
    """Test that bucket names containing "/" are rejected as invalid."""
    from smb.rgw import fetch_rgw_credentials

    executor = _TenantAwareFakeToolExecer()

    # Test fetch_rgw_credentials rejects bucket with "/"
    with pytest.raises(ValueError) as exc_info:
        fetch_rgw_credentials(executor, 'tenantA/bkt1', user_id='testuser')
    error_msg = str(exc_info.value)
    assert 'tenantA/bkt1' in error_msg
    assert 'should not contain' in error_msg


def test_tenant_aware_bucket_with_slash_error():
    """Test error when bucket name includes tenant prefix (e.g., tenantA/bucket)."""
    # Create RGWStorage with invalid bucket name containing "/"
    rgw_storage = smb.resources.RGWStorage(
        bucket='tenantA/bucket',  # Invalid: tenant should not be in bucket name
        user_id='testuser',
    )

    # Validation should fail due to "/" in bucket name
    with pytest.raises(ValueError) as exc_info:
        rgw_storage.validate()
    error_msg = str(exc_info.value)
    assert 'tenantA/bucket' in error_msg
    assert 'should not contain' in error_msg


def test_tenant_option_with_user_id():
    """Test tenant-aware setup with proper tenant$user format in user_id."""
    from smb.rgw import fetch_rgw_credentials

    executor = _TenantAwareFakeToolExecer()

    # Test with tenant in user_id (correct format)
    user_id, access_key, secret_key = fetch_rgw_credentials(
        executor, 'my-bucket', user_id='prodtenant$produser'
    )

    # Should return user_id in same format as input
    assert user_id == 'prodtenant$produser'
    assert access_key == 'TENANT_ACCESS_KEY_produser'
    assert secret_key == 'TENANT_SECRET_KEY_produser'


def test_tenant_aware_share_creation_with_proper_format():
    """Test RGW share creation with correct tenant$user format (not in bucket name)."""
    # Correct: tenant in user_id, not in bucket name
    share = smb.resources.Share(
        cluster_id='tenantshare',
        share_id='s1',
        name='Tenant Share',
        rgw=smb.resources.RGWStorage(
            bucket='my-bucket',  # Plain bucket name
            user_id='mytenant$myuser',  # Tenant in user_id
        ),
    )

    # This should validate successfully
    share.rgw.validate()

    # Verify the structure is correct
    assert share.rgw.bucket == 'my-bucket'
    assert share.rgw.user_id == 'mytenant$myuser'


def test_wrong_tenant_format_in_bucket_name():
    """Test various wrong bucket name formats with "/" character."""
    executor = _TenantAwareFakeToolExecer()

    # Test various invalid bucket formats
    invalid_buckets = [
        'tenantA/bkt1',
        'tenant/bucket',
        'prod/photos-bucket',
        'dev/my_bucket_name',
    ]

    # fetch_rgw_credentials should raise ValueError for buckets with "/"
    from smb.rgw import fetch_rgw_credentials

    for invalid_bucket in invalid_buckets:
        with pytest.raises(ValueError) as exc_info:
            fetch_rgw_credentials(
                executor, invalid_bucket, user_id='testuser'
            )
        error_msg = str(exc_info.value)
        assert invalid_bucket in error_msg
        assert 'should not contain' in error_msg


def test_rgw_credential_id_validation():
    """Test that tenant$user format is accepted for RGW credential IDs."""
    import smb.validation as validation

    # Test cases that should PASS
    valid_cases = [
        "testuser",
        "mytenant$testuser",
        "tenant123$user456",
        "a$b",
        "tenant-name$user-name",
    ]

    for test_id in valid_cases:
        # Should not raise ValueError
        validation.check_rgw_credential_id(test_id)

    # Test cases that should FAIL
    invalid_cases = ["", "tenant$", "tenant$$user", "$tenant"]

    for test_id in invalid_cases:
        with pytest.raises(ValueError):
            validation.check_rgw_credential_id(test_id)


def test_regular_id_still_rejects_dollar_sign():
    """Verify that regular IDs still reject $ character."""
    import smb.validation as validation

    # Regular IDs should NOT accept $
    with pytest.raises(ValueError):
        validation.check_id("tenant$user")

    # Regular IDs should accept normal format
    validation.check_id("testuser")  # Should not raise


class _TenantOnlyFakeToolExecer:
    """Mock executor where the bucket only exists under a tenant prefix.

    A plain (no-tenant) bucket stats lookup returns an error, simulating
    a real RGW deployment where the bucket belongs exclusively to a tenant.
    """

    def tool_exec(self, cmd: list[str]) -> tuple[int, str, str]:
        if 'radosgw-admin' in cmd and 'bucket' in cmd and 'stats' in cmd:
            bucket_idx = (
                cmd.index('--bucket') + 1 if '--bucket' in cmd else -1
            )
            bucket_name = cmd[bucket_idx] if bucket_idx > 0 else ''
            # Only succeed when bucket is looked up with tenant prefix
            if '/' in bucket_name:
                tenant, bucket = bucket_name.split('/', 1)
                owner = f"{tenant}$tenantuser"
                return (0, json.dumps({'owner': owner, 'bucket': bucket}), '')
            # Plain lookup fails — bucket does not exist without tenant
            return (1, '', f'bucket {bucket_name} not found')
        raise AssertionError(f"Unexpected command in mock: {cmd}")


def test_tenant_bucket_without_user_id_fails():
    """Test that omitting user_id for a tenant-only bucket raises ValueError."""
    from smb.rgw import fetch_rgw_credentials

    executor = _TenantOnlyFakeToolExecer()

    with pytest.raises(ValueError):
        fetch_rgw_credentials(executor, 'tenant-bucket', user_id='')
