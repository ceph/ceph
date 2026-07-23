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
