import pytest

import smb
from smb.handler import _FakeEarmarkResolver


def _cluster(**kwargs):
    if 'clustering' not in kwargs:
        kwargs['clustering'] = smb.enums.SMBClustering.NEVER
    return smb.resources.Cluster(**kwargs)


@pytest.fixture
def thandler():
    ext_store = smb.config_store.MemConfigStore()
    return smb.handler.ClusterConfigHandler(
        internal_store=smb.config_store.MemConfigStore(),
        # it's completely valid to put the public and priv contents
        # into a single store. Do that to simplify testing a bit.
        public_store=ext_store,
        priv_store=ext_store,
    )


def test_clusters_empty(thandler):
    clusters = thandler.cluster_ids()
    assert clusters == []


def test_shares_empty(thandler):
    clusters = thandler.share_ids()
    assert clusters == []


def test_internal_apply_cluster(thandler):
    cluster = _cluster(
        cluster_id='foo',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    rg = thandler.apply([cluster])
    assert rg.success, rg.to_simplified()
    assert ('clusters', 'foo') in thandler.internal_store.data


def test_cluster_add(thandler):
    cluster = _cluster(
        cluster_id='foo',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    rg = thandler.apply([cluster])
    assert rg.success, rg.to_simplified()

    clusters = thandler.cluster_ids()
    assert len(clusters) == 1
    assert 'foo' in clusters


def test_internal_apply_cluster_and_share(thandler):
    cluster = _cluster(
        cluster_id='foo',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='s1',
        name='Ess One',
        cephfs=smb.resources.CephFSStorage(
            volume='cephfs',
            path='/',
        ),
    )
    rg = thandler.apply([cluster, share])
    assert rg.success, rg.to_simplified()
    assert ('clusters', 'foo') in thandler.internal_store.data
    assert ('shares', 'foo.s1') in thandler.internal_store.data

    shares = thandler.share_ids()
    assert len(shares) == 1
    assert ('foo', 's1') in shares


def test_internal_apply_remove_cluster(thandler):
    thandler.internal_store.overwrite(
        {
            "clusters.foo": {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'user',
                'intent': 'present',
                'clustering': 'never',
                'user_group_settings': [
                    {
                        'source_type': 'empty',
                    }
                ],
            }
        }
    )

    clusters = thandler.cluster_ids()
    assert len(clusters) == 1
    assert 'foo' in clusters

    rmcluster = smb.resources.RemovedCluster(
        cluster_id='foo',
    )
    rg = thandler.apply([rmcluster])
    assert rg.success, rg.to_simplified()

    clusters = thandler.cluster_ids()
    assert len(clusters) == 0


def test_internal_apply_remove_shares(thandler):
    thandler.internal_store.overwrite(
        {
            'clusters.foo': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'user',
                'intent': 'present',
                'clustering': 'never',
                'user_group_settings': [
                    {
                        'source_type': 'empty',
                    }
                ],
            },
            'shares.foo.s1': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'foo',
                'share_id': 's1',
                'intent': 'present',
                'name': 'Ess One',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/',
                    'provider': 'samba-vfs',
                },
            },
            'shares.foo.stwo': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'foo',
                'share_id': 'stwo',
                'intent': 'present',
                'name': 'Ess Two',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/two',
                    'provider': 'samba-vfs',
                },
            },
        }
    )

    shares = thandler.share_ids()
    assert len(shares) == 2
    assert ('foo', 's1') in shares
    assert ('foo', 'stwo') in shares

    rmshare1 = smb.resources.RemovedShare(
        cluster_id='foo',
        share_id='s1',
    )
    rg = thandler.apply([rmshare1])
    assert rg.success, rg.to_simplified()

    shares = thandler.share_ids()
    assert len(shares) == 1

    rmshare2 = smb.resources.RemovedShare(
        cluster_id='foo',
        share_id='stwo',
    )
    rg = thandler.apply([rmshare1, rmshare2])
    assert rg.success, rg.to_simplified()

    shares = thandler.share_ids()
    assert len(shares) == 0

    # check the results
    rgs = rg.to_simplified()
    assert rgs['success']
    assert len(rgs['results']) == 2
    assert rgs['results'][0]['success']
    assert rgs['results'][0]['state'] == 'not present'
    assert rgs['results'][1]['success']
    assert rgs['results'][1]['state'] == 'removed'


def test_internal_apply_add_joinauth(thandler):
    thandler.internal_store.overwrite(
        {
            "clusters.foo": {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'user',
                'intent': 'present',
                'clustering': 'never',
                'user_group_settings': [
                    {
                        'source_type': 'empty',
                    }
                ],
            }
        }
    )

    assert len(thandler.join_auth_ids()) == 0
    ja = smb.resources.JoinAuth(
        auth_id='join1',
        auth=smb.resources.JoinAuthValues(
            username='testadmin',
            password='Passw0rd',
        ),
    )
    rg = thandler.apply([ja])
    assert rg.success, rg.to_simplified()

    assert len(thandler.join_auth_ids()) == 1


def test_internal_apply_add_usergroups(thandler):
    thandler.internal_store.overwrite(
        {
            "clusters.foo": {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'user',
                'intent': 'present',
                'clustering': 'never',
                'user_group_settings': [
                    {
                        'source_type': 'empty',
                    }
                ],
            }
        }
    )

    assert len(thandler.user_and_group_ids()) == 0
    ja = smb.resources.UsersAndGroups(
        users_groups_id='ug1',
        values=smb.resources.UserGroupSettings(
            users=[{"username": "foo"}],
            groups=[],
        ),
    )
    rg = thandler.apply([ja])
    assert rg.success, rg.to_simplified()

    assert len(thandler.user_and_group_ids()) == 1


def test_generate_config_basic(thandler):
    thandler.internal_store.overwrite(
        {
            'clusters.foo': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'user',
                'intent': 'present',
                'user_group_settings': [
                    {
                        'source_type': 'empty',
                    }
                ],
            },
            'shares.foo.s1': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'foo',
                'share_id': 's1',
                'intent': 'present',
                'name': 'Ess One',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/',
                    'provider': 'samba-vfs',
                },
            },
            'shares.foo.stwo': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'foo',
                'share_id': 'stwo',
                'intent': 'present',
                'name': 'Ess Two',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/two',
                    'provider': 'samba-vfs',
                },
            },
        }
    )

    cfg = thandler.generate_config('foo')
    assert cfg


def test_generate_config_ad(thandler):
    thandler.internal_store.overwrite(
        {
            'clusters.foo': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'active-directory',
                'intent': 'present',
                'domain_settings': {
                    'realm': 'dom1.example.com',
                    'join_sources': [
                        {
                            'source_type': 'resource',
                            'ref': 'foo1',
                        }
                    ],
                },
            },
            'join_auths.foo1': {
                'resource_type': 'ceph.smb.join.auth',
                'auth_id': 'foo1',
                'intent': 'present',
                'auth': {
                    'username': 'testadmin',
                    'password': 'Passw0rd',
                },
            },
            'shares.foo.s1': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'foo',
                'share_id': 's1',
                'intent': 'present',
                'name': 'Ess One',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/',
                    'provider': 'samba-vfs',
                },
            },
            'shares.foo.stwo': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'foo',
                'share_id': 'stwo',
                'intent': 'present',
                'name': 'Ess Two',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/two',
                    'provider': 'samba-vfs',
                },
            },
        }
    )

    cfg = thandler.generate_config('foo')
    assert cfg
    assert cfg['globals']['foo']['options']['realm'] == 'dom1.example.com'


def test_generate_config_with_login_control(thandler):
    thandler.internal_store.overwrite(
        {
            'clusters.foo': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'active-directory',
                'intent': 'present',
                'domain_settings': {
                    'realm': 'dom1.example.com',
                    'join_sources': [
                        {
                            'source_type': 'resource',
                            'ref': 'foo1',
                        }
                    ],
                },
            },
            'join_auths.foo1': {
                'resource_type': 'ceph.smb.join.auth',
                'auth_id': 'foo1',
                'intent': 'present',
                'auth': {
                    'username': 'testadmin',
                    'password': 'Passw0rd',
                },
            },
            'shares.foo.s1': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'foo',
                'share_id': 's1',
                'intent': 'present',
                'name': 'Ess One',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/',
                    'provider': 'samba-vfs',
                },
                'login_control': [
                    {
                        'name': 'dom1\\alan',
                        'category': 'user',
                        'access': 'read',
                    },
                    {
                        'name': 'dom1\\betsy',
                        'category': 'user',
                        'access': 'read-write',
                    },
                    {
                        'name': 'dom1\\chuck',
                        'category': 'user',
                        'access': 'admin',
                    },
                    {
                        'name': 'dom1\\ducky',
                        'category': 'user',
                        'access': 'none',
                    },
                    {
                        'name': 'dom1\\eggbert',
                        'category': 'user',
                        'access': 'read',
                    },
                    {
                        'name': 'dom1\\guards',
                        'category': 'group',
                        'access': 'read-write',
                    },
                ],
            },
        }
    )

    cfg = thandler.generate_config('foo')
    assert cfg
    assert cfg['shares']['Ess One']['options']
    shopts = cfg['shares']['Ess One']['options']
    assert shopts['invalid users'] == 'dom1\\ducky'
    assert shopts['read list'] == 'dom1\\alan dom1\\eggbert'
    assert shopts['write list'] == 'dom1\\betsy @dom1\\guards'
    assert shopts['admin users'] == 'dom1\\chuck'


def test_generate_config_with_login_control_restricted(thandler):
    thandler.internal_store.overwrite(
        {
            'clusters.foo': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'active-directory',
                'intent': 'present',
                'domain_settings': {
                    'realm': 'dom1.example.com',
                    'join_sources': [
                        {
                            'source_type': 'resource',
                            'ref': 'foo1',
                        }
                    ],
                },
            },
            'join_auths.foo1': {
                'resource_type': 'ceph.smb.join.auth',
                'auth_id': 'foo1',
                'intent': 'present',
                'auth': {
                    'username': 'testadmin',
                    'password': 'Passw0rd',
                },
            },
            'shares.foo.s1': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'foo',
                'share_id': 's1',
                'intent': 'present',
                'name': 'Ess One',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/',
                    'provider': 'samba-vfs',
                },
                'restrict_access': True,
                'login_control': [
                    {
                        'name': 'dom1\\alan',
                        'category': 'user',
                        'access': 'read',
                    },
                    {
                        'name': 'dom1\\betsy',
                        'category': 'user',
                        'access': 'read-write',
                    },
                    {
                        'name': 'dom1\\chuck',
                        'category': 'user',
                        'access': 'none',
                    },
                ],
            },
        }
    )

    cfg = thandler.generate_config('foo')
    assert cfg
    assert cfg['shares']['Ess One']['options']
    shopts = cfg['shares']['Ess One']['options']
    assert shopts['invalid users'] == 'dom1\\chuck'
    assert shopts['valid users'] == 'dom1\\alan dom1\\betsy'
    assert shopts['read list'] == 'dom1\\alan'
    assert shopts['write list'] == 'dom1\\betsy'


def test_error_result():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='s1',
        name='Ess One',
        cephfs=smb.resources.CephFSStorage(
            volume='cephfs',
            path='/',
        ),
    )
    err = smb.handler.ErrorResult(share, msg='test error')
    assert isinstance(err, smb.handler.Result)
    assert isinstance(err, Exception)

    data = err.to_simplified()
    assert data['resource']['cluster_id'] == 'foo'
    assert data['msg'] == 'test error'


def test_apply_type_error(thandler):
    # a resource component, not valid on its own
    r = smb.resources.CephFSStorage(
        volume='cephfs',
        path='/',
    )
    rg = thandler.apply([r])
    assert not rg.success
    assert rg.one().msg == 'not a valid smb resource'


def test_apply_no_matching_cluster_error(thandler):
    share = smb.resources.Share(
        cluster_id='woops',
        share_id='s1',
        name='Ess One',
        cephfs=smb.resources.CephFSStorage(
            volume='cephfs',
            path='/',
        ),
    )
    rg = thandler.apply([share])
    assert not rg.success


def test_apply_full_cluster_create(thandler):
    to_apply = [
        smb.resources.JoinAuth(
            auth_id='join1',
            auth=smb.resources.JoinAuthValues(
                username='testadmin',
                password='Passw0rd',
            ),
        ),
        _cluster(
            cluster_id='mycluster1',
            auth_mode=smb.enums.AuthMode.ACTIVE_DIRECTORY,
            domain_settings=smb.resources.DomainSettings(
                realm='MYDOMAIN.EXAMPLE.ORG',
                join_sources=[
                    smb.resources.JoinSource(
                        source_type=smb.enums.JoinSourceType.RESOURCE,
                        ref='join1',
                    ),
                ],
            ),
            custom_dns=['192.168.76.204'],
        ),
        smb.resources.Share(
            cluster_id='mycluster1',
            share_id='homedirs',
            name='Home Directries',
            cephfs=smb.resources.CephFSStorage(
                volume='cephfs',
                subvolume='homedirs',
                path='/',
            ),
        ),
        smb.resources.Share(
            cluster_id='mycluster1',
            share_id='archive',
            cephfs=smb.resources.CephFSStorage(
                volume='cephfs',
                path='/archive',
            ),
        ),
    ]
    results = thandler.apply(to_apply)
    assert results.success, results.to_simplified()
    assert len(list(results)) == 4

    assert 'mycluster1' in thandler.public_store.namespaces()
    ekeys = list(thandler.public_store.contents('mycluster1'))
    assert len(ekeys) == 4
    assert 'cluster-info' in ekeys
    assert 'config.smb' in ekeys
    assert 'spec.smb' in ekeys
    assert 'join.0.json' in ekeys

    jdata = thandler.public_store['mycluster1', 'join.0.json'].get()
    assert jdata == {'username': 'testadmin', 'password': 'Passw0rd'}
    cidata = thandler.public_store['mycluster1', 'cluster-info'].get()
    assert cidata['cluster_id'] == 'mycluster1'
    cfdata = thandler.public_store['mycluster1', 'config.smb'].get()
    assert 'mycluster1' in cfdata['configs']
    assert 'mycluster1' in cfdata['globals']
    assert len(cfdata['shares']) == 2
    assert 'Home Directries' in cfdata['shares']
    assert 'archive' in cfdata['shares']
    ssdata = thandler.public_store['mycluster1', 'spec.smb'].get()
    assert 'spec' in ssdata
    assert ssdata['spec']['cluster_id'] == 'mycluster1'
    assert ssdata['spec']['config_uri'] == 'mem:mycluster1/config.smb'
    assert ssdata['spec']['features'] == ['domain']
    assert ssdata['spec']['join_sources'] == ['mem:mycluster1/join.0.json']
    assert ssdata['spec']['custom_dns'] == ['192.168.76.204']


def test_apply_remove_share(thandler):
    test_apply_full_cluster_create(thandler)
    to_apply = [
        smb.resources.RemovedShare(
            cluster_id='mycluster1', share_id='archive'
        )
    ]

    results = thandler.apply(to_apply)
    assert results.success, results.to_simplified()
    assert len(list(results)) == 1

    assert 'mycluster1' in thandler.public_store.namespaces()
    ekeys = list(thandler.public_store.contents('mycluster1'))
    assert len(ekeys) == 4
    assert 'cluster-info' in ekeys
    assert 'config.smb' in ekeys
    assert 'spec.smb' in ekeys
    assert 'join.0.json' in ekeys

    # we deleted the archive share, so we can assert that only the homedirs
    # share is present in the config
    cfdata = thandler.public_store['mycluster1', 'config.smb'].get()
    assert 'mycluster1' in cfdata['configs']
    assert 'mycluster1' in cfdata['globals']
    assert len(cfdata['shares']) == 1
    assert 'Home Directries' in cfdata['shares']


def test_apply_update_password(thandler):
    test_apply_full_cluster_create(thandler)
    to_apply = [
        smb.resources.JoinAuth(
            auth_id='join1',
            auth=smb.resources.JoinAuthValues(
                username='testadmin',
                password='Zm9vYmFyCg',
            ),
        ),
    ]

    results = thandler.apply(to_apply)
    assert results.success, results.to_simplified()
    assert len(list(results)) == 1

    assert 'mycluster1' in thandler.public_store.namespaces()
    ekeys = list(thandler.public_store.contents('mycluster1'))
    assert len(ekeys) == 4
    assert 'cluster-info' in ekeys
    assert 'config.smb' in ekeys
    assert 'spec.smb' in ekeys
    assert 'join.0.json' in ekeys

    # we changed the password value. the store should reflect that
    jdata = thandler.public_store['mycluster1', 'join.0.json'].get()
    assert jdata == {'username': 'testadmin', 'password': 'Zm9vYmFyCg'}


def test_apply_add_second_cluster(thandler):
    test_apply_full_cluster_create(thandler)
    to_apply = [
        _cluster(
            cluster_id='coolcluster',
            auth_mode=smb.enums.AuthMode.ACTIVE_DIRECTORY,
            domain_settings=smb.resources.DomainSettings(
                realm='YOURDOMAIN.EXAMPLE.ORG',
                join_sources=[
                    smb.resources.JoinSource(
                        source_type=smb.enums.JoinSourceType.RESOURCE,
                        ref='coolcluster',
                    ),
                ],
            ),
        ),
        smb.resources.JoinAuth(
            auth_id='coolcluster',
            auth=smb.resources.JoinAuthValues(
                username='Jimmy',
                password='j4mb0ree!',
            ),
            linked_to_cluster='coolcluster',
        ),
        smb.resources.Share(
            cluster_id='coolcluster',
            share_id='images',
            cephfs=smb.resources.CephFSStorage(
                volume='imgvol',
                path='/',
            ),
        ),
    ]

    results = thandler.apply(to_apply)
    assert results.success, results.to_simplified()
    assert len(list(results)) == 3

    assert 'mycluster1' in thandler.public_store.namespaces()
    ekeys = list(thandler.public_store.contents('mycluster1'))
    assert len(ekeys) == 4
    assert 'cluster-info' in ekeys
    assert 'config.smb' in ekeys
    assert 'spec.smb' in ekeys
    assert 'join.0.json' in ekeys

    assert 'coolcluster' in thandler.public_store.namespaces()
    ekeys = list(thandler.public_store.contents('coolcluster'))
    assert len(ekeys) == 4
    assert 'cluster-info' in ekeys
    assert 'config.smb' in ekeys
    assert 'spec.smb' in ekeys
    assert 'join.0.json' in ekeys

    jdata = thandler.public_store['coolcluster', 'join.0.json'].get()
    assert jdata == {'username': 'Jimmy', 'password': 'j4mb0ree!'}
    cidata = thandler.public_store['coolcluster', 'cluster-info'].get()
    assert cidata['cluster_id'] == 'coolcluster'
    cfdata = thandler.public_store['coolcluster', 'config.smb'].get()
    assert 'coolcluster' in cfdata['configs']
    assert 'coolcluster' in cfdata['globals']
    assert len(cfdata['shares']) == 1
    assert 'images' in cfdata['shares']
    ssdata = thandler.public_store['coolcluster', 'spec.smb'].get()
    assert 'spec' in ssdata
    assert ssdata['spec']['cluster_id'] == 'coolcluster'
    assert ssdata['spec']['config_uri'] == 'mem:coolcluster/config.smb'
    assert ssdata['spec']['features'] == ['domain']
    assert ssdata['spec']['join_sources'] == ['mem:coolcluster/join.0.json']


def test_modify_cluster_only_touches_changed_cluster(thandler):
    test_apply_add_second_cluster(thandler)

    thandler.public_store.remove(('mycluster1', 'cluster-info'))
    ekeys = list(thandler.public_store.contents('mycluster1'))
    assert len(ekeys) == 3
    thandler.public_store.remove(('coolcluster', 'cluster-info'))
    ekeys = list(thandler.public_store.contents('coolcluster'))
    assert len(ekeys) == 3

    to_apply = [
        smb.resources.Share(
            cluster_id='coolcluster',
            share_id='photos',
            cephfs=smb.resources.CephFSStorage(
                volume='imgvol',
                path='/photos',
            ),
        ),
    ]
    results = thandler.apply(to_apply)
    assert results.success, results.to_simplified()
    assert len(list(results)) == 1

    # we didn't make any changes to mycluster1 or it's shares. so
    # we had no reason to update it... cluster-info should be missing
    assert 'mycluster1' in thandler.public_store.namespaces()
    ekeys = list(thandler.public_store.contents('mycluster1'))
    assert len(ekeys) == 3
    assert 'cluster-info' not in ekeys
    assert 'config.smb' in ekeys
    assert 'spec.smb' in ekeys
    assert 'join.0.json' in ekeys

    # we changed a share in coolcluster so we should have regenerated
    # the cluster-info object
    assert 'coolcluster' in thandler.public_store.namespaces()
    ekeys = list(thandler.public_store.contents('coolcluster'))
    assert len(ekeys) == 4
    assert 'cluster-info' in ekeys
    assert 'config.smb' in ekeys
    assert 'spec.smb' in ekeys
    assert 'join.0.json' in ekeys


def test_apply_remove_cluster(thandler):
    test_apply_full_cluster_create(thandler)
    assert ('clusters', 'mycluster1') in thandler.internal_store.data
    assert ('shares', 'mycluster1.archive') in thandler.internal_store.data
    assert ('shares', 'mycluster1.homedirs') in thandler.internal_store.data

    to_apply = [
        smb.resources.RemovedCluster(
            cluster_id='mycluster1',
        ),
        smb.resources.RemovedShare(
            cluster_id='mycluster1', share_id='archive'
        ),
        smb.resources.RemovedShare(
            cluster_id='mycluster1', share_id='homedirs'
        ),
    ]

    results = thandler.apply(to_apply)
    assert results.success, results.to_simplified()
    assert len(list(results)) == 3

    assert ('clusters', 'mycluster1') not in thandler.internal_store.data
    assert (
        'shares',
        'mycluster1.archive',
    ) not in thandler.internal_store.data
    assert (
        'shares',
        'mycluster1.homedirs',
    ) not in thandler.internal_store.data

    # verify that mycluster1 is gone from the public store
    assert 'mycluster1' not in thandler.public_store.namespaces()


def test_apply_remove_all_clusters(thandler):
    class FakeOrch:
        def __init__(self):
            self.deployed = set()

        def submit_smb_spec(self, spec):
            self.deployed.add(f'smb.{spec.service_id}')

        def remove_smb_service(self, service_name):
            assert service_name.startswith('smb.')
            self.deployed.remove(service_name)

    thandler._orch = FakeOrch()
    thandler._earmark_resolver = _FakeEarmarkResolver()
    test_apply_full_cluster_create(thandler)

    to_apply = [
        smb.resources.UsersAndGroups(
            users_groups_id='ug1',
            values=smb.resources.UserGroupSettings(
                users=[{"username": "foo"}],
                groups=[],
            ),
        ),
        _cluster(
            cluster_id='mycluster2',
            auth_mode=smb.enums.AuthMode.USER,
            user_group_settings=[
                smb.resources.UserGroupSource(
                    source_type=smb.resources.UserGroupSourceType.RESOURCE,
                    ref='ug1',
                ),
            ],
        ),
        _cluster(
            cluster_id='mycluster3',
            auth_mode=smb.enums.AuthMode.USER,
            user_group_settings=[
                smb.resources.UserGroupSource(
                    source_type=smb.resources.UserGroupSourceType.RESOURCE,
                    ref='ug1',
                ),
            ],
        ),
        smb.resources.Share(
            cluster_id='mycluster2',
            share_id='m2',
            cephfs=smb.resources.CephFSStorage(
                volume='imgvol',
                path='/',
            ),
        ),
        smb.resources.Share(
            cluster_id='mycluster3',
            share_id='m3',
            cephfs=smb.resources.CephFSStorage(
                volume='imgvol',
                path='/',
            ),
        ),
    ]

    results = thandler.apply(to_apply)
    assert results.success, results.to_simplified()
    assert ('clusters', 'mycluster1') in thandler.internal_store.data
    assert ('clusters', 'mycluster2') in thandler.internal_store.data
    assert ('clusters', 'mycluster3') in thandler.internal_store.data
    assert thandler._orch.deployed == {
        'smb.mycluster1',
        'smb.mycluster2',
        'smb.mycluster3',
    }

    to_apply = [
        smb.resources.RemovedCluster(
            cluster_id='mycluster1',
        ),
        smb.resources.RemovedShare(
            cluster_id='mycluster1', share_id='archive'
        ),
        smb.resources.RemovedShare(
            cluster_id='mycluster1', share_id='homedirs'
        ),
        smb.resources.RemovedCluster(
            cluster_id='mycluster2',
        ),
        smb.resources.RemovedShare(cluster_id='mycluster2', share_id='m2'),
        smb.resources.RemovedCluster(
            cluster_id='mycluster3',
        ),
        smb.resources.RemovedShare(cluster_id='mycluster3', share_id='m3'),
        smb.resources.UsersAndGroups(
            users_groups_id='ug1',
            intent=smb.enums.Intent.REMOVED,
        ),
    ]

    results = thandler.apply(to_apply)
    assert results.success, results.to_simplified()
    all_ns = thandler.public_store.namespaces()
    assert list(all_ns) == []
    assert thandler._orch.deployed == set()


def test_all_resources(thandler):
    test_apply_add_second_cluster(thandler)
    rall = thandler.all_resources()
    assert len(rall) == 7
    assert rall[0].resource_type == 'ceph.smb.cluster'
    assert rall[1].resource_type == 'ceph.smb.share'
    assert rall[2].resource_type == 'ceph.smb.share'
    assert rall[3].resource_type == 'ceph.smb.cluster'
    assert rall[4].resource_type == 'ceph.smb.share'
    assert rall[5].resource_type == 'ceph.smb.join.auth'
    assert rall[6].resource_type == 'ceph.smb.join.auth'


@pytest.mark.parametrize(
    "params",
    [
        # by single type
        dict(
            requests=['ceph.smb.cluster'],
            check=[
                {
                    'resource_type': 'ceph.smb.cluster',
                    'cluster_id': 'mycluster1',
                },
                {
                    'resource_type': 'ceph.smb.cluster',
                    'cluster_id': 'coolcluster',
                },
            ],
        ),
        dict(
            requests=['ceph.smb.share'],
            check=[
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'mycluster1',
                    'share_id': 'homedirs',
                },
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'mycluster1',
                    'share_id': 'archive',
                },
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'coolcluster',
                    'share_id': 'images',
                },
            ],
        ),
        # by mixed type
        dict(
            requests=['ceph.smb.cluster', 'ceph.smb.share'],
            check=[
                {
                    'resource_type': 'ceph.smb.cluster',
                    'cluster_id': 'mycluster1',
                },
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'mycluster1',
                    'share_id': 'homedirs',
                },
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'mycluster1',
                    'share_id': 'archive',
                },
                {
                    'resource_type': 'ceph.smb.cluster',
                    'cluster_id': 'coolcluster',
                },
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'coolcluster',
                    'share_id': 'images',
                },
            ],
        ),
        dict(
            requests=['ceph.smb.join.auth', 'ceph.smb.share'],
            check=[
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'mycluster1',
                    'share_id': 'homedirs',
                },
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'mycluster1',
                    'share_id': 'archive',
                },
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'coolcluster',
                    'share_id': 'images',
                },
                {
                    'resource_type': 'ceph.smb.join.auth',
                    'auth_id': 'join1',
                },
                {
                    'resource_type': 'ceph.smb.join.auth',
                    'auth_id': 'coolcluster',
                },
            ],
        ),
        # cluster with id
        dict(
            requests=['ceph.smb.cluster.coolcluster'],
            check=[
                {
                    'resource_type': 'ceph.smb.cluster',
                    'cluster_id': 'coolcluster',
                },
            ],
        ),
        # share with cluster id
        dict(
            requests=['ceph.smb.share.mycluster1'],
            check=[
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'mycluster1',
                    'share_id': 'homedirs',
                },
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'mycluster1',
                    'share_id': 'archive',
                },
            ],
        ),
        # share with cluster id, share id
        dict(
            requests=['ceph.smb.share.mycluster1.archive'],
            check=[
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'mycluster1',
                    'share_id': 'archive',
                },
            ],
        ),
        # miss
        dict(requests=['ceph.smb.usersgroups'], check=[]),
        # (for coverage sake)
        dict(
            requests=['ceph.smb.usersgroups'],
            insert_ug=True,
            check=[
                {
                    'resource_type': 'ceph.smb.usersgroups',
                    'users_groups_id': 'ug1',
                },
            ],
        ),
    ],
)
def test_matching_resources(thandler, params):
    test_apply_add_second_cluster(thandler)
    if params.get('insert_ug'):
        ja = smb.resources.UsersAndGroups(
            users_groups_id='ug1',
            values=smb.resources.UserGroupSettings(
                users=[{"username": "foo"}],
                groups=[],
            ),
        )
        rg = thandler.apply([ja])
        assert rg.success

    rall = thandler.matching_resources(params['requests'])
    check = params['check']
    assert len(rall) == len(check)
    for idx, (resource, chk) in enumerate(zip(rall, check)):
        for k, v in chk.items():
            assert (
                getattr(resource, k) == v
            ), f"resource.{k} != {v}, result #{idx}"


@pytest.mark.parametrize(
    'txt',
    [
        'foo.bar.baz',
        'tic/tac/toe',
        '',
        'ceph.smb.cluster.funky.town.band',
    ],
)
def test_invalid_resource_match_strs(thandler, txt):
    with pytest.raises(ValueError):
        thandler.matching_resources([txt])


def test_apply_cluster_linked_auth(thandler):
    to_apply = [
        smb.resources.JoinAuth(
            auth_id='join1',
            auth=smb.resources.JoinAuthValues(
                username='testadmin',
                password='Passw0rd',
            ),
            linked_to_cluster='mycluster1',
        ),
        _cluster(
            cluster_id='mycluster1',
            auth_mode=smb.enums.AuthMode.ACTIVE_DIRECTORY,
            domain_settings=smb.resources.DomainSettings(
                realm='MYDOMAIN.EXAMPLE.ORG',
                join_sources=[
                    smb.resources.JoinSource(
                        source_type=smb.enums.JoinSourceType.RESOURCE,
                        ref='join1',
                    ),
                ],
            ),
            custom_dns=['192.168.76.204'],
        ),
        smb.resources.Share(
            cluster_id='mycluster1',
            share_id='homedirs',
            name='Home Directries',
            cephfs=smb.resources.CephFSStorage(
                volume='cephfs',
                subvolume='homedirs',
                path='/',
            ),
        ),
    ]
    results = thandler.apply(to_apply)
    assert results.success, results.to_simplified()
    assert len(list(results)) == 3
    assert ('clusters', 'mycluster1') in thandler.internal_store.data
    assert ('shares', 'mycluster1.homedirs') in thandler.internal_store.data
    assert ('join_auths', 'join1') in thandler.internal_store.data

    to_apply = [
        smb.resources.RemovedCluster(
            cluster_id='mycluster1',
        ),
        smb.resources.RemovedShare(
            cluster_id='mycluster1',
            share_id='homedirs',
        ),
    ]
    results = thandler.apply(to_apply)
    assert results.success, results.to_simplified()
    assert len(list(results)) == 2
    assert ('clusters', 'mycluster1') not in thandler.internal_store.data
    assert (
        'shares',
        'mycluster1.homedirs',
    ) not in thandler.internal_store.data
    assert ('join_auths', 'join1') not in thandler.internal_store.data


def test_apply_cluster_bad_linked_auth(thandler):
    to_apply = [
        smb.resources.JoinAuth(
            auth_id='join1',
            auth=smb.resources.JoinAuthValues(
                username='testadmin',
                password='Passw0rd',
            ),
            linked_to_cluster='mycluster2',
        ),
        _cluster(
            cluster_id='mycluster1',
            auth_mode=smb.enums.AuthMode.ACTIVE_DIRECTORY,
            domain_settings=smb.resources.DomainSettings(
                realm='MYDOMAIN.EXAMPLE.ORG',
                join_sources=[
                    smb.resources.JoinSource(
                        source_type=smb.enums.JoinSourceType.RESOURCE,
                        ref='join1',
                    ),
                ],
            ),
            custom_dns=['192.168.76.204'],
        ),
    ]
    results = thandler.apply(to_apply)
    assert not results.success
    rs = results.to_simplified()
    assert len(rs['results']) == 2
    assert rs['results'][0]['msg'] == 'linked_to_cluster id not valid'
    assert rs['results'][1]['msg'] == 'join auth linked to different cluster'


def test_apply_cluster_bad_linked_ug(thandler):
    to_apply = [
        smb.resources.UsersAndGroups(
            users_groups_id='ug1',
            values=smb.resources.UserGroupSettings(
                users=[{"username": "foo"}],
                groups=[],
            ),
            linked_to_cluster='mycluster2',
        ),
        _cluster(
            cluster_id='mycluster1',
            auth_mode=smb.enums.AuthMode.USER,
            user_group_settings=[
                smb.resources.UserGroupSource(
                    source_type=smb.resources.UserGroupSourceType.RESOURCE,
                    ref='ug1',
                ),
            ],
        ),
    ]
    results = thandler.apply(to_apply)
    assert not results.success
    rs = results.to_simplified()
    assert len(rs['results']) == 2
    assert rs['results'][0]['msg'] == 'linked_to_cluster id not valid'
    assert (
        rs['results'][1]['msg']
        == 'users and groups linked to different cluster'
    )


def test_apply_with_create_only(thandler):
    test_apply_full_cluster_create(thandler)

    to_apply = [
        _cluster(
            cluster_id='mycluster1',
            auth_mode=smb.enums.AuthMode.ACTIVE_DIRECTORY,
            domain_settings=smb.resources.DomainSettings(
                realm='MYDOMAIN.EXAMPLE.ORG',
                join_sources=[
                    smb.resources.JoinSource(
                        source_type=smb.enums.JoinSourceType.RESOURCE,
                        ref='join1',
                    ),
                ],
            ),
            custom_dns=['192.168.76.204'],
        ),
        smb.resources.Share(
            cluster_id='mycluster1',
            share_id='homedirs',
            name='Altered Home Directries',
            cephfs=smb.resources.CephFSStorage(
                volume='cephfs',
                subvolume='homedirs',
                path='/',
            ),
        ),
        smb.resources.Share(
            cluster_id='mycluster1',
            share_id='foodirs',
            name='Foo Directries',
            cephfs=smb.resources.CephFSStorage(
                volume='cephfs',
                subvolume='homedirs',
                path='/foo',
            ),
        ),
    ]
    results = thandler.apply(to_apply, create_only=True)
    assert not results.success
    rs = results.to_simplified()
    assert len(rs['results']) == 3
    assert (
        rs['results'][0]['msg']
        == 'a resource with the same ID already exists'
    )
    assert (
        rs['results'][1]['msg']
        == 'a resource with the same ID already exists'
    )

    # no changes to the store
    assert (
        'shares',
        'mycluster1.foodirs',
    ) not in thandler.internal_store.data
    assert ('shares', 'mycluster1.homedirs') in thandler.internal_store.data
    assert (
        thandler.internal_store.data[('shares', 'mycluster1.homedirs')][
            'name'
        ]
        == 'Home Directries'
    )

    # foodirs share is new, it can be applied separately
    to_apply = [
        smb.resources.Share(
            cluster_id='mycluster1',
            share_id='foodirs',
            name='Foo Directries',
            cephfs=smb.resources.CephFSStorage(
                volume='cephfs',
                subvolume='homedirs',
                path='/foo',
            ),
        ),
    ]
    results = thandler.apply(to_apply, create_only=True)
    assert results.success
    rs = results.to_simplified()
    assert len(rs['results']) == 1
    assert (
        'shares',
        'mycluster1.foodirs',
    ) in thandler.internal_store.data


def test_remove_in_use_cluster(thandler):
    thandler.internal_store.overwrite(
        {
            'clusters.foo': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'active-directory',
                'intent': 'present',
                'domain_settings': {
                    'realm': 'dom1.example.com',
                    'join_sources': [
                        {
                            'source_type': 'resource',
                            'ref': 'foo1',
                        }
                    ],
                },
            },
            'join_auths.foo1': {
                'resource_type': 'ceph.smb.join.auth',
                'auth_id': 'foo1',
                'intent': 'present',
                'auth': {
                    'username': 'testadmin',
                    'password': 'Passw0rd',
                },
            },
            'shares.foo.s1': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'foo',
                'share_id': 's1',
                'intent': 'present',
                'name': 'Ess One',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/',
                    'provider': 'samba-vfs',
                },
            },
        }
    )

    to_apply = [
        smb.resources.RemovedCluster(
            cluster_id='foo',
        ),
    ]
    results = thandler.apply(to_apply)
    rs = results.to_simplified()
    assert not results.success
    assert 'cluster in use' in rs['results'][0]['msg']


def test_remove_in_use_join_auth(thandler):
    thandler.internal_store.overwrite(
        {
            'clusters.foo': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'active-directory',
                'intent': 'present',
                'domain_settings': {
                    'realm': 'dom1.example.com',
                    'join_sources': [
                        {
                            'source_type': 'resource',
                            'ref': 'foo1',
                        }
                    ],
                },
            },
            'join_auths.foo1': {
                'resource_type': 'ceph.smb.join.auth',
                'auth_id': 'foo1',
                'intent': 'present',
                'auth': {
                    'username': 'testadmin',
                    'password': 'Passw0rd',
                },
            },
            'shares.foo.s1': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'foo',
                'share_id': 's1',
                'intent': 'present',
                'name': 'Ess One',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/',
                    'provider': 'samba-vfs',
                },
            },
        }
    )

    to_apply = [
        smb.resources.JoinAuth(
            auth_id='foo1',
            intent=smb.enums.Intent.REMOVED,
        ),
    ]
    results = thandler.apply(to_apply)
    rs = results.to_simplified()
    assert not results.success
    assert 'resource in use' in rs['results'][0]['msg']


def test_remove_in_use_ug(thandler):
    thandler.internal_store.overwrite(
        {
            'clusters.foo': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'user',
                'intent': 'present',
                'user_group_settings': [
                    {
                        'source_type': 'resource',
                        'ref': 'foo1',
                    }
                ],
            },
            'users_and_groups.foo1': {
                'resource_type': 'ceph.smb.usersgroups',
                'users_groups_id': 'foo1',
                'intent': 'present',
                'values': {
                    'users': [{"username": "foo"}],
                    'groups': [],
                },
            },
            'shares.foo.s1': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'foo',
                'share_id': 's1',
                'intent': 'present',
                'name': 'Ess One',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/',
                    'provider': 'samba-vfs',
                },
            },
        }
    )

    to_apply = [
        smb.resources.UsersAndGroups(
            users_groups_id='foo1',
            intent=smb.enums.Intent.REMOVED,
        ),
    ]
    results = thandler.apply(to_apply)
    rs = results.to_simplified()
    assert not results.success
    assert 'resource in use' in rs['results'][0]['msg']


@pytest.mark.parametrize(
    "params",
    [
        # no conflict
        {
            'to_apply': [
                smb.resources.Share(
                    cluster_id='c1',
                    share_id='zeta',
                    name='Zeta Zoom',
                    cephfs=smb.resources.CephFSStorage(
                        volume='cephfs',
                        path='/zeta',
                    ),
                ),
            ],
        },
        # no conflict, name used only in c1 not c2
        {
            'to_apply': [
                smb.resources.Share(
                    cluster_id='c2',
                    share_id='max',
                    name='Beta Max',
                    cephfs=smb.resources.CephFSStorage(
                        volume='cephfs',
                        path='/max',
                    ),
                ),
            ],
        },
        # conflict with share in store
        {
            'to_apply': [
                smb.resources.Share(
                    cluster_id='c1',
                    share_id='zalpha',
                    name='Alphabet Soup',
                    cephfs=smb.resources.CephFSStorage(
                        volume='cephfs',
                        path='/zalpha',
                    ),
                ),
            ],
            'error_msg': 'share name already in use',
            'conflicts': {'alpha'},
        },
        # conflict with new share
        {
            'to_apply': [
                smb.resources.Share(
                    cluster_id='c1',
                    share_id='epsilon',
                    name='Epsilon Eggs',
                    cephfs=smb.resources.CephFSStorage(
                        volume='cephfs',
                        path='/eggs',
                    ),
                ),
                smb.resources.Share(
                    cluster_id='c1',
                    share_id='eggs',
                    name='Epsilon Eggs',
                    cephfs=smb.resources.CephFSStorage(
                        volume='cephfs',
                        path='/eggs',
                    ),
                ),
            ],
            'error_msg': 'share name already in use',
            'conflicts': {'eggs'},
        },
        # remove share, resue old name
        {
            'to_apply': [
                smb.resources.RemovedShare(
                    cluster_id='c1',
                    share_id='beta',
                ),
                smb.resources.Share(
                    cluster_id='c1',
                    share_id='macks',
                    name='Beta Max',
                    cephfs=smb.resources.CephFSStorage(
                        volume='cephfs',
                        path='/macks',
                    ),
                ),
            ],
        },
    ],
)
def test_share_name_in_use(thandler, params):
    thandler.internal_store.overwrite(
        {
            'clusters.c1': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'c1',
                'auth_mode': 'user',
                'intent': 'present',
                'clustering': 'never',
                'user_group_settings': [
                    {
                        'source_type': 'resource',
                        'ref': 'foo1',
                    }
                ],
            },
            'clusters.c2': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'c2',
                'auth_mode': 'user',
                'intent': 'present',
                'clustering': 'never',
                'user_group_settings': [
                    {
                        'source_type': 'resource',
                        'ref': 'foo1',
                    }
                ],
            },
            'users_and_groups.foo1': {
                'resource_type': 'ceph.smb.usersgroups',
                'users_groups_id': 'foo1',
                'intent': 'present',
                'values': {
                    'users': [{"username": "foo"}],
                    'groups': [],
                },
            },
            'shares.c1.alpha': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'c1',
                'share_id': 'alpha',
                'intent': 'present',
                'name': 'Alphabet Soup',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/alpha',
                    'provider': 'samba-vfs',
                },
            },
            'shares.c1.beta': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'c1',
                'share_id': 'beta',
                'intent': 'present',
                'name': 'Beta Max',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/beta',
                    'provider': 'samba-vfs',
                },
            },
            'shares.c1.gamma': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'c1',
                'share_id': 'gamma',
                'intent': 'present',
                'name': 'Gamma Raise',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/gamma',
                    'provider': 'samba-vfs',
                },
            },
            'shares.c2.soup': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'c2',
                'share_id': 'soup',
                'intent': 'present',
                'name': 'Alphabet Soup',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/soup',
                    'provider': 'samba-vfs',
                },
            },
            'shares.c2.salad': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'c2',
                'share_id': 'salad',
                'intent': 'present',
                'name': 'Word Salad',
                'readonly': False,
                'browseable': True,
                'cephfs': {
                    'volume': 'cephfs',
                    'path': '/salad',
                    'provider': 'samba-vfs',
                },
            },
        }
    )

    results = thandler.apply(params['to_apply'])
    rs = results.to_simplified()
    if not params.get('error_msg'):
        assert results.success
        return
    assert not results.success
    assert params['error_msg'] in rs['results'][0]['msg']
    assert rs['results'][0]['conflicting_share_id'] in params['conflicts']
