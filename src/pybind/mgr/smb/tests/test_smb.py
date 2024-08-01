import json

import pytest

import smb


def _cluster(**kwargs):
    if 'clustering' not in kwargs:
        kwargs['clustering'] = smb.enums.SMBClustering.NEVER
    return smb.resources.Cluster(**kwargs)


@pytest.fixture
def tmodule():
    internal_store = smb.config_store.MemConfigStore()
    public_store = smb.config_store.MemConfigStore()
    priv_store = smb.config_store.MemConfigStore()
    return smb.module.Module(
        'smb',
        '',
        '',
        internal_store=internal_store,
        public_store=public_store,
        priv_store=priv_store,
        path_resolver=smb.handler._FakePathResolver(),
        authorizer=smb.handler._FakeAuthorizer(),
        update_orchestration=False,
    )


def test_cluster_ls_empty(tmodule):
    clusters = tmodule.cluster_ls()
    assert clusters == []


def test_share_ls_empty(tmodule):
    clusters = tmodule.share_ls('foo')
    assert clusters == []


def test_internal_apply_cluster(tmodule):
    cluster = _cluster(
        cluster_id='foo',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    rg = tmodule._handler.apply([cluster])
    assert rg.success, rg.to_simplified()
    assert ('clusters', 'foo') in tmodule._internal_store.data


def test_cluster_add_cluster_ls(tmodule):
    cluster = _cluster(
        cluster_id='foo',
        auth_mode=smb.enums.AuthMode.USER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.resources.UserGroupSourceType.EMPTY,
            ),
        ],
    )
    rg = tmodule._handler.apply([cluster])
    assert rg.success, rg.to_simplified()

    clusters = tmodule.cluster_ls()
    assert len(clusters) == 1
    assert 'foo' in clusters


def test_internal_apply_cluster_and_share(tmodule):
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
    rg = tmodule._handler.apply([cluster, share])
    assert rg.success, rg.to_simplified()
    assert ('clusters', 'foo') in tmodule._internal_store.data
    assert ('shares', 'foo.s1') in tmodule._internal_store.data

    shares = tmodule.share_ls('foo')
    assert len(shares) == 1
    assert 's1' in shares


def test_internal_apply_remove_cluster(tmodule):
    tmodule._internal_store.overwrite(
        {
            "clusters.foo": {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'user',
                'intent': 'present',
                'user_group_settings': [
                    {
                        'source_type': 'empty',
                    }
                ],
            }
        }
    )

    clusters = tmodule.cluster_ls()
    assert len(clusters) == 1
    assert 'foo' in clusters

    rmcluster = smb.resources.RemovedCluster(
        cluster_id='foo',
    )
    rg = tmodule._handler.apply([rmcluster])
    assert rg.success, rg.to_simplified()

    clusters = tmodule.cluster_ls()
    assert len(clusters) == 0


def test_internal_apply_remove_shares(tmodule):
    tmodule._internal_store.overwrite(
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

    shares = tmodule.share_ls('foo')
    assert len(shares) == 2
    assert 's1' in shares
    assert 'stwo' in shares

    rmshare1 = smb.resources.RemovedShare(
        cluster_id='foo',
        share_id='s1',
    )
    rg = tmodule._handler.apply([rmshare1])
    assert rg.success, rg.to_simplified()

    shares = tmodule.share_ls('foo')
    assert len(shares) == 1

    rmshare2 = smb.resources.RemovedShare(
        cluster_id='foo',
        share_id='stwo',
    )
    rg = tmodule._handler.apply([rmshare1, rmshare2])
    assert rg.success, rg.to_simplified()

    shares = tmodule.share_ls('foo')
    assert len(shares) == 0

    # check the results
    rgs = rg.to_simplified()
    assert rgs['success']
    assert len(rgs['results']) == 2
    assert rgs['results'][0]['success']
    assert rgs['results'][0]['state'] == 'not present'
    assert rgs['results'][1]['success']
    assert rgs['results'][1]['state'] == 'removed'


def test_internal_apply_add_joinauth(tmodule):
    tmodule._internal_store.overwrite(
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

    assert len(tmodule._handler.join_auth_ids()) == 0
    ja = smb.resources.JoinAuth(
        auth_id='join1',
        auth=smb.resources.JoinAuthValues(
            username='testadmin',
            password='Passw0rd',
        ),
    )
    rg = tmodule._handler.apply([ja])
    assert rg.success, rg.to_simplified()

    assert len(tmodule._handler.join_auth_ids()) == 1


def test_internal_apply_add_usergroups(tmodule):
    tmodule._internal_store.overwrite(
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

    assert len(tmodule._handler.user_and_group_ids()) == 0
    ja = smb.resources.UsersAndGroups(
        users_groups_id='ug1',
        values=smb.resources.UserGroupSettings(
            users=[{"username": "foo"}],
            groups=[],
        ),
    )
    rg = tmodule._handler.apply([ja])
    assert rg.success, rg.to_simplified()

    assert len(tmodule._handler.user_and_group_ids()) == 1


def _example_cfg_1(tmodule):
    tmodule._internal_store.overwrite(
        {
            'clusters.foo': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'foo',
                'auth_mode': 'active-directory',
                'intent': 'present',
                'clustering': 'never',
                'domain_settings': {
                    'realm': 'dom1.example.com',
                    'join_sources': [
                        {
                            'source_type': 'resource',
                            'ref': 'foo',
                        }
                    ],
                },
            },
            'join_auths.foo': {
                'resource_type': 'ceph.smb.join.auth',
                'auth_id': 'foo',
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


def test_cmd_share_ls_json(tmodule):
    _example_cfg_1(tmodule)

    res, body, status = tmodule.share_ls.command('foo')
    assert res == 0
    bdata = json.loads(body)
    assert 's1' in bdata
    assert 'stwo' in bdata
    assert not status


def test_cmd_share_ls_yaml(tmodule):
    _example_cfg_1(tmodule)

    res, body, status = tmodule.share_ls.command('foo', format='yaml')
    assert res == 0
    assert '- s1\n' in body
    assert '- stwo\n' in body
    assert not status


def test_cmd_share_rm(tmodule):
    _example_cfg_1(tmodule)

    res, body, status = tmodule.share_rm.command('foo', 'stwo')
    assert res == 0
    bdata = json.loads(body)
    assert bdata['state'] == 'removed'

    res, body, status = tmodule.share_rm.command('fake', 'curly')
    assert res == 0
    bdata = json.loads(body)
    assert bdata['state'] == 'not present'


def test_cmd_share_create(tmodule):
    _example_cfg_1(tmodule)

    res, body, status = tmodule.share_create.command(
        cluster_id='foo',
        share_id='nu1',
        share_name='The Nu One',
        path='/nu/one',
        cephfs_volume='cephfs',
    )
    assert res == 0
    bdata = json.loads(body)
    assert bdata['state'] == 'created'


def test_cmd_apply_share(tmodule):
    _example_cfg_1(tmodule)

    inbuf = json.dumps(
        {
            'resource_type': 'ceph.smb.share',
            'cluster_id': 'foo',
            'share_id': 'test1',
            'cephfs': {
                'volume': 'example',
                'path': '/primrose',
            },
        }
    )

    res, body, status = tmodule.apply_resources.command(inbuf=inbuf)
    assert res == 0
    bdata = json.loads(body)
    assert bdata["success"]
    assert bdata["results"][0]["state"] == "created"


def test_share_dump_config(tmodule):
    _example_cfg_1(tmodule)

    cfg = tmodule.dump_config('foo')
    assert cfg == {
        'samba-container-config': "v0",
        'configs': {
            'foo': {
                'instance_name': 'foo',
                'instance_features': [],
                'shares': ['Ess One', 'Ess Two'],
                'globals': ['default', 'foo'],
            },
        },
        'shares': {
            'Ess One': {
                'options': {
                    'path': '/',
                    'read only': 'No',
                    'browseable': 'Yes',
                    'kernel share modes': 'no',
                    'x:ceph:id': 'foo.s1',
                    'vfs objects': 'acl_xattr ceph_new',
                    'acl_xattr:security_acl_name': 'user.NTACL',
                    'ceph_new:config_file': '/etc/ceph/ceph.conf',
                    'ceph_new:filesystem': 'cephfs',
                    'ceph_new:user_id': 'smb.fs.cluster.foo',
                },
            },
            'Ess Two': {
                'options': {
                    'path': '/two',
                    'read only': 'No',
                    'browseable': 'Yes',
                    'kernel share modes': 'no',
                    'x:ceph:id': 'foo.stwo',
                    'vfs objects': 'acl_xattr ceph_new',
                    'acl_xattr:security_acl_name': 'user.NTACL',
                    'ceph_new:config_file': '/etc/ceph/ceph.conf',
                    'ceph_new:filesystem': 'cephfs',
                    'ceph_new:user_id': 'smb.fs.cluster.foo',
                },
            },
        },
        'globals': {
            'default': {
                'options': {
                    'load printers': 'No',
                    'printing': 'bsd',
                    'printcap name': '/dev/null',
                    'disable spoolss': 'Yes',
                },
            },
            'foo': {
                'options': {
                    'idmap config * : backend': 'autorid',
                    'idmap config * : range': '2000-9999999',
                    'realm': 'dom1.example.com',
                    'security': 'ads',
                    'workgroup': 'DOM1',
                },
            },
        },
    }


def test_cluster_create_ad1(tmodule):
    _example_cfg_1(tmodule)

    result = tmodule.cluster_create(
        'fizzle',
        smb.enums.AuthMode.ACTIVE_DIRECTORY,
        domain_realm='fizzle.example.net',
        domain_join_user_pass=['Administrator%Passw0rd'],
        clustering='never',
    )
    assert result.success
    assert result.status['state'] == 'created'
    assert result.src.cluster_id == 'fizzle'
    assert result.src.domain_settings.realm == 'fizzle.example.net'
    assert len(result.src.domain_settings.join_sources) == 1
    assert (
        result.src.domain_settings.join_sources[0].source_type
        == smb.enums.JoinSourceType.RESOURCE
    )
    assert result.src.domain_settings.join_sources[0].ref.startswith('fizzle')
    assert 'additional_results' in result.status
    assert len(result.status['additional_results']) == 1
    assert (
        result.status['additional_results'][0]['resource']['resource_type']
        == 'ceph.smb.join.auth'
    )
    assert (
        result.status['additional_results'][0]['resource'][
            'linked_to_cluster'
        ]
        == 'fizzle'
    )
    assert result.status['additional_results'][0]['resource'][
        'auth_id'
    ].startswith('fizzle')


def test_cluster_create_ad2(tmodule):
    _example_cfg_1(tmodule)

    ja = smb.resources.JoinAuth(
        auth_id='jaad2',
        auth=smb.resources.JoinAuthValues(
            username='Administrator',
            password='hunter2',
        ),
    )
    rg = tmodule._handler.apply([ja])
    assert rg.success, rg.to_simplified()

    result = tmodule.cluster_create(
        'sizzle',
        smb.enums.AuthMode.ACTIVE_DIRECTORY,
        domain_realm='sizzle.example.net',
        domain_join_ref=['jaad2'],
        clustering='never',
    )
    assert result.success
    assert result.status['state'] == 'created'
    assert result.src.cluster_id == 'sizzle'
    assert result.src.domain_settings.realm == 'sizzle.example.net'
    assert len(result.src.domain_settings.join_sources) == 1
    assert (
        result.src.domain_settings.join_sources[0].source_type
        == smb.enums.JoinSourceType.RESOURCE
    )
    assert result.src.domain_settings.join_sources[0].ref == 'jaad2'


def test_cluster_create_user1(tmodule):
    _example_cfg_1(tmodule)

    ug = smb.resources.UsersAndGroups(
        users_groups_id='ug1',
        values=smb.resources.UserGroupSettings(
            users=[{"username": "foo"}],
            groups=[],
        ),
    )
    rg = tmodule._handler.apply([ug])
    assert rg.success, rg.to_simplified()

    result = tmodule.cluster_create(
        'dizzle',
        smb.enums.AuthMode.USER,
        user_group_ref=['ug1'],
        clustering='never',
    )
    assert result.success
    assert result.status['state'] == 'created'
    assert result.src.cluster_id == 'dizzle'
    assert len(result.src.user_group_settings) == 1


def test_cluster_create_user2(tmodule):
    _example_cfg_1(tmodule)

    result = tmodule.cluster_create(
        'dizzle',
        smb.enums.AuthMode.USER,
        define_user_pass=['alice%123letmein', 'bob%1n0wh4t1t15'],
        clustering='never',
    )
    assert result.success
    assert result.status['state'] == 'created'
    assert result.src.cluster_id == 'dizzle'
    assert len(result.src.user_group_settings) == 1
    assert (
        result.src.user_group_settings[0].source_type
        == smb.enums.UserGroupSourceType.RESOURCE
    )


def test_cluster_create_badpass(tmodule):
    _example_cfg_1(tmodule)

    with pytest.raises(ValueError):
        tmodule.cluster_create(
            'fizzle',
            smb.enums.AuthMode.ACTIVE_DIRECTORY,
            domain_realm='fizzle.example.net',
            domain_join_user_pass=['Administrator'],
            clustering='never',
        )


def test_cluster_rm(tmodule):
    _example_cfg_1(tmodule)

    result = tmodule.share_rm('foo', 'stwo')
    assert result.success
    result = tmodule.share_rm('foo', 's1')
    assert result.success
    result = tmodule.cluster_rm('foo')
    assert result.success


def test_dump_service_spec(tmodule):
    _example_cfg_1(tmodule)
    tmodule._public_store.overwrite(
        {
            'foo.config.smb': '',
        }
    )
    tmodule._priv_store.overwrite(
        {
            'foo.join.2b9902c05d08bcba.json': '',
            'foo.join.08129d4d3b8c37c7.json': '',
        }
    )

    cfg = tmodule.dump_service_spec('foo')
    assert cfg
    assert cfg['service_id'] == 'foo'
    assert cfg['spec']['cluster_id'] == 'foo'
    assert cfg['spec']['features'] == ['domain']
    assert cfg['spec']['config_uri'] == 'mem:foo/config.smb'
    assert len(cfg['spec']['join_sources']) == 2


def test_cmd_show_resource_json(tmodule):
    _example_cfg_1(tmodule)

    res, body, status = tmodule.show.command(['ceph.smb.cluster.foo'])
    assert res == 0
    assert (
        body.strip()
        == """
{
  "resource_type": "ceph.smb.cluster",
  "cluster_id": "foo",
  "auth_mode": "active-directory",
  "intent": "present",
  "domain_settings": {
    "realm": "dom1.example.com",
    "join_sources": [
      {
        "source_type": "resource",
        "ref": "foo"
      }
    ]
  },
  "clustering": "never"
}
    """.strip()
    )


def test_cmd_show_resource_yaml(tmodule):
    _example_cfg_1(tmodule)

    res, body, status = tmodule.show.command(
        ['ceph.smb.cluster.foo'], format='yaml'
    )
    assert res == 0
    assert (
        body.strip()
        == """
resource_type: ceph.smb.cluster
cluster_id: foo
auth_mode: active-directory
intent: present
domain_settings:
  realm: dom1.example.com
  join_sources:
  - source_type: resource
    ref: foo
clustering: never
""".strip()
    )


def test_apply_invalid_res(tmodule):
    result = tmodule.apply_resources(
        """
resource_type: ceph.smb.cluster
cluster_id: ""
auth_mode: doop
"""
    )
    assert not result.success
    assert 'doop' in result.to_simplified()['results'][0]['msg']


def test_show_all(tmodule):
    _example_cfg_1(tmodule)
    out = tmodule.show()
    assert 'resources' in out
    res = out['resources']
    assert len(res) == 4
    assert {r['resource_type'] for r in res} == {
        'ceph.smb.cluster',
        'ceph.smb.share',
        'ceph.smb.join.auth',
    }


def test_show_shares(tmodule):
    _example_cfg_1(tmodule)
    out = tmodule.show(['ceph.smb.share'])
    assert 'resources' in out
    res = out['resources']
    assert len(res) == 2
    assert {r['resource_type'] for r in res} == {
        'ceph.smb.share',
    }


def test_show_shares_in_cluster(tmodule):
    _example_cfg_1(tmodule)
    out = tmodule.show(['ceph.smb.share.foo'])
    assert 'resources' in out
    res = out['resources']
    assert len(res) == 2
    assert {r['resource_type'] for r in res} == {
        'ceph.smb.share',
    }
    assert {r['cluster_id'] for r in res} == {'foo'}


def test_show_specific_share(tmodule):
    _example_cfg_1(tmodule)
    out = tmodule.show(['ceph.smb.share.foo.s1'])
    assert 'resources' not in out
    assert out['resource_type'] == 'ceph.smb.share'
    assert out['cluster_id'] == 'foo'
    assert out['share_id'] == 's1'


def test_show_nomatches(tmodule):
    _example_cfg_1(tmodule)
    out = tmodule.show(['ceph.smb.share.foo.whoops'])
    assert 'resources' in out
    assert out['resources'] == []


def test_show_invalid_input(tmodule):
    _example_cfg_1(tmodule)
    with pytest.raises(smb.cli.InvalidInputValue):
        tmodule.show(['ceph.smb.export'])


def test_show_cluster_without_shares(tmodule):
    # this cluster will have no shares associated with it
    tmodule._internal_store.overwrite(
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
                            'ref': 'foo',
                        }
                    ],
                },
            },
            'join_auths.foo': {
                'resource_type': 'ceph.smb.join.auth',
                'auth_id': 'foo',
                'intent': 'present',
                'auth': {
                    'username': 'testadmin',
                    'password': 'Passw0rd',
                },
            },
        }
    )

    res, body, status = tmodule.show.command(['ceph.smb.cluster.foo'])
    assert res == 0
    assert (
        body.strip()
        == """
{
  "resource_type": "ceph.smb.cluster",
  "cluster_id": "foo",
  "auth_mode": "active-directory",
  "intent": "present",
  "domain_settings": {
    "realm": "dom1.example.com",
    "join_sources": [
      {
        "source_type": "resource",
        "ref": "foo"
      }
    ]
  }
}
    """.strip()
    )
