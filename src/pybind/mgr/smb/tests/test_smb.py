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
        earmark_resolver=smb.handler._FakeEarmarkResolver(),
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


def test_show_password_filter_hidden(tmodule):
    _example_cfg_1(tmodule)
    out = tmodule.show(password_filter=smb.enums.PasswordFilter.HIDDEN)
    assert 'resources' in out
    res = out['resources']
    assert len(res) == 4
    ja = [r for r in res if r['resource_type'] == 'ceph.smb.join.auth']
    assert ja
    join_auth = ja[0]
    assert join_auth['auth']['username'] == 'testadmin'
    assert join_auth['auth']['password'] == '****************'


def test_show_password_filter_b64(tmodule):
    _example_cfg_1(tmodule)
    out = tmodule.show(password_filter=smb.enums.PasswordFilter.BASE64)
    assert 'resources' in out
    res = out['resources']
    assert len(res) == 4
    ja = [r for r in res if r['resource_type'] == 'ceph.smb.join.auth']
    assert ja
    join_auth = ja[0]
    assert join_auth['auth']['username'] == 'testadmin'
    assert join_auth['auth']['password'] == 'UGFzc3cwcmQ='


def test_apply_password_filter(tmodule):
    _example_cfg_1(tmodule)

    txt = json.dumps(
        {
            'resource_type': 'ceph.smb.usersgroups',
            'users_groups_id': 'ug1',
            'intent': 'present',
            'values': {
                'users': [
                    {'username': 'foo', 'password': 'YWJyYWNhZGFicmE='},
                    {'username': 'bar', 'password': 'eHl6enk='},
                ],
                'groups': [],
            },
        }
    )

    rg = tmodule.apply_resources(
        txt, password_filter=smb.enums.InputPasswordFilter.BASE64
    )
    assert rg.success, rg.to_simplified()
    ts = rg.to_simplified()
    assert len(ts['results']) == 1
    r = ts['results'][0]['resource']
    assert r['resource_type'] == 'ceph.smb.usersgroups'
    assert len(r['values']['users']) == 2
    # filtered passwords of command output should match input by default
    assert r['values']['users'][0]['password'] == 'YWJyYWNhZGFicmE='
    assert r['values']['users'][1]['password'] == 'eHl6enk='

    # get unfiltered object
    out = tmodule.show(['ceph.smb.usersgroups.ug1'])
    assert out['resource_type'] == 'ceph.smb.usersgroups'
    assert len(out['values']['users']) == 2
    assert out['values']['users'][0]['password'] == 'abracadabra'
    assert out['values']['users'][1]['password'] == 'xyzzy'


def test_apply_password_filter_in_out(tmodule):
    _example_cfg_1(tmodule)

    txt = json.dumps(
        {
            'resource_type': 'ceph.smb.usersgroups',
            'users_groups_id': 'ug1',
            'intent': 'present',
            'values': {
                'users': [
                    {'username': 'foo', 'password': 'YWJyYWNhZGFicmE='},
                    {'username': 'bar', 'password': 'eHl6enk='},
                ],
                'groups': [],
            },
        }
    )

    rg = tmodule.apply_resources(
        txt,
        password_filter=smb.enums.InputPasswordFilter.BASE64,
        password_filter_out=smb.enums.PasswordFilter.HIDDEN,
    )
    assert rg.success, rg.to_simplified()
    ts = rg.to_simplified()
    assert len(ts['results']) == 1
    r = ts['results'][0]['resource']
    assert r['resource_type'] == 'ceph.smb.usersgroups'
    assert len(r['values']['users']) == 2
    # filtered passwords of command output should match input by default
    assert r['values']['users'][0]['password'] == '****************'
    assert r['values']['users'][1]['password'] == '****************'

    # get unfiltered object
    out = tmodule.show(['ceph.smb.usersgroups.ug1'])
    assert out['resource_type'] == 'ceph.smb.usersgroups'
    assert len(out['values']['users']) == 2
    assert out['values']['users'][0]['password'] == 'abracadabra'
    assert out['values']['users'][1]['password'] == 'xyzzy'


cert1 = """
-----BEGIN CERTIFICATE-----
MIIGFjCCA/6gAwIBAgIUZLL4QTx5ESBYQYS761DcZ7S1c24wDQYJKoZIhvcNAQEN
BQAwgYgxCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJNQTEPMA0GA1UEBwwGTG93ZWxs
MR8wHQYDVQQKDBZCaXJjaCBTdHJlZXQgQ29tcHV0aW5nMRYwFAYDVQQDDA1Kb2hu
IE11bGxpZ2FuMSIwIAYJKoZIhvcNAQkBFhNqb2hubUBhc3luY2hyb25vLnVzMB4X
DTI1MDYzMDE5MzAwM1oXDTI2MDcyNDE5MzAwM1owbjELMAkGA1UEBhMCVVMxCzAJ
BgNVBAgMAk1BMQ8wDQYDVQQHDAZMb3dlbGwxHzAdBgNVBAoMFkJpcmNoIFN0cmVl
dCBDb21wdXRpbmcxIDAeBgNVBAMMF0ROUzpjZXBoMC5jeC5mZG9wZW4ubmV0MIIC
IjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEApZYqA73a8ojX7QsCJHiXh0J2
KKEqDU6k0Yjoie9raYCP/aaiJpffSjhKl1rYuIqjBUG5D0tdT3sRw3m96Nw6gkhM
5J8r02muQpJqmzPmfAn75IVjRkJ9OsHyS1Mf9GADTfv3pMBkwqqrGb8NxWQXeS4s
PLPBv8SI4ozFNwwlEvZ0kesI4Qf0VRZ1ieSzAArjDWWFX8kURMt6UzN8opnxGvzT
cfY4J0iKCYBK6Vqmf/OrMg3IjDojKaQqBlMPAQURyiYeF1hfDrcqGQC6S4Iz5mDt
ZsMywFQFlEhkWkhJdMMkY4bqvn01BKXl3WY0HY5pPslRWWfj4aQeBb8DFH+rFeTf
I/S02ECE/SKc+O7JJa23HtzJspiaK/MV6XQUDDWYdFQEfLhQb3y3RuYJ7C0WZDMc
EmJHuB1D0/RS5xWiukTyRbOFf0Dbzn07PPUycE5BaCJ/ekwpMBvYQ6uCZq19CRAE
v5j7oyC1+rjOCKpTBPGCFWbODJmf5LrfcZLX/VtR+vu3a28OKmbxvdQ3uzLPwjFx
szzsJRn4URyI5hxl3K0w5Yptd/mvdnSeQTnX9TmMFE/G+EdlGxZtc695mOvWX6gK
ezwSqwtxVAZ18x/we6NZUkeuaC4+Xec8HoowHYmfRUH1P69ZXAuKKSIZizuvDYIF
tfcDeDY6s0wp3SKQ1bUCAwEAAaOBkDCBjTAJBgNVHRMEAjAAMEAGA1UdEQQ5MDeC
E2NlcGgwLmN4LmZkb3Blbi5uZXSCGnJjLnNtYi5jZXBoMC5jeC5mZG9wZW4ubmV0
hwTAqEzIMB0GA1UdDgQWBBR9bOCw+6pMkeS1HnAuCFhmoM7NPzAfBgNVHSMEGDAW
gBRsCQk9OUjWypZgtyH+5LxzZ4eBJDANBgkqhkiG9w0BAQ0FAAOCAgEAF6u76+6C
JkQEqBSYU09JQT8JDWX3AUZDXoCIpv2F1UD26ueAIaYD1dkpKDFg0UOOBwC7TiR9
uf210HtY5ic++Bm5xavhRk65FGwypv65SqjfehqTiRU+b0my0LG2OaAVrUcKWdbn
ZvwiBr1I7Wyn0MV1Ko7sqZh0j7Y4kPXCa2D8QG1inr9YBQQpid7CUwNeS5eYAVbP
gI4zTYKKvJMYPr4lTqsweCDOpctC7fwVb43XGTRVhVXQdOux9n5emROx/Ok0d2xX
mi5rlUfMxlrWjs7KK336x8z31i3w+1xc1ESaF0eP1byikvpbYBN1dEYahilMaSVl
3IpDYCwCFMU+ZZUZdqVyQQL+lTxqsc2orzzFgfv594hkEdYNlJ/z/f1b8idYk3g1
WTrixgd+KYcHoCCS8pHFVs8lankBqQGMckZmIyzfP7RxY43j6XTV+4791O4o0waZ
I5AwhUmgJj7G2Mp1jacMlHtZPqC0iDlci5fh6KpzVjPzrqA2sIN+9yJAlX8teJnC
adKnxoY+AbqwLLTHfGx/W0W8jUxmea0eYufgUqxoQv5qdREafcuchGM36bKukVYb
L3pqleYyvguwxxcc2MJvXjgAiZ5EsNJ2TCr4Mt0mZP406BhEQxfvpBSdRXTiHGJ/
KNDwOknnnEhdXshW5M8G8ZhkahG8YABHTBw=
-----END CERTIFICATE-----
"""


def test_tls_credential(tmodule):
    _example_cfg_1(tmodule)

    txt = json.dumps(
        {
            'resource_type': 'ceph.smb.tls.credential',
            'tls_credential_id': 'tc1',
            'intent': 'present',
            'credential_type': 'cert',
            'value': cert1,
        }
    )

    rg = tmodule.apply_resources(txt)
    assert rg.success, rg.to_simplified()
    ts = rg.to_simplified()
    assert len(ts['results']) == 1
    r = ts['results'][0]['resource']
    assert r['resource_type'] == 'ceph.smb.tls.credential'
    out = tmodule.show()
    res = out.get('resources')
    assert res
    assert len(res) == 5
    clusters = [r for r in res if r['resource_type'] == 'ceph.smb.cluster']
    assert len(clusters) == 1
    shares = [r for r in res if r['resource_type'] == 'ceph.smb.share']
    assert len(shares) == 2
    jauths = [r for r in res if r['resource_type'] == 'ceph.smb.join.auth']
    assert len(jauths) == 1
    tcs = [r for r in res if r['resource_type'] == 'ceph.smb.tls.credential']
    assert len(tcs) == 1
    assert tcs[0]['credential_type'] == 'cert'
    assert tcs[0]['value'] == cert1


def test_tls_credential_yaml_show(tmodule):
    _example_cfg_1(tmodule)

    txt = json.dumps(
        {
            'resource_type': 'ceph.smb.tls.credential',
            'tls_credential_id': 'tc1',
            'intent': 'present',
            'credential_type': 'cert',
            'value': cert1,
        }
    )

    rg = tmodule.apply_resources(txt)
    assert rg.success, rg.to_simplified()
    ts = rg.to_simplified()
    assert len(ts['results']) == 1
    r = ts['results'][0]['resource']
    assert r['resource_type'] == 'ceph.smb.tls.credential'
    res, body, status = tmodule.show.command(
        ['ceph.smb.tls.credential'], format='yaml'
    )
    assert res == 0
    body = body.strip()
    assert 'value: |' in body


def _keybridge_example():
    return [
        {
            'resource_type': 'ceph.smb.tls.credential',
            'tls_credential_id': 'cert1',
            'intent': 'present',
            'credential_type': 'cert',
            'value': cert1,
        },
        {
            'resource_type': 'ceph.smb.tls.credential',
            'tls_credential_id': 'key1',
            'intent': 'present',
            'credential_type': 'key',
            'value': cert1,
        },
        {
            'resource_type': 'ceph.smb.tls.credential',
            'tls_credential_id': 'cacert1',
            'intent': 'present',
            'credential_type': 'ca-cert',
            'value': cert1,
        },
        {
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
            "keybridge": {
                "scopes": [
                    {"name": "mem"},
                    {
                        "name": "kmip",
                        "kmip_hosts": ["zorg.example.net"],
                        "kmip_port": 78989,
                        "kmip_cert": {"ref": "cert1"},
                        "kmip_key": {"ref": "key1"},
                        "kmip_ca_cert": {"ref": "cacert1"},
                    },
                ],
            },
        },
        {
            'resource_type': 'ceph.smb.join.auth',
            'auth_id': 'foo',
            'intent': 'present',
            'auth': {
                'username': 'testadmin',
                'password': 'Passw0rd',
            },
        },
        {
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
                "fscrypt_key": {
                    "scope": "mem",
                    "name": "bob",
                },
            },
        },
    ]


def test_keybridge_config(tmodule):
    txt = json.dumps(_keybridge_example())

    rg = tmodule.apply_resources(txt)
    assert rg.success, rg.to_simplified()


@pytest.mark.parametrize(
    "params",
    [
        dict(scopes=[{'name': 'joe'}], expected='invalid scope type'),
        dict(scopes=[{'name': 'mem.joe'}], expected='invalid scope name'),
        dict(scopes=[{'name': 'kmip.00'}], expected='invalid scope name'),
        dict(scopes=[{'name': 'kmip.'}], expected='invalid scope name'),
        dict(scopes=[{'name': 'kmip._'}], expected='invalid scope name'),
        dict(scopes=[], enabled=True, expected='at least one scope'),
        dict(scopes=[{'name': 'kmip'}], expected='kmip hostname'),
        dict(
            scopes=[{'name': 'kmip', 'kmip_hosts': ['foo.example.org']}],
            expected='kmip default port',
        ),
        dict(
            scopes=[
                {
                    'name': 'kmip',
                    'kmip_hosts': ['foo.example.org'],
                    'kmip_port': 67890,
                }
            ],
            expected='cert',
        ),
        dict(
            scopes=[
                {
                    'name': 'mem',
                    'kmip_hosts': ['foo.example.org'],
                    'kmip_port': 67890,
                }
            ],
            expected='mem',
        ),
    ],
)
def test_keybridge_config_scope_error(tmodule, params):
    example = _keybridge_example()
    if enabled := params.get('enabled'):
        example[3]['keybridge']['enabled'] = enabled
    example[3]['keybridge']['scopes'] = params['scopes']
    txt = json.dumps(example)

    rg = tmodule.apply_resources(txt)
    assert not rg.success, rg.to_simplified()
    failures = [r for r in rg if not r.success]
    assert len(failures) == 1
    failure = failures[0]
    assert params['expected'] in failure.msg


@pytest.mark.parametrize(
    "params",
    [
        dict(fkey={'scope': 'mem', 'name': ''}, expected='valid'),
        dict(fkey={'scope': 'mem', 'name': '-'}, expected='valid'),
        dict(fkey={'scope': '-', 'name': 'foo'}, expected='valid'),
        dict(
            fkey={'scope': 'mim', 'name': 'foo'},
            expected='invalid scope type',
        ),
        dict(
            fkey={'scope': 'mem.bob', 'name': 'foo'},
            expected='invalid scope name',
        ),
        dict(
            fkey={'scope': 'kmip.-', 'name': 'foo'},
            expected='invalid scope name',
        ),
        dict(
            fkey={'scope': 'kmip.foo', 'name': 'foo'},
            expected='scope name not known',
        ),
    ],
)
def test_share_fscrypt_config_error(tmodule, params):
    example = _keybridge_example()
    example[-1]['cephfs']['fscrypt_key'] = params['fkey']
    txt = json.dumps(example)

    rg = tmodule.apply_resources(txt)
    assert not rg.success, rg.to_simplified()
    failures = [r for r in rg if not r.success]
    assert len(failures) == 1
    failure = failures[0]
    assert params['expected'] in failure.msg
