import pytest

import smb.resourcelib
import smb.resources
from smb import enums


@pytest.mark.parametrize(
    "params",
    [
        # minimal share (removed)
        {
            'data': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'fakecluster1',
                'share_id': 'myshare1',
                'intent': 'removed',
            },
            'expected': [
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'fakecluster1',
                    'share_id': 'myshare1',
                    'intent': 'removed',
                }
            ],
        },
        # present share
        {
            'data': {
                'resource_type': 'ceph.smb.share',
                'cluster_id': 'fakecluster2',
                'share_id': 'myshare1',
                'intent': 'present',
                'browseable': False,
                'cephfs': {
                    'volume': 'cephfs',
                },
            },
            'expected': [
                {
                    'resource_type': 'ceph.smb.share',
                    'cluster_id': 'fakecluster2',
                    'share_id': 'myshare1',
                    'intent': 'present',
                    'name': 'myshare1',
                    'browseable': False,
                    'readonly': False,
                    'cephfs': {
                        'volume': 'cephfs',
                        'path': '/',
                        'provider': 'samba-vfs',
                    },
                }
            ],
        },
        # removed cluster
        {
            'data': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'nocluster',
                'intent': 'removed',
            },
            'expected': [
                {
                    'resource_type': 'ceph.smb.cluster',
                    'cluster_id': 'nocluster',
                    'intent': 'removed',
                }
            ],
        },
        # cluster
        {
            'data': {
                'resource_type': 'ceph.smb.cluster',
                'cluster_id': 'nocluster',
                'auth_mode': 'active-directory',
                'domain_settings': {
                    'realm': 'FAKE.DOMAIN.TEST',
                    'join_sources': [
                        {'source_type': 'resource', 'ref': 'mydomauth1'},
                    ],
                },
            },
            'expected': [
                {
                    'resource_type': 'ceph.smb.cluster',
                    'cluster_id': 'nocluster',
                    'intent': 'present',
                    'auth_mode': 'active-directory',
                    'domain_settings': {
                        'realm': 'FAKE.DOMAIN.TEST',
                        'join_sources': [
                            {'source_type': 'resource', 'ref': 'mydomauth1'},
                        ],
                    },
                }
            ],
        },
    ],
)
def test_load_simplify_resources(params):
    data = params.get('data')
    loaded = smb.resourcelib.load(data)
    # test round tripping because asserting equality on the
    # objects is not simple
    sdata = [obj.to_simplified() for obj in loaded]
    assert params['expected'] == sdata


YAML1 = """
resource_type: ceph.smb.cluster
cluster_id: chacha
auth_mode: active-directory
domain_settings:
  realm: CEPH.SINK.TEST
  join_sources:
    - source_type: resource
      ref: bob
    - source_type: password
      auth:
        username: Administrator
        password: fallb4kP4ssw0rd
---
resource_type: ceph.smb.share
cluster_id: chacha
share_id: s1
cephfs:
  volume: cephfs
  path: /
---
resource_type: ceph.smb.share
cluster_id: chacha
share_id: s2
name: My Second Share
cephfs:
  volume: cephfs
  subvolume: cool/beans
---
resource_type: ceph.smb.share
cluster_id: chacha
share_id: s0
intent: removed
# deleted this test share
---
resource_type: ceph.smb.join.auth
auth_id: bob
values:
  username: BobTheAdmin
  password: someJunkyPassw0rd
---
resource_type: ceph.smb.join.auth
auth_id: alice
intent: removed
# alice left the company
"""


def test_load_yaml_resource_yaml1():
    import yaml

    loaded = smb.resourcelib.load(yaml.safe_load_all(YAML1))
    assert len(loaded) == 6

    assert isinstance(loaded[0], smb.resources.Cluster)
    cluster = loaded[0]
    assert cluster.cluster_id == 'chacha'
    assert cluster.intent == enums.Intent.PRESENT
    assert cluster.auth_mode == enums.AuthMode.ACTIVE_DIRECTORY
    assert cluster.domain_settings.realm == 'CEPH.SINK.TEST'
    assert len(cluster.domain_settings.join_sources) == 2
    jsrc = cluster.domain_settings.join_sources
    assert jsrc[0].source_type == enums.JoinSourceType.RESOURCE
    assert jsrc[0].ref == 'bob'
    assert jsrc[1].source_type == enums.JoinSourceType.PASSWORD
    assert jsrc[1].auth.username == 'Administrator'
    assert jsrc[1].auth.password == 'fallb4kP4ssw0rd'

    assert isinstance(loaded[1], smb.resources.Share)
    assert isinstance(loaded[2], smb.resources.Share)
    assert isinstance(loaded[3], smb.resources.RemovedShare)
    assert isinstance(loaded[4], smb.resources.JoinAuth)
    assert isinstance(loaded[5], smb.resources.JoinAuth)


YAML2 = """
resource_type: ceph.smb.cluster
cluster_id: rhumba
auth_mode: user
user_group_settings:
  - source_type: resource
    ref: rhumbausers
custom_global_config:
  "hostname lookups": yes
placement:
  hosts:
    - cephnode0
    - cephnode2
    - cephnode4
---
resource_type: ceph.smb.share
cluster_id: rhumba
share_id: us1
name: User Share 1
cephfs:
  volume: cephfs
  path: /share1
  subvolumegroup: sg1
  subvolume: chevron
---
resource_type: ceph.smb.share
cluster_id: rhumba
share_id: us2
name: Useful Stuff
cephfs:
  volume: volume2
  subvolume: foo/bar
  path: /things/and/stuff
custom_config:
  "hosts allow": "adminbox"
---
# the 'nope' share should not exist
resource_type: ceph.smb.share
cluster_id: rhumba
share_id: nope
intent: removed
---
resource_type: ceph.smb.usersgroups
users_groups_id: rhumbausers
intent: present
values:
  users:
    - name: charlie
      password: 7unaF1sh
    - name: lucky
      password: CH4rmz
    - name: jgg
      password: h0H0h0_gg
  groups:
    - name: mascots
"""


def test_load_yaml_resource_yaml2():
    import yaml

    loaded = smb.resourcelib.load(yaml.safe_load_all(YAML2))
    assert len(loaded) == 5

    assert isinstance(loaded[0], smb.resources.Cluster)
    assert isinstance(loaded[1], smb.resources.Share)
    assert isinstance(loaded[2], smb.resources.Share)
    assert isinstance(loaded[3], smb.resources.RemovedShare)
    assert isinstance(loaded[4], smb.resources.UsersAndGroups)


@pytest.mark.parametrize(
    "params",
    [
        # too many slashes in subvolumegroup
        {
            "yaml": """
resource_type: ceph.smb.share
cluster_id: blat
share_id: bs1
name: Bad Share 1
cephfs:
  volume: cephfs
  path: /share1
  subvolumegroup: foo/bar
  subvolume: baz
""",
            "exc_type": ValueError,
            "error": "invalid subvolumegroup",
        },
        # too many slashes in subvolume
        {
            "yaml": """
resource_type: ceph.smb.share
cluster_id: blat
share_id: bs1
name: Bad Share 1
cephfs:
  volume: cephfs
  path: /share1
  subvolumegroup: foo
  subvolume: baz/qqqqq
""",
            "exc_type": ValueError,
            "error": "invalid subvolume",
        },
        # too many slashes in subvolume (autosplit)
        {
            "yaml": """
resource_type: ceph.smb.share
cluster_id: blat
share_id: bs1
name: Bad Share 1
cephfs:
  volume: cephfs
  path: /share1
  subvolume: foo/baz/qqqqq
""",
            "exc_type": ValueError,
            "error": "invalid subvolume",
        },
        # missing volume value
        {
            "yaml": """
resource_type: ceph.smb.share
cluster_id: blat
share_id: bs1
name: Bad Share 1
cephfs:
  volume: ""
  path: /share1
  subvolume: foo
""",
            "exc_type": ValueError,
            "error": "volume",
        },
        # missing cluster_id value
        {
            "yaml": """
resource_type: ceph.smb.share
cluster_id: ""
share_id: whee
name: Bad Share 1
cephfs:
  volume: abc
  path: /share1
  subvolume: foo
""",
            "exc_type": ValueError,
            "error": "cluster_id",
        },
        # missing share_id value
        {
            "yaml": """
resource_type: ceph.smb.share
cluster_id: blat
share_id: ""
name: Bad Share 1
cephfs:
  volume: abc
  path: /share1
  subvolume: foo
""",
            "exc_type": ValueError,
            "error": "share_id",
        },
        # missing cluster settings
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: narf
intent: present
""",
            "exc_type": smb.resourcelib.MissingRequiredFieldError,
            "error": None,
        },
        # missing cluster_id
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: ""
auth_mode: active-directory
intent: present
""",
            "exc_type": ValueError,
            "error": "cluster_id",
        },
        # missing domain settings
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: active-directory
domain_settings:
""",
            "exc_type": ValueError,
            "error": "active directory",
        },
        # extra user/group settings
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: active-directory
domain_settings:
  realm: CEPH.SINK.TEST
  join_sources: []
user_group_settings:
  - source_type: resource
    ref: rhumbausers
""",
            "exc_type": ValueError,
            "error": "not supported",
        },
        # missing user/group settings
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: user
""",
            "exc_type": ValueError,
            "error": "required",
        },
        # extra domain settings
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: user
user_group_settings:
  - source_type: resource
    ref: rhumbausers
domain_settings:
  realm: CEPH.SINK.TEST
  join_sources: []
""",
            "exc_type": ValueError,
            "error": "not supported",
        },
        # u/g inline missing
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: user
user_group_settings:
  - source_type: inline
""",
            "exc_type": ValueError,
            "error": "requires values",
        },
        # u/g inline extra uri
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: user
user_group_settings:
  - source_type: inline
    values:
      users: []
      groups: []
    uri: http://foo.bar.example.com/baz.txt
""",
            "exc_type": ValueError,
            "error": "does not take",
        },
        # u/g inline extra ref
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: user
user_group_settings:
  - source_type: inline
    values:
      users: []
      groups: []
    ref: xyz
""",
            "exc_type": ValueError,
            "error": "does not take",
        },
        # u/g uri missing
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: user
user_group_settings:
  - source_type: http_uri
""",
            "exc_type": ValueError,
            "error": "requires",
        },
        # u/g uri extra values
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: user
user_group_settings:
  - source_type: http_uri
    values:
      users: []
      groups: []
    uri: http://foo.bar.example.com/baz.txt
""",
            "exc_type": ValueError,
            "error": "does not take",
        },
        # u/g uri extra ref
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: user
user_group_settings:
  - source_type: http_uri
    uri: http://boop.example.net
    ref: xyz
""",
            "exc_type": ValueError,
            "error": "does not take",
        },
        # u/g resource missing
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: user
user_group_settings:
  - source_type: resource
""",
            "exc_type": ValueError,
            "error": "requires",
        },
        # u/g resource extra values
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: user
user_group_settings:
  - source_type: resource
    ref: xyz
    uri: http://example.net/foo
""",
            "exc_type": ValueError,
            "error": "does not take",
        },
        # u/g resource extra resource
        {
            "yaml": """
resource_type: ceph.smb.cluster
cluster_id: randolph
intent: present
auth_mode: user
user_group_settings:
  - source_type: resource
    ref: xyz
    values:
      users: []
      groups: []
""",
            "exc_type": ValueError,
            "error": "does not take",
        },
    ],
)
def test_load_error(params):
    import yaml

    data = yaml.safe_load_all(params['yaml'])
    with pytest.raises(params['exc_type'], match=params['error']):
        smb.resourcelib.load(data)


def test_cluster_placement_1():
    import yaml

    yaml_str = """
resource_type: ceph.smb.cluster
cluster_id: rhumba
auth_mode: user
user_group_settings:
  - source_type: resource
    ref: rhumbausers
custom_global_config:
  "hostname lookups": yes
placement:
  hosts:
    - cephnode0
    - cephnode2
    - cephnode4
"""
    data = yaml.safe_load_all(yaml_str)
    loaded = smb.resources.load(data)
    assert loaded
    cluster = loaded[0]
    assert cluster.placement is not None
    assert len(cluster.placement.hosts) == 3

    sd = cluster.to_simplified()
    assert sd
    assert 'placement' in sd
    assert sd['placement'] == {
        'hosts': ['cephnode0', 'cephnode2', 'cephnode4']
    }


def test_cluster_placement_2():
    import yaml

    yaml_str = """
resource_type: ceph.smb.cluster
cluster_id: rhumba
auth_mode: user
user_group_settings:
  - source_type: resource
    ref: rhumbausers
custom_global_config:
  "hostname lookups": yes
placement:
  count: 3
  label: ilovesmb
"""
    data = yaml.safe_load_all(yaml_str)
    loaded = smb.resources.load(data)
    assert loaded
    cluster = loaded[0]
    assert cluster.placement is not None
    assert len(cluster.placement.hosts) == 0
    assert cluster.placement.label == 'ilovesmb'
    assert cluster.placement.count == 3

    sd = cluster.to_simplified()
    assert sd
    assert 'placement' in sd
    assert sd['placement'] == {'count': 3, 'label': 'ilovesmb'}
