# py.test -v -s tests/test_buildpackages.py

from mock import patch, Mock

from .. import buildpackages
from teuthology import packaging

def test_get_tag_branch_sha1():
    gitbuilder = packaging.GitbuilderProject(
        'ceph',
        {
            'os_type': 'centos',
            'os_version': '7.0',
        })
    (tag, branch, sha1) = buildpackages.get_tag_branch_sha1(gitbuilder)
    assert tag == None
    assert branch == None
    assert sha1 is not None

    gitbuilder = packaging.GitbuilderProject(
        'ceph',
        {
            'os_type': 'centos',
            'os_version': '7.0',
            'sha1': 'asha1',
        })
    (tag, branch, sha1) = buildpackages.get_tag_branch_sha1(gitbuilder)
    assert tag == None
    assert branch == None
    assert sha1 == 'asha1'

    remote = Mock
    remote.arch = 'x86_64'
    remote.os = Mock
    remote.os.name = 'ubuntu'
    remote.os.version = '14.04'
    remote.os.codename = 'trusty'
    remote.system_type = 'deb'
    ctx = Mock
    ctx.cluster = Mock
    ctx.cluster.remotes = {remote: ['client.0']}

    expected_tag = 'v0.94.1'
    expected_sha1 = 'expectedsha1'
    def check_output(cmd, shell):
        assert shell == True
        return expected_sha1 + " refs/tags/" + expected_tag
    with patch.multiple(
            buildpackages,
            check_output=check_output,
    ):
        gitbuilder = packaging.GitbuilderProject(
            'ceph',
            {
                'os_type': 'centos',
                'os_version': '7.0',
                'sha1': 'asha1',
                'all': {
                    'tag': tag,
                },
            },
            ctx = ctx,
            remote = remote)
        (tag, branch, sha1) = buildpackages.get_tag_branch_sha1(gitbuilder)
        assert tag == expected_tag
        assert branch == None
        assert sha1 == expected_sha1

    expected_branch = 'hammer'
    expected_sha1 = 'otherexpectedsha1'
    def check_output(cmd, shell):
        assert shell == True
        return expected_sha1 + " refs/heads/" + expected_branch
    with patch.multiple(
            buildpackages,
            check_output=check_output,
    ):
        gitbuilder = packaging.GitbuilderProject(
            'ceph',
            {
                'os_type': 'centos',
                'os_version': '7.0',
                'sha1': 'asha1',
                'all': {
                    'branch': branch,
                },
            },
            ctx = ctx,
            remote = remote)
        (tag, branch, sha1) = buildpackages.get_tag_branch_sha1(gitbuilder)
        assert tag == None
        assert branch == expected_branch
        assert sha1 == expected_sha1

def test_lookup_configs():
    expected_system_type = 'deb'
    def make_remote():
        remote = Mock()
        remote.arch = 'x86_64'
        remote.os = Mock()
        remote.os.name = 'ubuntu'
        remote.os.version = '14.04'
        remote.os.codename = 'trusty'
        remote.system_type = expected_system_type
        return remote
    ctx = Mock()
    class cluster:
        remote1 = make_remote()
        remote2 = make_remote()
        remotes = {
            remote1: ['client.0'],
            remote2: ['mon.a','osd.0'],
        }
        def only(self, role):
            result = Mock()
            if role in ('client.0',):
                result.remotes = { cluster.remote1: None }
            elif role in ('osd.0', 'mon.a'):
                result.remotes = { cluster.remote2: None }
            else:
                result.remotes = None
            return result
    ctx.cluster = cluster()
    ctx.config = {
        'roles': [ ['client.0'], ['mon.a','osd.0'] ],
    }

    # nothing -> nothing
    assert buildpackages.lookup_configs(ctx, {}) == []
    assert buildpackages.lookup_configs(ctx, {1:[1,2,3]}) == []
    assert buildpackages.lookup_configs(ctx, [[1,2,3]]) == []
    assert buildpackages.lookup_configs(ctx, None) == []

    #
    # the overrides applies to install and to install.upgrade
    # that have no tag, branch or sha1
    #
    config = {
        'overrides': {
            'install': {
                'ceph': {
                    'sha1': 'overridesha1',
                    'tag': 'overridetag',
                    'branch': 'overridebranch',
                },
            },
        },
        'tasks': [
            {
                'install': {
                    'sha1': 'installsha1',
                },
            },
            {
                'install.upgrade': {
                    'osd.0': {
                    },
                    'client.0': {
                        'sha1': 'client0sha1',
                    },
                },
            }
        ],
    }
    ctx.config = config
    expected_configs = [{'branch': 'overridebranch', 'sha1': 'overridesha1', 'tag': 'overridetag'},
                        {'project': 'ceph', 'branch': 'overridebranch', 'sha1': 'overridesha1', 'tag': 'overridetag'},
                        {'project': 'ceph', 'sha1': 'client0sha1'}]

    assert buildpackages.lookup_configs(ctx, config) == expected_configs
