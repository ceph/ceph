import os
import pytest
import yaml

from mock import patch, Mock

from teuthology.task import install


class TestInstall(object):

    def _get_default_package_list(self, project='ceph', debug=False):
        path = os.path.join(
            os.path.dirname(__file__),
            '..', '..', 'task', 'packages.yaml',
        )
        pkgs = yaml.safe_load(open(path))[project]
        if not debug:
            pkgs['deb'] = filter(
                lambda p: not p.endswith('-dbg'),
                pkgs['deb']
            )
            pkgs['rpm'] = filter(
                lambda p: not p.endswith('-debuginfo'),
                pkgs['rpm']
            )
        return pkgs

    def test_get_package_list_debug(self):
        default_pkgs = self._get_default_package_list(debug=True)
        config = dict(debuginfo=True)
        result = install.get_package_list(ctx=None, config=config)
        assert result == default_pkgs

    def test_get_package_list_no_debug(self):
        default_pkgs = self._get_default_package_list(debug=False)
        config = dict(debuginfo=False)
        result = install.get_package_list(ctx=None, config=config)
        assert result == default_pkgs

    def test_get_package_list_custom_rpm(self):
        default_pkgs = self._get_default_package_list(debug=False)
        rpms = ['rpm1', 'rpm2', 'rpm2-debuginfo']
        config = dict(packages=dict(rpm=rpms))
        result = install.get_package_list(ctx=None, config=config)
        assert result['rpm'] == ['rpm1', 'rpm2']
        assert result['deb'] == default_pkgs['deb']

    @patch("teuthology.task.install._get_gitbuilder_project")
    @patch("teuthology.task.install.packaging.get_package_version")
    def test_verify_ceph_version_success(self, m_get_package_version,
                                         m_gitbuilder_project):
        gb = Mock()
        gb.version = "0.89.0"
        gb.project = "ceph"
        m_gitbuilder_project.return_value = gb
        m_get_package_version.return_value = "0.89.0"
        install.verify_package_version(Mock(), Mock(), Mock())

    @patch("teuthology.task.install._get_gitbuilder_project")
    @patch("teuthology.task.install.packaging.get_package_version")
    def test_verify_ceph_version_failed(self, m_get_package_version,
                                        m_gitbuilder_project):
        gb = Mock()
        gb.version = "0.89.0"
        gb.project = "ceph"
        m_gitbuilder_project.return_value = gb
        m_get_package_version.return_value = "0.89.1"
        config = Mock()
        # when it looks for config.get('extras') it won't find it
        config.get.return_value = False
        with pytest.raises(RuntimeError):
            install.verify_package_version(Mock(), config, Mock())

    @patch("teuthology.task.install._get_gitbuilder_project")
    @patch("teuthology.task.install.packaging.get_package_version")
    def test_skip_when_using_ceph_deploy(self, m_get_package_version,
                                         m_gitbuilder_project):
        gb = Mock()
        gb.version = "0.89.0"
        gb.project = "ceph"
        m_gitbuilder_project.return_value = gb
        # ceph isn't installed because ceph-deploy would install it
        m_get_package_version.return_value = None
        config = Mock()
        config.extras = True
        install.verify_package_version(Mock(), config, Mock())

    def test_get_flavor_default(self):
        config = dict()
        assert install.get_flavor(config) == 'basic'

    def test_get_flavor_simple(self):
        config = dict(
            flavor='notcmalloc'
        )
        assert install.get_flavor(config) == 'notcmalloc'

    def test_get_flavor_valgrind(self):
        config = dict(
            valgrind=True
        )
        assert install.get_flavor(config) == 'notcmalloc'

    @patch("teuthology.misc.get_system_type")
    @patch("teuthology.task.install.verify_package_version")
    def test_upgrade_common(self,
                            m_verify_package_version,
                            m_get_system_type):
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
                if role in ('osd.0', 'mon.a'):
                    result.remotes = { cluster.remote2: None }
                return result
        ctx.cluster = cluster()
        config = {
            'client.0': {
                'sha1': 'expectedsha1',
            },
        }
        ctx.config = {
            'roles': [ ['client.0'], ['mon.a','osd.0'] ],
            'tasks': [
                {
                    'install.upgrade': config,
                },
            ],
        }
        m_get_system_type.return_value = "deb"
        def upgrade(ctx, node, remote, pkgs, system_type):
            assert system_type == expected_system_type
        assert install.upgrade_common(ctx, config, upgrade) == 1
        expected_config = {
            'project': 'ceph',
            'sha1': 'expectedsha1',
        }
        m_verify_package_version.assert_called_with(ctx,
                                                    expected_config,
                                                    cluster.remote1)
    def test_upgrade_remote_to_config(self):
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
        assert install.upgrade_remote_to_config(ctx, {}) == {}

        # select the remote for the osd.0 role
        # the 'ignored' role does not exist and is ignored
        # the remote for mon.a is the same as for osd.0 and
        # is silently ignored (actually it could be the other
        # way around, depending on how the keys are hashed)
        config = {
            'osd.0': {
                'sha1': 'expectedsha1',
            },
            'ignored': None,
            'mon.a': {
                'sha1': 'expectedsha1',
            },
        }
        expected_config = {
            cluster.remote2: {
                'project': 'ceph',
                'sha1': 'expectedsha1',
            },
        }
        assert install.upgrade_remote_to_config(ctx, config) == expected_config

        # select all nodes, regardless
        config = {
            'all': {
                'sha1': 'expectedsha1',
            },
        }
        expected_config = {
            cluster.remote1: {
                'project': 'ceph',
                'sha1': 'expectedsha1',
            },
            cluster.remote2: {
                'project': 'ceph',
                'sha1': 'expectedsha1',
            },
        }
        assert install.upgrade_remote_to_config(ctx, config) == expected_config

        # verify that install overrides are used as default
        # values for the upgrade task, not as override
        ctx.config['overrides'] = {
            'install': {
                'ceph': {
                    'sha1': 'overridesha1',
                    'tag': 'overridetag',
                    'branch': 'overridebranch',
                },
            },
        }
        config = {
            'client.0': {
                'sha1': 'expectedsha1',
            },
            'osd.0': {
            },
        }
        expected_config = {
            cluster.remote1: {
                'project': 'ceph',
                'sha1': 'expectedsha1',
            },
            cluster.remote2: {
                'project': 'ceph',
                'sha1': 'overridesha1',
                'tag': 'overridetag',
                'branch': 'overridebranch',
            },
        }
        assert install.upgrade_remote_to_config(ctx, config) == expected_config
