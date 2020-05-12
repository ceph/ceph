import os
import pytest
import yaml

from mock import patch, Mock

from teuthology.task import install


class TestInstall(object):

    def _get_default_package_list(self, project='ceph', debug=False):
        path = os.path.join(
            os.path.dirname(__file__),
            '..', '..', 'task', 'install', 'packages.yaml',
        )
        pkgs = yaml.safe_load(open(path))[project]
        if not debug:
            pkgs['deb'] = [p for p in pkgs['deb']
                                        if not p.endswith('-dbg')]
            pkgs['rpm'] = [p for p in pkgs['rpm']
                                        if not p.endswith('-debuginfo')]
        return pkgs

    def test_get_package_list_debug(self):
        default_pkgs = self._get_default_package_list(debug=True)
        default_pkgs['rpm'].sort()
        default_pkgs['deb'].sort()
        config = dict(debuginfo=True)
        result = install.get_package_list(ctx=None, config=config)
        result['rpm'].sort()
        result['deb'].sort()
        assert result == default_pkgs

    def test_get_package_list_no_debug(self):
        default_pkgs = self._get_default_package_list(debug=False)
        default_pkgs['rpm'].sort()
        default_pkgs['deb'].sort()
        config = dict(debuginfo=False)
        result = install.get_package_list(ctx=None, config=config)
        result['rpm'].sort()
        result['deb'].sort()
        assert result == default_pkgs

    def test_get_package_list_custom_rpm(self):
        default_pkgs = self._get_default_package_list(debug=False)
        default_pkgs['rpm'].sort()
        default_pkgs['deb'].sort()
        rpms = ['rpm1', 'rpm2', 'rpm2-debuginfo']
        config = dict(packages=dict(rpm=rpms))
        result = install.get_package_list(ctx=None, config=config)
        result['rpm'].sort()
        result['deb'].sort()
        assert result['rpm'] == ['rpm1', 'rpm2']
        assert result['deb'] == default_pkgs['deb']

    @patch("teuthology.task.install._get_builder_project")
    @patch("teuthology.task.install.packaging.get_package_version")
    def test_get_upgrade_version(self, m_get_package_version,
                                         m_gitbuilder_project):
        gb = Mock()
        gb.version = "11.0.0"
        gb.project = "ceph"
        m_gitbuilder_project.return_value = gb
        m_get_package_version.return_value = "11.0.0"
        install.get_upgrade_version(Mock(), Mock(), Mock())

    @patch("teuthology.task.install._get_builder_project")
    @patch("teuthology.task.install.packaging.get_package_version")
    def test_verify_ceph_version_success(self, m_get_package_version,
                                         m_gitbuilder_project):
        gb = Mock()
        gb.version = "0.89.0"
        gb.project = "ceph"
        m_gitbuilder_project.return_value = gb
        m_get_package_version.return_value = "0.89.0"
        config = dict()
        install.verify_package_version(Mock(), config, Mock())

    @patch("teuthology.task.install._get_builder_project")
    @patch("teuthology.task.install.packaging.get_package_version")
    def test_verify_ceph_version_failed(self, m_get_package_version,
                                        m_gitbuilder_project):
        gb = Mock()
        gb.version = "0.89.0"
        gb.project = "ceph"
        m_gitbuilder_project.return_value = gb
        m_get_package_version.return_value = "0.89.1"
        config = dict()
        with pytest.raises(RuntimeError):
            install.verify_package_version(Mock(), config, Mock())

    @patch("teuthology.task.install._get_builder_project")
    @patch("teuthology.task.install.packaging.get_package_version")
    def test_skip_when_using_ceph_deploy(self, m_get_package_version,
                                         m_gitbuilder_project):
        gb = Mock()
        gb.version = "0.89.0"
        gb.project = "ceph"
        m_gitbuilder_project.return_value = gb
        # ceph isn't installed because ceph-deploy would install it
        m_get_package_version.return_value = None
        config = dict()
        config['extras'] = True
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

    def test_upgrade_is_downgrade(self):
        assert_ok_vals = [
            ('9.0.0', '10.0.0'),
            ('10.2.2-63-g8542898-1trusty', '10.2.2-64-gabcdef1-1trusty'),
            ('11.0.0-918.g13c13c7', '11.0.0-2165.gabcdef1')
        ]
        for t in assert_ok_vals:
            assert install._upgrade_is_downgrade(t[0], t[1]) == False

    @patch("teuthology.packaging.get_package_version")
    @patch("teuthology.misc.get_system_type")
    @patch("teuthology.task.install.verify_package_version")
    @patch("teuthology.task.install.get_upgrade_version")
    def test_upgrade_common(self,
                            m_get_upgrade_version,
                            m_verify_package_version,
                            m_get_system_type,
                            m_get_package_version):
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
        m_get_upgrade_version.return_value = "11.0.0"
        m_get_package_version.return_value = "10.2.4"
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


    @patch("teuthology.task.install.packaging.get_package_version")
    @patch("teuthology.task.install.redhat.set_deb_repo")
    def test_rh_install_deb_pkgs(self, m_set_rh_deb_repo, m_get_pkg_version):
        ctx = Mock()
        remote = Mock()
        version = '1.3.2'
        rh_ds_yaml = dict()
        rh_ds_yaml = {
            'versions': {'deb': {'mapped': {'1.3.2': '0.94.5'}}},
            'pkgs': {'deb': ['pkg1', 'pkg2']},
            'extra_system_packages': {'deb': ['es_pkg1', 'es_pkg2']},
            'extra_packages': {'deb': ['e_pkg1', 'e_pkg2']},
        }
        m_get_pkg_version.return_value = "0.94.5"
        install.redhat.install_deb_pkgs(ctx, remote, version, rh_ds_yaml)

    @patch("teuthology.task.install.packaging.get_package_version")
    def test_rh_install_pkgs(self, m_get_pkg_version):
        ctx = Mock()
        remote = Mock()
        version = '1.3.2'
        rh_ds_yaml = dict()
        rh_ds_yaml = {
            'versions': {'rpm': {'mapped': {'1.3.2': '0.94.5',
                                            '1.3.1': '0.94.3'}}},
            'pkgs': {'rpm': ['pkg1', 'pkg2']},
            'extra_system_packages': {'rpm': ['es_pkg1', 'es_pkg2']},
            'extra_packages': {'rpm': ['e_pkg1', 'e_pkg2']},
        }

        m_get_pkg_version.return_value = "0.94.5"
        install.redhat.install_pkgs(ctx, remote, version, rh_ds_yaml)
        version = '1.3.1'
        with pytest.raises(RuntimeError) as e:
            install.redhat.install_pkgs(ctx, remote, version, rh_ds_yaml)
        assert "Version check failed" in str(e)
