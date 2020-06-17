import os
import pytest
import tempfile

from copy import deepcopy
from mock import Mock, patch

from teuthology.config import config
from teuthology.orchestra.opsys import OS
from teuthology.suite import util


REPO_PROJECTS_AND_URLS = [
    'ceph',
    'https://github.com/not_ceph/ceph.git',
]


@pytest.mark.parametrize('project_or_url', REPO_PROJECTS_AND_URLS)
@patch('subprocess.check_output')
def test_git_branch_exists(m_check_output, project_or_url):
    m_check_output.return_value = ''
    assert False == util.git_branch_exists(
        project_or_url, 'nobranchnowaycanthappen')
    m_check_output.return_value = b'HHH branch'
    assert True == util.git_branch_exists(project_or_url, 'master')


@pytest.fixture
def git_repository(request):
    d = tempfile.mkdtemp()
    os.system("""
    cd {d}
    git init
    touch A
    git config user.email 'you@example.com'
    git config user.name 'Your Name'
    git add A
    git commit -m 'A' A
    """.format(d=d))

    def fin():
        os.system("rm -fr " + d)
    request.addfinalizer(fin)
    return d


class TestUtil(object):
    def setup(self):
        config.use_shaman = False

    @patch('requests.get')
    def test_get_hash_success(self, m_get):
        mock_resp = Mock()
        mock_resp.ok = True
        mock_resp.text = "the_hash"
        m_get.return_value = mock_resp
        result = util.get_gitbuilder_hash()
        assert result == "the_hash"

    @patch('requests.get')
    def test_get_hash_fail(self, m_get):
        mock_resp = Mock()
        mock_resp.ok = False
        m_get.return_value = mock_resp
        result = util.get_gitbuilder_hash()
        assert result is None

    @patch('requests.get')
    def test_package_version_for_hash(self, m_get):
        mock_resp = Mock()
        mock_resp.ok = True
        mock_resp.text = "the_version"
        m_get.return_value = mock_resp
        result = util.package_version_for_hash("hash")
        assert result == "the_version"

    @patch('requests.get')
    def test_get_branch_info(self, m_get):
        mock_resp = Mock()
        mock_resp.ok = True
        mock_resp.json.return_value = "some json"
        m_get.return_value = mock_resp
        result = util.get_branch_info("teuthology", "master")
        m_get.assert_called_with(
            "https://api.github.com/repos/ceph/teuthology/git/refs/heads/master"
        )
        assert result == "some json"

    @patch('teuthology.lock.query')
    def test_get_arch_fail(self, m_query):
        m_query.list_locks.return_value = False
        util.get_arch('magna')
        m_query.list_locks.assert_called_with(machine_type="magna", count=1)

    @patch('teuthology.lock.query')
    def test_get_arch_success(self, m_query):
        m_query.list_locks.return_value = [{"arch": "arch"}]
        result = util.get_arch('magna')
        m_query.list_locks.assert_called_with(
            machine_type="magna",
            count=1
        )
        assert result == "arch"

    def test_build_git_url_github(self):
        assert 'project' in util.build_git_url('project')
        owner = 'OWNER'
        git_url = util.build_git_url('project', project_owner=owner)
        assert owner in git_url

    @patch('teuthology.config.TeuthologyConfig.get_ceph_qa_suite_git_url')
    def test_build_git_url_ceph_qa_suite_custom(
            self,
            m_get_ceph_qa_suite_git_url):
        url = 'http://foo.com/some'
        m_get_ceph_qa_suite_git_url.return_value = url + '.git'
        assert url == util.build_git_url('ceph-qa-suite')

    @patch('teuthology.config.TeuthologyConfig.get_ceph_git_url')
    def test_build_git_url_ceph_custom(self, m_get_ceph_git_url):
        url = 'http://foo.com/some'
        m_get_ceph_git_url.return_value = url + '.git'
        assert url == util.build_git_url('ceph')

    @patch('teuthology.config.TeuthologyConfig.get_ceph_cm_ansible_git_url')
    def test_build_git_url_ceph_cm_ansible_custom(self, m_get_ceph_cm_ansible_git_url):
        url = 'http://foo.com/some'
        m_get_ceph_cm_ansible_git_url.return_value = url + '.git'
        assert url == util.build_git_url('ceph-cm-ansible')

    @patch('teuthology.config.TeuthologyConfig.get_ceph_git_url')
    def test_git_ls_remote(self, m_get_ceph_git_url, git_repository):
        m_get_ceph_git_url.return_value = git_repository
        assert util.git_ls_remote('ceph', 'nobranch') is None
        assert util.git_ls_remote('ceph', 'master') is not None

    @patch('teuthology.suite.util.requests.get')
    def test_find_git_parent(self, m_requests_get):
        refresh_resp = Mock(ok=True)
        history_resp = Mock(ok=True)
        history_resp.json.return_value = {'sha1s': ['sha1', 'sha1_p']}
        m_requests_get.side_effect = [refresh_resp, history_resp]
        parent_sha1 = util.find_git_parent('ceph', 'sha1')
        assert len(m_requests_get.mock_calls) == 2
        assert parent_sha1 == 'sha1_p'


class TestFlavor(object):

    def test_get_install_task_flavor_bare(self):
        config = dict(
            tasks=[
                dict(
                    install=dict(),
                ),
            ],
        )
        assert util.get_install_task_flavor(config) == 'basic'

    def test_get_install_task_flavor_simple(self):
        config = dict(
            tasks=[
                dict(
                    install=dict(
                        flavor='notcmalloc',
                    ),
                ),
            ],
        )
        assert util.get_install_task_flavor(config) == 'notcmalloc'

    def test_get_install_task_flavor_override_simple(self):
        config = dict(
            tasks=[
                dict(install=dict()),
            ],
            overrides=dict(
                install=dict(
                    flavor='notcmalloc',
                ),
            ),
        )
        assert util.get_install_task_flavor(config) == 'notcmalloc'

    def test_get_install_task_flavor_override_project(self):
        config = dict(
            tasks=[
                dict(install=dict()),
            ],
            overrides=dict(
                install=dict(
                    ceph=dict(
                        flavor='notcmalloc',
                    ),
                ),
            ),
        )
        assert util.get_install_task_flavor(config) == 'notcmalloc'


class TestMissingPackages(object):
    """
    Tests the functionality that checks to see if a
    scheduled job will have missing packages in gitbuilder.
    """
    def setup(self):
        package_versions = {
            'sha1': {
                'ubuntu': {
                    '14.04': {
                        'basic': '1.0'
                    }
                }
            }
        }
        self.pv = package_versions

    def test_os_in_package_versions(self):
        assert self.pv == util.get_package_versions(
            "sha1",
            "ubuntu",
            "14.04",
            "basic",
            package_versions=self.pv
        )

    @patch("teuthology.suite.util.package_version_for_hash")
    def test_os_not_in_package_versions(self, m_package_versions_for_hash):
        m_package_versions_for_hash.return_value = "1.1"
        result = util.get_package_versions(
            "sha1",
            "rhel",
            "7.0",
            "basic",
            package_versions=self.pv
        )
        expected = deepcopy(self.pv)
        expected['sha1'].update(
            {
                'rhel': {
                    '7.0': {
                        'basic': '1.1'
                    }
                }
            }
        )
        assert result == expected

    @patch("teuthology.suite.util.package_version_for_hash")
    def test_package_versions_not_found(self, m_package_versions_for_hash):
        # if gitbuilder returns a status that's not a 200, None is returned
        m_package_versions_for_hash.return_value = None
        result = util.get_package_versions(
            "sha1",
            "rhel",
            "7.0",
            "basic",
            package_versions=self.pv
        )
        assert result == self.pv

    @patch("teuthology.suite.util.package_version_for_hash")
    def test_no_package_versions_kwarg(self, m_package_versions_for_hash):
        m_package_versions_for_hash.return_value = "1.0"
        result = util.get_package_versions(
            "sha1",
            "ubuntu",
            "14.04",
            "basic",
        )
        expected = deepcopy(self.pv)
        assert result == expected

    def test_distro_has_packages(self):
        result = util.has_packages_for_distro(
            "sha1",
            "ubuntu",
            "14.04",
            "basic",
            package_versions=self.pv,
        )
        assert result

    def test_distro_does_not_have_packages(self):
        result = util.has_packages_for_distro(
            "sha1",
            "rhel",
            "7.0",
            "basic",
            package_versions=self.pv,
        )
        assert not result

    @patch("teuthology.suite.util.get_package_versions")
    def test_has_packages_no_package_versions(self, m_get_package_versions):
        m_get_package_versions.return_value = self.pv
        result = util.has_packages_for_distro(
            "sha1",
            "rhel",
            "7.0",
            "basic",)
        assert not result


class TestDistroDefaults(object):
    def setup(self):
        config.use_shaman = False

    def test_distro_defaults_saya(self):
        expected = ('armv7l', 'saucy',
                    OS(name='ubuntu', version='13.10', codename='saucy'))
        assert util.get_distro_defaults('ubuntu', 'saya') == expected

    def test_distro_defaults_plana(self):
        expected = ('x86_64', 'trusty',
                    OS(name='ubuntu', version='14.04', codename='trusty'))
        assert util.get_distro_defaults('ubuntu', 'plana') == expected

    def test_distro_defaults_debian(self):
        expected = ('x86_64', 'wheezy',
                    OS(name='debian', version='7', codename='wheezy'))
        assert util.get_distro_defaults('debian', 'magna') == expected

    def test_distro_defaults_centos(self):
        expected = ('x86_64', 'centos7',
                    OS(name='centos', version='7', codename='core'))
        assert util.get_distro_defaults('centos', 'magna') == expected

    def test_distro_defaults_fedora(self):
        expected = ('x86_64', 'fedora20',
                    OS(name='fedora', version='20', codename='heisenbug'))
        assert util.get_distro_defaults('fedora', 'magna') == expected

    def test_distro_defaults_default(self):
        expected = ('x86_64', 'centos7',
                    OS(name='centos', version='7', codename='core'))
        assert util.get_distro_defaults('rhel', 'magna') == expected
