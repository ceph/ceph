from copy import deepcopy
from datetime import datetime

from mock import patch, Mock

from teuthology import suite


class TestSuiteOffline(object):
    def test_name_timestamp_passed(self):
        stamp = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        name = suite.make_run_name('suite', 'ceph', 'kernel', 'flavor',
                                   'mtype', timestamp=stamp)
        assert str(stamp) in name

    def test_name_timestamp_not_passed(self):
        stamp = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        name = suite.make_run_name('suite', 'ceph', 'kernel', 'flavor',
                                   'mtype')
        assert str(stamp) in name

    def test_name_user(self):
        name = suite.make_run_name('suite', 'ceph', 'kernel', 'flavor',
                                   'mtype', user='USER')
        assert name.startswith('USER-')

    def test_gitbuilder_url(self):
        ref_url = "http://gitbuilder.ceph.com/ceph-deb-squeeze-x86_64-basic/"
        assert suite.get_gitbuilder_url('ceph', 'squeeze', 'deb', 'x86_64',
                                        'basic') == ref_url

    def test_substitute_placeholders(self):
        input_dict = dict(
            suite='suite',
            suite_branch='suite_branch',
            ceph_branch='ceph_branch',
            ceph_hash='ceph_hash',
            teuthology_branch='teuthology_branch',
            machine_type='machine_type',
            distro='distro',
        )
        output_dict = suite.substitute_placeholders(suite.dict_templ,
                                                    input_dict)
        assert output_dict['suite'] == 'suite'
        assert isinstance(suite.dict_templ['suite'], suite.Placeholder)
        assert isinstance(
            suite.dict_templ['overrides']['admin_socket']['branch'],
            suite.Placeholder)

    def test_null_placeholders_dropped(self):
        input_dict = dict(
            suite='suite',
            suite_branch='suite_branch',
            ceph_branch='ceph_branch',
            ceph_hash='ceph_hash',
            teuthology_branch='teuthology_branch',
            machine_type='machine_type',
            distro=None,
        )
        output_dict = suite.substitute_placeholders(suite.dict_templ,
                                                    input_dict)
        assert 'os_type' not in output_dict

    @patch('teuthology.suite.get_gitbuilder_url')
    @patch('requests.get')
    def test_get_hash_success(self, m_get, m_get_gitbuilder_url):
        m_get_gitbuilder_url.return_value = "http://baseurl.com"
        mock_resp = Mock()
        mock_resp.ok = True
        mock_resp.text = "the_hash"
        m_get.return_value = mock_resp
        result = suite.get_hash()
        m_get.assert_called_with("http://baseurl.com/ref/master/sha1")
        assert result == "the_hash"

    @patch('teuthology.suite.get_gitbuilder_url')
    @patch('requests.get')
    def test_get_hash_fail(self, m_get, m_get_gitbuilder_url):
        m_get_gitbuilder_url.return_value = "http://baseurl.com"
        mock_resp = Mock()
        mock_resp.ok = False
        m_get.return_value = mock_resp
        result = suite.get_hash()
        assert result is None

    @patch('teuthology.suite.get_gitbuilder_url')
    @patch('requests.get')
    def test_package_version_for_hash(self, m_get, m_get_gitbuilder_url):
        m_get_gitbuilder_url.return_value = "http://baseurl.com"
        mock_resp = Mock()
        mock_resp.ok = True
        mock_resp.text = "the_version"
        m_get.return_value = mock_resp
        result = suite.package_version_for_hash("hash")
        m_get.assert_called_with("http://baseurl.com/sha1/hash/version")
        assert result == "the_version"

    @patch('requests.get')
    def test_get_branch_info(self, m_get):
        mock_resp = Mock()
        mock_resp.ok = True
        mock_resp.json.return_value = "some json"
        m_get.return_value = mock_resp
        result = suite.get_branch_info("teuthology", "master")
        m_get.assert_called_with(
            "https://api.github.com/repos/ceph/teuthology/git/refs/heads/master"
        )
        assert result == "some json"

    @patch('teuthology.suite.lock')
    def test_get_arch_fail(self, m_lock):
        m_lock.list_locks.return_value = False
        suite.get_arch('magna')
        m_lock.list_locks.assert_called_with(machine_type="magna", count=1)

    @patch('teuthology.suite.lock')
    def test_get_arch_success(self, m_lock):
        m_lock.list_locks.return_value = [{"arch": "arch"}]
        result = suite.get_arch('magna')
        m_lock.list_locks.assert_called_with(
            machine_type="magna",
            count=1
        )
        assert result == "arch"

    def test_combine_path(self):
        result = suite.combine_path("/path/to/left", "right/side")
        assert result == "/path/to/left/right/side"

    def test_combine_path_no_right(self):
        result = suite.combine_path("/path/to/left", None)
        assert result == "/path/to/left"


class TestMissingPackages(object):
    """
    Tests the functionality that checks to see if a
    scheduled job will have missing packages in gitbuilder.
    """
    def setup(self):
        package_versions = dict(
            sha1=dict(
                ubuntu="1.0"
            )
        )
        self.pv = package_versions

    def test_os_in_package_versions(self):
        assert self.pv == suite.get_package_versions(
            "sha1",
            "ubuntu",
            package_versions=self.pv
        )

    @patch("teuthology.suite.package_version_for_hash")
    def test_os_not_in_package_versions(self, m_package_versions_for_hash):
        m_package_versions_for_hash.return_value = "1.1"
        result = suite.get_package_versions(
            "sha1",
            "rhel",
            package_versions=self.pv
        )
        expected = deepcopy(self.pv)
        expected['sha1'].update(dict(rhel="1.1"))
        assert result == expected

    @patch("teuthology.suite.package_version_for_hash")
    def test_package_versions_not_found(self, m_package_versions_for_hash):
        # if gitbuilder returns a status that's not a 200, None is returned
        m_package_versions_for_hash.return_value = None
        result = suite.get_package_versions(
            "sha1",
            "rhel",
            package_versions=self.pv
        )
        assert result == self.pv

    @patch("teuthology.suite.package_version_for_hash")
    def test_no_package_versions_kwarg(self, m_package_versions_for_hash):
        m_package_versions_for_hash.return_value = "1.0"
        result = suite.get_package_versions(
            "sha1",
            "ubuntu",
        )
        expected = deepcopy(self.pv)
        assert result == expected

    def test_distro_has_packages(self):
        result = suite.has_packages_for_distro(
            "sha1",
            "ubuntu",
            package_versions=self.pv,
        )
        assert result

    def test_distro_does_not_have_packages(self):
        result = suite.has_packages_for_distro(
            "sha1",
            "rhel",
            package_versions=self.pv,
        )
        assert not result

    @patch("teuthology.suite.get_package_versions")
    def test_has_packages_no_package_versions(self, m_get_package_versions):
        m_get_package_versions.return_value = self.pv
        result = suite.has_packages_for_distro(
            "sha1",
            "rhel",
        )
        assert not result


class TestDistroDefaults(object):

    def test_distro_defaults_saya(self):
        assert suite.get_distro_defaults('ubuntu', 'saya') == ('armv7l',
                                                               'saucy', 'deb')

    def test_distro_defaults_plana(self):
        assert suite.get_distro_defaults('ubuntu', 'plana') == ('x86_64',
                                                                'precise',
                                                                'deb')

    def test_distro_defaults_debian(self):
        assert suite.get_distro_defaults('debian', 'magna') == ('x86_64',
                                                                'wheezy',
                                                                'deb')

    def test_distro_defaults_centos(self):
        assert suite.get_distro_defaults('centos', 'magna') == ('x86_64',
                                                                'centos6',
                                                                'rpm')

    def test_distro_defaults_fedora(self):
        assert suite.get_distro_defaults('fedora', 'magna') == ('x86_64',
                                                                'fedora20',
                                                                'rpm')

    def test_distro_defaults_default(self):
        assert suite.get_distro_defaults('rhel', 'magna') == ('x86_64',
                                                              'rhel7_0',
                                                              'rpm')
