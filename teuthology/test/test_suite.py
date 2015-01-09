from copy import deepcopy
from datetime import datetime

from mock import patch

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

    def test_distro_defaults_saya(self):
        assert suite.get_distro_defaults('ubuntu', 'saya') == ('armv7l',
                                                               'saucy', 'deb')

    def test_distro_defaults_plana(self):
        assert suite.get_distro_defaults('ubuntu', 'plana') == ('x86_64',
                                                                'precise',
                                                                'deb')

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
