from copy import deepcopy
from datetime import datetime

from mock import patch, Mock

from teuthology import suite
from teuthology.config import config


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
        ref_url = "http://{host}/ceph-deb-squeeze-x86_64-basic/".format(
            host=config.gitbuilder_host
        )
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
            archive_upload='archive_upload',
            archive_upload_key='archive_upload_key',
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
            archive_upload='archive_upload',
            archive_upload_key='archive_upload_key',
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


class TestFlavor(object):
    def test_get_install_task_flavor_bare(self):
        config = dict(
            tasks=[
                dict(
                    install=dict(),
                ),
            ],
        )
        assert suite.get_install_task_flavor(config) == 'basic'

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
        assert suite.get_install_task_flavor(config) == 'notcmalloc'

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
        assert suite.get_install_task_flavor(config) == 'notcmalloc'

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
        assert suite.get_install_task_flavor(config) == 'notcmalloc'


class TestMissingPackages(object):
    """
    Tests the functionality that checks to see if a
    scheduled job will have missing packages in gitbuilder.
    """
    def setup(self):
        package_versions = dict(
            sha1=dict(
                ubuntu=dict(
                    basic="1.0",
                )
            )
        )
        self.pv = package_versions

    def test_os_in_package_versions(self):
        assert self.pv == suite.get_package_versions(
            "sha1",
            "ubuntu",
            "basic",
            package_versions=self.pv
        )

    @patch("teuthology.suite.package_version_for_hash")
    def test_os_not_in_package_versions(self, m_package_versions_for_hash):
        m_package_versions_for_hash.return_value = "1.1"
        result = suite.get_package_versions(
            "sha1",
            "rhel",
            "basic",
            package_versions=self.pv
        )
        expected = deepcopy(self.pv)
        expected['sha1'].update(dict(rhel=dict(basic="1.1")))
        assert result == expected

    @patch("teuthology.suite.package_version_for_hash")
    def test_package_versions_not_found(self, m_package_versions_for_hash):
        # if gitbuilder returns a status that's not a 200, None is returned
        m_package_versions_for_hash.return_value = None
        result = suite.get_package_versions(
            "sha1",
            "rhel",
            "basic",
            package_versions=self.pv
        )
        assert result == self.pv

    @patch("teuthology.suite.package_version_for_hash")
    def test_no_package_versions_kwarg(self, m_package_versions_for_hash):
        m_package_versions_for_hash.return_value = "1.0"
        result = suite.get_package_versions(
            "sha1",
            "ubuntu",
            "basic",
        )
        expected = deepcopy(self.pv)
        assert result == expected

    def test_distro_has_packages(self):
        result = suite.has_packages_for_distro(
            "sha1",
            "ubuntu",
            "basic",
            package_versions=self.pv,
        )
        assert result

    def test_distro_does_not_have_packages(self):
        result = suite.has_packages_for_distro(
            "sha1",
            "rhel",
            "basic",
            package_versions=self.pv,
        )
        assert not result

    @patch("teuthology.suite.get_package_versions")
    def test_has_packages_no_package_versions(self, m_get_package_versions):
        m_get_package_versions.return_value = self.pv
        result = suite.has_packages_for_distro(
            "sha1",
            "rhel",
            "basic",
        )
        assert not result


class TestDistroDefaults(object):

    def test_distro_defaults_saya(self):
        assert suite.get_distro_defaults('ubuntu', 'saya') == ('armv7l',
                                                               'saucy', 'deb')

    def test_distro_defaults_plana(self):
        assert suite.get_distro_defaults('ubuntu', 'plana') == ('x86_64',
                                                                'trusty',
                                                                'deb')

    def test_distro_defaults_debian(self):
        assert suite.get_distro_defaults('debian', 'magna') == ('x86_64',
                                                                'wheezy',
                                                                'deb')

    def test_distro_defaults_centos(self):
        assert suite.get_distro_defaults('centos', 'magna') == ('x86_64',
                                                                'centos7',
                                                                'rpm')

    def test_distro_defaults_fedora(self):
        assert suite.get_distro_defaults('fedora', 'magna') == ('x86_64',
                                                                'fedora20',
                                                                'rpm')

    def test_distro_defaults_default(self):
        assert suite.get_distro_defaults('rhel', 'magna') == ('x86_64',
                                                              'centos7',
                                                              'rpm')


def make_fake_fstools(fake_filesystem):
    """
    Build a fake listdir() and isfile(), to be used instead of
    os.listir() and os.isfile()

    An example fake_filesystem value:
        >>> fake_fs = {
            'a_directory': {
                'another_directory': {
                    'a_file': None,
                    'another_file': None,
                },
                'random_file': None,
                'yet_another_directory': {
                    'empty_directory': {},
                },
            },
        }

        >>> fake_listdir = make_fake_listdir(fake_fs)
        >>> fake_listdir('a_directory/yet_another_directory')
        ['empty_directory']
        >>> fake_isfile('a_directory/yet_another_directory')
        False

    :param fake_filesystem: A dict representing a filesystem layout
    """
    assert isinstance(fake_filesystem, dict)

    def fake_listdir(path, fsdict=False):
        if fsdict is False:
            fsdict = fake_filesystem

        remainder = path.strip('/') + '/'
        subdict = fsdict
        while '/' in remainder:
            next_dir, remainder = remainder.split('/', 1)
            if next_dir not in subdict:
                raise OSError(
                    '[Errno 2] No such file or directory: %s' % next_dir)
            subdict = subdict.get(next_dir)
            if not isinstance(subdict, dict):
                raise OSError('[Errno 20] Not a directory: %s' % next_dir)
            if subdict and not remainder:
                return subdict.keys()
        return []

    def fake_isfile(path, fsdict=False):
        if fsdict is False:
            fsdict = fake_filesystem

        components = path.strip('/').split('/')
        subdict = fsdict
        for component in components:
            if component not in subdict:
                raise OSError(
                    '[Errno 2] No such file or directory: %s' % component)
            subdict = subdict.get(component)
        if subdict is None:
            return True
        else:
            return False

    def fake_isdir(path, fsdict=False):
        return not fake_isfile(path)
    return fake_listdir, fake_isfile, fake_isdir


class TestBuildMatrix(object):
    def fragment_occurences(self, jobs, fragment):
        # What fraction of jobs contain fragment?
        count = 0
        for (description, fragment_list) in jobs:
            for item in fragment_list:
                if item.endswith(fragment):
                    count += 1
        return count / float(len(jobs))

    def test_concatenate_1x2x3(self):
        fake_fs = {
            'd0_0': {
                '+': None,
                'd1_0': {
                    'd1_0_0.yaml': None,
                },
                'd1_1': {
                    'd1_1_0.yaml': None,
                    'd1_1_1.yaml': None,
                },
                'd1_2': {
                    'd1_2_0.yaml': None,
                    'd1_2_1.yaml': None,
                    'd1_2_2.yaml': None,
                },
            },
        }
        fake_listdir, fake_isfile, fake_isdir = make_fake_fstools(fake_fs)
        result = suite.build_matrix('d0_0', fake_isfile, fake_isdir,
                                    fake_listdir)
        assert len(result) == 1

    def test_convolve_2x2(self):
        fake_fs = {
            'd0_0': {
                '%': None,
                'd1_0': {
                    'd1_0_0.yaml': None,
                    'd1_0_1.yaml': None,
                },
                'd1_1': {
                    'd1_1_0.yaml': None,
                    'd1_1_1.yaml': None,
                },
            },
        }
        fake_listdir, fake_isfile, fake_isdir = make_fake_fstools(fake_fs)
        result = suite.build_matrix('d0_0', fake_isfile, fake_isdir,
                                    fake_listdir)
        assert len(result) == 4
        assert self.fragment_occurences(result, 'd1_1_1.yaml') == 0.5

    def test_convolve_2x2x2(self):
        fake_fs = {
            'd0_0': {
                '%': None,
                'd1_0': {
                    'd1_0_0.yaml': None,
                    'd1_0_1.yaml': None,
                },
                'd1_1': {
                    'd1_1_0.yaml': None,
                    'd1_1_1.yaml': None,
                },
                'd1_2': {
                    'd1_2_0.yaml': None,
                    'd1_2_1.yaml': None,
                },
            },
        }
        fake_listdir, fake_isfile, fake_isdir = make_fake_fstools(fake_fs)
        result = suite.build_matrix('d0_0', fake_isfile, fake_isdir,
                                    fake_listdir)
        assert len(result) == 8
        assert self.fragment_occurences(result, 'd1_2_0.yaml') == 0.5

    def test_convolve_1x2x4(self):
        fake_fs = {
            'd0_0': {
                '%': None,
                'd1_0': {
                    'd1_0_0.yaml': None,
                },
                'd1_1': {
                    'd1_1_0.yaml': None,
                    'd1_1_1.yaml': None,
                },
                'd1_2': {
                    'd1_2_0.yaml': None,
                    'd1_2_1.yaml': None,
                    'd1_2_2.yaml': None,
                    'd1_2_3.yaml': None,
                },
            },
        }
        fake_listdir, fake_isfile, fake_isdir = make_fake_fstools(fake_fs)
        result = suite.build_matrix('d0_0', fake_isfile, fake_isdir,
                                    fake_listdir)
        assert len(result) == 8
        assert self.fragment_occurences(result, 'd1_2_2.yaml') == 0.25

    def test_convolve_with_concat(self):
        fake_fs = {
            'd0_0': {
                '%': None,
                'd1_0': {
                    'd1_0_0.yaml': None,
                },
                'd1_1': {
                    'd1_1_0.yaml': None,
                    'd1_1_1.yaml': None,
                },
                'd1_2': {
                    '+': None,
                    'd1_2_0.yaml': None,
                    'd1_2_1.yaml': None,
                    'd1_2_2.yaml': None,
                    'd1_2_3.yaml': None,
                },
            },
        }
        fake_listdir, fake_isfile, fake_isdir = make_fake_fstools(fake_fs)
        result = suite.build_matrix('d0_0', fake_isfile, fake_isdir,
                                    fake_listdir)
        assert len(result) == 2
        for i in result:
            assert 'd0_0/d1_2/d1_2_0.yaml' in i[1]
            assert 'd0_0/d1_2/d1_2_1.yaml' in i[1]
            assert 'd0_0/d1_2/d1_2_2.yaml' in i[1]
            assert 'd0_0/d1_2/d1_2_3.yaml' in i[1]

    def test_emulate_teuthology_noceph(self):
        fake_fs = {
            'teuthology': {
                'no-ceph': {
                    '%': None,
                    'clusters': {
                        'single.yaml': None,
                    },
                    'distros': {
                        'baremetal.yaml': None,
                        'rhel7.0.yaml': None,
                        'ubuntu12.04.yaml': None,
                        'ubuntu14.04.yaml': None,
                        'vps.yaml': None,
                        'vps_centos6.5.yaml': None,
                        'vps_debian7.yaml': None,
                        'vps_rhel6.4.yaml': None,
                        'vps_rhel6.5.yaml': None,
                        'vps_rhel7.0.yaml': None,
                        'vps_ubuntu14.04.yaml': None,
                    },
                    'tasks': {
                        'teuthology.yaml': None,
                    },
                },
            },
        }
        fake_listdir, fake_isfile, fake_isdir = make_fake_fstools(fake_fs)
        result = suite.build_matrix('teuthology/no-ceph', fake_isfile,
                                    fake_isdir, fake_listdir)
        assert len(result) == 11
        assert self.fragment_occurences(result, 'vps.yaml') == 1 / 11.0

    def test_empty_dirs(self):
        fake_fs = {
            'teuthology': {
                'no-ceph': {
                    '%': None,
                    'clusters': {
                        'single.yaml': None,
                    },
                    'distros': {
                        'baremetal.yaml': None,
                        'rhel7.0.yaml': None,
                        'ubuntu12.04.yaml': None,
                        'ubuntu14.04.yaml': None,
                        'vps.yaml': None,
                        'vps_centos6.5.yaml': None,
                        'vps_debian7.yaml': None,
                        'vps_rhel6.4.yaml': None,
                        'vps_rhel6.5.yaml': None,
                        'vps_rhel7.0.yaml': None,
                        'vps_ubuntu14.04.yaml': None,
                    },
                    'tasks': {
                        'teuthology.yaml': None,
                    },
                },
            },
        }
        fake_listdir, fake_isfile, fake_isdir = make_fake_fstools(fake_fs)
        result = suite.build_matrix('teuthology/no-ceph', fake_isfile,
                                    fake_isdir, fake_listdir)
        fake_fs2 = {
            'teuthology': {
                'no-ceph': {
                    '%': None,
                    'clusters': {
                        'single.yaml': None,
                    },
                    'distros': {
                        'empty': {},
                        'baremetal.yaml': None,
                        'rhel7.0.yaml': None,
                        'ubuntu12.04.yaml': None,
                        'ubuntu14.04.yaml': None,
                        'vps.yaml': None,
                        'vps_centos6.5.yaml': None,
                        'vps_debian7.yaml': None,
                        'vps_rhel6.4.yaml': None,
                        'vps_rhel6.5.yaml': None,
                        'vps_rhel7.0.yaml': None,
                        'vps_ubuntu14.04.yaml': None,
                    },
                    'tasks': {
                        'teuthology.yaml': None,
                    },
                    'empty': {},
                },
            },
        }
        fake_listdir2, fake_isfile2, fake_isdir2 = make_fake_fstools(fake_fs2)
        result2 = suite.build_matrix('teuthology/no-ceph', fake_isfile2,
                                     fake_isdir2, fake_listdir2)
        assert len(result) == 11
        assert len(result2) == len(result)

    def test_disable_extension(self):
        fake_fs = {
            'teuthology': {
                'no-ceph': {
                    '%': None,
                    'clusters': {
                        'single.yaml': None,
                    },
                    'distros': {
                        'baremetal.yaml': None,
                        'rhel7.0.yaml': None,
                        'ubuntu12.04.yaml': None,
                        'ubuntu14.04.yaml': None,
                        'vps.yaml': None,
                        'vps_centos6.5.yaml': None,
                        'vps_debian7.yaml': None,
                        'vps_rhel6.4.yaml': None,
                        'vps_rhel6.5.yaml': None,
                        'vps_rhel7.0.yaml': None,
                        'vps_ubuntu14.04.yaml': None,
                    },
                    'tasks': {
                        'teuthology.yaml': None,
                    },
                },
            },
        }
        fake_listdir, fake_isfile, fake_isdir = make_fake_fstools(fake_fs)
        result = suite.build_matrix('teuthology/no-ceph', fake_isfile,
                                    fake_isdir, fake_listdir)
        fake_fs2 = {
            'teuthology': {
                'no-ceph': {
                    '%': None,
                    'clusters': {
                        'single.yaml': None,
                    },
                    'distros': {
                        'baremetal.yaml': None,
                        'rhel7.0.yaml': None,
                        'ubuntu12.04.yaml': None,
                        'ubuntu14.04.yaml': None,
                        'vps.yaml': None,
                        'vps_centos6.5.yaml': None,
                        'vps_debian7.yaml': None,
                        'vps_rhel6.4.yaml': None,
                        'vps_rhel6.5.yaml': None,
                        'vps_rhel7.0.yaml': None,
                        'vps_ubuntu14.04.yaml': None,
                        'forcefilevps_ubuntu14.04.yaml.disable': None,
                        'forcefilevps_ubuntu14.04.yaml.anotherextension': None,
                    },
                    'tasks': {
                        'teuthology.yaml': None,
                        'forcefilevps_ubuntu14.04notyaml': None,
                    },
                    'forcefilevps_ubuntu14.04notyaml': None,
                    'tasks.disable': {
                        'teuthology2.yaml': None,
                        'forcefilevps_ubuntu14.04notyaml': None,
                    },
                },
            },
        }
        fake_listdir2, fake_isfile2, fake_isdir2 = make_fake_fstools(fake_fs2)
        result2 = suite.build_matrix('teuthology/no-ceph', fake_isfile2,
                                     fake_isdir2, fake_listdir2)
        assert len(result) == 11
        assert len(result2) == len(result)

    def test_sort_order(self):
        # This test ensures that 'ceph' comes before 'ceph-thrash' when yaml
        # fragments are sorted.
        fake_fs = {
            'thrash': {
                '%': None,
                'ceph-thrash': {'default.yaml': None},
                'ceph': {'base.yaml': None},
                'clusters': {'mds-1active-1standby.yaml': None},
                'debug': {'mds_client.yaml': None},
                'fs': {'btrfs.yaml': None},
                'msgr-failures': {'none.yaml': None},
                'overrides': {'whitelist_wrongly_marked_down.yaml': None},
                'tasks': {'cfuse_workunit_suites_fsstress.yaml': None},
            },
        }
        fake_listdir, fake_isfile, fake_isdir = make_fake_fstools(fake_fs)
        result = suite.build_matrix('thrash', fake_isfile,
                                    fake_isdir, fake_listdir)
        assert len(result) == 1
        assert self.fragment_occurences(result, 'base.yaml') == 1
        fragments = result[0][1]
        assert fragments[0] == 'thrash/ceph/base.yaml'
        assert fragments[1] == 'thrash/ceph-thrash/default.yaml'

