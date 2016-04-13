from copy import deepcopy
from datetime import datetime

from mock import patch, Mock, DEFAULT

from fake_fs import make_fake_fstools
from teuthology import suite
from scripts.suite import main
from teuthology.config import config

import os
import pytest
import tempfile
import random

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
        suite_hash = 'suite_hash'
        input_dict = dict(
            suite='suite',
            suite_branch='suite_branch',
            suite_hash=suite_hash,
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
        assert output_dict['suite_sha1'] == suite_hash
        assert isinstance(suite.dict_templ['suite'], suite.Placeholder)
        assert isinstance(
            suite.dict_templ['overrides']['admin_socket']['branch'],
            suite.Placeholder)

    def test_null_placeholders_dropped(self):
        input_dict = dict(
            suite='suite',
            suite_branch='suite_branch',
            suite_hash='suite_hash',
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

    def test_build_git_url_github(self):
        assert 'project' in suite.build_git_url('project')
        owner = 'OWNER'
        assert owner in suite.build_git_url('project', project_owner=owner)

    @patch('teuthology.config.TeuthologyConfig.get_ceph_qa_suite_git_url')
    def test_build_git_url_ceph_qa_suite_custom(self, m_get_ceph_qa_suite_git_url):
        url = 'http://foo.com/some'
        m_get_ceph_qa_suite_git_url.return_value = url + '.git'
        assert url == suite.build_git_url('ceph-qa-suite')

    @patch('teuthology.config.TeuthologyConfig.get_ceph_git_url')
    def test_build_git_url_ceph_custom(self, m_get_ceph_git_url):
        url = 'http://foo.com/some'
        m_get_ceph_git_url.return_value = url + '.git'
        assert url == suite.build_git_url('ceph')

    @patch('teuthology.config.TeuthologyConfig.get_ceph_git_url')
    def test_git_ls_remote(self, m_get_ceph_git_url, git_repository):
        m_get_ceph_git_url.return_value = git_repository
        assert None == suite.git_ls_remote('ceph', 'nobranch')
        assert suite.git_ls_remote('ceph', 'master') is not None

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
        fake_listdir, fake_isfile, fake_isdir, _ = make_fake_fstools(fake_fs)
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
        fake_listdir, fake_isfile, fake_isdir, _ = make_fake_fstools(fake_fs)
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
        fake_listdir, fake_isfile, fake_isdir, _ = make_fake_fstools(fake_fs)
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
        fake_listdir, fake_isfile, fake_isdir, _ = make_fake_fstools(fake_fs)
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
        fake_listdir, fake_isfile, fake_isdir, _ = make_fake_fstools(fake_fs)
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
        fake_listdir, fake_isfile, fake_isdir, _ = make_fake_fstools(fake_fs)
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
        fake_listdir, fake_isfile, fake_isdir, _ = make_fake_fstools(fake_fs)
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
        fake_listdir2, fake_isfile2, fake_isdir2, _ = make_fake_fstools(fake_fs2)
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
        fake_listdir, fake_isfile, fake_isdir, _ = make_fake_fstools(fake_fs)
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
        fake_listdir2, fake_isfile2, fake_isdir2, _ = make_fake_fstools(fake_fs2)
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
        fake_listdir, fake_isfile, fake_isdir, _ = make_fake_fstools(fake_fs)
        result = suite.build_matrix('thrash', fake_isfile,
                                    fake_isdir, fake_listdir)
        assert len(result) == 1
        assert self.fragment_occurences(result, 'base.yaml') == 1
        fragments = result[0][1]
        assert fragments[0] == 'thrash/ceph/base.yaml'
        assert fragments[1] == 'thrash/ceph-thrash/default.yaml'

class TestSubset(object):
    MAX_FACETS = 10
    MAX_FANOUT = 3
    MAX_DEPTH = 3
    MAX_SUBSET = 10
    @staticmethod
    def generate_fake_fs(max_facets, max_fanout, max_depth):
        def yamilify(name):
            return name + ".yaml"
        def name_generator():
            x = 0
            while True:
                yield(str(x))
                x += 1
        def generate_tree(
                max_facets, max_fanout, max_depth, namegen, top=True):
            if max_depth is 0:
                return None
            if max_facets is 0:
                return None
            items = random.choice(range(max_fanout))
            if items is 0 and top:
                items = 1
            if items is 0:
                return None
            sub_max_facets = max_facets / items
            tree = {}
            for i in range(items):
                subtree = generate_tree(
                    sub_max_facets, max_fanout,
                    max_depth - 1, namegen, top=False)
                if subtree is not None:
                    tree[namegen.next()] = subtree
                else:
                    tree[yamilify(namegen.next())] = None
            random.choice([
                lambda: tree.update({'%': None}),
                lambda: None])()
            return tree
        return {
            'root':  generate_tree(
                max_facets, max_fanout, max_depth, name_generator())
        }

    @staticmethod
    def generate_subset(maxsub):
        den = random.choice(range(maxsub-1))+1
        return (random.choice(range(den)), den)

    @staticmethod
    def generate_description_list(tree, subset):
        fake_listdir, fake_isfile, fake_isdir, _ = make_fake_fstools(tree)
        mat, first, matlimit = suite._get_matrix(
            'root', _isfile=fake_isfile, _isdir=fake_isdir,
            _listdir=fake_listdir, subset=subset)
        return [i[0] for i in suite.generate_combinations(
            'root', mat, first, matlimit)], mat, first, matlimit

    @staticmethod
    def verify_facets(tree, description_list, subset, mat, first, matlimit):
        def flatten(tree):
            for k,v in tree.iteritems():
                if v is None and '.yaml' in k:
                    yield k
                elif v is not None and '.disable' not in k:
                    for x in flatten(v):
                        yield x
        def pptree(tree, tabs=0):
            ret = ""
            for k, v in tree.iteritems():
                if v is None:
                    ret += ('\t'*tabs) + k.ljust(10) + "\n"
                else:
                    ret += ('\t'*tabs) + (k + ':').ljust(10) + "\n"
                    ret += pptree(v, tabs+1)
            return ret
        for facet in flatten(tree):
            found = False
            for i in description_list:
                if facet in i:
                    found = True
                    break
            if not found:
                print "tree\n{tree}\ngenerated list\n{desc}\n\nfrom matrix\n\n{matrix}\nsubset {subset} without facet {fac}".format(
                    tree=pptree(tree),
                    desc='\n'.join(description_list),
                    subset=subset,
                    matrix=str(mat),
                    fac=facet)
                all_desc = suite.generate_combinations(
                    'root',
                    mat,
                    0,
                    mat.size())
                for i, desc in zip(xrange(mat.size()), all_desc):
                    if i == first:
                        print '=========='
                    print i, desc
                    if i + 1 == matlimit:
                        print '=========='
            assert found

    def test_random(self):
        for i in xrange(10000):
            tree = self.generate_fake_fs(
                self.MAX_FACETS,
                self.MAX_FANOUT,
                self.MAX_DEPTH)
            subset = self.generate_subset(self.MAX_SUBSET)
            dlist, mat, first, matlimit = self.generate_description_list(tree, subset)
            self.verify_facets(tree, dlist, subset, mat, first, matlimit)

@patch('subprocess.check_output')
def test_git_branch_exists(m_check_output):
    m_check_output.return_value = ''
    assert False == suite.git_branch_exists('ceph', 'nobranchnowaycanthappen')
    m_check_output.return_value = 'HHH branch'
    assert True == suite.git_branch_exists('ceph', 'master')

@patch.object(suite.ResultsReporter, 'get_jobs')
def test_wait_success(m_get_jobs, caplog):
    results = [
        [{'status': 'queued', 'job_id': '2'}],
        [],
    ]
    final = [
        {'status': 'pass', 'job_id': '1',
         'description': 'DESC1', 'log_href': 'http://URL1'},
        {'status': 'fail', 'job_id': '2',
         'description': 'DESC2', 'log_href': 'http://URL2'},
        {'status': 'pass', 'job_id': '3',
         'description': 'DESC3', 'log_href': 'http://URL3'},
    ]
    def get_jobs(name, **kwargs):
        if kwargs['fields'] == ['job_id', 'status']:
            return in_progress.pop(0)
        else:
            return final
    m_get_jobs.side_effect = get_jobs
    suite.WAIT_PAUSE = 1

    in_progress = deepcopy(results)
    assert 0 == suite.wait('name', 1, 'http://UPLOAD_URL')
    assert m_get_jobs.called_with('name', fields=['job_id', 'status'])
    assert 0 == len(in_progress)
    assert 'fail http://UPLOAD_URL/name/2' in caplog.text()

    in_progress = deepcopy(results)
    in_progress = deepcopy(results)
    assert 0 == suite.wait('name', 1, None)
    assert m_get_jobs.called_with('name', fields=['job_id', 'status'])
    assert 0 == len(in_progress)
    assert 'fail http://URL2' in caplog.text()

@patch.object(suite.ResultsReporter, 'get_jobs')
def test_wait_fails(m_get_jobs):
    results = []
    results.append([{'status': 'queued', 'job_id': '2'}])
    results.append([{'status': 'queued', 'job_id': '2'}])
    results.append([{'status': 'queued', 'job_id': '2'}])
    def get_jobs(name, **kwargs):
        return results.pop(0)
    m_get_jobs.side_effect = get_jobs
    suite.WAIT_PAUSE = 1
    suite.WAIT_MAX_JOB_TIME = 1
    with pytest.raises(suite.WaitException) as error:
        suite.wait('name', 1, None)
        assert 'abc' in str(error)

class TestSuiteMain(object):

    def test_main(self):
        suite_name = 'SUITE'
        throttle = '3'
        machine_type = 'burnupi'

        def prepare_and_schedule(**kwargs):
            assert kwargs['job_config']['suite'] == suite_name
            assert kwargs['throttle'] == throttle

        def fake_str(*args, **kwargs):
            return 'fake'

        def fake_bool(*args, **kwargs):
            return True

        with patch.multiple(
                suite,
                fetch_repos=DEFAULT,
                prepare_and_schedule=prepare_and_schedule,
                get_hash=fake_str,
                package_version_for_hash=fake_str,
                git_branch_exists=fake_bool,
                git_ls_remote=fake_str,
                ):
            main(['--suite', suite_name,
                  '--throttle', throttle,
                  '--machine-type', machine_type,
                  ])

    def test_schedule_suite(self):
        suite_name = 'noop'
        throttle = '3'
        machine_type = 'burnupi'

        with patch.multiple(
                suite,
                fetch_repos=DEFAULT,
                teuthology_schedule=DEFAULT,
                sleep=DEFAULT,
                get_arch=lambda x: 'x86_64',
                git_ls_remote=lambda *args: '12345',
                package_version_for_hash=lambda *args: 'fake-9.5',
                ) as m:
            config.suite_verify_ceph_hash = True
            main(['--suite', suite_name,
                  '--suite-dir', 'teuthology/test',
                  '--throttle', throttle,
                  '--machine-type', machine_type])
            m['sleep'].assert_called_with(int(throttle))

    def test_schedule_suite_noverify(self):
        suite_name = 'noop'
        throttle = '3'
        machine_type = 'burnupi'

        with patch.multiple(
                suite,
                fetch_repos=DEFAULT,
                teuthology_schedule=DEFAULT,
                sleep=DEFAULT,
                get_arch=lambda x: 'x86_64',
                git_ls_remote=lambda *args: '1234',
                get_hash=DEFAULT,
                package_version_for_hash=lambda *args: 'fake-9.5',
                ) as m:
            config.suite_verify_ceph_hash = False
            main(['--suite', suite_name,
                  '--suite-dir', 'teuthology/test',
                  '--throttle', throttle,
                  '--machine-type', machine_type])
            m['sleep'].assert_called_with(int(throttle))
            m['get_hash'].assert_not_called()
