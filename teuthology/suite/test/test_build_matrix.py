import os
import random

from mock import patch, MagicMock

from teuthology.suite import build_matrix
from teuthology.test.fake_fs import make_fake_fstools


class TestBuildMatrixSimple(object):
    def test_combine_path(self):
        result = build_matrix.combine_path("/path/to/left", "right/side")
        assert result == "/path/to/left/right/side"

    def test_combine_path_no_right(self):
        result = build_matrix.combine_path("/path/to/left", None)
        assert result == "/path/to/left"


class TestBuildMatrix(object):

    patchpoints = [
        'os.path.exists',
        'os.listdir',
        'os.path.isfile',
        'os.path.isdir',
        '__builtin__.open',
    ]

    def setup(self):
        self.mocks = dict()
        self.patchers = dict()
        for ppoint in self.__class__.patchpoints:
            self.mocks[ppoint] = MagicMock()
            self.patchers[ppoint] = patch(ppoint, self.mocks[ppoint])

    def start_patchers(self, fake_fs):
        fake_fns = make_fake_fstools(fake_fs)
        # relies on fake_fns being in same order as patchpoints
        for ppoint, fn in zip(self.__class__.patchpoints, fake_fns):
            self.mocks[ppoint].side_effect = fn
        for patcher in self.patchers.values():
            patcher.start()

    def stop_patchers(self):
        for patcher in self.patchers.values():
            patcher.stop()

    def teardown(self):
        self.stop_patchers()

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
        self.start_patchers(fake_fs)
        result = build_matrix.build_matrix('d0_0')
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
        self.start_patchers(fake_fs)
        result = build_matrix.build_matrix('d0_0')
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
        self.start_patchers(fake_fs)
        result = build_matrix.build_matrix('d0_0')
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
        self.start_patchers(fake_fs)
        result = build_matrix.build_matrix('d0_0')
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
        self.start_patchers(fake_fs)
        result = build_matrix.build_matrix('d0_0')
        assert len(result) == 2
        for i in result:
            assert 'd0_0/d1_2/d1_2_0.yaml' in i[1]
            assert 'd0_0/d1_2/d1_2_1.yaml' in i[1]
            assert 'd0_0/d1_2/d1_2_2.yaml' in i[1]
            assert 'd0_0/d1_2/d1_2_3.yaml' in i[1]

    def test_random_dollar_sign_2x2x3(self):
        fake_fs = {
            'd0_0': {
                '$': None,
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
                    'd1_2_2.yaml': None,
                },
            },
        }
        fake_fs1 = {
            'd0_0$': {
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
                    'd1_2_2.yaml': None,
                },
            },
        }
        self.start_patchers(fake_fs)
        result = build_matrix.build_matrix('d0_0')
        assert len(result) == 1
        self.stop_patchers()
        self.start_patchers(fake_fs1)
        result = build_matrix.build_matrix('d0_0$')
        assert len(result) == 1

    def test_random_dollar_sign_with_concat(self):
        fake_fs = {
            'd0_0': {
                '$': None,
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
        fake_fs1 = {
            'd0_0$': {
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
        for fs, root in [(fake_fs,'d0_0'), (fake_fs1,'d0_0$')]:
            self.start_patchers(fs)
            result = build_matrix.build_matrix(root)
            assert len(result) == 1
            if result[0][0][1:].startswith('d1_2'):
                for i in result:
                    assert os.path.join(root, 'd1_2/d1_2_0.yaml') in i[1]
                    assert os.path.join(root, 'd1_2/d1_2_1.yaml') in i[1]
                    assert os.path.join(root, 'd1_2/d1_2_2.yaml') in i[1]
                    assert os.path.join(root, 'd1_2/d1_2_3.yaml') in i[1]
            if root == 'd0_0':
                self.stop_patchers()

    def test_random_dollar_sign_with_convolve(self):
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
                    '$': None,
                    'd1_2_0.yaml': None,
                    'd1_2_1.yaml': None,
                    'd1_2_2.yaml': None,
                },
            },
        }
        self.start_patchers(fake_fs)
        result = build_matrix.build_matrix('d0_0')
        assert len(result) == 4
        fake_fs1 = {
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
                'd1_2$': {
                    'd1_2_0.yaml': None,
                    'd1_2_1.yaml': None,
                    'd1_2_2.yaml': None,
                },
            },
        }
        self.stop_patchers()
        self.start_patchers(fake_fs1)
        result = build_matrix.build_matrix('d0_0')
        assert len(result) == 4

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
        self.start_patchers(fake_fs)
        result = build_matrix.build_matrix('teuthology/no-ceph')
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
        self.start_patchers(fake_fs)
        result = build_matrix.build_matrix('teuthology/no-ceph')
        self.stop_patchers()

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
        self.start_patchers(fake_fs2)
        result2 = build_matrix.build_matrix('teuthology/no-ceph')
        assert len(result) == 11
        assert len(result2) == len(result)

    def test_hidden(self):
        fake_fs = {
            'teuthology': {
                'no-ceph': {
                    '%': None,
                    '.qa': None,
                    'clusters': {
                        'single.yaml': None,
                        '.qa': None,
                    },
                    'distros': {
                        '.qa': None,
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
                        '.qa': None,
                        'teuthology.yaml': None,
                    },
                    '.foo': {
                        '.qa': None,
                        'teuthology.yaml': None,
                    },
                },
            },
        }
        self.start_patchers(fake_fs)
        result = build_matrix.build_matrix('teuthology/no-ceph')
        self.stop_patchers()

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
                    },
                    'tasks': {
                        'teuthology.yaml': None,
                    },
                },
            },
        }
        self.start_patchers(fake_fs2)
        result2 = build_matrix.build_matrix('teuthology/no-ceph')
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
        self.start_patchers(fake_fs)
        result = build_matrix.build_matrix('teuthology/no-ceph')
        self.stop_patchers()

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
        self.start_patchers(fake_fs2)
        result2 = build_matrix.build_matrix('teuthology/no-ceph')
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
        self.start_patchers(fake_fs)
        result = build_matrix.build_matrix('thrash')
        assert len(result) == 1
        assert self.fragment_occurences(result, 'base.yaml') == 1
        fragments = result[0][1]
        assert fragments[0] == 'thrash/ceph/base.yaml'
        assert fragments[1] == 'thrash/ceph-thrash/default.yaml'

class TestSubset(object):
    patchpoints = [
        'os.path.exists',
        'os.listdir',
        'os.path.isfile',
        'os.path.isdir',
        '__builtin__.open',
    ]

    def setup(self):
        self.mocks = dict()
        self.patchers = dict()
        for ppoint in self.__class__.patchpoints:
            self.mocks[ppoint] = MagicMock()
            self.patchers[ppoint] = patch(ppoint, self.mocks[ppoint])

    def start_patchers(self, fake_fs):
        fake_fns = make_fake_fstools(fake_fs)
        # relies on fake_fns being in same order as patchpoints
        for ppoint, fn in zip(self.__class__.patchpoints, fake_fns):
            self.mocks[ppoint].side_effect = fn
        for patcher in self.patchers.values():
            patcher.start()

    def stop_patchers(self):
        for patcher in self.patchers.values():
            patcher.stop()

    # test_random() manages start/stop patchers on its own; no teardown

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
        mat, first, matlimit = build_matrix._get_matrix(
            'root', subset=subset)
        return [i[0] for i in build_matrix.generate_combinations(
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
                all_desc = build_matrix.generate_combinations(
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
            self.start_patchers(tree)
            dlist, mat, first, matlimit = self.generate_description_list(tree, subset)
            self.verify_facets(tree, dlist, subset, mat, first, matlimit)
            self.stop_patchers()
