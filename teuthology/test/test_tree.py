# -*- coding: utf-8 -*-
from fake_fs import make_fake_fstools
from teuthology.tree import tree_with_info, extract_info

realistic_fs = {
    'basic': {
        '%': None,
        'base': {
            'install.yaml':
            """# desc: install ceph
install:
"""
        },
        'clusters': {
            'fixed-1.yaml':
            """# desc: single node cluster
roles:
- [osd.0, osd.1, osd.2, mon.a, mon.b, mon.c, client.0]
"""
        },
        'workloads': {
            'rbd_api_tests_old_format.yaml':
            """# desc: c/c++ librbd api tests with format 1 images
# rbd_features: none
overrides:
  ceph:
    conf:
      client:
        rbd default format: 1
tasks:
- workunit:
    env:
      RBD_FEATURES: 0
    clients:
      client.0:
        - rbd/test_librbd.sh
""",
            'rbd_api_tests.yaml':
            """# desc: c/c++ librbd api tests with default settings
# rbd_features: default
tasks:
- workunit:
    clients:
      client.0:
        - rbd/test_librbd.sh
""",
        },
    },
}


expected_tree = """├── %
├── base
│   └── install.yaml
├── clusters
│   └── fixed-1.yaml
└── workloads
    ├── rbd_api_tests.yaml
    └── rbd_api_tests_old_format.yaml""".split('\n')


expected_desc = [
    '',
    '',
    ' install ceph',
    '',
    ' single node cluster',
    '',
    ' c/c++ librbd api tests with default settings',
    ' c/c++ librbd api tests with format 1 images',
]


expected_rbd_features = [
    '',
    '',
    '',
    '',
    '',
    '',
    ' default',
    ' none',
]


class TestTree(object):

    def test_no_filters(self):
        fake_listdir, _, fake_isdir, fake_open = \
            make_fake_fstools(realistic_fs)
        rows = tree_with_info('basic', [], '', [], fake_listdir,
                              fake_isdir, fake_open)
        assert rows == [ [x] for x in expected_tree]

    def test_single_filter(self):
        fake_listdir, _, fake_isdir, fake_open = \
            make_fake_fstools(realistic_fs)
        rows = tree_with_info('basic', ['desc'], '', [], fake_listdir,
                              fake_isdir, fake_open)
        assert rows == map(list, zip(expected_tree, expected_desc))

        rows = tree_with_info('basic', ['rbd_features'], '', [],
                              fake_listdir, fake_isdir, fake_open)
        assert rows == map(list, zip(expected_tree, expected_rbd_features))

    def test_no_matching(self):
        fake_listdir, _, fake_isdir, fake_open = \
            make_fake_fstools(realistic_fs)
        rows = tree_with_info('basic', ['extra'], '', [], fake_listdir,
                              fake_isdir, fake_open)
        assert rows == map(list, zip(expected_tree, [''] * len(expected_tree)))

    def test_multiple_filters(self):
        fake_listdir, _, fake_isdir, fake_open = \
            make_fake_fstools(realistic_fs)
        rows = tree_with_info('basic', ['desc', 'rbd_features'], '', [],
                              fake_listdir, fake_isdir, fake_open)
        assert rows == map(list, zip(expected_tree,
                                     expected_desc,
                                     expected_rbd_features))

        rows = tree_with_info('basic', ['rbd_features', 'desc'], '', [],
                              fake_listdir, fake_isdir, fake_open)
        assert rows == map(list, zip(expected_tree,
                                expected_rbd_features,
                                expected_desc))

    def test_extract_info_dir(self):
        simple_fs = {'a': {'b': '# foo:'}}
        _, _, fake_isdir, fake_open = make_fake_fstools(simple_fs)
        info = extract_info('a', [], fake_isdir, fake_open)
        assert info == {}

        info = extract_info('a', ['foo', 'bar'], fake_isdir, fake_open)
        assert info == {'foo': '', 'bar': ''}
