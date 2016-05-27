# -*- coding: utf-8 -*-
import pytest

from fake_fs import make_fake_fstools
from teuthology.describe_tests import (tree_with_info, extract_info,
                                       get_combinations)
from teuthology.exceptions import ParseError
from mock import MagicMock, patch

realistic_fs = {
    'basic': {
        '%': None,
        'base': {
            'install.yaml':
            """meta:
- desc: install ceph
install:
"""
        },
        'clusters': {
            'fixed-1.yaml':
            """meta:
- desc: single node cluster
roles:
- [osd.0, osd.1, osd.2, mon.a, mon.b, mon.c, client.0]
"""
        },
        'workloads': {
            'rbd_api_tests_old_format.yaml':
            """meta:
- desc: c/c++ librbd api tests with format 1 images
  rbd_features: none
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
            """meta:
- desc: c/c++ librbd api tests with default settings
  rbd_features: default
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


expected_facets = [
    '',
    '',
    'base',
    '',
    'clusters',
    '',
    'workloads',
    'workloads',
]


expected_desc = [
    '',
    '',
    'install ceph',
    '',
    'single node cluster',
    '',
    'c/c++ librbd api tests with default settings',
    'c/c++ librbd api tests with format 1 images',
]


expected_rbd_features = [
    '',
    '',
    '',
    '',
    '',
    '',
    'default',
    'none',
]


class TestDescribeTests(object):

    patchpoints = [
        'os.path.exists',
        'os.listdir',
        'os.path.isfile',
        'os.path.isdir',
        '__builtin__.open',
    ]
    fake_fns = make_fake_fstools(realistic_fs)

    def setup(self):
        self.mocks = dict()
        self.patchers = dict()
        klass = self.__class__
        for ppoint, fn in zip(klass.patchpoints, klass.fake_fns):
            mockobj = MagicMock()
            patcher = patch(ppoint, mockobj)
            mockobj.side_effect = fn
            patcher.start()
            self.mocks[ppoint] = mockobj
            self.patchers[ppoint] = patcher

    def stop_patchers(self):
        for patcher in self.patchers.values():
            patcher.stop()

    def teardown(self):
        self.stop_patchers()

    @staticmethod
    def assert_expected_combo_headers(headers):
        assert headers == (['subsuite depth 0'] +
                           sorted(set(filter(bool, expected_facets))))

    def test_no_filters(self):
        rows = tree_with_info('basic', [], False, '', [])
        assert rows == [[x] for x in expected_tree]

    def test_single_filter(self):
        rows = tree_with_info('basic', ['desc'], False, '', [])
        assert rows == map(list, zip(expected_tree, expected_desc))

        rows = tree_with_info('basic', ['rbd_features'], False, '', [])
        assert rows == map(list, zip(expected_tree, expected_rbd_features))

    def test_single_filter_with_facets(self):
        rows = tree_with_info('basic', ['desc'], True, '', [])
        assert rows == map(list, zip(expected_tree, expected_facets,
                                     expected_desc))

        rows = tree_with_info('basic', ['rbd_features'], True, '', [])
        assert rows == map(list, zip(expected_tree, expected_facets,
                                     expected_rbd_features))

    def test_no_matching(self):
        rows = tree_with_info('basic', ['extra'], False, '', [])
        assert rows == map(list, zip(expected_tree, [''] * len(expected_tree)))

        rows = tree_with_info('basic', ['extra'], True, '', [])
        assert rows == map(list, zip(expected_tree, expected_facets,
                                     [''] * len(expected_tree)))

    def test_multiple_filters(self):
        rows = tree_with_info('basic', ['desc', 'rbd_features'], False, '', [])
        assert rows == map(list, zip(expected_tree,
                                     expected_desc,
                                     expected_rbd_features))

        rows = tree_with_info('basic', ['rbd_features', 'desc'], False, '', [])
        assert rows == map(list, zip(expected_tree,
                                     expected_rbd_features,
                                     expected_desc))

    def test_multiple_filters_with_facets(self):
        rows = tree_with_info('basic', ['desc', 'rbd_features'], True, '', [])
        assert rows == map(list, zip(expected_tree,
                                     expected_facets,
                                     expected_desc,
                                     expected_rbd_features))

        rows = tree_with_info('basic', ['rbd_features', 'desc'], True, '', [])
        assert rows == map(list, zip(expected_tree,
                                     expected_facets,
                                     expected_rbd_features,
                                     expected_desc))

    def test_combinations_only_facets(self):
        headers, rows = get_combinations('basic', [], None, 1, None, None, True)
        self.assert_expected_combo_headers(headers)
        assert rows == [['basic', 'install', 'fixed-1', 'rbd_api_tests']]

    def test_combinations_desc_features(self):
        headers, rows = get_combinations('basic', ['desc', 'rbd_features'],
                                         None, 1, None, None, False)
        assert headers == ['desc', 'rbd_features']
        descriptions = '\n'.join([
            'install ceph',
            'single node cluster',
            'c/c++ librbd api tests with default settings',
        ])
        assert rows == [[descriptions, 'default']]

    def test_combinations_filter_in(self):
        headers, rows = get_combinations('basic', [], None, 0, ['old_format'],
                                         None, True)
        self.assert_expected_combo_headers(headers)
        assert rows == [['basic', 'install', 'fixed-1',
                         'rbd_api_tests_old_format']]

    def test_combinations_filter_out(self):
        headers, rows = get_combinations('basic', [], None, 0, None,
                                         ['old_format'], True)
        self.assert_expected_combo_headers(headers)
        assert rows == [['basic', 'install', 'fixed-1', 'rbd_api_tests']]


@patch('__builtin__.open')
@patch('os.path.isdir')
def test_extract_info_dir(m_isdir, m_open):
    simple_fs = {'a': {'b.yaml': 'meta: [{foo: c}]'}}
    _, _, _, m_isdir.side_effect, m_open.side_effect = \
        make_fake_fstools(simple_fs)
    info = extract_info('a', [])
    assert info == {}

    info = extract_info('a', ['foo', 'bar'])
    assert info == {'foo': '', 'bar': ''}

    info = extract_info('a/b.yaml', ['foo', 'bar'])
    assert info == {'foo': 'c', 'bar': ''}


@patch('__builtin__.open')
@patch('os.path.isdir')
def check_parse_error(fs, m_isdir, m_open):
    _, _, _, m_isdir.side_effect, m_open.side_effect = make_fake_fstools(fs)
    with pytest.raises(ParseError):
        a = extract_info('a.yaml', ['a'])
        raise Exception(str(a))


def test_extract_info_too_many_elements():
    check_parse_error({'a.yaml': 'meta: [{a: b}, {b: c}]'})


def test_extract_info_not_a_list():
    check_parse_error({'a.yaml': 'meta: {a: b}'})


def test_extract_info_not_a_dict():
    check_parse_error({'a.yaml': 'meta: [[a, b]]'})


@patch('__builtin__.open')
@patch('os.path.isdir')
def test_extract_info_empty_file(m_isdir, m_open):
    simple_fs = {'a.yaml': ''}
    _, _, _, m_isdir.side_effect, m_open.side_effect = \
        make_fake_fstools(simple_fs)
    info = extract_info('a.yaml', [])
    assert info == {}
