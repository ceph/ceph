# -*- coding: utf-8 -*-
import pytest

from fake_fs import make_fake_fstools
from teuthology.describe_tests import (tree_with_info, extract_info,
                                       get_combinations)
from teuthology.exceptions import ParseError

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

    def setup(self):
        self.fake_exists, self.fake_listdir, self.fake_isfile,
            self.fake_isdir, self.fake_open = make_fake_fstools(realistic_fs)

    @staticmethod
    def assert_expected_combo_headers(headers):
        assert headers == (['subsuite depth 0'] +
                           sorted(set(filter(bool, expected_facets))))

    def test_no_filters(self):
        rows = tree_with_info('basic', [], False, '', [],
                              self.fake_listdir, self.fake_isdir,
                              self.fake_open)
        assert rows == [[x] for x in expected_tree]

    def test_single_filter(self):
        rows = tree_with_info('basic', ['desc'], False, '', [],
                              self.fake_listdir, self.fake_isdir,
                              self.fake_open)
        assert rows == map(list, zip(expected_tree, expected_desc))

        rows = tree_with_info('basic', ['rbd_features'], False, '', [],
                              self.fake_listdir, self.fake_isdir,
                              self.fake_open)
        assert rows == map(list, zip(expected_tree, expected_rbd_features))

    def test_single_filter_with_facets(self):
        rows = tree_with_info('basic', ['desc'], True, '', [],
                              self.fake_listdir, self.fake_isdir,
                              self.fake_open)
        assert rows == map(list, zip(expected_tree, expected_facets,
                                     expected_desc))

        rows = tree_with_info('basic', ['rbd_features'], True, '', [],
                              self.fake_listdir, self.fake_isdir,
                              self.fake_open)
        assert rows == map(list, zip(expected_tree, expected_facets,
                                     expected_rbd_features))

    def test_no_matching(self):
        rows = tree_with_info('basic', ['extra'], False, '', [],
                              self.fake_listdir, self.fake_isdir,
                              self.fake_open)
        assert rows == map(list, zip(expected_tree, [''] * len(expected_tree)))

        rows = tree_with_info('basic', ['extra'], True, '', [],
                              self.fake_listdir, self.fake_isdir,
                              self.fake_open)
        assert rows == map(list, zip(expected_tree, expected_facets,
                                     [''] * len(expected_tree)))

    def test_multiple_filters(self):
        rows = tree_with_info('basic', ['desc', 'rbd_features'], False,
                              '', [], self.fake_listdir,
                              self.fake_isdir, self.fake_open)
        assert rows == map(list, zip(expected_tree,
                                     expected_desc,
                                     expected_rbd_features))

        rows = tree_with_info('basic', ['rbd_features', 'desc'], False,
                              '', [], self.fake_listdir,
                              self.fake_isdir, self.fake_open)
        assert rows == map(list, zip(expected_tree,
                                     expected_rbd_features,
                                     expected_desc))

    def test_multiple_filters_with_facets(self):
        rows = tree_with_info('basic', ['desc', 'rbd_features'], True,
                              '', [], self.fake_listdir,
                              self.fake_isdir, self.fake_open)
        assert rows == map(list, zip(expected_tree,
                                     expected_facets,
                                     expected_desc,
                                     expected_rbd_features))

        rows = tree_with_info('basic', ['rbd_features', 'desc'], True,
                              '', [], self.fake_listdir,
                              self.fake_isdir, self.fake_open)
        assert rows == map(list, zip(expected_tree,
                                     expected_facets,
                                     expected_rbd_features,
                                     expected_desc))

    def test_combinations_only_facets(self):
        headers, rows = get_combinations('basic', [], None, 1, None,
                                         None, True,
                                         self.fake_isdir, self.fake_open,
                                         self.fake_isfile,
                                         self.fake_listdir)
        self.assert_expected_combo_headers(headers)
        assert rows == [['basic', 'install', 'fixed-1', 'rbd_api_tests']]

    def test_combinations_desc_features(self):
        headers, rows = get_combinations('basic', ['desc', 'rbd_features'],
                                         None, 1, None, None, False,
                                         self.fake_isdir, self.fake_open,
                                         self.fake_isfile,
                                         self.fake_listdir)
        assert headers == ['desc', 'rbd_features']
        descriptions = '\n'.join([
            'install ceph',
            'single node cluster',
            'c/c++ librbd api tests with default settings',
        ])
        assert rows == [[descriptions, 'default']]

    def test_combinations_filter_in(self):
        headers, rows = get_combinations('basic', [], None, 0, ['old_format'],
                                         None, True,
                                         self.fake_isdir, self.fake_open,
                                         self.fake_isfile, self.fake_listdir)
        self.assert_expected_combo_headers(headers)
        assert rows == [['basic', 'install', 'fixed-1',
                         'rbd_api_tests_old_format']]

    def test_combinations_filter_out(self):
        headers, rows = get_combinations('basic', [], None, 0, None,
                                         ['old_format'], True,
                                         self.fake_isdir, self.fake_open,
                                         self.fake_isfile, self.fake_listdir)
        self.assert_expected_combo_headers(headers)
        assert rows == [['basic', 'install', 'fixed-1', 'rbd_api_tests']]


def test_extract_info_dir():
    simple_fs = {'a': {'b.yaml': 'meta: [{foo: c}]'}}
    _, _, _, fake_isdir, fake_open = make_fake_fstools(simple_fs)
    info = extract_info('a', [], fake_isdir, fake_open)
    assert info == {}

    info = extract_info('a', ['foo', 'bar'], fake_isdir, fake_open)
    assert info == {'foo': '', 'bar': ''}

    info = extract_info('a/b.yaml', ['foo', 'bar'], fake_isdir, fake_open)
    assert info == {'foo': 'c', 'bar': ''}


def check_parse_error(fs):
    _, _, _, fake_isdir, fake_open = make_fake_fstools(fs)
    with pytest.raises(ParseError):
        a = extract_info('a.yaml', ['a'], fake_isdir, fake_open)
        raise Exception(str(a))


def test_extract_info_too_many_elements():
    check_parse_error({'a.yaml': 'meta: [{a: b}, {b: c}]'})


def test_extract_info_not_a_list():
    check_parse_error({'a.yaml': 'meta: {a: b}'})


def test_extract_info_not_a_dict():
    check_parse_error({'a.yaml': 'meta: [[a, b]]'})


def test_extract_info_empty_file():
    simple_fs = {'a.yaml': ''}
    _, _, _, fake_isdir, fake_open = make_fake_fstools(simple_fs)
    info = extract_info('a.yaml', [], fake_isdir, fake_open)
    assert info == {}
