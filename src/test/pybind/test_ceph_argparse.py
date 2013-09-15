#!/usr/bin/nosetests --nocapture
# -*- mode:python; tab-width:4; indent-tabs-mode:t -*-
# vim: ts=4 sw=4 smarttab expandtab
#
# Ceph - scalable distributed file system
#
# Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#

from nose.tools import eq_ as eq
from nose.tools import *

from ceph_argparse import validate_command, parse_json_funcsigs

import os
import re
import json

def get_command_descriptions(what):
    buffer = os.popen("./get_command_descriptions " + "--" + what
					  + " 2>&1 | grep cmd000").read()
    return re.sub(r'^.*?(\{.*\})', '\g<1>', buffer)

def test_parse_json_funcsigs():
    commands = get_command_descriptions("all")
    cmd_json = parse_json_funcsigs(commands, 'cli')

    # syntax error https://github.com/ceph/ceph/pull/585
    commands = get_command_descriptions("pull585")
    assert_raises(TypeError, parse_json_funcsigs, commands, 'cli')

sigdict = parse_json_funcsigs(get_command_descriptions("all"), 'cli')


class TestArgparse:

    def assert_valid_command(self, args):
        result = validate_command(sigdict, args)
        assert_not_in(result, [None, {}])

    def check_1_natural_arg(self, prefix, command):
        self.assert_valid_command([prefix, command, '1'])
        assert_equal({}, validate_command(sigdict, [prefix, command]))
        assert_equal({}, validate_command(sigdict, [prefix, command, '-1']))
        assert_equal({}, validate_command(sigdict, [prefix, command, '1',
                                                    '1']))

    def check_0_or_1_natural_arg(self, prefix, command):
        self.assert_valid_command([prefix, command, '1'])
        self.assert_valid_command([prefix, command])
        assert_equal({}, validate_command(sigdict, [prefix, command, '-1']))
        assert_equal({}, validate_command(sigdict, [prefix, command, '1',
                                                    '1']))

    def check_1_string_arg(self, prefix, command):
        assert_equal({}, validate_command(sigdict, [prefix, command]))
        self.assert_valid_command([prefix, command, 'string'])
        assert_equal({}, validate_command(sigdict, [prefix,
                                                    command,
                                                    'string',
                                                    'toomany']))

    def check_1_or_more_string_args(self, prefix, command):
        assert_equal({}, validate_command(sigdict, [prefix,
                                                    command]))
        self.assert_valid_command([prefix,
                                   command,
                                   'string'])
        self.assert_valid_command([prefix,
                                   command,
                                   'string',
                                   'more string'])

    def check_no_arg(self, prefix, command):
        self.assert_valid_command([prefix,
                                   command])
        assert_equal({}, validate_command(sigdict, [prefix,
                                                    command,
                                                    'toomany']))


class TestPG(TestArgparse):

    def test_stat(self):
        self.assert_valid_command(['pg', 'stat'])

    def test_getmap(self):
        self.assert_valid_command(['pg', 'getmap'])

    def test_send_pg_creates(self):
        self.assert_valid_command(['pg', 'send_pg_creates'])

    def test_dump(self):
        self.assert_valid_command(['pg', 'dump'])
        self.assert_valid_command(['pg', 'dump',
                                   'all',
                                   'summary',
                                   'sum',
                                   'delta',
                                   'pools',
                                   'osds',
                                   'pgs',
                                   'pgs_brief'])
        assert_equal({}, validate_command(sigdict, ['pg', 'dump', 'invalid']))

    def test_dump_json(self):
        self.assert_valid_command(['pg', 'dump_json'])
        self.assert_valid_command(['pg', 'dump_json',
                                   'all',
                                   'summary',
                                   'sum',
                                   'pools',
                                   'osds',
                                   'pgs'])
        assert_equal({}, validate_command(sigdict, ['pg', 'dump_json',
                                                    'invalid']))

    def test_dump_pools_json(self):
        self.assert_valid_command(['pg', 'dump_pools_json'])

    def test_dump_pools_stuck(self):
        self.assert_valid_command(['pg', 'dump_stuck'])
        self.assert_valid_command(['pg', 'dump_stuck',
                                   'inactive',
                                   'unclean',
                                   'stale'])
        assert_equal({}, validate_command(sigdict, ['pg', 'dump_stuck',
                                                    'invalid']))
        self.assert_valid_command(['pg', 'dump_stuck',
                                   'inactive',
                                   '1234'])

    def one_pgid(self, command):
        self.assert_valid_command(['pg', command, '1.1'])
        assert_equal({}, validate_command(sigdict, ['pg', command]))
        assert_equal({}, validate_command(sigdict, ['pg', command, '1']))

    def test_map(self):
        self.one_pgid('map')

    def test_scrub(self):
        self.one_pgid('scrub')

    def test_deep_scrub(self):
        self.one_pgid('deep-scrub')

    def test_repair(self):
        self.one_pgid('repair')

    def test_debug(self):
        self.assert_valid_command(['pg',
                                   'debug',
                                   'unfound_objects_exist'])
        self.assert_valid_command(['pg',
                                   'debug',
                                   'degraded_pgs_exist'])
        assert_equal({}, validate_command(sigdict, ['pg', 'debug']))
        assert_equal({}, validate_command(sigdict, ['pg', 'debug',
                                                    'invalid']))

    def test_force_create_pg(self):
        self.one_pgid('force_create_pg')

    def set_ratio(self, command):
        self.assert_valid_command(['pg',
                                   command,
                                   '0.0'])
        assert_equal({}, validate_command(sigdict, ['pg', command]))
        assert_equal({}, validate_command(sigdict, ['pg',
                                                    command,
                                                    '2.0']))

    def test_set_full_ratio(self):
        self.set_ratio('set_full_ratio')

    def test_set_nearfull_ratio(self):
        self.set_ratio('set_nearfull_ratio')


class TestAuth(TestArgparse):

    def test_export(self):
        self.assert_valid_command(['auth', 'export'])
        self.assert_valid_command(['auth',
                                   'export',
                                   'string'])
        assert_equal({}, validate_command(sigdict, ['auth',
                                                    'export',
                                                    'string',
                                                    'toomany']))

    def test_get(self):
        self.check_1_string_arg('auth', 'get')

    def test_get_key(self):
        self.check_1_string_arg('auth', 'get-key')

    def test_print_key(self):
        self.check_1_string_arg('auth', 'print-key')
        self.check_1_string_arg('auth', 'print_key')

    def test_list(self):
        self.check_no_arg('auth', 'list')

    def test_import(self):
        self.check_no_arg('auth', 'import')

    def test_add(self):
        self.check_1_or_more_string_args('auth', 'add')

    def test_get_or_create_key(self):
        self.check_1_or_more_string_args('auth', 'get-or-create-key')

    def test_get_or_create(self):
        self.check_1_or_more_string_args('auth', 'get-or-create')

    def test_caps(self):
        assert_equal({}, validate_command(sigdict, ['auth',
                                                    'caps']))
        assert_equal({}, validate_command(sigdict, ['auth',
                                                    'caps',
                                                    'string']))
        self.assert_valid_command(['auth',
                                   'caps',
                                   'string',
                                   'more string'])

    def test_del(self):
        self.check_1_string_arg('auth', 'del')


class TestMonitor(TestArgparse):

    def test_compact(self):
        self.assert_valid_command(['compact'])

    def test_scrub(self):
        self.assert_valid_command(['scrub'])

    def test_fsid(self):
        self.assert_valid_command(['fsid'])

    def test_log(self):
        assert_equal({}, validate_command(sigdict, ['log']))
        self.assert_valid_command(['log', 'a logtext'])
        self.assert_valid_command(['log', 'a logtext', 'and another'])

    def test_injectargs(self):
        assert_equal({}, validate_command(sigdict, ['injectargs']))
        self.assert_valid_command(['injectargs', 'one'])
        self.assert_valid_command(['injectargs', 'one', 'two'])

    def test_status(self):
        self.assert_valid_command(['status'])

    def test_health(self):
        self.assert_valid_command(['health'])
        self.assert_valid_command(['health', 'detail'])
        assert_equal({}, validate_command(sigdict, ['health', 'invalid']))
        assert_equal({}, validate_command(sigdict, ['health', 'detail',
                                                    'toomany']))

    def test_df(self):
        self.assert_valid_command(['df'])
        self.assert_valid_command(['df', 'detail'])
        assert_equal({}, validate_command(sigdict, ['df', 'invalid']))
        assert_equal({}, validate_command(sigdict, ['df', 'detail',
                                                    'toomany']))

    def test_report(self):
        self.assert_valid_command(['report'])
        self.assert_valid_command(['report', 'tag1'])
        self.assert_valid_command(['report', 'tag1', 'tag2'])

    def test_quorum_status(self):
        self.assert_valid_command(['quorum_status'])

    def test_mon_status(self):
        self.assert_valid_command(['mon_status'])

    def test_sync_force(self):
        self.assert_valid_command(['sync',
                                   'force',
                                   '--yes-i-really-mean-it',
                                   '--i-know-what-i-am-doing'])
        assert_equal({}, validate_command(sigdict, ['sync']))
        assert_equal({}, validate_command(sigdict, ['sync',
                                                    'force']))
        assert_equal({}, validate_command(sigdict, ['sync',
                                                    'force',
                                                    '--yes-i-really-mean-it']))
        assert_equal({}, validate_command(sigdict, ['sync',
                                                    'force',
                                                    '--yes-i-really-mean-it',
                                                    '--i-know-what-i-am-doing',
                                                    'toomany']))

    def test_heap(self):
        assert_equal({}, validate_command(sigdict, ['heap']))
        assert_equal({}, validate_command(sigdict, ['heap', 'invalid']))
        self.assert_valid_command(['heap', 'dump'])
        self.assert_valid_command(['heap', 'start_profiler'])
        self.assert_valid_command(['heap', 'stop_profiler'])
        self.assert_valid_command(['heap', 'release'])
        self.assert_valid_command(['heap', 'stats'])

    def test_quorum(self):
        assert_equal({}, validate_command(sigdict, ['quorum']))
        assert_equal({}, validate_command(sigdict, ['quorum', 'invalid']))
        self.assert_valid_command(['quorum', 'enter'])
        self.assert_valid_command(['quorum', 'exit'])
        assert_equal({}, validate_command(sigdict, ['quorum',
                                                    'enter',
                                                    'toomany']))

    def test_tell(self):
        assert_equal({}, validate_command(sigdict, ['tell']))
        assert_equal({}, validate_command(sigdict, ['tell', 'invalid']))
        for name in ('osd', 'mon', 'client', 'mds'):
            assert_equal({}, validate_command(sigdict, ['tell', name]))
            assert_equal({}, validate_command(sigdict, ['tell',
                                                        name + ".42"]))
            self.assert_valid_command(['tell', name + ".42", 'something'])
            self.assert_valid_command(['tell', name + ".42",
                                       'something',
                                       'something else'])


class TestMDS(TestArgparse):

    def test_stat(self):
        self.check_no_arg('mds', 'stat')

    def test_dump(self):
        self.check_0_or_1_natural_arg('mds', 'dump')

    def test_tell(self):
        self.assert_valid_command(['mds', 'tell',
                                   'someone',
                                   'something'])
        self.assert_valid_command(['mds', 'tell',
                                   'someone',
                                   'something',
                                   'something else'])
        assert_equal({}, validate_command(sigdict, ['mds', 'tell']))
        assert_equal({}, validate_command(sigdict, ['mds', 'tell',
                                                    'someone']))

    def test_compat_show(self):
        self.assert_valid_command(['mds', 'compat', 'show'])
        assert_equal({}, validate_command(sigdict, ['mds', 'compat']))
        assert_equal({}, validate_command(sigdict, ['mds', 'compat',
                                                    'show', 'toomany']))

    def test_stop(self):
        self.assert_valid_command(['mds', 'stop', 'someone'])
        assert_equal({}, validate_command(sigdict, ['mds', 'stop']))
        assert_equal({}, validate_command(sigdict, ['mds', 'stop',
                                                    'someone', 'toomany']))

    def test_deactivate(self):
        self.assert_valid_command(['mds', 'deactivate', 'someone'])
        assert_equal({}, validate_command(sigdict, ['mds', 'deactivate']))
        assert_equal({}, validate_command(sigdict, ['mds', 'deactivate',
                                                    'someone', 'toomany']))

    def test_set_max_mds(self):
        self.check_1_natural_arg('mds', 'set_max_mds')

    def test_setmap(self):
        self.check_1_natural_arg('mds', 'setmap')

    def test_set_state(self):
        self.assert_valid_command(['mds', 'set_state', '1', '2'])
        assert_equal({}, validate_command(sigdict, ['mds', 'set_state']))
        assert_equal({}, validate_command(sigdict, ['mds', 'set_state', '-1']))
        assert_equal({}, validate_command(sigdict, ['mds', 'set_state',
                                                    '1', '-1']))
        assert_equal({}, validate_command(sigdict, ['mds', 'set_state',
                                                    '1', '21']))

    def test_fail(self):
        self.check_1_string_arg('mds', 'fail')

    def test_rm(self):
        assert_equal({}, validate_command(sigdict, ['mds', 'rm']))
        assert_equal({}, validate_command(sigdict, ['mds', 'rm', '1']))
        for name in ('osd', 'mon', 'client', 'mds'):
            self.assert_valid_command(['mds', 'rm', '1', name + '.42'])
            assert_equal({}, validate_command(sigdict, ['mds', 'rm',
                                                        '-1', name + '.42']))
            assert_equal({}, validate_command(sigdict, ['mds', 'rm',
                                                        '-1', name]))
            assert_equal({}, validate_command(sigdict, ['mds', 'rm',
                                                        '1', name + '.42',
                                                        'toomany']))

    def test_rmfailed(self):
        self.check_1_natural_arg('mds', 'rmfailed')

    def test_cluster_down(self):
        self.check_no_arg('mds', 'cluster_down')

    def test_cluster_up(self):
        self.check_no_arg('mds', 'cluster_up')

    def test_compat_rm_compat(self):
        self.assert_valid_command(['mds', 'compat', 'rm_compat', '1'])
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_compat']))
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_compat', '-1']))
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_compat', '1', '1']))

    def test_incompat_rm_incompat(self):
        self.assert_valid_command(['mds', 'compat', 'rm_incompat', '1'])
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_incompat']))
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_incompat', '-1']))
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_incompat', '1', '1']))

    def test_add_data_pool(self):
        self.check_1_natural_arg('mds', 'add_data_pool')

    def test_remove_data_pool(self):
        self.check_1_natural_arg('mds', 'remove_data_pool')

    def test_newfs(self):
        self.assert_valid_command(['mds', 'newfs', '1', '2',
                                   '--yes-i-really-mean-it'])
        assert_equal({}, validate_command(sigdict, ['mds', 'newfs']))
        assert_equal({}, validate_command(sigdict, ['mds', 'newfs', '1']))
        assert_equal({}, validate_command(sigdict, ['mds', 'newfs', '1', '1']))
        assert_equal({}, validate_command(sigdict, ['mds', 'newfs', '1', '1',
                                                    'no I dont']))
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'newfs',
                                                    '1',
                                                    '2',
                                                    '--yes-i-really-mean-it',
                                                    'toomany']))
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'newfs',
                                                    '-1',
                                                    '2',
                                                    '--yes-i-really-mean-it']))
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'newfs',
                                                    '1',
                                                    '-1',
                                                    '--yes-i-really-mean-it']))


class TestMon(TestArgparse):

    def test_dump(self):
        self.check_0_or_1_natural_arg('mon', 'dump')

    def test_stat(self):
        self.check_no_arg('mon', 'stat')

    def test_getmap(self):
        self.check_0_or_1_natural_arg('mon', 'getmap')

    def test_add(self):
        self.assert_valid_command(['mon', 'add', 'name', '1.2.3.4:1234'])
        assert_equal({}, validate_command(sigdict, ['mon', 'add']))
        assert_equal({}, validate_command(sigdict, ['mon', 'add', 'name']))
        assert_equal({}, validate_command(sigdict, ['mon', 'add',
                                                    'name',
                                                    '400.500.600.700']))
        assert_equal({}, validate_command(sigdict, ['mon', 'add', 'name',
                                                    '1.2.3.4:1234',
                                                    'toomany']))

    def test_remove(self):
        self.assert_valid_command(['mon', 'remove', 'name'])
        assert_equal({}, validate_command(sigdict, ['mon', 'remove']))
        assert_equal({}, validate_command(sigdict, ['mon', 'remove',
                                                    'name', 'toomany']))
# Local Variables:
# compile-command: "cd ../.. ; make -j4 && 
#  PYTHONPATH=pybind nosetests --stop \
#  test/pybind/test_ceph_argparse.py # test_ceph_argparse.py:TestOSD.test_rm"
# End:
