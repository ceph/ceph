#!/usr/bin/py.test
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
from ceph_argparse import validate_command, parse_json_funcsigs

import os
import re
import json
import pytest

def get_command_descriptions(what):
    return os.popen("./get_command_descriptions " + "--" + what).read()

def test_parse_json_funcsigs():
    commands = get_command_descriptions("all")
    cmd_json = parse_json_funcsigs(commands, 'cli')

    # syntax error https://github.com/ceph/ceph/pull/585
    commands = get_command_descriptions("pull585")
    pytest.raises(TypeError, parse_json_funcsigs, commands, 'cli')

sigdict = parse_json_funcsigs(get_command_descriptions("all"), 'cli')


class TestArgparse:

    def assert_valid_command(self, args):
        result = validate_command(sigdict, args)
        assert result != None
        assert result != {}

    def check_1_natural_arg(self, prefix, command):
        self.assert_valid_command([prefix, command, '1'])
        assert {} == validate_command(sigdict, [prefix, command])
        assert {} == validate_command(sigdict, [prefix, command, '-1'])
        assert {} == validate_command(sigdict, [prefix, command, '1',
                                                    '1'])

    def check_0_or_1_natural_arg(self, prefix, command):
        self.assert_valid_command([prefix, command, '1'])
        self.assert_valid_command([prefix, command])
        assert {} == validate_command(sigdict, [prefix, command, '-1'])
        assert {} == validate_command(sigdict, [prefix, command, '1',
                                                    '1'])

    def check_1_string_arg(self, prefix, command):
        assert {} == validate_command(sigdict, [prefix, command])
        self.assert_valid_command([prefix, command, 'string'])
        assert {} == validate_command(sigdict, [prefix,
                                                    command,
                                                    'string',
                                                    'toomany'])

    def check_1_or_more_string_args(self, prefix, command):
        assert {} == validate_command(sigdict, [prefix,
                                                    command])
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
        assert {} == validate_command(sigdict, [prefix,
                                                    command,
                                                    'toomany'])


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
        assert {} == validate_command(sigdict, ['pg', 'dump', 'invalid'])

    def test_dump_json(self):
        self.assert_valid_command(['pg', 'dump_json'])
        self.assert_valid_command(['pg', 'dump_json',
                                   'all',
                                   'summary',
                                   'sum',
                                   'pools',
                                   'osds',
                                   'pgs'])
        assert {} == validate_command(sigdict, ['pg', 'dump_json',
                                                    'invalid'])

    def test_dump_pools_json(self):
        self.assert_valid_command(['pg', 'dump_pools_json'])

    def test_dump_pools_stuck(self):
        self.assert_valid_command(['pg', 'dump_stuck'])
        self.assert_valid_command(['pg', 'dump_stuck',
                                   'inactive',
                                   'unclean',
                                   'stale'])
        assert {} == validate_command(sigdict, ['pg', 'dump_stuck',
                                                    'invalid'])
        self.assert_valid_command(['pg', 'dump_stuck',
                                   'inactive',
                                   '1234'])

    def one_pgid(self, command):
        self.assert_valid_command(['pg', command, '1.1'])
        assert {} == validate_command(sigdict, ['pg', command])
        assert {} == validate_command(sigdict, ['pg', command, '1'])

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
        assert {} == validate_command(sigdict, ['pg', 'debug'])
        assert {} == validate_command(sigdict, ['pg', 'debug',
                                                    'invalid'])

    def test_force_create_pg(self):
        self.one_pgid('force_create_pg')

    def set_ratio(self, command):
        self.assert_valid_command(['pg',
                                   command,
                                   '0.0'])
        assert {} == validate_command(sigdict, ['pg', command])
        assert {} == validate_command(sigdict, ['pg',
                                                    command,
                                                    '2.0'])

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
        assert {} == validate_command(sigdict, ['auth',
                                                    'export',
                                                    'string',
                                                    'toomany'])

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
        assert {} == validate_command(sigdict, ['auth',
                                                    'caps'])
        assert {} == validate_command(sigdict, ['auth',
                                                    'caps',
                                                    'string'])
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
        assert {} == validate_command(sigdict, ['log'])
        self.assert_valid_command(['log', 'a logtext'])
        self.assert_valid_command(['log', 'a logtext', 'and another'])

    def test_injectargs(self):
        assert {} == validate_command(sigdict, ['injectargs'])
        self.assert_valid_command(['injectargs', 'one'])
        self.assert_valid_command(['injectargs', 'one', 'two'])

    def test_status(self):
        self.assert_valid_command(['status'])

    def test_health(self):
        self.assert_valid_command(['health'])
        self.assert_valid_command(['health', 'detail'])
        assert {} == validate_command(sigdict, ['health', 'invalid'])
        assert {} == validate_command(sigdict, ['health', 'detail',
                                                    'toomany'])

    def test_df(self):
        self.assert_valid_command(['df'])
        self.assert_valid_command(['df', 'detail'])
        assert {} == validate_command(sigdict, ['df', 'invalid'])
        assert {} == validate_command(sigdict, ['df', 'detail',
                                                    'toomany'])

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
        self.assert_valid_command(['sync',
                                   'force',
                                   '--yes-i-really-mean-it'])
        self.assert_valid_command(['sync',
                                   'force'])
        assert {} == validate_command(sigdict, ['sync'])
        assert {} == validate_command(sigdict, ['sync',
                                                    'force',
                                                    '--yes-i-really-mean-it',
                                                    '--i-know-what-i-am-doing',
                                                    'toomany'])

    def test_heap(self):
        assert {} == validate_command(sigdict, ['heap'])
        assert {} == validate_command(sigdict, ['heap', 'invalid'])
        self.assert_valid_command(['heap', 'dump'])
        self.assert_valid_command(['heap', 'start_profiler'])
        self.assert_valid_command(['heap', 'stop_profiler'])
        self.assert_valid_command(['heap', 'release'])
        self.assert_valid_command(['heap', 'stats'])

    def test_quorum(self):
        assert {} == validate_command(sigdict, ['quorum'])
        assert {} == validate_command(sigdict, ['quorum', 'invalid'])
        self.assert_valid_command(['quorum', 'enter'])
        self.assert_valid_command(['quorum', 'exit'])
        assert {} == validate_command(sigdict, ['quorum',
                                                    'enter',
                                                    'toomany'])

    def test_tell(self):
        assert {} == validate_command(sigdict, ['tell'])
        assert {} == validate_command(sigdict, ['tell', 'invalid'])
        for name in ('osd', 'mon', 'client', 'mds'):
            assert {} == validate_command(sigdict, ['tell', name])
            assert {} == validate_command(sigdict, ['tell',
                                                        name + ".42"])
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
        assert {} == validate_command(sigdict, ['mds', 'tell'])
        assert {} == validate_command(sigdict, ['mds', 'tell',
                                                    'someone'])

    def test_compat_show(self):
        self.assert_valid_command(['mds', 'compat', 'show'])
        assert {} == validate_command(sigdict, ['mds', 'compat'])
        assert {} == validate_command(sigdict, ['mds', 'compat',
                                                    'show', 'toomany'])

    def test_stop(self):
        self.assert_valid_command(['mds', 'stop', 'someone'])
        assert {} == validate_command(sigdict, ['mds', 'stop'])
        assert {} == validate_command(sigdict, ['mds', 'stop',
                                                    'someone', 'toomany'])

    def test_deactivate(self):
        self.assert_valid_command(['mds', 'deactivate', 'someone'])
        assert {} == validate_command(sigdict, ['mds', 'deactivate'])
        assert {} == validate_command(sigdict, ['mds', 'deactivate',
                                                    'someone', 'toomany'])

    def test_set_max_mds(self):
        self.check_1_natural_arg('mds', 'set_max_mds')

    def test_setmap(self):
        self.check_1_natural_arg('mds', 'setmap')

    def test_set_state(self):
        self.assert_valid_command(['mds', 'set_state', '1', '2'])
        assert {} == validate_command(sigdict, ['mds', 'set_state'])
        assert {} == validate_command(sigdict, ['mds', 'set_state', '-1'])
        assert {} == validate_command(sigdict, ['mds', 'set_state',
                                                    '1', '-1'])
        assert {} == validate_command(sigdict, ['mds', 'set_state',
                                                    '1', '21'])

    def test_fail(self):
        self.check_1_string_arg('mds', 'fail')

    def test_rm(self):
        assert {} == validate_command(sigdict, ['mds', 'rm'])
        assert {} == validate_command(sigdict, ['mds', 'rm', '1'])
        for name in ('osd', 'mon', 'client', 'mds'):
            self.assert_valid_command(['mds', 'rm', '1', name + '.42'])
            assert {} == validate_command(sigdict, ['mds', 'rm',
                                                        '-1', name + '.42'])
            assert {} == validate_command(sigdict, ['mds', 'rm',
                                                        '-1', name])
            assert {} == validate_command(sigdict, ['mds', 'rm',
                                                        '1', name + '.42',
                                                        'toomany'])

    def test_rmfailed(self):
        self.check_1_natural_arg('mds', 'rmfailed')

    def test_cluster_down(self):
        self.check_no_arg('mds', 'cluster_down')

    def test_cluster_up(self):
        self.check_no_arg('mds', 'cluster_up')

    def test_compat_rm_compat(self):
        self.assert_valid_command(['mds', 'compat', 'rm_compat', '1'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_compat'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_compat', '-1'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_compat', '1', '1'])

    def test_incompat_rm_incompat(self):
        self.assert_valid_command(['mds', 'compat', 'rm_incompat', '1'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_incompat'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_incompat', '-1'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_incompat', '1', '1'])

    def test_mds_set(self):
        self.assert_valid_command(['mds', 'set', 'allow_new_snaps'])
        self.assert_valid_command(['mds', 'set', 'allow_new_snaps', 'sure'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'set',
                                                    'invalid'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'set',
                                                    'allow_new_snaps',
													'sure',
													'toomany'])

    def test_mds_unset(self):
        self.assert_valid_command(['mds', 'unset', 'allow_new_snaps'])
        self.assert_valid_command(['mds', 'unset', 'allow_new_snaps', 'sure'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'unset',
                                                    'invalid'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'unset',
                                                    'allow_new_snaps',
													'sure',
													'toomany'])

    def test_add_data_pool(self):
        self.assert_valid_command(['mds', 'add_data_pool', '1'])
        self.assert_valid_command(['mds', 'add_data_pool', 'foo'])

    def test_remove_data_pool(self):
        self.assert_valid_command(['mds', 'remove_data_pool', '1'])
        self.assert_valid_command(['mds', 'remove_data_pool', 'foo'])

    def test_newfs(self):
        self.assert_valid_command(['mds', 'newfs', '1', '2',
                                   '--yes-i-really-mean-it'])
        self.assert_valid_command(['mds', 'newfs', '1', '2'])
        assert {} == validate_command(sigdict, ['mds', 'newfs'])
        assert {} == validate_command(sigdict, ['mds', 'newfs', '1'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'newfs',
                                                    '1',
                                                    '2',
                                                    '--yes-i-really-mean-it',
                                                    'toomany'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'newfs',
                                                    '-1',
                                                    '2',
                                                    '--yes-i-really-mean-it'])
        assert {} == validate_command(sigdict, ['mds',
                                                    'newfs',
                                                    '1',
                                                    '-1',
                                                    '--yes-i-really-mean-it'])


class TestMon(TestArgparse):

    def test_dump(self):
        self.check_0_or_1_natural_arg('mon', 'dump')

    def test_stat(self):
        self.check_no_arg('mon', 'stat')

    def test_getmap(self):
        self.check_0_or_1_natural_arg('mon', 'getmap')

    def test_add(self):
        self.assert_valid_command(['mon', 'add', 'name', '1.2.3.4:1234'])
        assert {} == validate_command(sigdict, ['mon', 'add'])
        assert {} == validate_command(sigdict, ['mon', 'add', 'name'])
        assert {} == validate_command(sigdict, ['mon', 'add',
                                                    'name',
                                                    '400.500.600.700'])
        assert {} == validate_command(sigdict, ['mon', 'add', 'name',
                                                    '1.2.3.4:1234',
                                                    'toomany'])

    def test_remove(self):
        self.assert_valid_command(['mon', 'remove', 'name'])
        assert {} == validate_command(sigdict, ['mon', 'remove'])
        assert {} == validate_command(sigdict, ['mon', 'remove',
                                                    'name', 'toomany'])


class TestOSD(TestArgparse):

    def test_stat(self):
        self.check_no_arg('osd', 'stat')

    def test_dump(self):
        self.check_0_or_1_natural_arg('osd', 'dump')

    def test_osd_tree(self):
        self.check_0_or_1_natural_arg('osd', 'tree')

    def test_osd_ls(self):
        self.check_0_or_1_natural_arg('osd', 'ls')

    def test_osd_getmap(self):
        self.check_0_or_1_natural_arg('osd', 'getmap')

    def test_osd_getcrushmap(self):
        self.check_0_or_1_natural_arg('osd', 'getcrushmap')

    def test_perf(self):
        self.check_no_arg('osd', 'perf')

    def test_getmaxosd(self):
        self.check_no_arg('osd', 'getmaxosd')

    def test_find(self):
        self.check_1_natural_arg('osd', 'find')

    def test_map(self):
        self.assert_valid_command(['osd', 'map', 'poolname', 'objectname'])
        assert {} == validate_command(sigdict, ['osd', 'map'])
        assert {} == validate_command(sigdict, ['osd', 'map', 'poolname'])
        assert {} == validate_command(sigdict, ['osd', 'map',
                                                    'poolname', 'objectname',
                                                    'toomany'])

    def test_metadata(self):
        self.check_1_natural_arg('osd', 'metadata')

    def test_scrub(self):
        self.check_1_string_arg('osd', 'scrub')

    def test_deep_scrub(self):
        self.check_1_string_arg('osd', 'deep-scrub')

    def test_repair(self):
        self.check_1_string_arg('osd', 'repair')

    def test_lspools(self):
        self.assert_valid_command(['osd', 'lspools'])
        self.assert_valid_command(['osd', 'lspools', '1'])
        self.assert_valid_command(['osd', 'lspools', '-1'])
        assert {} == validate_command(sigdict, ['osd', 'lspools',
                                                    '1', 'toomany'])

    def test_blacklist_ls(self):
        self.assert_valid_command(['osd', 'blacklist', 'ls'])
        assert {} == validate_command(sigdict, ['osd', 'blacklist'])
        assert {} == validate_command(sigdict, ['osd', 'blacklist',
                                                    'ls', 'toomany'])

    def test_crush_rule(self):
        assert {} == validate_command(sigdict, ['osd', 'crush'])
        assert {} == validate_command(sigdict, ['osd', 'crush', 'rule'])
        for subcommand in ('list', 'ls', 'dump'):
            self.assert_valid_command(['osd', 'crush', 'rule', subcommand])
            assert {} == validate_command(sigdict, ['osd', 'crush',
                                                        'rule', subcommand,
                                                        'toomany'])

    def test_crush_dump(self):
        self.assert_valid_command(['osd', 'crush', 'dump'])
        assert {} == validate_command(sigdict, ['osd', 'crush'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'dump', 'toomany'])

    def test_setcrushmap(self):
        self.check_no_arg('osd', 'setcrushmap')

    def test_crush_add_bucket(self):
        self.assert_valid_command(['osd', 'crush', 'add-bucket',
                                   'name', 'type'])
        assert {} == validate_command(sigdict, ['osd', 'crush'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'add-bucket'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'add-bucket', 'name',
                                                    'type',
                                                    'toomany'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'add-bucket', '!!!',
                                                    'type'])

    def check_crush_setter(self, setter):
        self.assert_valid_command(['osd', 'crush', setter,
                                   '*', '2.3', 'AZaz09-_.='])
        self.assert_valid_command(['osd', 'crush', setter,
                                   'osd.0', '2.3', 'AZaz09-_.='])
        self.assert_valid_command(['osd', 'crush', setter,
                                   '0', '2.3', 'AZaz09-_.='])
        self.assert_valid_command(['osd', 'crush', setter,
                                   '0', '2.3', 'AZaz09-_.=', 'AZaz09-_.='])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    setter,
                                                    'osd.0'])
        ret = validate_command(sigdict, ['osd', 'crush',
                                             setter,
                                             'osd.0',
                                             '-1.0'])
        assert ret in [None, {}]
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    setter,
                                                    'osd.0',
                                                    '1.0',
                                                    '!!!'])

    def test_crush_set(self):
        assert {} == validate_command(sigdict, ['osd', 'crush'])
        self.check_crush_setter('set')

    def test_crush_add(self):
        assert {} == validate_command(sigdict, ['osd', 'crush'])
        self.check_crush_setter('add')

    def test_crush_create_or_move(self):
        assert {} == validate_command(sigdict, ['osd', 'crush'])
        self.check_crush_setter('create-or-move')

    def test_crush_move(self):
        self.assert_valid_command(['osd', 'crush', 'move',
                                   'AZaz09-_.', 'AZaz09-_.='])
        self.assert_valid_command(['osd', 'crush', 'move',
                                   '0', 'AZaz09-_.=', 'AZaz09-_.='])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'move'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'move', 'AZaz09-_.'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'move', '!!!',
                                                    'AZaz09-_.='])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'move', 'AZaz09-_.',
                                                    '!!!'])

    def test_crush_link(self):
        self.assert_valid_command(['osd', 'crush', 'link',
                                   'name', 'AZaz09-_.='])
        self.assert_valid_command(['osd', 'crush', 'link',
                                   'name', 'AZaz09-_.=', 'AZaz09-_.='])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'link'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'link',
                                                    'name'])

    def test_crush_rm(self):
        for alias in ('rm', 'remove', 'unlink'):
            self.assert_valid_command(['osd', 'crush', alias, 'AZaz09-_.'])
            self.assert_valid_command(['osd', 'crush', alias,
                                       'AZaz09-_.', 'AZaz09-_.'])
            assert {} == validate_command(sigdict, ['osd', 'crush',
                                                        alias])
            assert {} == validate_command(sigdict, ['osd', 'crush',
                                                        alias,
                                                        'AZaz09-_.',
                                                        'AZaz09-_.',
                                                        'toomany'])

    def test_crush_reweight(self):
        self.assert_valid_command(['osd', 'crush', 'reweight',
                                   'AZaz09-_.', '2.3'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'reweight'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'reweight',
                                                    'AZaz09-_.'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'reweight',
                                                    'AZaz09-_.',
                                                    '-1.0'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'reweight',
                                                    '!!!',
                                                    '2.3'])

    def test_crush_tunables(self):
        for tunable in ('legacy', 'argonaut', 'bobtail', 'optimal', 'default'):
            self.assert_valid_command(['osd', 'crush', 'tunables',
                                       tunable])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'tunables'])
        assert None == validate_command(sigdict, ['osd', 'crush',
                                                      'default', 'toomany'])

    def test_crush_rule_create_simple(self):
        self.assert_valid_command(['osd', 'crush', 'rule', 'create-simple',
                                   'AZaz09-_.', 'AZaz09-_.', 'AZaz09-_.'])
        assert None == validate_command(sigdict, ['osd', 'crush',
                                                      'create-simple'])
        assert None == validate_command(sigdict, ['osd', 'crush',
                                                      'create-simple',
                                                      'AZaz09-_.'])
        assert None == validate_command(sigdict, ['osd', 'crush',
                                                      'create-simple',
                                                      'AZaz09-_.',
                                                      'AZaz09-_.'])
        assert None == validate_command(sigdict, ['osd', 'crush',
                                                      'create-simple',
                                                      '!!!',
                                                      'AZaz09-_.',
                                                      'AZaz09-_.'])
        assert None == validate_command(sigdict, ['osd', 'crush',
                                                      'create-simple',
                                                      'AZaz09-_.',
                                                      '|||',
                                                      'AZaz09-_.'])
        assert None == validate_command(sigdict, ['osd', 'crush',
                                                      'create-simple',
                                                      'AZaz09-_.',
                                                      'AZaz09-_.',
                                                      '+++'])
        assert None == validate_command(sigdict, ['osd', 'crush',
                                                      'create-simple',
                                                      'AZaz09-_.',
                                                      'AZaz09-_.',
                                                      'AZaz09-_.',
                                                      'toomany'])

    def test_crush_rule_rm(self):
        self.assert_valid_command(['osd', 'crush', 'rule', 'rm', 'AZaz09-_.'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'rule', 'rm'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'rule', 'rm',
                                                    '!!!!'])
        assert {} == validate_command(sigdict, ['osd', 'crush',
                                                    'rule', 'rm',
                                                    'AZaz09-_.',
                                                    'toomany'])

    def test_setmaxosd(self):
        self.check_1_natural_arg('osd', 'setmaxosd')

    def test_pause(self):
        self.check_no_arg('osd', 'pause')

    def test_unpause(self):
        self.check_no_arg('osd', 'unpause')

    def test_set_unset(self):
        for action in ('set', 'unset'):
            for flag in ('pause', 'noup', 'nodown', 'noout', 'noin',
                         'nobackfill', 'norecover', 'noscrub', 'nodeep-scrub'):
                self.assert_valid_command(['osd', action, flag])
            assert {} == validate_command(sigdict, ['osd', action])
            assert {} == validate_command(sigdict, ['osd', action,
                                                        'invalid'])
            assert {} == validate_command(sigdict, ['osd', action,
                                                        'pause', 'toomany'])

    def test_cluster_snap(self):
        assert None == validate_command(sigdict, ['osd', 'cluster_snap'])

    def test_down(self):
        self.check_1_or_more_string_args('osd', 'down')

    def test_out(self):
        self.check_1_or_more_string_args('osd', 'out')

    def test_in(self):
        self.check_1_or_more_string_args('osd', 'in')

    def test_rm(self):
        self.check_1_or_more_string_args('osd', 'rm')

    def test_reweight(self):
        self.assert_valid_command(['osd', 'reweight', '1', '0.1'])
        assert {} == validate_command(sigdict, ['osd', 'reweight'])
        assert {} == validate_command(sigdict, ['osd', 'reweight',
                                                    '1'])
        assert {} == validate_command(sigdict, ['osd', 'reweight',
                                                    '1', '2.0'])
        assert {} == validate_command(sigdict, ['osd', 'reweight',
                                                    '-1', '0.1'])
        assert {} == validate_command(sigdict, ['osd', 'reweight',
                                                    '1', '0.1',
                                                    'toomany'])

    def test_lost(self):
        self.assert_valid_command(['osd', 'lost', '1',
                                   '--yes-i-really-mean-it'])
        self.assert_valid_command(['osd', 'lost', '1'])
        assert {} == validate_command(sigdict, ['osd', 'lost'])
        assert {} == validate_command(sigdict, ['osd', 'lost',
                                                    '1',
                                                    'what?'])
        assert {} == validate_command(sigdict, ['osd', 'lost',
                                                    '-1',
                                                    '--yes-i-really-mean-it'])
        assert {} == validate_command(sigdict, ['osd', 'lost',
                                                    '1',
                                                    '--yes-i-really-mean-it',
                                                    'toomany'])

    def test_create(self):
        uuid = '12345678123456781234567812345678'
        self.assert_valid_command(['osd', 'create'])
        self.assert_valid_command(['osd', 'create',
                                   uuid])
        assert {} == validate_command(sigdict, ['osd', 'create',
                                                    'invalid'])
        assert {} == validate_command(sigdict, ['osd', 'create',
                                                    uuid,
                                                    'toomany'])

    def test_blacklist(self):
        for action in ('add', 'rm'):
            self.assert_valid_command(['osd', 'blacklist', action,
                                       '1.2.3.4/567'])
            self.assert_valid_command(['osd', 'blacklist', action,
                                       '1.2.3.4'])
            self.assert_valid_command(['osd', 'blacklist', action,
                                       '1.2.3.4/567', '600.40'])
            self.assert_valid_command(['osd', 'blacklist', action,
                                       '1.2.3.4', '600.40'])
            assert {} == validate_command(sigdict, ['osd', 'blacklist',
                                                        action,
                                                        'invalid',
                                                        '600.40'])
            assert {} == validate_command(sigdict, ['osd', 'blacklist',
                                                        action,
                                                        '1.2.3.4/567',
                                                        '-1.0'])
            assert {} == validate_command(sigdict, ['osd', 'blacklist',
                                                        action,
                                                        '1.2.3.4/567',
                                                        '600.40',
                                                        'toomany'])

    def test_pool_mksnap(self):
        self.assert_valid_command(['osd', 'pool', 'mksnap',
                                   'poolname', 'snapname'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'mksnap'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'mksnap',
                                                    'poolname'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'mksnap',
                                                    'poolname', 'snapname',
                                                    'toomany'])

    def test_pool_rmsnap(self):
        self.assert_valid_command(['osd', 'pool', 'rmsnap',
                                   'poolname', 'snapname'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'rmsnap'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'rmsnap',
                                                    'poolname'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'rmsnap',
                                                    'poolname', 'snapname',
                                                    'toomany'])

    def test_pool_create(self):
        self.assert_valid_command(['osd', 'pool', 'create',
                                   'poolname', '128'])
        self.assert_valid_command(['osd', 'pool', 'create',
                                   'poolname', '128', '128'])
        self.assert_valid_command(['osd', 'pool', 'create',
                                   'poolname', '128', '128',
                                   'foo=bar'])
        self.assert_valid_command(['osd', 'pool', 'create',
                                   'poolname', '128', '128',
                                   'foo=bar', 'baz=frob'])
        self.assert_valid_command(['osd', 'pool', 'create',
                                   'poolname', '128',
                                   'foo=bar', 'baz=frob'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'create'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'create',
                                                    'poolname'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'create',
                                                    'poolname', '-1'])

    def test_pool_delete(self):
        self.assert_valid_command(['osd', 'pool', 'delete',
                                   'poolname', 'poolname',
                                   '--yes-i-really-really-mean-it'])
        self.assert_valid_command(['osd', 'pool', 'delete',
                                   'poolname', 'poolname'])
        self.assert_valid_command(['osd', 'pool', 'delete',
                                   'poolname'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'delete'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'delete',
                                                    'poolname', 'poolname',
                                                    'not really'])
        assert {} == validate_command(sigdict,
                                          ['osd', 'pool', 'delete',
                                           'poolname', 'poolname',
                                           '--yes-i-really-really-mean-it',
                                           'toomany'])

    def test_pool_rename(self):
        self.assert_valid_command(['osd', 'pool', 'rename',
                                   'poolname', 'othername'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'rename'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'rename',
                                                    'poolname'])
        assert {} == validate_command(sigdict, ['osd', 'pool', 'rename',
                                                    'poolname', 'othername',
                                                    'toomany'])

    def test_pool_get(self):
        for var in ('size', 'min_size', 'crash_replay_interval',
                    'pg_num', 'pgp_num', 'crush_ruleset'):
            self.assert_valid_command(['osd', 'pool', 'get', 'poolname', var])
        assert {} == validate_command(sigdict, ['osd', 'pool'])
        assert {} == validate_command(sigdict, ['osd', 'pool',
                                                    'get'])
        assert {} == validate_command(sigdict, ['osd', 'pool',
                                                    'get', 'poolname'])
        assert {} == validate_command(sigdict, ['osd', 'pool',
                                                    'get', 'poolname',
                                                    'size', 'toomany'])
        assert {} == validate_command(sigdict, ['osd', 'pool',
                                                    'get', 'poolname',
                                                    'invalid'])

    def test_pool_set(self):
        for var in ('size', 'min_size', 'crash_replay_interval',
                    'pg_num', 'pgp_num', 'crush_ruleset',
					'hashpspool'):
            self.assert_valid_command(['osd', 'pool',
                                       'set', 'poolname', var, 'value'])
        assert {} == validate_command(sigdict, ['osd', 'pool',
                                                'set'])
        assert {} ==  validate_command(sigdict, ['osd', 'pool',
                                                 'set', 'poolname'])
        assert {} == validate_command(sigdict, ['osd', 'pool',
                                                'set', 'poolname',
                                                'size', 'value',
                                                'toomany'])

    def test_pool_set_quota(self):
        for field in ('max_objects', 'max_bytes'):
            self.assert_valid_command(['osd', 'pool', 'set-quota',
                                       'poolname', field, '10K'])
        assert {} == validate_command(sigdict, ['osd', 'pool',
                                                    'set-quota'])
        assert {} == validate_command(sigdict, ['osd', 'pool',
                                                    'set-quota',
                                                    'poolname'])
        assert {} == validate_command(sigdict, ['osd', 'pool',
                                                    'set-quota',
                                                    'poolname',
                                                    'max_objects'])
        assert {} == validate_command(sigdict, ['osd', 'pool',
                                                    'set-quota',
                                                    'poolname',
                                                    'invalid',
                                                    '10K'])
        assert {} == validate_command(sigdict, ['osd', 'pool',
                                                    'set-quota',
                                                    'poolname',
                                                    'max_objects',
                                                    '10K',
                                                    'toomany'])

    def test_reweight_by_utilization(self):
        self.assert_valid_command(['osd', 'reweight-by-utilization'])
        self.assert_valid_command(['osd', 'reweight-by-utilization', '100'])
        assert {} == validate_command(sigdict, ['osd',
                                                    'reweight-by-utilization',
                                                    '50'])
        assert {} == validate_command(sigdict, ['osd',
                                                    'reweight-by-utilization',
                                                    '100',
                                                    'toomany'])

    def test_thrash(self):
        self.check_1_natural_arg('osd', 'thrash')

    def test_tier_op(self):
        for op in ('add', 'remove', 'set-overlay'):
            self.assert_valid_command(['osd', 'tier', op,
                                       'poolname', 'othername'])
            assert {} == validate_command(sigdict, ['osd', 'tier', op])
            assert {} == validate_command(sigdict, ['osd', 'tier', op,
                                                        'poolname'])
            assert {} == validate_command(sigdict, ['osd', 'tier', op,
                                                        'poolname',
                                                        'othername',
                                                        'toomany'])

    def test_tier_cache_mode(self):
        for mode in ('none', 'writeback', 'invalidate+forward', 'readonly'):
            self.assert_valid_command(['osd', 'tier', 'cache-mode',
                                       'poolname', mode])
        assert {} == validate_command(sigdict, ['osd', 'tier',
                                                    'cache-mode'])
        assert {} == validate_command(sigdict, ['osd', 'tier',
                                                    'cache-mode',
                                                    'invalid'])

    def test_tier_remove_overlay(self):
        self.assert_valid_command(['osd', 'tier', 'remove-overlay',
                                   'poolname'])
        assert {} == validate_command(sigdict, ['osd', 'tier',
                                                    'remove-overlay'])
        assert {} == validate_command(sigdict, ['osd', 'tier',
                                                    'remove-overlay',
                                                    'poolname',
                                                    'toomany'])


class TestConfigKey(TestArgparse):

    def test_get(self):
        self.check_1_string_arg('config-key', 'get')

    def test_put(self):
        self.assert_valid_command(['config-key', 'put',
                                   'key'])
        self.assert_valid_command(['config-key', 'put',
                                   'key', 'value'])
        assert {} == validate_command(sigdict, ['config-key', 'put'])
        assert {} == validate_command(sigdict, ['config-key', 'put',
                                                    'key', 'value',
                                                    'toomany'])

    def test_del(self):
        self.check_1_string_arg('config-key', 'del')

    def test_exists(self):
        self.check_1_string_arg('config-key', 'exists')

    def test_list(self):
        self.check_no_arg('config-key', 'list')
#
# Local Variables:
# compile-command: "cd ../.. ; make -j4 && 
#  PYTHONPATH=pybind py.test \
#  test/pybind/test_ceph_argparse.py
# "
# End:
#
