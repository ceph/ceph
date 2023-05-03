#!/usr/bin/env python3
# -*- mode:python; tab-width:4; indent-tabs-mode:nil; coding:utf-8 -*-
# vim: ts=4 sw=4 smarttab expandtab fileencoding=utf-8
#
# Ceph - scalable distributed file system
#
# Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#

from ceph_argparse import validate_command, parse_json_funcsigs, validate, \
    parse_funcsig, ArgumentError, ArgumentTooFew, ArgumentMissing, \
    ArgumentNumber, ArgumentValid

import os
import random
import re
import string
import sys
import unittest
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


def get_command_descriptions(what):
    print ("get_command_descriptions --" + what)
    CEPH_BIN = os.environ.get('CEPH_BIN', ".")
    with os.popen(CEPH_BIN + "/get_command_descriptions " + "--" + what) as output_file:
        output_contents = output_file.read()
    return output_contents


class ParseJsonFuncsigs(unittest.TestCase):
    def test_parse_json_funcsigs(self):
        commands = get_command_descriptions("all")
        cmd_json = parse_json_funcsigs(commands, 'cli')

        # syntax error https://github.com/ceph/ceph/pull/585
        commands = get_command_descriptions("pull585")
        self.assertRaises(TypeError, parse_json_funcsigs, commands, 'cli')

sigdict = parse_json_funcsigs(get_command_descriptions("all"), 'cli')


class TestArgparse(unittest.TestCase):

    def _assert_valid_command(self, args):
        result = validate_command(sigdict, args)
        self.assertNotIn(result, [{}, None])

    def check_1_natural_arg(self, prefix, command):
        self._assert_valid_command([prefix, command, '1'])
        self.assertEqual({}, validate_command(sigdict, [prefix, command]))
        self.assertEqual({}, validate_command(sigdict, [prefix, command, '-1']))
        self.assertEqual({}, validate_command(sigdict, [prefix, command, '1',
                                                        '1']))

    def check_0_or_1_natural_arg(self, prefix, command):
        self._assert_valid_command([prefix, command, '1'])
        self._assert_valid_command([prefix, command])
        self.assertEqual({}, validate_command(sigdict, [prefix, command, '-1']))
        self.assertEqual({}, validate_command(sigdict, [prefix, command, '1',
                                                        '1']))

    def check_1_string_arg(self, prefix, command):
        self.assertEqual({}, validate_command(sigdict, [prefix, command]))
        self._assert_valid_command([prefix, command, 'string'])
        self.assertEqual({}, validate_command(sigdict, [prefix,
                                                        command,
                                                        'string',
                                                        'toomany']))

    def check_0_or_1_string_arg(self, prefix, command):
        self._assert_valid_command([prefix, command, 'string'])
        self._assert_valid_command([prefix, command])
        self.assertEqual({}, validate_command(sigdict, [prefix,
                                                        command,
                                                        'string',
                                                        'toomany']))

    def check_1_or_more_string_args(self, prefix, command):
        self.assertEqual({}, validate_command(sigdict, [prefix,
                                                        command]))
        self._assert_valid_command([prefix, command, 'string'])
        self._assert_valid_command([prefix, command, 'string', 'more string'])

    def check_no_arg(self, prefix, command):
        self._assert_valid_command([prefix, command])
        self.assertEqual({}, validate_command(sigdict, [prefix,
                                                        command,
                                                        'toomany']))

    def _capture_output(self, args, stdout=None, stderr=None):
        if stdout:
            stdout = StringIO()
            sys.stdout = stdout
        if stderr:
            stderr = StringIO()
            sys.stderr = stderr
        ret = validate_command(sigdict, args)
        if stdout:
            stdout = stdout.getvalue().strip()
        if stderr:
            stderr = stderr.getvalue().strip()
        return ret, stdout, stderr


class TestBasic(unittest.TestCase):

    def test_non_ascii_in_non_options(self):
        # ArgumentPrefix("no match for {0}".format(s)) is not able to convert
        # unicode str parameter into str. and validate_command() should not
        # choke on it.
        self.assertEqual({}, validate_command(sigdict, [u'章鱼和鱿鱼']))
        self.assertEqual({}, validate_command(sigdict, [u'–w']))
        # actually we always pass unicode strings to validate_command() in "ceph"
        # CLI, but we also use bytestrings in our tests, so make sure it does not
        # break.
        self.assertEqual({}, validate_command(sigdict, ['章鱼和鱿鱼']))
        self.assertEqual({}, validate_command(sigdict, ['–w']))


class TestPG(TestArgparse):

    def test_stat(self):
        self._assert_valid_command(['pg', 'stat'])

    def test_getmap(self):
        self._assert_valid_command(['pg', 'getmap'])

    def test_dump(self):
        valid_commands = {
            'pg dump': {'prefix': 'pg dump'},
            'pg dump all summary sum delta pools osds pgs pgs_brief':
            {'prefix': 'pg dump',
             'dumpcontents':
             'all summary sum delta pools osds pgs pgs_brief'.split()
             },
            'pg dump --dumpcontents summary,sum':
            {'prefix': 'pg dump',
             'dumpcontents': 'summary,sum'.split(',')
             }
        }
        for command, expected_result in valid_commands.items():
            actual_result = validate_command(sigdict, command.split())
            expected_result['target'] = ('mon-mgr', '')
            self.assertEqual(expected_result, actual_result)
        invalid_commands = ['pg dump invalid']
        for command in invalid_commands:
            actual_result = validate_command(sigdict, command.split())
            self.assertEqual({}, actual_result)

    def test_dump_json(self):
        self._assert_valid_command(['pg', 'dump_json'])
        self._assert_valid_command(['pg', 'dump_json',
                                    'all',
                                    'summary',
                                    'sum',
                                    'pools',
                                    'osds',
                                    'pgs'])
        self.assertEqual({}, validate_command(sigdict, ['pg', 'dump_json',
                                                        'invalid']))

    def test_dump_pools_json(self):
        self._assert_valid_command(['pg', 'dump_pools_json'])

    def test_dump_pools_stuck(self):
        self._assert_valid_command(['pg', 'dump_stuck'])
        self._assert_valid_command(['pg', 'dump_stuck',
                                    'inactive',
                                    'unclean',
                                    'stale'])
        self.assertEqual({}, validate_command(sigdict, ['pg', 'dump_stuck',
                                                        'invalid']))
        self._assert_valid_command(['pg', 'dump_stuck',
                                    'inactive',
                                    '1234'])

    def one_pgid(self, command):
        self._assert_valid_command(['pg', command, '1.1'])
        self.assertEqual({}, validate_command(sigdict, ['pg', command]))
        self.assertEqual({}, validate_command(sigdict, ['pg', command, '1']))

    def test_map(self):
        self.one_pgid('map')

    def test_scrub(self):
        self.one_pgid('scrub')

    def test_deep_scrub(self):
        self.one_pgid('deep-scrub')

    def test_repair(self):
        self.one_pgid('repair')

    def test_debug(self):
        self._assert_valid_command(['pg',
                                    'debug',
                                    'unfound_objects_exist'])
        self._assert_valid_command(['pg',
                                    'debug',
                                    'degraded_pgs_exist'])
        self.assertEqual({}, validate_command(sigdict, ['pg', 'debug']))
        self.assertEqual({}, validate_command(sigdict, ['pg', 'debug',
                                                        'invalid']))

    def test_pg_missing_args_output(self):
        ret, _, stderr = self._capture_output(['pg'], stderr=True)
        self.assertEqual({}, ret)
        self.assertRegexpMatches(stderr, re.compile('no valid command found.* closest matches'))

    def test_pg_wrong_arg_output(self):
        ret, _, stderr = self._capture_output(['pg', 'map', 'bad-pgid'],
                                              stderr=True)
        self.assertEqual({}, ret)
        self.assertIn("Invalid command", stderr)


class TestAuth(TestArgparse):

    def test_export(self):
        self._assert_valid_command(['auth', 'export'])
        self._assert_valid_command(['auth', 'export', 'string'])
        self.assertEqual({}, validate_command(sigdict, ['auth',
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
        prefix = 'auth get-or-create-key'
        entity = 'client.test'
        caps = ['mon',
                'allow r',
                'osd',
                'allow rw pool=nfs-ganesha namespace=test, allow rw tag cephfs data=user_test_fs',
                'mds',
                'allow rw path=/']
        cmd = prefix.split() + [entity] + caps
        self.assertEqual(
            {
                'prefix': prefix,
                'entity': entity,
                'caps': caps
            }, validate_command(sigdict, cmd))

    def test_get_or_create(self):
        self.check_1_or_more_string_args('auth', 'get-or-create')

    def test_caps(self):
        self.assertEqual({}, validate_command(sigdict, ['auth',
                                                        'caps']))
        self.assertEqual({}, validate_command(sigdict, ['auth',
                                                        'caps',
                                                        'string']))
        self._assert_valid_command(['auth',
                                    'caps',
                                    'string',
                                    'more string'])

    def test_del(self):
        self.check_1_string_arg('auth', 'del')


class TestMonitor(TestArgparse):

    def test_compact(self):
        self._assert_valid_command(['compact'])

    def test_fsid(self):
        self._assert_valid_command(['fsid'])

    def test_log(self):
        self.assertEqual({}, validate_command(sigdict, ['log']))
        self._assert_valid_command(['log', 'a logtext'])
        self._assert_valid_command(['log', 'a logtext', 'and another'])

    def test_injectargs(self):
        self.assertEqual({}, validate_command(sigdict, ['injectargs']))
        self._assert_valid_command(['injectargs', 'one'])
        self._assert_valid_command(['injectargs', 'one', 'two'])

    def test_status(self):
        self._assert_valid_command(['status'])

    def test_health(self):
        self._assert_valid_command(['health'])
        self._assert_valid_command(['health', 'detail'])
        self.assertEqual({}, validate_command(sigdict, ['health', 'invalid']))
        self.assertEqual({}, validate_command(sigdict, ['health', 'detail',
                                                        'toomany']))

    def test_df(self):
        self._assert_valid_command(['df'])
        self._assert_valid_command(['df', 'detail'])
        self.assertEqual({}, validate_command(sigdict, ['df', 'invalid']))
        self.assertEqual({}, validate_command(sigdict, ['df', 'detail',
                                                        'toomany']))

    def test_report(self):
        self._assert_valid_command(['report'])
        self._assert_valid_command(['report', 'tag1'])
        self._assert_valid_command(['report', 'tag1', 'tag2'])

    def test_quorum_status(self):
        self._assert_valid_command(['quorum_status'])

    def test_tell(self):
        self.assertEqual({}, validate_command(sigdict, ['tell']))
        self.assertEqual({}, validate_command(sigdict, ['tell', 'invalid']))
        for name in ('osd', 'mon', 'client', 'mds'):
            self.assertEqual({}, validate_command(sigdict, ['tell', name]))
            self.assertEqual({}, validate_command(sigdict, ['tell',
                                                            name + ".42"]))
            self._assert_valid_command(['tell', name + ".42", 'something'])
            self._assert_valid_command(['tell', name + ".42",
                                        'something',
                                        'something else'])


class TestMDS(TestArgparse):

    def test_stat(self):
        self.check_no_arg('mds', 'stat')

    def test_compat_show(self):
        self._assert_valid_command(['mds', 'compat', 'show'])
        self.assertEqual({}, validate_command(sigdict, ['mds', 'compat']))
        self.assertEqual({}, validate_command(sigdict, ['mds', 'compat',
                                                        'show', 'toomany']))

    def test_set_state(self):
        self._assert_valid_command(['mds', 'set_state', '1', '2'])
        self.assertEqual({}, validate_command(sigdict, ['mds', 'set_state']))
        self.assertEqual({}, validate_command(sigdict, ['mds', 'set_state', '-1']))
        self.assertEqual({}, validate_command(sigdict, ['mds', 'set_state',
                                                        '1', '-1']))
        self.assertEqual({}, validate_command(sigdict, ['mds', 'set_state',
                                                        '1', '21']))

    def test_fail(self):
        self.check_1_string_arg('mds', 'fail')

    def test_rm(self):
        # Valid: single GID argument present
        self._assert_valid_command(['mds', 'rm', '1'])

        # Missing GID arg: invalid
        self.assertEqual({}, validate_command(sigdict, ['mds', 'rm']))
        # Extra arg: invalid
        self.assertEqual({}, validate_command(sigdict, ['mds', 'rm', '1', 'mds.42']))

    def test_rmfailed(self):
        self._assert_valid_command(['mds', 'rmfailed', '0'])
        self._assert_valid_command(['mds', 'rmfailed', '0', '--yes-i-really-mean-it'])
        self.assertEqual({}, validate_command(sigdict, ['mds', 'rmfailed', '0',
                                                        '--yes-i-really-mean-it',
                                                        'toomany']))

    def test_compat_rm_compat(self):
        self._assert_valid_command(['mds', 'compat', 'rm_compat', '1'])
        self.assertEqual({}, validate_command(sigdict, ['mds',
                                                        'compat',
                                                        'rm_compat']))
        self.assertEqual({}, validate_command(sigdict, ['mds',
                                                        'compat',
                                                        'rm_compat', '-1']))
        self.assertEqual({}, validate_command(sigdict, ['mds',
                                                        'compat',
                                                        'rm_compat',
                                                        '1',
                                                        '1']))

    def test_incompat_rm_incompat(self):
        self._assert_valid_command(['mds', 'compat', 'rm_incompat', '1'])
        self.assertEqual({}, validate_command(sigdict, ['mds',
                                                        'compat',
                                                        'rm_incompat']))
        self.assertEqual({}, validate_command(sigdict, ['mds',
                                                        'compat',
                                                        'rm_incompat', '-1']))
        self.assertEqual({}, validate_command(sigdict, ['mds',
                                                        'compat',
                                                        'rm_incompat',
                                                        '1',
                                                        '1']))


class TestFS(TestArgparse):
    
    def test_dump(self):
        self.check_0_or_1_natural_arg('fs', 'dump')
    
    def test_fs_new(self):
        self._assert_valid_command(['fs', 'new', 'default', 'metadata', 'data'])

    def test_fs_set_max_mds(self):
        self._assert_valid_command(['fs', 'set', 'default', 'max_mds', '1'])
        self._assert_valid_command(['fs', 'set', 'default', 'max_mds', '2'])

    def test_fs_set_cluster_down(self):
        self._assert_valid_command(['fs', 'set', 'default', 'down', 'true'])

    def test_fs_set_cluster_up(self):
        self._assert_valid_command(['fs', 'set', 'default', 'down', 'false'])

    def test_fs_set_cluster_joinable(self):
        self._assert_valid_command(['fs', 'set', 'default', 'joinable', 'true'])

    def test_fs_set_cluster_not_joinable(self):
        self._assert_valid_command(['fs', 'set', 'default', 'joinable', 'false'])

    def test_fs_set(self):
        self._assert_valid_command(['fs', 'set', 'default', 'max_file_size', '2'])
        self._assert_valid_command(['fs', 'set', 'default', 'allow_new_snaps', 'no'])
        self.assertEqual({}, validate_command(sigdict, ['fs',
                                                        'set',
                                                        'invalid']))

    def test_fs_add_data_pool(self):
        self._assert_valid_command(['fs', 'add_data_pool', 'default', '1'])
        self._assert_valid_command(['fs', 'add_data_pool', 'default', 'foo'])

    def test_fs_remove_data_pool(self):
        self._assert_valid_command(['fs', 'rm_data_pool', 'default', '1'])
        self._assert_valid_command(['fs', 'rm_data_pool', 'default', 'foo'])

    def test_fs_rm(self):
        self._assert_valid_command(['fs', 'rm', 'default'])
        self._assert_valid_command(['fs', 'rm', 'default', '--yes-i-really-mean-it'])
        self.assertEqual({}, validate_command(sigdict, ['fs', 'rm', 'default', '--yes-i-really-mean-it', 'toomany']))

    def test_fs_ls(self):
        self._assert_valid_command(['fs', 'ls'])
        self.assertEqual({}, validate_command(sigdict, ['fs', 'ls', 'toomany']))

    def test_fs_set_default(self):
        self._assert_valid_command(['fs', 'set-default', 'cephfs'])
        self.assertEqual({}, validate_command(sigdict, ['fs', 'set-default']))
        self.assertEqual({}, validate_command(sigdict, ['fs', 'set-default', 'cephfs', 'toomany']))


class TestMon(TestArgparse):

    def test_dump(self):
        self.check_0_or_1_natural_arg('mon', 'dump')

    def test_stat(self):
        self.check_no_arg('mon', 'stat')

    def test_getmap(self):
        self.check_0_or_1_natural_arg('mon', 'getmap')

    def test_add(self):
        self._assert_valid_command(['mon', 'add', 'name', '1.2.3.4:1234'])
        self.assertEqual({}, validate_command(sigdict, ['mon', 'add']))
        self.assertEqual({}, validate_command(sigdict, ['mon', 'add', 'name']))
        self.assertEqual({}, validate_command(sigdict, ['mon', 'add',
                                                        'name',
                                                        '400.500.600.700']))

    def test_remove(self):
        self._assert_valid_command(['mon', 'remove', 'name'])
        self.assertEqual({}, validate_command(sigdict, ['mon', 'remove']))
        self.assertEqual({}, validate_command(sigdict, ['mon', 'remove',
                                                        'name', 'toomany']))


class TestOSD(TestArgparse):

    def test_stat(self):
        self.check_no_arg('osd', 'stat')

    def test_dump(self):
        self.check_0_or_1_natural_arg('osd', 'dump')

    def test_osd_tree(self):
        self.check_0_or_1_natural_arg('osd', 'tree')
        cmd = 'osd tree down,out'
        self.assertEqual(
            {
                'prefix': 'osd tree',
                'states': ['down', 'out']
            }, validate_command(sigdict, cmd.split()))

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
        self._assert_valid_command(['osd', 'map', 'poolname', 'objectname'])
        self._assert_valid_command(['osd', 'map', 'poolname', 'objectname', 'nspace'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'map']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'map', 'poolname']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'map',
                                                        'poolname', 'objectname', 'nspace',
                                                        'toomany']))

    def test_metadata(self):
        self.check_0_or_1_natural_arg('osd', 'metadata')

    def test_scrub(self):
        self.check_1_string_arg('osd', 'scrub')

    def test_deep_scrub(self):
        self.check_1_string_arg('osd', 'deep-scrub')

    def test_repair(self):
        self.check_1_string_arg('osd', 'repair')

    def test_lspools(self):
        self._assert_valid_command(['osd', 'lspools'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'lspools',
                                                        'toomany']))

    def test_blocklist_ls(self):
        self._assert_valid_command(['osd', 'blocklist', 'ls'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'blocklist']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'blocklist',
                                                        'ls', 'toomany']))

    def test_crush_rule(self):
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush', 'rule']))
        for subcommand in ('list', 'ls'):
            self._assert_valid_command(['osd', 'crush', 'rule', subcommand])
            self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                            'rule', subcommand,
                                                            'toomany']))

    def test_crush_rule_dump(self):
        self._assert_valid_command(['osd', 'crush', 'rule', 'dump'])
        self._assert_valid_command(['osd', 'crush', 'rule', 'dump', 'RULE'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'rule', 'dump',
                                                        'RULE',
                                                        'toomany']))

    def test_crush_dump(self):
        self._assert_valid_command(['osd', 'crush', 'dump'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'dump',
                                                        'toomany']))

    def test_setcrushmap(self):
        self.check_no_arg('osd', 'setcrushmap')

    def test_crush_add_bucket(self):
        self._assert_valid_command(['osd', 'crush', 'add-bucket',
                                    'name', 'type'])
        self._assert_valid_command(['osd', 'crush', 'add-bucket',
                                    'name', 'type', 'root=foo-root', 'host=foo-host'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'add-bucket']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'add-bucket', '^^^',
                                                        'type']))

    def test_crush_rename_bucket(self):
        self._assert_valid_command(['osd', 'crush', 'rename-bucket',
                                    'srcname', 'dstname'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'rename-bucket']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'rename-bucket',
                                                        'srcname']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'rename-bucket',
                                                        'srcname',
                                                        'dstname',
                                                        'toomany']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'rename-bucket', '^^^',
                                                        'dstname']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'rename-bucket',
                                                        'srcname',
                                                        '^^^^']))

    def _check_crush_setter(self, setter):
        self._assert_valid_command(['osd', 'crush', setter,
                                    '*', '2.3', 'AZaz09-_.='])
        self._assert_valid_command(['osd', 'crush', setter,
                                    'osd.0', '2.3', 'AZaz09-_.='])
        self._assert_valid_command(['osd', 'crush', setter,
                                    '0', '2.3', 'AZaz09-_.='])
        self._assert_valid_command(['osd', 'crush', setter,
                                    '0', '2.3', 'AZaz09-_.=', 'AZaz09-_.='])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        setter,
                                                        'osd.0']))
        ret = validate_command(sigdict, ['osd', 'crush',
                                         setter,
                                         'osd.0',
                                         '-1.0'])
        assert ret in [None, {}]
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        setter,
                                                        'osd.0',
                                                        '1.0',
                                                        '^^^']))

    def test_crush_set(self):
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush']))
        self._check_crush_setter('set')

    def test_crush_add(self):
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush']))
        self._check_crush_setter('add')

    def test_crush_create_or_move(self):
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush']))
        self._check_crush_setter('create-or-move')

    def test_crush_move(self):
        self._assert_valid_command(['osd', 'crush', 'move',
                                    'AZaz09-_.', 'AZaz09-_.='])
        self._assert_valid_command(['osd', 'crush', 'move',
                                    '0', 'AZaz09-_.=', 'AZaz09-_.='])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'move']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'move', 'AZaz09-_.']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'move', '^^^',
                                                        'AZaz09-_.=']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'move', 'AZaz09-_.',
                                                        '^^^']))

    def test_crush_link(self):
        self._assert_valid_command(['osd', 'crush', 'link',
                                    'name', 'AZaz09-_.='])
        self._assert_valid_command(['osd', 'crush', 'link',
                                    'name', 'AZaz09-_.=', 'AZaz09-_.='])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'link']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'link',
                                                        'name']))

    def test_crush_rm(self):
        for alias in ('rm', 'remove', 'unlink'):
            self._assert_valid_command(['osd', 'crush', alias, 'AZaz09-_.'])
            self._assert_valid_command(['osd', 'crush', alias,
                                        'AZaz09-_.', 'AZaz09-_.'])
            self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                            alias]))
            self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                            alias,
                                                            'AZaz09-_.',
                                                            'AZaz09-_.',
                                                            'toomany']))

    def test_crush_reweight(self):
        self._assert_valid_command(['osd', 'crush', 'reweight',
                                    'AZaz09-_.', '2.3'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'reweight']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'reweight',
                                                        'AZaz09-_.']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'reweight',
                                                        'AZaz09-_.',
                                                        '-1.0']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'reweight',
                                                        '^^^',
                                                        '2.3']))

    def test_crush_tunables(self):
        for tunable in ('legacy', 'argonaut', 'bobtail', 'firefly',
                        'optimal', 'default'):
            self._assert_valid_command(['osd', 'crush', 'tunables',
                                        tunable])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'tunables']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'tunables',
                                                        'default', 'toomany']))

    def test_crush_rule_create_simple(self):
        self._assert_valid_command(['osd', 'crush', 'rule', 'create-simple',
                                    'AZaz09-_.', 'AZaz09-_.', 'AZaz09-_.'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                        'create-simple']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                        'create-simple',
                                                        'AZaz09-_.']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                        'create-simple',
                                                        'AZaz09-_.',
                                                        'AZaz09-_.']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                        'create-simple',
                                                        '^^^',
                                                        'AZaz09-_.',
                                                        'AZaz09-_.']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                        'create-simple',
                                                        'AZaz09-_.',
                                                        '|||',
                                                        'AZaz09-_.']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                        'create-simple',
                                                        'AZaz09-_.',
                                                        'AZaz09-_.',
                                                        '+++']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                        'create-simple',
                                                        'AZaz09-_.',
                                                        'AZaz09-_.',
                                                        'AZaz09-_.',
                                                        'toomany']))

    def test_crush_rule_create_erasure(self):
        self._assert_valid_command(['osd', 'crush', 'rule', 'create-erasure',
                                    'AZaz09-_.'])
        self._assert_valid_command(['osd', 'crush', 'rule', 'create-erasure',
                                    'AZaz09-_.', 'whatever'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                        'create-erasure']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                        'create-erasure',
                                                        '^^^']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                        'create-erasure',
                                                        'name', '^^^']))

    def test_crush_rule_rm(self):
        self._assert_valid_command(['osd', 'crush', 'rule', 'rm', 'AZaz09-_.'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'rule', 'rm']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'rule', 'rm',
                                                        '^^^^']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'crush',
                                                        'rule', 'rm',
                                                        'AZaz09-_.',
                                                        'toomany']))

    def test_setmaxosd(self):
        self.check_1_natural_arg('osd', 'setmaxosd')

    def test_pause(self):
        self.check_no_arg('osd', 'pause')

    def test_unpause(self):
        self.check_no_arg('osd', 'unpause')

    def test_erasure_code_profile_set(self):
        self._assert_valid_command(['osd', 'erasure-code-profile', 'set',
                                    'name'])
        self._assert_valid_command(['osd', 'erasure-code-profile', 'set',
                                    'name', 'A=B'])
        self._assert_valid_command(['osd', 'erasure-code-profile', 'set',
                                    'name', 'A=B', 'C=D'])
        self.assertEqual({}, validate_command(sigdict, ['osd',
                                                        'erasure-code-profile',
                                                        'set']))
        self.assertEqual({}, validate_command(sigdict, ['osd',
                                                        'erasure-code-profile',
                                                        'set',
                                                        '^^^^']))

    def test_erasure_code_profile_get(self):
        self._assert_valid_command(['osd', 'erasure-code-profile', 'get',
                                    'name'])
        self.assertEqual({}, validate_command(sigdict, ['osd',
                                                        'erasure-code-profile',
                                                        'get']))
        self.assertEqual({}, validate_command(sigdict, ['osd',
                                                        'erasure-code-profile',
                                                        'get',
                                                        '^^^^']))

    def test_erasure_code_profile_rm(self):
        self._assert_valid_command(['osd', 'erasure-code-profile', 'rm',
                                    'name'])
        self.assertEqual({}, validate_command(sigdict, ['osd',
                                                        'erasure-code-profile',
                                                        'rm']))
        self.assertEqual({}, validate_command(sigdict, ['osd',
                                                        'erasure-code-profile',
                                                        'rm',
                                                        '^^^^']))

    def test_erasure_code_profile_ls(self):
        self._assert_valid_command(['osd', 'erasure-code-profile', 'ls'])
        self.assertEqual({}, validate_command(sigdict, ['osd',
                                                        'erasure-code-profile',
                                                        'ls',
                                                        'toomany']))

    def test_set_unset(self):
        for action in ('set', 'unset'):
            for flag in ('pause', 'noup', 'nodown', 'noout', 'noin',
                         'nobackfill', 'norecover', 'noscrub', 'nodeep-scrub'):
                self._assert_valid_command(['osd', action, flag])
            self.assertEqual({}, validate_command(sigdict, ['osd', action]))
            self.assertEqual({}, validate_command(sigdict, ['osd', action,
                                                            'invalid']))
            self.assertEqual({}, validate_command(sigdict, ['osd', action,
                                                            'pause',
                                                            'toomany']))

    def test_down(self):
        self.check_1_or_more_string_args('osd', 'down')

    def test_out(self):
        self.check_1_or_more_string_args('osd', 'out')

    def test_in(self):
        self.check_1_or_more_string_args('osd', 'in')

    def test_rm(self):
        self.check_1_or_more_string_args('osd', 'rm')

    def test_reweight(self):
        self._assert_valid_command(['osd', 'reweight', '1', '0.1'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'reweight']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'reweight',
                                                        '1']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'reweight',
                                                        '1', '2.0']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'reweight',
                                                        '-1', '0.1']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'reweight',
                                                        '1', '0.1',
                                                        'toomany']))

    def test_lost(self):
        self._assert_valid_command(['osd', 'lost', '1',
                                    '--yes-i-really-mean-it'])
        self._assert_valid_command(['osd', 'lost', '1'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'lost']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'lost',
                                                        '1',
                                                        'what?']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'lost',
                                                        '-1',
                                                        '--yes-i-really-mean-it']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'lost',
                                                        '1',
                                                        '--yes-i-really-mean-it',
                                                        'toomany']))

    def test_create(self):
        uuid = '12345678123456781234567812345678'
        self._assert_valid_command(['osd', 'create'])
        self._assert_valid_command(['osd', 'create', uuid])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'create',
                                                        'invalid']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'create',
                                                        uuid,
                                                        'toomany']))

    def test_blocklist(self):
        for action in ('add', 'rm'):
            self._assert_valid_command(['osd', 'blocklist', action,
                                        '1.2.3.4/567'])
            self._assert_valid_command(['osd', 'blocklist', action,
                                        '1.2.3.4'])
            self._assert_valid_command(['osd', 'blocklist', action,
                                        '1.2.3.4/567', '600.40'])
            self._assert_valid_command(['osd', 'blocklist', action,
                                        '1.2.3.4', '600.40'])
            
            self._assert_valid_command(['osd', 'blocklist', action,
                                        'v1:1.2.3.4', '600.40'])
            self._assert_valid_command(['osd', 'blocklist', action,
                                        'v1:1.2.3.4/0', '600.40'])
            self._assert_valid_command(['osd', 'blocklist', action,
                                        'v2:2001:0db8:85a3:0000:0000:8a2e:0370:7334', '600.40'])
            self._assert_valid_command(['osd', 'blocklist', action,
                                        'v2:fe80::1/0', '600.40'])
            self._assert_valid_command(['osd', 'blocklist', action,
                                        'v2:[2607:f298:4:2243::5522]:0/0', '600.40'])
            self._assert_valid_command(['osd', 'blocklist', action,
                                        '[2001:0db8::85a3:0000:8a2e:0370:7334]:0/0', '600.40'])
            
            self.assertEqual({}, validate_command(sigdict, ['osd', 'blocklist',
                                                            action,
                                                            'invalid',
                                                            '600.40']))
            self.assertEqual({}, validate_command(sigdict, ['osd', 'blocklist',
                                                            action,
                                                            '1.2.3.4/567',
                                                            '-1.0']))
            self.assertEqual({}, validate_command(sigdict, ['osd', 'blocklist',
                                                            action,
                                                            '1.2.3.4/567',
                                                            '600.40',
                                                            'toomany']))
            self.assertEqual({}, validate_command(sigdict, ['osd', 'blocklist',
                                                            action,
                                                            'v2:1.2.3.4/567',
                                                            '600.40']))
            self.assertEqual({}, validate_command(sigdict, ['osd', 'blocklist',
                                                            action,
                                                            'v1:1.2.3.4:65536/567',
                                                            '600.40']))

    def test_pool_mksnap(self):
        self._assert_valid_command(['osd', 'pool', 'mksnap',
                                    'poolname', 'snapname'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'mksnap']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'mksnap',
                                                        'poolname']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'mksnap',
                                                        'poolname', 'snapname',
                                                        'toomany']))

    def test_pool_rmsnap(self):
        self._assert_valid_command(['osd', 'pool', 'rmsnap',
                                   'poolname', 'snapname'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'rmsnap']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'rmsnap',
                                                        'poolname']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'rmsnap',
                                                        'poolname', 'snapname',
                                                        'toomany']))

    def test_pool_kwargs(self):
        """
        Use the pool creation command to exercise keyword-style arguments
        since it has lots of parameters
        """
        # Simply use a keyword arg instead of a positional arg, in its
        # normal order (pgp_num after pg_num)
        self.assertEqual(
            {
                "prefix": "osd pool create",
                "pool": "foo",
                "pg_num": 8,
                "pgp_num": 16
            }, validate_command(sigdict, [
                'osd', 'pool', 'create', "foo", "8", "--pgp_num", "16"]))

        # Again, but using the "--foo=bar" style
        self.assertEqual(
            {
                "prefix": "osd pool create",
                "pool": "foo",
                "pg_num": 8,
                "pgp_num": 16
            }, validate_command(sigdict, [
                'osd', 'pool', 'create', "foo", "8", "--pgp_num=16"]))

        # Specify keyword args in a different order than their definitions
        # (pgp_num after pool_type)
        self.assertEqual(
            {
                "prefix": "osd pool create",
                "pool": "foo",
                "pg_num": 8,
                "pgp_num": 16,
                "pool_type": "replicated"
            }, validate_command(sigdict, [
                'osd', 'pool', 'create', "foo", "8",
                "--pool_type", "replicated",
                "--pgp_num", "16"]))

        # Use a keyword argument that doesn't exist, should fail validation
        self.assertEqual({}, validate_command(sigdict,
            ['osd', 'pool', 'create', "foo", "8", "--foo=bar"]))

    def test_foo(self):
        # Long form of a boolean argument (--foo=true)
        self.assertEqual(
            {
                "prefix": "osd pool delete",
                "pool": "foo",
                "pool2": "foo",
                "yes_i_really_really_mean_it": True
            }, validate_command(sigdict, [
                'osd', 'pool', 'delete', "foo", "foo",
                "--yes-i-really-really-mean-it=true"]))

    def test_pool_bool_args(self):
        """
        Use pool deletion to exercise boolean arguments since it has
        the --yes-i-really-really-mean-it flags
        """

        # Short form of a boolean argument (--foo)
        self.assertEqual(
            {
                "prefix": "osd pool delete",
                "pool": "foo",
                "pool2": "foo",
                "yes_i_really_really_mean_it": True
            }, validate_command(sigdict, [
                'osd', 'pool', 'delete', "foo", "foo",
                "--yes-i-really-really-mean-it"]))

        # Long form of a boolean argument (--foo=true)
        self.assertEqual(
            {
                "prefix": "osd pool delete",
                "pool": "foo",
                "pool2": "foo",
                "yes_i_really_really_mean_it": True
            }, validate_command(sigdict, [
                'osd', 'pool', 'delete', "foo", "foo",
                "--yes-i-really-really-mean-it=true"]))

        # Negative form of a boolean argument (--foo=false)
        self.assertEqual(
            {
                "prefix": "osd pool delete",
                "pool": "foo",
                "pool2": "foo",
                "yes_i_really_really_mean_it": False
            }, validate_command(sigdict, [
                'osd', 'pool', 'delete', "foo", "foo",
                "--yes-i-really-really-mean-it=false"]))

        # Invalid value boolean argument (--foo=somethingelse)
        self.assertEqual({}, validate_command(sigdict, [
                'osd', 'pool', 'delete', "foo", "foo",
                "--yes-i-really-really-mean-it=rhubarb"]))

    def test_pool_create(self):
        self._assert_valid_command(['osd', 'pool', 'create',
                                    'poolname', '128'])
        self._assert_valid_command(['osd', 'pool', 'create',
                                    'poolname', '128', '128'])
        self._assert_valid_command(['osd', 'pool', 'create',
                                    'poolname', '128', '128',
                                    'replicated'])
        self._assert_valid_command(['osd', 'pool', 'create',
                                    'poolname', '128', '128',
                                    'erasure', 'A-Za-z0-9-_.', 'rule^^'])
        self._assert_valid_command(['osd', 'pool', 'create', 'poolname'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'create']))
        # invalid pg_num and pgp_num, like "-1", could spill over to
        # erasure_code_profile and rule as they are valid profile and rule
        # names, so validate_commands() cannot identify such cases.
        # but if they are matched by profile and rule, the "rule" argument
        # won't get a chance to be matched anymore.
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'create',
                                                        'poolname',
                                                        '-1', '-1',
                                                        'rule']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'create',
                                                        'poolname',
                                                        '128', '128',
                                                        'erasure', '^^^',
                                                        'rule']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'create',
                                                        'poolname',
                                                        '128', '128',
                                                        'erasure', 'profile',
                                                        'rule',
                                                        'toomany']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'create',
                                                        'poolname',
                                                        '128', '128',
                                                        'INVALID', 'profile',
                                                        'rule']))

    def test_pool_delete(self):
        self._assert_valid_command(['osd', 'pool', 'delete',
                                    'poolname', 'poolname',
                                    '--yes-i-really-really-mean-it'])
        self._assert_valid_command(['osd', 'pool', 'delete',
                                    'poolname', 'poolname'])
        self._assert_valid_command(['osd', 'pool', 'delete',
                                    'poolname'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'delete']))
        self.assertEqual({}, validate_command(sigdict,
                                          ['osd', 'pool', 'delete',
                                           'poolname', 'poolname',
                                           '--yes-i-really-really-mean-it',
                                           'toomany']))

    def test_pool_rename(self):
        self._assert_valid_command(['osd', 'pool', 'rename',
                                    'poolname', 'othername'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'rename']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'rename',
                                                        'poolname']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool', 'rename',
                                                        'poolname', 'othername',
                                                        'toomany']))

    def test_pool_get(self):
        for var in ('size', 'min_size',
                    'pg_num', 'pgp_num', 'crush_rule', 'fast_read',
                    'scrub_min_interval', 'scrub_max_interval',
                    'deep_scrub_interval', 'recovery_priority',
                    'recovery_op_priority'):
            self._assert_valid_command(['osd', 'pool', 'get', 'poolname', var])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool',
                                                        'get']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool',
                                                        'get', 'poolname']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool',
                                                        'get', 'poolname',
                                                        'size', 'toomany']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool',
                                                        'get', 'poolname',
                                                        'invalid']))

    def test_pool_set(self):
        for var in ('size', 'min_size',
                    'pg_num', 'pgp_num', 'crush_rule',
                    'hashpspool', 'fast_read',
                    'scrub_min_interval', 'scrub_max_interval',
                    'deep_scrub_interval', 'recovery_priority',
                    'recovery_op_priority'):
            self._assert_valid_command(['osd', 'pool',
                                       'set', 'poolname', var, 'value'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool',
                                                        'set']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool',
                                                        'set', 'poolname']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool',
                                                        'set', 'poolname',
                                                        'size', 'value',
                                                        'toomany']))

    def test_pool_set_quota(self):
        for field in ('max_objects', 'max_bytes'):
            self._assert_valid_command(['osd', 'pool', 'set-quota',
                                        'poolname', field, '10K'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool',
                                                        'set-quota']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool',
                                                        'set-quota',
                                                        'poolname']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool',
                                                        'set-quota',
                                                        'poolname',
                                                        'max_objects']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool',
                                                        'set-quota',
                                                        'poolname',
                                                        'invalid',
                                                        '10K']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'pool',
                                                        'set-quota',
                                                        'poolname',
                                                        'max_objects',
                                                        '10K',
                                                        'toomany']))

    def test_reweight_by_utilization(self):
        self._assert_valid_command(['osd', 'reweight-by-utilization'])
        self._assert_valid_command(['osd', 'reweight-by-utilization', '100'])
        self._assert_valid_command(['osd', 'reweight-by-utilization', '100', '.1'])
        self.assertEqual({}, validate_command(sigdict, ['osd',
                                                        'reweight-by-utilization',
                                                        '100',
                                                        'toomany']))

    def test_tier_op(self):
        for op in ('add', 'remove', 'set-overlay'):
            self._assert_valid_command(['osd', 'tier', op,
                                        'poolname', 'othername'])
            self.assertEqual({}, validate_command(sigdict, ['osd', 'tier', op]))
            self.assertEqual({}, validate_command(sigdict, ['osd', 'tier', op,
                                                            'poolname']))
            self.assertEqual({}, validate_command(sigdict, ['osd', 'tier', op,
                                                            'poolname',
                                                            'othername',
                                                            'toomany']))

    def test_tier_cache_mode(self):
        for mode in ('none', 'writeback', 'readonly', 'readproxy'):
            self._assert_valid_command(['osd', 'tier', 'cache-mode',
                                        'poolname', mode])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'tier',
                                                        'cache-mode']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'tier',
                                                        'cache-mode',
                                                        'invalid']))

    def test_tier_remove_overlay(self):
        self._assert_valid_command(['osd', 'tier', 'remove-overlay',
                                    'poolname'])
        self.assertEqual({}, validate_command(sigdict, ['osd', 'tier',
                                                        'remove-overlay']))
        self.assertEqual({}, validate_command(sigdict, ['osd', 'tier',
                                                        'remove-overlay',
                                                        'poolname',
                                                        'toomany']))

    def _set_ratio(self, command):
        self._assert_valid_command(['osd', command, '0.0'])
        self.assertEqual({}, validate_command(sigdict, ['osd', command]))
        self.assertEqual({}, validate_command(sigdict, ['osd', command, '2.0']))

    def test_set_full_ratio(self):
        self._set_ratio('set-full-ratio')

    def test_set_backfillfull_ratio(self):
        self._set_ratio('set-backfillfull-ratio')

    def test_set_nearfull_ratio(self):
        self._set_ratio('set-nearfull-ratio')


class TestConfigKey(TestArgparse):

    def test_get(self):
        self.check_1_string_arg('config-key', 'get')

    def test_put(self):
        self._assert_valid_command(['config-key', 'put',
                                    'key'])
        self._assert_valid_command(['config-key', 'put',
                                    'key', 'value'])
        self.assertEqual({}, validate_command(sigdict, ['config-key', 'put']))
        self.assertEqual({}, validate_command(sigdict, ['config-key', 'put',
                                                        'key', 'value',
                                                        'toomany']))

    def test_del(self):
        self.check_1_string_arg('config-key', 'del')

    def test_exists(self):
        self.check_1_string_arg('config-key', 'exists')

    def test_dump(self):
        self.check_0_or_1_string_arg('config-key', 'dump')

    def test_list(self):
        self.check_no_arg('config-key', 'list')


class TestValidate(unittest.TestCase):

    ARGS = 0
    KWARGS = 1
    KWARGS_EQ = 2
    MIXED = 3

    def setUp(self):
        self.prefix = ['some', 'random', 'cmd']
        self.args_dict = [
            {'name': 'variable_one', 'type': 'CephString'},
            {'name': 'variable_two', 'type': 'CephString'},
            {'name': 'variable_three', 'type': 'CephString'},
            {'name': 'variable_four', 'type': 'CephInt'},
            {'name': 'variable_five', 'type': 'CephString'}]
        self.args = []
        for d in self.args_dict:
            if d['type'] == 'CephInt':
                val = "{}".format(random.randint(0, 100))
            elif d['type'] == 'CephString':
                letters = string.ascii_letters
                str_len = random.randint(5, 10)
                val = ''.join(random.choice(letters) for _ in range(str_len))
            else:
                raise skipTest()

            self.args.append((d['name'], val))

        self.sig = parse_funcsig(self.prefix + self.args_dict)

    def _arg_kwarg_test(self, prefix, args, sig, arg_type=0):
        """
        Runs validate in different arg/kargs ways.

        :param prefix: List of prefix commands (that can't be kwarged)
        :param args: a list of kwarg, arg pairs: [(k1, v1), (k2, v2), ...]
        :param sig: The sig to match
        :param arg_type: how to build the args to send. As positional args (ARGS),
                     as long kwargs (KWARGS [--k v]), other style long kwargs
                     (KWARGS_EQ (--k=v]), and mixed (MIXED) where there will be
                     a random mix of the above.
        :return: None, the method will assert.
        """
        final_args = list(prefix)
        for k, v in args:
            a_type = arg_type
            if a_type == self.MIXED:
                a_type = random.choice((self.ARGS,
                                        self.KWARGS,
                                        self.KWARGS_EQ))
            if a_type == self.ARGS:
                final_args.append(v)
            elif a_type == self.KWARGS:
                final_args.extend(["--{}".format(k), v])
            else:
                final_args.append("--{}={}".format(k, v))

        try:
            validate(final_args, sig)
        except (ArgumentError, ArgumentMissing,
                ArgumentNumber, ArgumentTooFew, ArgumentValid) as ex:
            self.fail("Validation failed: {}".format(str(ex)))

    def test_args_and_kwargs_validate(self):
        for arg_type in (self.ARGS, self.KWARGS, self.KWARGS_EQ, self.MIXED):
            self._arg_kwarg_test(self.prefix, self.args, self.sig, arg_type)


if __name__ == '__main__':
    unittest.main()


# Local Variables:
# compile-command: "cd ../../..; cmake --build build --target get_command_descriptions -j4 &&
#  CEPH_BIN=build/bin \
#  PYTHONPATH=src/pybind python3 \
#  src/test/pybind/test_ceph_argparse.py"
# End:
