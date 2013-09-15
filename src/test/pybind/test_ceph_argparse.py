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
# Local Variables:
# compile-command: "cd ../.. ; make -j4 && 
#  PYTHONPATH=pybind nosetests --stop \
#  test/pybind/test_ceph_argparse.py # test_ceph_argparse.py:TestOSD.test_rm"
# End:
