#!/usr/bin/env python
#
# Copyright (C) 2016 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
from mock import patch, DEFAULT
from ceph_fs_report import main
import json
import mock
import os
import subprocess

from fixture_git import FixtureGit

class TestCephFSReport(object):

    def setup_class(self):
        self.repo = subprocess.check_output(
            "git rev-parse --show-toplevel", shell=True).strip()

    def setup(self):
        self.fixture_git = FixtureGit().setup()

    def teardown(self):
        self.fixture_git.teardown()
        
    def test_fetch_prs(self):
        limit = 5
        cache = 'tests-pr.json'
        os.system("rm -f " + cache)
        r = main.CephFSReport()
        r.run(['--verbose', 
               '--cache',
               '--pr-cache', cache,
               '--repo', self.repo])
        assert not os.path.exists(cache)
        r.fetch_prs(limit)
        assert os.path.exists(cache)
        payload = "PAYLOAD"
        json.dump(payload, open(cache, 'w'))
        r.fetch_prs(limit)
        assert r.prs == payload
        os.system("rm -f " + cache)

    def test_is_devel(self):
        r = main.CephFSReport()
        assert r.is_devel('master', '2001-01-01T00:00:00Z')
        assert r.is_devel('next', '2001-01-01T00:00:00Z')
        assert not r.is_devel('infernalis', '3000-01-01T00:00:00Z')
        assert r.is_devel('infernalis', '2015-10-01T00:00:00Z')
        assert not r.is_devel('hammer', '3000-01-01T00:00:00Z')
        assert r.is_devel('hammer', '2015-03-01T00:00:00Z')

    def test_is_selected_labels(self):
        r = main.CephFSReport()
        assert r.is_selected_labels(['foo/core'], 123)
        assert not r.is_selected_labels(['foo/rgw', 'foo/cleanup'], 123)
        assert not r.is_selected_labels([], 123)

    @mock.patch.object(main.CephFSReport, 'get_issue')
    def test_process_pr(self, m_get_issue):
        m_get_issue.return_value = { 'labels': [{'name': 'core'}] }
        number = 1234
        r = main.CephFSReport()
        assert "OK" == r.process_pr({
            'number': number,
            'created_at': '2015-03-01T00:00:00Z',
            'base': {
                'ref': 'master',
            }
        })
        assert number in r.prs
        
        assert "SKIP not dev" == r.process_pr({
            'number': number,
            'created_at': '2015-03-01T00:00:00Z',
            'base': {
                'ref': 'zebulon',
            }
        })

        m_get_issue.return_value = { 'labels': [] }
        assert "SKIP not selected" == r.process_pr({
            'number': number,
            'created_at': '2015-03-01T00:00:00Z',
            'base': {
                'ref': 'master',
            }
        })
