#
# Copyright (c) 2015 Red Hat, Inc.
#
# Author: Loic Dachary <loic@dachary.org>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
import argparse
import logging
import os
import pytest
import tempfile

import teuthology
from teuthology import misc
from teuthology.openstack import TeuthologyOpenStack
import scripts.openstack

class TestTeuthologyOpenStack(object):

    @classmethod
    def setup_class(self):
        if 'OS_AUTH_URL' not in os.environ:
            pytest.skip('no OS_AUTH_URL environment variable')

        teuthology.log.setLevel(logging.DEBUG)
        teuthology.misc.read_config(argparse.Namespace())

        ip = TeuthologyOpenStack.create_floating_ip()
        if ip:
            ip_id = TeuthologyOpenStack.get_floating_ip_id(ip)
            misc.sh("openstack ip floating delete " + ip_id)
            self.can_create_floating_ips = True
        else:
            self.can_create_floating_ips = False
        
    def setup(self):
        self.key_filename = tempfile.mktemp()
        self.key_name = 'teuthology-test'
        self.name = 'teuthology-test'
        self.clobber()
        misc.sh("""
openstack keypair create {key_name} > {key_filename}
chmod 600 {key_filename}
        """.format(key_filename=self.key_filename,
                   key_name=self.key_name))
        self.options = ['--key-name', self.key_name,
                        '--key-filename', self.key_filename,
                        '--name', self.name,
                        '--verbose']

    def teardown(self):
        self.clobber()
        os.unlink(self.key_filename)

    def clobber(self):
        misc.sh("""
openstack server delete {name} --wait || true
openstack keypair delete {key_name} || true
        """.format(key_name=self.key_name,
                   name=self.name))

    def test_create(self, capsys):
        teuthology_argv = [
            '--suite', 'upgrade/hammer',
            '--dry-run',
            '--ceph', 'master',
            '--kernel', 'distro',
            '--flavor', 'gcov',
            '--distro', 'ubuntu',
            '--suite-branch', 'hammer',
            '--email', 'loic@dachary.org',
            '--num', '10',
            '--limit', '23',
            '--subset', '1/2',
            '--priority', '101',
            '--timeout', '234',
            '--filter', 'trasher',
            '--filter-out', 'erasure-code',
        ]
        argv = (self.options +
                ['--upload',
                 '--archive-upload', 'user@archive:/tmp'] +
                teuthology_argv)
        args = scripts.openstack.parse_args(argv)
        teuthology = TeuthologyOpenStack(args, None, argv)
        teuthology.user_data = 'teuthology/openstack/test/user-data-test1.txt'
        teuthology.teuthology_suite = 'echo --'

        teuthology.main()
        assert 'Ubuntu 14.04' in teuthology.ssh("lsb_release -a")
        variables = teuthology.ssh("grep 'substituded variables' /var/log/cloud-init.log")
        assert "nworkers=" + str(args.simultaneous_jobs) in variables
        assert "username=" + teuthology.username in variables
        assert "upload=--archive-upload user@archive:/tmp" in variables
        assert os.environ['OS_AUTH_URL'] in variables

        out, err = capsys.readouterr()
        assert " ".join(teuthology_argv) in out

        if self.can_create_floating_ips:
            ip = teuthology.get_floating_ip(self.name)
        teuthology.teardown()
        if self.can_create_floating_ips:
            assert teuthology.get_floating_ip_id(ip) == None

    def test_floating_ip(self):
        if not self.can_create_floating_ips:
            pytest.skip('unable to create floating ips')

        expected = TeuthologyOpenStack.create_floating_ip()
        ip = TeuthologyOpenStack.get_unassociated_floating_ip()
        assert expected == ip
        ip_id = TeuthologyOpenStack.get_floating_ip_id(ip)
        misc.sh("openstack ip floating delete " + ip_id)
