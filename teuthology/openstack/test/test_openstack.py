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
import subprocess
import tempfile
from mock import patch

import teuthology
from teuthology import misc
from teuthology.config import set_config_attr
from teuthology.openstack import TeuthologyOpenStack, OpenStack, OpenStackInstance
import scripts.openstack


class TestOpenStackInstance(object):

    teuthology_instance = """
{
  "OS-EXT-STS:task_state": null,
  "addresses": "Ext-Net=167.114.233.32",
  "image": "Ubuntu 14.04 (0d315a8d-75e3-418a-80e4-48e62d599627)",
  "OS-EXT-STS:vm_state": "active",
  "OS-SRV-USG:launched_at": "2015-08-17T12:22:13.000000",
  "flavor": "vps-ssd-1 (164fcc7e-7771-414f-a607-b388cb7b7aa0)",
  "id": "f3ca32d7-212b-458b-a0d4-57d1085af953",
  "security_groups": [
    {
      "name": "default"
    }
  ],
  "user_id": "3a075820e5d24fda96cd340b87fd94e9",
  "OS-DCF:diskConfig": "AUTO",
  "accessIPv4": "",
  "accessIPv6": "",
  "progress": 0,
  "OS-EXT-STS:power_state": 1,
  "project_id": "62cf1be03cec403c8ed8e64df55732ea",
  "config_drive": "",
  "status": "ACTIVE",
  "updated": "2015-11-03T13:48:53Z",
  "hostId": "bcdf964b6f724e573c07156ff85b4db1707f6f0969f571cf33e0468d",
  "OS-SRV-USG:terminated_at": null,
  "key_name": "loic",
  "properties": "",
  "OS-EXT-AZ:availability_zone": "nova",
  "name": "mrdarkdragon",
  "created": "2015-08-17T12:21:31Z",
  "os-extended-volumes:volumes_attached": [{"id": "627e2631-fbb3-48cd-b801-d29cd2a76f74"}, {"id": "09837649-0881-4ee2-a560-adabefc28764"}, {"id": "44e5175b-6044-40be-885a-c9ddfb6f75bb"}]
}
    """

    teuthology_instance_no_addresses = """
{
  "OS-EXT-STS:task_state": null,
  "addresses": "",
  "image": "Ubuntu 14.04 (0d315a8d-75e3-418a-80e4-48e62d599627)",
  "OS-EXT-STS:vm_state": "active",
  "OS-SRV-USG:launched_at": "2015-08-17T12:22:13.000000",
  "flavor": "vps-ssd-1 (164fcc7e-7771-414f-a607-b388cb7b7aa0)",
  "id": "f3ca32d7-212b-458b-a0d4-57d1085af953",
  "security_groups": [
    {
      "name": "default"
    }
  ],
  "user_id": "3a075820e5d24fda96cd340b87fd94e9",
  "OS-DCF:diskConfig": "AUTO",
  "accessIPv4": "",
  "accessIPv6": "",
  "progress": 0,
  "OS-EXT-STS:power_state": 1,
  "project_id": "62cf1be03cec403c8ed8e64df55732ea",
  "config_drive": "",
  "status": "ACTIVE",
  "updated": "2015-11-03T13:48:53Z",
  "hostId": "bcdf964b6f724e573c07156ff85b4db1707f6f0969f571cf33e0468d",
  "OS-SRV-USG:terminated_at": null,
  "key_name": "loic",
  "properties": "",
  "OS-EXT-AZ:availability_zone": "nova",
  "name": "mrdarkdragon",
  "created": "2015-08-17T12:21:31Z",
  "os-extended-volumes:volumes_attached": []
}
    """

    def test_init(self):
        with patch.multiple(
                misc,
                sh=lambda cmd: self.teuthology_instance,
        ):
            o = OpenStackInstance('NAME')
            assert o['id'] == 'f3ca32d7-212b-458b-a0d4-57d1085af953'
        o = OpenStackInstance('NAME', {"id": "OTHER"})
        assert o['id'] == "OTHER"

    def test_get_created(self):
        with patch.multiple(
                misc,
                sh=lambda cmd: self.teuthology_instance,
        ):
            o = OpenStackInstance('NAME')
            assert o.get_created() > 0

    def test_exists(self):
        with patch.multiple(
                misc,
                sh=lambda cmd: self.teuthology_instance,
        ):
            o = OpenStackInstance('NAME')
            assert o.exists()
        def sh_raises(cmd):
            raise subprocess.CalledProcessError('FAIL', 'BAD')
        with patch.multiple(
                misc,
                sh=sh_raises,
        ):
            o = OpenStackInstance('NAME')
            assert not o.exists()

    def test_volumes(self):
        with patch.multiple(
                misc,
                sh=lambda cmd: self.teuthology_instance,
        ):
            o = OpenStackInstance('NAME')
            assert len(o.get_volumes()) == 3

    def test_get_addresses(self):
        answers = [
            self.teuthology_instance_no_addresses,
            self.teuthology_instance,
        ]
        def sh(self):
            return answers.pop(0)
        with patch.multiple(
                misc,
                sh=sh,
        ):
            o = OpenStackInstance('NAME')
            assert o.get_addresses() == 'Ext-Net=167.114.233.32'

    def test_get_ip_neutron(self):
        instance_id = '8e1fd70a-3065-46f8-9c30-84dc028c1834'
        ip = '10.10.10.4'
        def sh(cmd):
            if 'neutron subnet-list' in cmd:
                return """
[
  {
    "ip_version": 6,
    "id": "c45b9661-b2ba-4817-9e3a-f8f63bf32989"
  },
  {
    "ip_version": 4,
    "id": "e03a3dbc-afc8-4b52-952e-7bf755397b50"
  }
]
                """
            elif 'neutron port-list' in cmd:
                return ("""
[
  {
    "device_id": "915504ad-368b-4cce-be7c-4f8a83902e28",
    "fixed_ips": "{\\"subnet_id\\": \\"e03a3dbc-afc8-4b52-952e-7bf755397b50\\", \\"ip_address\\": \\"10.10.10.1\\"}\\n{\\"subnet_id\\": \\"c45b9661-b2ba-4817-9e3a-f8f63bf32989\\", \\"ip_address\\": \\"2607:f298:6050:9afc::1\\"}"
  },
  {
    "device_id": "{instance_id}",
    "fixed_ips": "{\\"subnet_id\\": \\"e03a3dbc-afc8-4b52-952e-7bf755397b50\\", \\"ip_address\\": \\"{ip}\\"}\\n{\\"subnet_id\\": \\"c45b9661-b2ba-4817-9e3a-f8f63bf32989\\", \\"ip_address\\": \\"2607:f298:6050:9afc:f816:3eff:fe07:76c1\\"}"
  },
  {
    "device_id": "17e4a968-4caa-4cee-8e4b-f950683a02bd",
    "fixed_ips": "{\\"subnet_id\\": \\"e03a3dbc-afc8-4b52-952e-7bf755397b50\\", \\"ip_address\\": \\"10.10.10.5\\"}\\n{\\"subnet_id\\": \\"c45b9661-b2ba-4817-9e3a-f8f63bf32989\\", \\"ip_address\\": \\"2607:f298:6050:9afc:f816:3eff:fe9c:37f0\\"}"
  }
]
                """.replace('{instance_id}', instance_id).
                        replace('{ip}', ip))
            else:
                raise Exception("unexpected " + cmd)
        with patch.multiple(
                misc,
                sh=sh,
        ):
            assert ip == OpenStackInstance(
                instance_id,
                { 'id': instance_id },
            ).get_ip_neutron()

class TestOpenStack(object):

    def test_interpret_hints(self):
        defaults = {
            'machine': {
                'ram': 0,
                'disk': 0,
                'cpus': 0,
            },
            'volumes': {
                'count': 0,
                'size': 0,
            },
        }
        expected_disk = 10 # first hint larger than the second
        expected_ram = 20 # second hint larger than the first
        expected_cpus = 0 # not set, hence zero by default
        expected_count = 30 # second hint larger than the first
        expected_size = 40 # does not exist in the first hint
        hints = [
            {
                'machine': {
                    'ram': 2,
                    'disk': expected_disk,
                },
                'volumes': {
                    'count': 9,
                    'size': expected_size,
                },
            },
            {
                'machine': {
                    'ram': expected_ram,
                    'disk': 3,
                },
                'volumes': {
                    'count': expected_count,
                },
            },
        ]
        hint = OpenStack().interpret_hints(defaults, hints)
        assert hint == {
            'machine': {
                'ram': expected_ram,
                'disk': expected_disk,
                'cpus': expected_cpus,
            },
            'volumes': {
                'count': expected_count,
                'size': expected_size,
            }
        }
        assert defaults == OpenStack().interpret_hints(defaults, None)

    def test_set_provider(self):
        auth = os.environ.get('OS_AUTH_URL', None)
        os.environ['OS_AUTH_URL'] = 'cloud.ovh.net'
        assert OpenStack().set_provider() == 'ovh'
        if auth != None:
            os.environ['OS_AUTH_URL'] = auth
        else:
            del os.environ['OS_AUTH_URL']

class TestTeuthologyOpenStack(object):

    @classmethod
    def setup_class(self):
        if 'OS_AUTH_URL' not in os.environ:
            pytest.skip('no OS_AUTH_URL environment variable')

        teuthology.log.setLevel(logging.DEBUG)
        set_config_attr(argparse.Namespace())

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

    def test_create(self, caplog):
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
            '--throttle', '3',
        ]
        archive_upload = 'user@archive:/tmp'
        argv = (self.options +
                ['--teuthology-git-url', 'TEUTHOLOGY_URL',
                 '--teuthology-branch', 'TEUTHOLOGY_BRANCH',
                 '--upload',
                 '--archive-upload', archive_upload] +
                teuthology_argv)
        args = scripts.openstack.parse_args(argv)
        teuthology_argv.extend([
            '--archive-upload', archive_upload,
            '--archive-upload-url', args.archive_upload_url,
        ])
        teuthology = TeuthologyOpenStack(args, None, argv)
        teuthology.user_data = 'teuthology/openstack/test/user-data-test1.txt'
        teuthology.teuthology_suite = 'echo --'

        teuthology.main()
        assert 0 == teuthology.ssh("lsb_release -a")
        assert 0 == teuthology.ssh("grep 'substituded variables' /var/log/cloud-init.log")
        l = caplog.text()
        assert 'Ubuntu 14.04' in l
        assert "nworkers=" + str(args.simultaneous_jobs) in l
        assert "username=" + teuthology.username in l
        assert "upload=--archive-upload user@archive:/tmp" in l
        assert "clone=git clone -b TEUTHOLOGY_BRANCH TEUTHOLOGY_URL" in l
        assert os.environ['OS_AUTH_URL'] in l
        assert " ".join(teuthology_argv) in l

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
