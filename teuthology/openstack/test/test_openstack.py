#
# Copyright (c) 2015,2016 Red Hat, Inc.
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
import time
from mock import patch

import teuthology
from teuthology import misc
from teuthology.config import set_config_attr
from teuthology.openstack import TeuthologyOpenStack, OpenStack, OpenStackInstance
from teuthology.openstack import NoFlavorException
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

    flavors = """[
          {
            "Name": "eg-60",
            "RAM": 60000,
            "Ephemeral": 0,
            "VCPUs": 16,
            "Is Public": true,
            "Disk": 1600,
            "ID": "0297d7ac-fe6f-4ff1-b6e7-0b8b0908c94f"
          },
          {
            "Name": "win-sp-60",
            "RAM": 60000,
            "Ephemeral": 0,
            "VCPUs": 4,
            "Is Public": true,
            "Disk": 400,
            "ID": "0417a0e6-f68a-4b8b-a642-ca5ecb9652f7"
          },
          {
            "Name": "win-sp-240",
            "RAM": 240000,
            "Ephemeral": 0,
            "VCPUs": 16,
            "Is Public": true,
            "Disk": 1600,
            "ID": "07885848-8831-486d-8525-91484c09cc7e"
          },
          {
            "Name": "vps-ssd-1",
            "RAM": 2000,
            "Ephemeral": 0,
            "VCPUs": 1,
            "Is Public": true,
            "Disk": 10,
            "ID": "164fcc7e-7771-414f-a607-b388cb7b7aa0"
          },
          {
            "Name": "eg-120",
            "RAM": 120000,
            "Ephemeral": 0,
            "VCPUs": 32,
            "Is Public": true,
            "Disk": 1600,
            "ID": "1f1efedf-ec91-4a42-acd7-f5cf64b02d3c"
          },
          {
            "Name": "win-eg-7",
            "RAM": 7000,
            "Ephemeral": 0,
            "VCPUs": 2,
            "Is Public": true,
            "Disk": 200,
            "ID": "377ded36-491f-4ad7-9eb4-876798b2aea9"
          },
          {
            "Name": "eg-30",
            "RAM": 30000,
            "Ephemeral": 0,
            "VCPUs": 8,
            "Is Public": true,
            "Disk": 800,
            "ID": "3c1d6170-0097-4b5c-a3b3-adff1b7a86e0"
          },
          {
            "Name": "eg-15",
            "RAM": 15000,
            "Ephemeral": 0,
            "VCPUs": 4,
            "Is Public": true,
            "Disk": 400,
            "ID": "675558ea-04fe-47a2-83de-40be9b2eacd4"
          },
          {
            "Name": "win-eg-30",
            "RAM": 30000,
            "Ephemeral": 0,
            "VCPUs": 8,
            "Is Public": true,
            "Disk": 800,
            "ID": "6e12cae3-0492-483c-aa39-54a0dcaf86dd"
          },
          {
            "Name": "vps-ssd-2",
            "RAM": 4000,
            "Ephemeral": 0,
            "VCPUs": 1,
            "Is Public": true,
            "Disk": 20,
            "ID": "7939cc5c-79b1-45c0-be2d-aa935d92faa1"
          },
          {
            "Name": "sp-60",
            "RAM": 60000,
            "Ephemeral": 0,
            "VCPUs": 4,
            "Is Public": true,
            "Disk": 400,
            "ID": "80d8510a-79cc-4307-8db7-d1965c9e8ddb"
          },
          {
            "Name": "win-sp-30",
            "RAM": 30000,
            "Ephemeral": 0,
            "VCPUs": 2,
            "Is Public": true,
            "Disk": 200,
            "ID": "8be9dc29-3eca-499b-ae2d-e3c99699131a"
          },
          {
            "Name": "sp-120",
            "RAM": 120000,
            "Ephemeral": 0,
            "VCPUs": 8,
            "Is Public": true,
            "Disk": 800,
            "ID": "ac74cb45-d895-47dd-b9cf-c17778033d83"
          },
          {
            "Name": "win-eg-15",
            "RAM": 15000,
            "Ephemeral": 0,
            "VCPUs": 4,
            "Is Public": true,
            "Disk": 400,
            "ID": "ae900175-72bd-4fbc-8ab2-4673b468aa5b"
          },
          {
            "Name": "win-eg-120",
            "RAM": 120000,
            "Ephemeral": 0,
            "VCPUs": 32,
            "Is Public": true,
            "Disk": 1600,
            "ID": "b798e44e-bf71-488c-9335-f20bf5976547"
          },
          {
            "Name": "sp-30",
            "RAM": 30000,
            "Ephemeral": 0,
            "VCPUs": 2,
            "Is Public": true,
            "Disk": 200,
            "ID": "d1acf88d-6f55-4c5c-a914-4ecbdbd50d6b"
          },
          {
            "Name": "win-eg-60",
            "RAM": 60000,
            "Ephemeral": 0,
            "VCPUs": 16,
            "Is Public": true,
            "Disk": 1600,
            "ID": "def75cbd-a4b1-4f82-9152-90c65df9587b"
          },
          {
            "Name": "vps-ssd-3",
            "RAM": 8000,
            "Ephemeral": 0,
            "VCPUs": 2,
            "Is Public": true,
            "Disk": 40,
            "ID": "e43d7458-6b82-4a78-a712-3a4dc6748cf4"
          },
          {
            "Name": "sp-240",
            "RAM": 240000,
            "Ephemeral": 0,
            "VCPUs": 16,
            "Is Public": true,
            "Disk": 1600,
            "ID": "ed286e2c-769f-4c47-ac52-b8de7a4891f6"
          },
          {
            "Name": "win-sp-120",
            "RAM": 120000,
            "Ephemeral": 0,
            "VCPUs": 8,
            "Is Public": true,
            "Disk": 800,
            "ID": "f247dc56-395b-49de-9a62-93ccc4fff4ed"
          },
          {
            "Name": "eg-7",
            "RAM": 7000,
            "Ephemeral": 0,
            "VCPUs": 2,
            "Is Public": true,
            "Disk": 200,
            "ID": "fa3cc551-0358-4170-be64-56ea432b064c"
          }
    ]"""

    @patch('teuthology.misc.sh')
    def test_sorted_flavors(self, m_sh):
        o = OpenStack()
        select = '^(vps|eg)-'
        m_sh.return_value = TestOpenStack.flavors
        flavors = o.get_sorted_flavors('arch', select)
        assert ['vps-ssd-1',
                'vps-ssd-2',
                'eg-7',
                'vps-ssd-3',
                'eg-15',
                'eg-30',
                'eg-60',
                'eg-120',
        ] == [ f['Name'] for f in flavors ]
        m_sh.assert_called_with("openstack flavor list -f json")

    def test_flavor(self):
        def get_sorted_flavors(self, arch, select):
            return [
                {
                    'Name': 'too_small',
                    'RAM': 2048,
                    'Disk': 50,
                    'VCPUs': 1,
                },
            ]
        with patch.multiple(
                OpenStack,
                get_sorted_flavors=get_sorted_flavors,
        ):
            with pytest.raises(NoFlavorException):
                hint = { 'ram': 1000, 'disk': 40, 'cpus': 2 }
                OpenStack().flavor(hint, 'arch', None)

        flavor = 'good-flavor'
        def get_sorted_flavors(self, arch, select):
            return [
                {
                    'Name': flavor,
                    'RAM': 2048,
                    'Disk': 50,
                    'VCPUs': 2,
                },
            ]
        with patch.multiple(
                OpenStack,
                get_sorted_flavors=get_sorted_flavors,
        ):
            hint = { 'ram': 1000, 'disk': 40, 'cpus': 2 }
            assert flavor == OpenStack().flavor(hint, 'arch', None)

    def test_flavor_range(self):
        flavors = [
                {
                    'Name': 'too_small',
                    'RAM': 2048,
                    'Disk': 50,
                    'VCPUs': 1,
                },
        ]
        def get_sorted_flavors(self, arch, select):
            return flavors

        min = { 'ram': 1000, 'disk': 40, 'cpus': 2 }
        good = { 'ram': 4000, 'disk': 40, 'cpus': 2 }

        #
        # there are no flavors in the required range
        #
        with patch.multiple(
                OpenStack,
                get_sorted_flavors=get_sorted_flavors,
        ):
            with pytest.raises(NoFlavorException):
                OpenStack().flavor_range(min, good, 'arch', None)

        #
        # there is one flavor in the required range
        #
        flavors.append({
                    'Name': 'min',
                    'RAM': 2048,
                    'Disk': 40,
                    'VCPUs': 2,
        })

        with patch.multiple(
                OpenStack,
                get_sorted_flavors=get_sorted_flavors,
        ):

            assert 'min' == OpenStack().flavor_range(min, good, 'arch', None)

        #
        # out of the two flavors in the required range, get the bigger one
        #
        flavors.append({
                    'Name': 'good',
                    'RAM': 3000,
                    'Disk': 40,
                    'VCPUs': 2,
        })

        with patch.multiple(
                OpenStack,
                get_sorted_flavors=get_sorted_flavors,
        ):

            assert 'good' == OpenStack().flavor_range(min, good, 'arch', None)

        #
        # there is one flavor bigger or equal to good, get this one
        #
        flavors.append({
                    'Name': 'best',
                    'RAM': 4000,
                    'Disk': 40,
                    'VCPUs': 2,
        })

        with patch.multiple(
                OpenStack,
                get_sorted_flavors=get_sorted_flavors,
        ):

            assert 'best' == OpenStack().flavor_range(min, good, 'arch', None)

        #
        # there are two flavors bigger or equal to good, get the smallest one
        #
        flavors.append({
                    'Name': 'too_big',
                    'RAM': 30000,
                    'Disk': 400,
                    'VCPUs': 20,
        })

        with patch.multiple(
                OpenStack,
                get_sorted_flavors=get_sorted_flavors,
        ):

            assert 'best' == OpenStack().flavor_range(min, good, 'arch', None)


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

    def test_get_provider(self):
        auth = os.environ.get('OS_AUTH_URL', None)
        os.environ['OS_AUTH_URL'] = 'cloud.ovh.net'
        assert OpenStack().get_provider() == 'ovh'
        if auth != None:
            os.environ['OS_AUTH_URL'] = auth
        else:
            del os.environ['OS_AUTH_URL']

    def test_get_os_url(self):
        o = OpenStack()
        #
        # Only for OVH
        #
        o.provider = 'something'
        assert "" == o.get_os_url("server ")
        o.provider = 'ovh'
        assert "" == o.get_os_url("unknown ")
        type2cmd = {
            'compute': ('server', 'flavor'),
            'network': ('ip', 'security', 'network'),
            'image': ('image',),
            'volume': ('volume',),
        }
        os.environ['OS_REGION_NAME'] = 'REGION'
        os.environ['OS_TENANT_ID'] = 'TENANT'
        for (type, cmds) in type2cmd.iteritems():
            for cmd in cmds:
                assert ("//" + type) in o.get_os_url(cmd + " ")
        for type in type2cmd.keys():
            assert ("//" + type) in o.get_os_url("whatever ", type=type)
        del os.environ['OS_REGION_NAME']
        del os.environ['OS_TENANT_ID']

    @patch('teuthology.misc.sh')
    def test_cache_token(self, m_sh):
        token = 'TOKEN VALUE'
        m_sh.return_value = token
        OpenStack.token = None
        o = OpenStack()
        #
        # Only for OVH
        #
        o.provider = 'something'
        assert False == o.cache_token()
        o.provider = 'ovh'
        #
        # Set the environment with the token
        #
        assert 'OS_TOKEN_VALUE' not in os.environ
        assert 'OS_TOKEN_EXPIRES' not in os.environ
        assert True == o.cache_token()
        m_sh.assert_called_with('openstack -q token issue -c id -f value')
        assert token == os.environ['OS_TOKEN_VALUE']
        assert token == OpenStack.token
        assert time.time() < int(os.environ['OS_TOKEN_EXPIRES'])
        assert time.time() < OpenStack.token_expires
        #
        # Reset after it expires
        #
        token_expires = int(time.time()) - 2000
        OpenStack.token_expires = token_expires
        assert True == o.cache_token()
        assert time.time() < int(os.environ['OS_TOKEN_EXPIRES'])
        assert time.time() < OpenStack.token_expires
        del os.environ['OS_TOKEN_VALUE']
        del os.environ['OS_TOKEN_EXPIRES']

    @patch('teuthology.misc.sh')
    def test_cache_token_from_environment(self, m_sh):
        OpenStack.token = None
        o = OpenStack()
        o.provider = 'ovh'
        token = 'TOKEN VALUE'
        os.environ['OS_TOKEN_VALUE'] = token
        token_expires = int(time.time()) + OpenStack.token_cache_duration
        os.environ['OS_TOKEN_EXPIRES'] = str(token_expires)
        assert True == o.cache_token()
        assert token == OpenStack.token
        assert token_expires == OpenStack.token_expires
        m_sh.assert_not_called()
        del os.environ['OS_TOKEN_VALUE']
        del os.environ['OS_TOKEN_EXPIRES']
        
    @patch('teuthology.misc.sh')
    def test_cache_token_expired_environment(self, m_sh):
        token = 'TOKEN VALUE'
        m_sh.return_value = token
        OpenStack.token = None
        o = OpenStack()
        o.provider = 'ovh'
        os.environ['OS_TOKEN_VALUE'] = token
        token_expires = int(time.time()) - 2000
        os.environ['OS_TOKEN_EXPIRES'] = str(token_expires)
        assert True == o.cache_token()
        m_sh.assert_called_with('openstack -q token issue -c id -f value')
        assert token == os.environ['OS_TOKEN_VALUE']
        assert token == OpenStack.token
        assert time.time() < int(os.environ['OS_TOKEN_EXPIRES'])
        assert time.time() < OpenStack.token_expires
        del os.environ['OS_TOKEN_VALUE']
        del os.environ['OS_TOKEN_EXPIRES']

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
            OpenStack().run("ip floating delete " + ip_id)
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
        l = caplog.text
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
        OpenStack().run("ip floating delete " + ip_id)
