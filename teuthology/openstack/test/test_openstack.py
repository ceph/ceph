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


class TestOpenStackBase(object):

    def setup(self):
        OpenStack.token = None
        OpenStack.token_expires = None
        self.environ = {}
        for k in os.environ.keys():
            if k.startswith('OS_'):
                self.environ[k] = os.environ[k]

    def teardown(self):
        OpenStack.token = None
        OpenStack.token_expires = None
        for k in os.environ.keys():
            if k.startswith('OS_'):
                if k in self.environ:
                    os.environ[k] = self.environ[k]
                else:
                    del os.environ[k]

class TestOpenStackInstance(TestOpenStackBase):

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

    @classmethod
    def setup_class(self):
        if 'OS_AUTH_URL' not in os.environ:
            pytest.skip('no OS_AUTH_URL environment variable')

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

class TestOpenStack(TestOpenStackBase):

    flavors = """[
  {
    "Name": "eg-120-ssd",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 800,
    "ID": "008f75de-c467-4d15-8f70-79c8fbe19538"
  },
  {
    "Name": "hg-60",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 1600,
    "ID": "0297d7ac-fe6f-4ff1-b6e7-0b8b0908c94f"
  },
  {
    "Name": "win-sp-120-ssd-flex",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 50,
    "ID": "039e31f2-6541-46c8-85cf-7f47fab7ad78"
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
    "Name": "hg-120-ssd",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 800,
    "ID": "042aefc6-b713-4a7e-ada5-3ff81daa1960"
  },
  {
    "Name": "win-sp-60-flex",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 50,
    "ID": "0609290c-ad2a-40f0-8c66-c755dd38fe3f"
  },
  {
    "Name": "win-eg-120",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 800,
    "ID": "0651080f-5d07-44b1-a759-7ea4594b669e"
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
    "Name": "win-hg-60-ssd",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 800,
    "ID": "079aa0a2-5e48-4e58-8205-719bc962736e"
  },
  {
    "Name": "eg-120",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 1600,
    "ID": "090f8b8c-673c-4ab8-9a07-6e54a8776e7b"
  },
  {
    "Name": "win-hg-15-ssd-flex",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 50,
    "ID": "10e10c58-d29f-4ff6-a1fd-085c35a3bd9b"
  },
  {
    "Name": "eg-15-ssd",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 200,
    "ID": "1340a920-0f2f-4c1b-8d74-e2502258da73"
  },
  {
    "Name": "win-eg-30-ssd-flex",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 50,
    "ID": "13e54752-fbd0-47a6-aa93-e5a67dfbc743"
  },
  {
    "Name": "eg-120-ssd-flex",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 50,
    "ID": "15c07a54-2dfb-41d9-aa73-6989fd8cafc2"
  },
  {
    "Name": "win-eg-120-ssd-flex",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 50,
    "ID": "15e0dfcc-10f4-4e70-8ac1-30bc323273e2"
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
    "Name": "win-sp-120-flex",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 50,
    "ID": "169415e1-0979-4527-94fb-638c885bbd8c"
  },
  {
    "Name": "win-hg-60-flex",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 50,
    "ID": "16f13d5b-be27-4b8b-88da-959d3904d3ba"
  },
  {
    "Name": "win-sp-30-ssd",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 100,
    "ID": "1788102b-ab80-4a0c-b819-541deaca7515"
  },
  {
    "Name": "win-sp-240-flex",
    "RAM": 240000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 50,
    "ID": "17bcfa14-135f-442f-9397-a4dc25265560"
  },
  {
    "Name": "win-eg-60-ssd-flex",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 50,
    "ID": "194ca9ba-04af-4d86-ba37-d7da883a7eab"
  },
  {
    "Name": "win-eg-60-flex",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 50,
    "ID": "19ff8837-4751-4f6c-a82b-290bc53c83c1"
  },
  {
    "Name": "win-eg-30-flex",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 50,
    "ID": "1aaef5e5-4df9-4462-80d3-701683ab9ff0"
  },
  {
    "Name": "eg-15",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 400,
    "ID": "1cd85b81-5e4d-477a-a127-eb496b1d75de"
  },
  {
    "Name": "hg-120",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 1600,
    "ID": "1f1efedf-ec91-4a42-acd7-f5cf64b02d3c"
  },
  {
    "Name": "hg-15-ssd-flex",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 50,
    "ID": "20347a07-a289-4c07-a645-93cb5e8e2d30"
  },
  {
    "Name": "win-eg-7-ssd",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 100,
    "ID": "20689394-bd77-4f4d-900e-52cc8a86aeb4"
  },
  {
    "Name": "win-sp-60-ssd-flex",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 50,
    "ID": "21104d99-ba7b-47a0-9133-7e884710089b"
  },
  {
    "Name": "win-sp-120-ssd",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 400,
    "ID": "23c21ecc-9ee8-4ad3-bd9f-aa17a3faf84e"
  },
  {
    "Name": "win-hg-15-ssd",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 200,
    "ID": "24e293ed-bc54-4f26-8fb7-7b9457d08e66"
  },
  {
    "Name": "eg-15-ssd-flex",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 50,
    "ID": "25f3534a-89e5-489d-aa8b-63f62e76875b"
  },
  {
    "Name": "win-eg-60",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 800,
    "ID": "291173f1-ea1d-410b-8045-667361a4addb"
  },
  {
    "Name": "sp-30-ssd-flex",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 50,
    "ID": "2b646463-2efa-428b-94ed-4059923c3636"
  },
  {
    "Name": "win-eg-120-flex",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 50,
    "ID": "2c74df82-29d2-4b1a-a32c-d5633e7359b4"
  },
  {
    "Name": "win-eg-15-ssd",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 200,
    "ID": "2fe4344f-d701-4bc4-8dcd-6d0b5d83fa13"
  },
  {
    "Name": "sp-30-flex",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 50,
    "ID": "31487b30-eeb6-472f-a9b6-38ace6587ebc"
  },
  {
    "Name": "win-sp-240-ssd",
    "RAM": 240000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 800,
    "ID": "325b602f-ecc4-4444-90bd-5a2cf4e0da53"
  },
  {
    "Name": "win-hg-7",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 200,
    "ID": "377ded36-491f-4ad7-9eb4-876798b2aea9"
  },
  {
    "Name": "sp-30-ssd",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 100,
    "ID": "382f2831-4dba-40c4-bb7a-6fadff71c4db"
  },
  {
    "Name": "hg-30",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 800,
    "ID": "3c1d6170-0097-4b5c-a3b3-adff1b7a86e0"
  },
  {
    "Name": "hg-60-flex",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 50,
    "ID": "3c669730-b5cd-4e44-8bd2-bc8d9f984ab2"
  },
  {
    "Name": "sp-240-ssd-flex",
    "RAM": 240000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 50,
    "ID": "3d66fea3-26f2-4195-97ab-fdea3b836099"
  },
  {
    "Name": "sp-240-flex",
    "RAM": 240000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 50,
    "ID": "40c781f7-d7a7-4b0d-bcca-5304aeabbcd9"
  },
  {
    "Name": "hg-7-flex",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 50,
    "ID": "42730e52-147d-46b8-9546-18e31e5ac8a9"
  },
  {
    "Name": "eg-30-ssd",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 400,
    "ID": "463f30e9-7d7a-4693-944f-142067cf553b"
  },
  {
    "Name": "hg-15-flex",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 50,
    "ID": "534f07c6-91af-44c8-9e62-156360fe8359"
  },
  {
    "Name": "win-sp-30-flex",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 50,
    "ID": "55533fdf-ad57-4aa7-a2c6-ee31bb94e77b"
  },
  {
    "Name": "win-hg-60-ssd-flex",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 50,
    "ID": "58b24234-3804-4c4f-9eb6-5406a3a13758"
  },
  {
    "Name": "hg-7-ssd-flex",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 50,
    "ID": "596c1276-8e53-40a0-b183-cdd9e9b1907d"
  },
  {
    "Name": "win-hg-30-ssd",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 400,
    "ID": "5c54dc08-28b9-4860-9f24-a2451b2a28ec"
  },
  {
    "Name": "eg-7",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 200,
    "ID": "5e409dbc-3f4b-46e8-a629-a418c8497922"
  },
  {
    "Name": "hg-30-flex",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 50,
    "ID": "656423ea-0551-48c6-9e0f-ec6e15952029"
  },
  {
    "Name": "hg-15",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 400,
    "ID": "675558ea-04fe-47a2-83de-40be9b2eacd4"
  },
  {
    "Name": "eg-60-flex",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 50,
    "ID": "68a8e4e1-d291-46e8-a724-fbb1c4b9b051"
  },
  {
    "Name": "hg-30-ssd",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 400,
    "ID": "6ab72807-e0a5-4e9f-bbb9-7cbbf0038b26"
  },
  {
    "Name": "win-hg-30",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 800,
    "ID": "6e12cae3-0492-483c-aa39-54a0dcaf86dd"
  },
  {
    "Name": "win-hg-7-ssd",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 100,
    "ID": "6ead771c-e8b9-424c-afa0-671280416422"
  },
  {
    "Name": "win-hg-30-flex",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 50,
    "ID": "70ded741-8f58-4bb9-8cfd-5e838b66b5f3"
  },
  {
    "Name": "win-sp-30-ssd-flex",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 50,
    "ID": "7284d104-a260-421d-8cee-6dc905107b25"
  },
  {
    "Name": "win-eg-120-ssd",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 800,
    "ID": "72c0b262-855d-40bb-a3e9-fd989a1bc466"
  },
  {
    "Name": "win-hg-7-flex",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 50,
    "ID": "73961591-c5f1-436f-b641-1a506eddaef4"
  },
  {
    "Name": "sp-240-ssd",
    "RAM": 240000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 800,
    "ID": "7568d834-3b16-42ce-a2c1-0654e0781160"
  },
  {
    "Name": "win-eg-60-ssd",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 800,
    "ID": "75f7fe5c-f87a-41d8-a961-a0169d02c268"
  },
  {
    "Name": "eg-7-ssd-flex",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 50,
    "ID": "77e1db73-0b36-4e37-8e47-32c2d2437ca9"
  },
  {
    "Name": "eg-60-ssd-flex",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 50,
    "ID": "78df4e30-98ca-4362-af68-037d958edaf0"
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
    "Name": "win-hg-120-ssd-flex",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 50,
    "ID": "835e734a-46b6-4cb2-be68-e8678fd71059"
  },
  {
    "Name": "win-eg-7",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 200,
    "ID": "84869b00-b43a-4523-babd-d47d206694e9"
  },
  {
    "Name": "win-eg-7-ssd-flex",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 50,
    "ID": "852308f8-b8bf-44a4-af41-cbc27437b275"
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
    "Name": "win-hg-7-ssd-flex",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 50,
    "ID": "8d704cfd-05b2-4d4a-add2-e2868bcc081f"
  },
  {
    "Name": "eg-30",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 800,
    "ID": "901f77c2-73f6-4fae-b28a-18b829b55a17"
  },
  {
    "Name": "sp-60-ssd-flex",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 50,
    "ID": "944b92fb-9a0c-406d-bb9f-a1d93cda9f01"
  },
  {
    "Name": "eg-30-flex",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 50,
    "ID": "965472c7-eb54-4d4d-bd6e-01ebb694a631"
  },
  {
    "Name": "sp-120-ssd",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 400,
    "ID": "97824a8c-e683-49a8-a70a-ead64240395c"
  },
  {
    "Name": "hg-60-ssd",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 800,
    "ID": "9831d7f1-3e79-483d-8958-88e3952c7ea2"
  },
  {
    "Name": "eg-60",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 1600,
    "ID": "9e1f13d0-4fcc-4abc-a9e6-9c76d662c92d"
  },
  {
    "Name": "win-eg-30-ssd",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 400,
    "ID": "9e6b85fa-6f37-45ce-a3d6-11ab40a28fad"
  },
  {
    "Name": "hg-120-flex",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 50,
    "ID": "9ed787cc-a0db-400b-8cc1-49b6384a1000"
  },
  {
    "Name": "sp-120-flex",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 50,
    "ID": "9f3cfdf7-b850-47cc-92be-33aefbfd2b05"
  },
  {
    "Name": "hg-60-ssd-flex",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 50,
    "ID": "a37bdf17-e1b1-41cc-a67f-ed665a120446"
  },
  {
    "Name": "win-hg-120-ssd",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 800,
    "ID": "aa753e73-dadb-4528-9c4a-24e36fc41bf4"
  },
  {
    "Name": "win-sp-240-ssd-flex",
    "RAM": 240000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 50,
    "ID": "abc007b8-cc44-4b6b-9606-fd647b03e101"
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
    "Name": "win-hg-15",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 400,
    "ID": "ae900175-72bd-4fbc-8ab2-4673b468aa5b"
  },
  {
    "Name": "win-eg-15-ssd-flex",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 50,
    "ID": "aeb37dbf-d7c9-4fd7-93f1-f3818e488ede"
  },
  {
    "Name": "hg-7-ssd",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 100,
    "ID": "b1dc776c-b6e3-4a96-b230-850f570db3d5"
  },
  {
    "Name": "sp-60-ssd",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 200,
    "ID": "b24df495-10f3-466e-95ab-26f0f6839a2f"
  },
  {
    "Name": "win-hg-120",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 1600,
    "ID": "b798e44e-bf71-488c-9335-f20bf5976547"
  },
  {
    "Name": "eg-7-ssd",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 100,
    "ID": "b94e6623-913d-4147-b2a3-34ccf6fe7a5e"
  },
  {
    "Name": "eg-15-flex",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 50,
    "ID": "bb5fdda8-34ec-40c8-a4e3-308b9e2c9ee2"
  },
  {
    "Name": "win-eg-7-flex",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 50,
    "ID": "c65384f6-4665-461a-a292-2f3f5a016244"
  },
  {
    "Name": "eg-60-ssd",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 800,
    "ID": "c678f1a8-6542-4f9d-89af-ffc98715d674"
  },
  {
    "Name": "hg-30-ssd-flex",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 50,
    "ID": "d147a094-b653-41e7-9250-8d4da3044334"
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
    "Name": "sp-120-ssd-flex",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 50,
    "ID": "d2d33e8e-58b1-4661-8141-826c47f82166"
  },
  {
    "Name": "hg-120-ssd-flex",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 50,
    "ID": "d7322c37-9881-4a57-9b40-2499fe2e8f42"
  },
  {
    "Name": "win-hg-15-flex",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 50,
    "ID": "daf597ea-fbbc-4c71-a35e-5b41d33ccc6c"
  },
  {
    "Name": "win-hg-30-ssd-flex",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 50,
    "ID": "dcfd834c-3932-47a3-8b4b-cdfeecdfde2c"
  },
  {
    "Name": "win-hg-60",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 16,
    "Is Public": true,
    "Disk": 1600,
    "ID": "def75cbd-a4b1-4f82-9152-90c65df9587b"
  },
  {
    "Name": "eg-30-ssd-flex",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 50,
    "ID": "e04c7ad6-a5de-45f5-93c9-f3343bdfe8d1"
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
    "Name": "win-eg-15-flex",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 50,
    "ID": "e8bd3402-7310-4a0f-8b99-d9212359c957"
  },
  {
    "Name": "win-eg-30",
    "RAM": 30000,
    "Ephemeral": 0,
    "VCPUs": 8,
    "Is Public": true,
    "Disk": 800,
    "ID": "ebf7a997-e2f8-42f4-84f7-33a3d53d1af9"
  },
  {
    "Name": "eg-120-flex",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 50,
    "ID": "ec852ed3-1e42-4c59-abc3-12bcd26abec8"
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
    "Name": "win-sp-60-ssd",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 200,
    "ID": "ed835a73-d9a0-43ee-bd89-999c51d8426d"
  },
  {
    "Name": "win-eg-15",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 400,
    "ID": "f06056c1-a2d4-40e7-a7d8-e5bfabada72e"
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
    "Name": "eg-7-flex",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 50,
    "ID": "f476f959-ffa6-46f2-94d8-72293570604d"
  },
  {
    "Name": "sp-60-flex",
    "RAM": 60000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 50,
    "ID": "f52db47a-315f-49d4-bc5c-67dd118e7ac0"
  },
  {
    "Name": "win-hg-120-flex",
    "RAM": 120000,
    "Ephemeral": 0,
    "VCPUs": 32,
    "Is Public": true,
    "Disk": 50,
    "ID": "f6cb8144-5d98-4057-b44f-46da342fb571"
  },
  {
    "Name": "hg-7",
    "RAM": 7000,
    "Ephemeral": 0,
    "VCPUs": 2,
    "Is Public": true,
    "Disk": 200,
    "ID": "fa3cc551-0358-4170-be64-56ea432b064c"
  },
  {
    "Name": "hg-15-ssd",
    "RAM": 15000,
    "Ephemeral": 0,
    "VCPUs": 4,
    "Is Public": true,
    "Disk": 200,
    "ID": "ff48c2cf-c17f-4682-aaf6-31d66786f808"
  }
    ]"""

    @classmethod
    def setup_class(self):
        if 'OS_AUTH_URL' not in os.environ:
            pytest.skip('no OS_AUTH_URL environment variable')

    @patch('teuthology.misc.sh')
    def test_sorted_flavors(self, m_sh):
        o = OpenStack()
        select = '^(vps|hg)-.*ssd'
        m_sh.return_value = TestOpenStack.flavors
        flavors = o.get_sorted_flavors('arch', select)
        assert [u'vps-ssd-1',
                u'vps-ssd-2',
                u'hg-7-ssd-flex',
                u'hg-7-ssd',
                u'vps-ssd-3',
                u'hg-15-ssd-flex',
                u'hg-15-ssd',
                u'hg-30-ssd-flex',
                u'hg-30-ssd',
                u'hg-60-ssd-flex',
                u'hg-60-ssd',
                u'hg-120-ssd-flex',
                u'hg-120-ssd',
        ] == [ f['Name'] for f in flavors ]
        m_sh.assert_called_with("openstack --quiet flavor list -f json")

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
                OpenStack().flavor(hint, 'arch')

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
            assert flavor == OpenStack().flavor(hint, 'arch')

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
                OpenStack().flavor_range(min, good, 'arch')

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

            assert 'min' == OpenStack().flavor_range(min, good, 'arch')

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

            assert 'good' == OpenStack().flavor_range(min, good, 'arch')

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

            assert 'best' == OpenStack().flavor_range(min, good, 'arch')

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

            assert 'best' == OpenStack().flavor_range(min, good, 'arch')


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
        for (type, cmds) in type2cmd.items():
            for cmd in cmds:
                assert ("//" + type) in o.get_os_url(cmd + " ")
        for type in type2cmd.keys():
            assert ("//" + type) in o.get_os_url("whatever ", type=type)

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

class TestTeuthologyOpenStack(TestOpenStackBase):

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
        super(TestTeuthologyOpenStack, self).setup()
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
        super(TestTeuthologyOpenStack, self).teardown()
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
                 '--ceph-workbench-git-url', 'CEPH_WORKBENCH_URL',
                 '--ceph-workbench-branch', 'CEPH_WORKBENCH_BRANCH',
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
        assert ("ceph_workbench="
                " --ceph-workbench-branch CEPH_WORKBENCH_BRANCH"
                " --ceph-workbench-git-url CEPH_WORKBENCH_URL") in l
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
