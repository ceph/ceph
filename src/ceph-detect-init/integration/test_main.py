#!/usr/bin/env python
#
# Copyright (C) 2015 SUSE LINUX GmbH
# Copyright (C) 2015 <contact@redhat.com>
#
# Author: Owen Synge <osynge@suse.com>
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
import logging
import shutil
import subprocess
import testtools

from ceph_detect_init import main

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG)


def run(os):
    name = 'ceph-detect-init-' + os
    shutil.rmtree(name, ignore_errors=True)
    script = """\
docker build -t {name} --file integration/{os}.dockerfile .
toplevel=$(git rev-parse --show-toplevel)
mkdir {name}
cat > {name}/try.sh <<EOF
  virtualenv {name}
  . {name}/bin/activate
  pip install -r requirements.txt
  python setup.py install
  ceph-detect-init > {name}/init
EOF

docker run -v $toplevel:$toplevel -w $(pwd) --user $(id -u) {name} bash -x {name}/try.sh
""".format(name=name,
           os=os)
    subprocess.check_call(script, shell=True)
    init = open(name + '/init').read().strip()
    shutil.rmtree(name)
    return init


class TestCephDetectInit(testtools.TestCase):

    def test_alpine_3_4(self):
        self.assertEqual('openrc', run('alpine-3.4'))

    def test_centos_6(self):
        self.assertEqual('sysvinit', run('centos-6'))

    def test_centos_7(self):
        self.assertEqual('sysvinit', run('centos-7'))

    def test_ubuntu_12_04(self):
        self.assertEqual('upstart', run('ubuntu-12.04'))

    def test_ubuntu_14_04(self):
        self.assertEqual('upstart', run('ubuntu-14.04'))

    def test_ubuntu_15_04(self):
        self.assertEqual('upstart', run('ubuntu-15.04'))

    def test_debian_squeeze(self):
        self.assertEqual('sysvinit', run('debian-squeeze'))

    def test_debian_wheezy(self):
        self.assertEqual('sysvinit', run('debian-wheezy'))

    def test_debian_jessie(self):
        self.assertEqual('sysvinit', run('debian-jessie'))

    def test_debian_sid(self):
        self.assertEqual('sysvinit', run('debian-sid'))

    def test_fedora_21(self):
        self.assertEqual('sysvinit', run('fedora-21'))

    def test_opensuse_13_1(self):
        self.assertEqual('systemd', run('opensuse-13.1'))

    def test_opensuse_13_2(self):
        self.assertEqual('systemd', run('opensuse-13.2'))

# Local Variables:
# compile-command: "cd .. ; .tox/py27/bin/py.test integration/test_main.py"
# End:
