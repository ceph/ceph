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
import shutil
import tempfile
from ceph_fs_report import util

class FixtureGit(object):

    def setup(self):
        self.d = tempfile.mkdtemp()
        util.sh("""
        cd {dir}
        git init
        echo a >> master.txt ; git add master.txt ; git commit -m 'master' master.txt # noqa
        git branch cuttlefish
        echo a >> cuttlefish.txt ; git add cuttlefish.txt ; git commit -m 'cuttlefish' cuttlefish.txt # noqa
        git checkout master
        """.format(dir=self.d))
        return self

    def teardown(self):
        shutil.rmtree(self.d)
