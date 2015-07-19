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
import json
import os
import subprocess
import tempfile
import shutil

import teuthology.lock
import teuthology.nuke
import teuthology.misc
import teuthology.schedule
import teuthology.suite
import teuthology.openstack
import scripts.schedule
import scripts.lock
import scripts.suite

class Integration(object):

    @classmethod
    def setup_class(self):
        teuthology.log.setLevel(logging.DEBUG)
        teuthology.misc.read_config(argparse.Namespace())
        self.teardown_class()

    @classmethod
    def teardown_class(self):
        os.system("sudo /etc/init.d/beanstalkd restart")
        # if this fails it will not show the error but some weird
        # INTERNALERROR> IndexError: list index out of range
        # move that to def tearDown for debug and when it works move it
        # back in tearDownClass so it is not called on every test
        all_instances = teuthology.misc.sh("openstack server list -f json --long")
        for instance in json.loads(all_instances):
            if 'teuthology=' in instance['Properties']:
                teuthology.misc.sh("openstack server delete --wait " + instance['ID'])
        teuthology.misc.sh("""
teuthology/openstack/setup-openstack.sh \
  --populate-paddles
        """)

    def setup_worker(self):
        self.logs = self.d + "/log"
        os.mkdir(self.logs, 0o755)
        self.archive = self.d + "/archive"
        os.mkdir(self.archive, 0o755)
        self.worker_cmd = ("teuthology-worker --tube openstack " +
                           "-l " + self.logs + " "
                           "--archive-dir " + self.archive + " ")
	logging.info(self.worker_cmd)
        self.worker = subprocess.Popen(self.worker_cmd,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       shell=True)

    def wait_worker(self):
        if not self.worker:
            return

        (stdoutdata, stderrdata) = self.worker.communicate()
        stdoutdata = stdoutdata.decode('utf-8')
        stderrdata = stderrdata.decode('utf-8')
        logging.info(self.worker_cmd + ":" +
                     " stdout " + stdoutdata +
                     " stderr " + stderrdata + " end ")
        assert self.worker.returncode == 0
        self.worker = None

    def get_teuthology_log(self):
        # the archive is removed before each test, there must
        # be only one run and one job
        run = os.listdir(self.archive)[0]
        job = os.listdir(os.path.join(self.archive, run))[0]
        path = os.path.join(self.archive, run, job, 'teuthology.log')
        return open(path, 'r').read()

class TestSuite(Integration):

    def setup(self):
        self.d = tempfile.mkdtemp()
        self.setup_worker()
        logging.info("TestSuite: done worker")

    def teardown(self):
        self.wait_worker()
        shutil.rmtree(self.d)

    def test_suite_noop(self):
        cwd = os.getcwd()
        args = ['--suite', 'noop',
                '--suite-dir', cwd + '/teuthology/openstack/test',
                '--machine-type', 'openstack',
                '--verbose']
        logging.info("TestSuite:test_suite_noop")
        scripts.suite.main(args)
        self.wait_worker()
        log = self.get_teuthology_log()
        assert "teuthology.run:pass" in log
        assert "Well done" in log

    def test_suite_nuke(self):
        cwd = os.getcwd()
        args = ['--suite', 'nuke',
                '--suite-dir', cwd + '/teuthology/openstack/test',
                '--machine-type', 'openstack',
                '--verbose']
        logging.info("TestSuite:test_suite_nuke")
        scripts.suite.main(args)
        self.wait_worker()
        log = self.get_teuthology_log()
        assert "teuthology.run:FAIL" in log
        locks = teuthology.lock.list_locks(locked=True)
        assert len(locks) == 0

class TestSchedule(Integration):

    def setup(self):
        self.d = tempfile.mkdtemp()
        self.setup_worker()

    def teardown(self):
        self.wait_worker()
        shutil.rmtree(self.d)

    def test_schedule_stop_worker(self):
        job = 'teuthology/openstack/test/stop_worker.yaml'
        args = ['--name', 'fake',
                '--verbose',
                '--owner', 'test@test.com',
                '--worker', 'openstack',
                job]
        scripts.schedule.main(args)
        self.wait_worker()

    def test_schedule_noop(self):
        job = 'teuthology/openstack/test/noop.yaml'
        args = ['--name', 'fake',
                '--verbose',
                '--owner', 'test@test.com',
                '--worker', 'openstack',
                job]
        scripts.schedule.main(args)
        self.wait_worker()
        log = self.get_teuthology_log()
        assert "teuthology.run:pass" in log
        assert "Well done" in log

    def test_schedule_resources_hint(self):
        """It is tricky to test resources hint in a provider agnostic way. The
        best way seems to ask for at least 1GB of RAM and 10GB
        disk. Some providers do not offer a 1GB RAM flavor (OVH for
        instance) and the 2GB RAM will be chosen instead. It however
        seems unlikely that a 4GB RAM will be chosen because it would
        mean such a provider has nothing under that limit and it's a
        little too high.

        Since the default when installing is to ask for 7000 MB, we
        can reasonably assume that the hint has been taken into
        account if the instance has less than 4GB RAM.
        """
        try:
            teuthology.misc.sh("openstack volume list")
            job = 'teuthology/openstack/test/resources_hint.yaml'
            has_cinder = True
        except subprocess.CalledProcessError:
            job = 'teuthology/openstack/test/resources_hint_no_cinder.yaml'
            has_cinder = False
        args = ['--name', 'fake',
                '--verbose',
                '--owner', 'test@test.com',
                '--worker', 'openstack',
                job]
        scripts.schedule.main(args)
        self.wait_worker()
        log = self.get_teuthology_log()
        assert "teuthology.run:pass" in log
        assert "RAM size ok" in log
        if has_cinder:
            assert "Disk size ok" in log

class TestLock(Integration):

    def setup(self):
        self.options = ['--verbose',
                        '--machine-type', 'openstack' ]

    def test_main(self):
        args = scripts.lock.parse_args(self.options + ['--lock'])
        assert teuthology.lock.main(args) == 0

    def test_lock_unlock(self):
        for image in teuthology.openstack.OpenStack.image2url.keys():
            (os_type, os_version) = image.split('-')
            args = scripts.lock.parse_args(self.options +
                                           ['--lock-many', '1',
                                            '--os-type', os_type,
                                            '--os-version', os_version])
            assert teuthology.lock.main(args) == 0
            locks = teuthology.lock.list_locks(locked=True)
            assert len(locks) == 1
            args = scripts.lock.parse_args(self.options +
                                           ['--unlock', locks[0]['name']])
            assert teuthology.lock.main(args) == 0

    def test_list(self, capsys):
        args = scripts.lock.parse_args(self.options + ['--list', '--all'])
        teuthology.lock.main(args)
        out, err = capsys.readouterr()
        assert 'machine_type' in out
        assert 'openstack' in out

class TestNuke(Integration):

    def setup(self):
        self.options = ['--verbose',
                        '--machine-type', 'openstack']

    def test_nuke(self):
        image = teuthology.openstack.OpenStack.image2url.keys()[0]

        (os_type, os_version) = image.split('-')
        args = scripts.lock.parse_args(self.options +
                                       ['--lock-many', '1',
                                        '--os-type', os_type,
                                        '--os-version', os_version])
        assert teuthology.lock.main(args) == 0
        locks = teuthology.lock.list_locks(locked=True)
        logging.info('list_locks = ' + str(locks))
        assert len(locks) == 1
        ctx = argparse.Namespace(name=None,
                                 config={
                                     'targets': { locks[0]['name']: None },
                                 },
                                 owner=locks[0]['locked_by'],
                                 teuthology_config={})
        teuthology.nuke.nuke(ctx, should_unlock=True)
        locks = teuthology.lock.list_locks(locked=True)
        assert len(locks) == 0
