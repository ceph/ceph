import json
import logging
import datetime

from .mgr_test_case import MgrTestCase


log = logging.getLogger(__name__)
UUID = 'd5775432-0742-44a3-a435-45095e32e6b1'
DATEFMT = '%Y-%m-%d %H:%M:%S.%f'


class TestCrash(MgrTestCase):

    def setUp(self):
        super(TestCrash, self).setUp()
        self.setup_mgrs()
        self._load_module('crash')

        # Whip up some crash data
        self.crashes = dict()
        now = datetime.datetime.utcnow()

        for i in (0, 1, 3, 4, 8):
            timestamp = now - datetime.timedelta(days=i)
            timestamp = timestamp.strftime(DATEFMT) + 'Z'
            crash_id = '_'.join((timestamp, UUID)).replace(' ', '_')
            self.crashes[crash_id] = {
                'crash_id': crash_id, 'timestamp': timestamp,
            }

            self.assertEqual(
                0,
                self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                    'crash', 'post', '-i', '-',
                    stdin=json.dumps(self.crashes[crash_id]),
                )
            )

        retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'crash', 'ls',
        )
        log.warning("setUp: crash ls returns %s" % retstr)

        self.oldest_crashid = crash_id

    def tearDown(self):
        for crash in self.crashes.values():
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'crash', 'rm', crash['crash_id']
            )

    def test_info(self):
        for crash in self.crashes.values():
            log.warning('test_info: crash %s' % crash)
            retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'crash', 'ls'
            )
            log.warning('ls output: %s' % retstr)
            retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'crash', 'info', crash['crash_id'],
            )
            log.warning('crash info output: %s' % retstr)
            crashinfo = json.loads(retstr)
            self.assertIn('crash_id', crashinfo)
            self.assertIn('timestamp', crashinfo)

    def test_ls(self):
        retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'crash', 'ls',
        )
        for crash in self.crashes.values():
            self.assertIn(crash['crash_id'], retstr)

    def test_rm(self):
        crashid = next(iter(self.crashes.keys()))
        self.assertEqual(
            0,
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'crash', 'rm', crashid,
            )
        )

        retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'crash', 'ls',
        )
        self.assertNotIn(crashid, retstr)

    def test_stat(self):
        retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'crash', 'stat',
        )
        self.assertIn('5 crashes recorded', retstr)
        self.assertIn('4 older than 1 days old:', retstr)
        self.assertIn('3 older than 3 days old:', retstr)
        self.assertIn('1 older than 7 days old:', retstr)

    def test_prune(self):
        self.assertEqual(
            0,
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'crash', 'prune', '5'
            )
        )
        retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'crash', 'ls',
        )
        self.assertNotIn(self.oldest_crashid, retstr)
