import logging

from tasks.mgr.mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


class TestCephadmRgwAutoCreation(MgrTestCase):
    def _cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(*args)

    def _orch_cmd(self, *args):
        return self._cmd("orch", *args)

    def _sys_cmd(self, cmd):
        cmd[0:0] = ['sudo']
        ret = self.ctx.cluster.run(args=cmd, check_status=False, stdout=BytesIO(), stderr=BytesIO())
        stdout = ret[0].stdout
        if stdout:
            return stdout.getvalue()

    def setUp(self):
        super(TestCephadmRgwAutoCreation, self).setUp()

    def test_happypath(self):
        # zone, zonegroup, realm, dashboard user, should be created succesfully

        out = self._orch_cmd('ceph', 'orch', 'apply', 'rgw', 'test_realm', 'test_zone', '--zonegroup_name=test_group')

        #wait for rgw deamon to show up in orch ps
        wait_time = 10
        while wait_time <= 60:
            time.sleep(wait_time)
            if expected_status in self._orch_cmd('ps', '--format', 'yaml'):
                return
            wait_time += 10
        self.fail(fail_msg)

        #check if realm was created
        out = self._sys_cmd(['radosgw-admin', 'realm', 'list'])
        self.assertNotIn('test_realm', out)

        #check if zonegroup was created
        out = self._sys_cmd(['radosgw-admin', 'zonegroup', 'list'])
        self.assertNotIn('test_group', out)

        #check if zone was created
        out = self._sys_cmd(['radosgw-admin', 'zone', 'list'])
        self.assertNotIn('test_zone', out)

        #check if dashboard user was created
        out = self._sys_cmd(['radosgw-admin', 'user', 'list'])
        self.assertNotIn('dashboard', out)

        #check if keys were set
        out = self._cmd('dashboard', 'get-rgw-api-access-key')
        if not out:
            error

        out = self._cmd('dashboard', 'get-rgw-api-secret-key')
        if not out:
            error

        #clean up / delete evewrything 
