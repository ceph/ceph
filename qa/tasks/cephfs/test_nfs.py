import os
import json
import time
import errno
import logging
from io import BytesIO

from tasks.mgr.mgr_test_case import MgrTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


class TestNFS(MgrTestCase):
    def _nfs_cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("nfs", *args)

    def _orch_cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("orch", *args)

    def _sys_cmd(self, cmd):
        cmd[0:0] = ['sudo']
        ret = self.ctx.cluster.run(args=cmd, check_status=False, stdout=BytesIO(), stderr=BytesIO())
        stdout = ret[0].stdout
        if stdout:
            # It's RemoteProcess defined in teuthology/orchestra/run.py
            return stdout.getvalue()

    def setUp(self):
        super(TestNFS, self).setUp()
        self._load_module("cephadm")
        self._orch_cmd("set", "backend", "cephadm")

        self.cluster_id = "test"
        self.export_type = "cephfs"
        self.pseudo_path = "/cephfs"
        self.expected_name = 'nfs.ganesha-test'

    def _check_port_status(self):
        log.info("NETSTAT")
        self._sys_cmd(['netstat', '-tnlp'])

    def _check_nfs_server_status(self):
        res = self._sys_cmd(['systemctl', 'status', 'nfs-server'])
        if isinstance(res, bytes) and b'Active: active' in res:
            self._disable_nfs()

    def _disable_nfs(self):
        log.info("Disabling NFS")
        self._sys_cmd(['systemctl', 'disable', 'nfs-server', '--now'])

    def _check_nfs_status(self):
        return self._orch_cmd('ls', 'nfs')

    def _check_idempotency(self, *args):
        for _ in range(2):
            self._nfs_cmd(*args)

    def test_create_cluster(self):
        self._check_nfs_server_status()
        self._nfs_cmd("cluster", "create", self.export_type, self.cluster_id)
        time.sleep(8)
        orch_output = self._check_nfs_status()
        expected_status = '1/1'
        try:
            if self.expected_name not in orch_output or expected_status not in orch_output:
                raise CommandFailedError("NFS Ganesha cluster could not be deployed")
        except (TypeError, CommandFailedError):
            raise

    def test_create_cluster_idempotent(self):
        self._check_nfs_server_status()
        self._check_idempotency("cluster", "create", self.export_type, self.cluster_id)

    def test_delete_cluster(self):
        self.test_create_cluster()
        self._nfs_cmd("cluster", "delete", self.cluster_id)
        time.sleep(8)
        orch_output = self._check_nfs_status()
        self.assertEqual("No services reported\n", orch_output)

    def test_delete_cluster_idempotent(self):
        self._check_idempotency("cluster", "delete", self.cluster_id)
