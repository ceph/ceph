import json
import time
import logging
from io import BytesIO

from tasks.mgr.mgr_test_case import MgrTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


class TestNFS(MgrTestCase):
    def _cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(*args)

    def _nfs_cmd(self, *args):
        return self._cmd("nfs", *args)

    def _orch_cmd(self, *args):
        return self._cmd("orch", *args)

    def _sys_cmd(self, cmd):
        cmd[0:0] = ['sudo']
        ret = self.ctx.cluster.run(args=cmd, check_status=False, stdout=BytesIO(), stderr=BytesIO())
        stdout = ret[0].stdout
        if stdout:
            return stdout.getvalue()

    def setUp(self):
        super(TestNFS, self).setUp()
        self.cluster_id = "test"
        self.export_type = "cephfs"
        self.pseudo_path = "/cephfs"
        self.path = "/"
        self.fs_name = "nfs-cephfs"
        self.expected_name = "nfs.ganesha-test"

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

    def _test_idempotency(self, cmd_func, cmd_args):
        cmd_func()
        ret = self.mgr_cluster.mon_manager.raw_cluster_cmd_result(*cmd_args)
        if ret != 0:
            raise RuntimeError("Idempotency test failed")

    def _test_create_cluster(self):
        self._check_nfs_server_status()
        self._nfs_cmd('cluster', 'create', self.export_type, self.cluster_id)
        time.sleep(8)
        orch_output = self._check_nfs_status()
        expected_status = '1/1'
        if self.expected_name not in orch_output or expected_status not in orch_output:
            raise RuntimeError("NFS Ganesha cluster could not be deployed")

    def _test_delete_cluster(self):
        self._nfs_cmd('cluster', 'delete', self.cluster_id)
        expected_output = "No services reported\n"
        wait_time = 10
        while wait_time <= 60:
            time.sleep(wait_time)
            orch_output = self._check_nfs_status()
            if expected_output == orch_output:
                return
            wait_time += 10
        self.fail("NFS Ganesha cluster could not be deleted")

    def _create_export(self, export_id, create_fs=False, extra_cmd=None):
        if create_fs:
            self._cmd('fs', 'volume', 'create', self.fs_name)
        export_cmd = ['nfs', 'export', 'create', 'cephfs', self.fs_name, self.cluster_id]
        if isinstance(extra_cmd, list):
            export_cmd.extend(extra_cmd)
        else:
            export_cmd.append(self.pseudo_path)

        self._cmd(*export_cmd)
        res = self._sys_cmd(['rados', '-p', 'nfs-ganesha', '-N', self.cluster_id, 'get', f'export-{export_id}', '-'])
        if res == b'':
            raise RuntimeError("Export cannot be created")

    def _create_default_export(self):
            self._test_create_cluster()
            self._create_export(export_id='1', create_fs=True)

    def _delete_export(self):
        self._nfs_cmd('export', 'delete', self.cluster_id, self.pseudo_path)

    def _check_export_obj_deleted(self, conf_obj=False):
        rados_obj_ls = self._sys_cmd(['rados', '-p', 'nfs-ganesha', '-N', self.cluster_id, 'ls'])

        if b'export-' in rados_obj_ls or (conf_obj and b'conf-nfs' in rados_obj_ls):
            raise RuntimeError("Delete export failed")

    def test_create_and_delete_cluster(self):
        self._test_create_cluster()
        self._test_delete_cluster()

    def test_create_delete_cluster_idempotency(self):
        self._test_idempotency(self._test_create_cluster, ['nfs', 'cluster', 'create', self.export_type,
                                                           self.cluster_id])
        self._test_idempotency(self._test_delete_cluster, ['nfs', 'cluster', 'delete', self.cluster_id])

    def test_export_create_and_delete(self):
        self._create_default_export()
        self._delete_export()
        self._check_export_obj_deleted()
        self._test_delete_cluster()

    def test_create_delete_export_idempotency(self):
        self._test_idempotency(self._create_default_export, ['nfs', 'export', 'create', 'cephfs',
                                                             self.fs_name, self.cluster_id,
                                                             self.pseudo_path])
        self._test_idempotency(self._delete_export, ['nfs', 'export', 'delete', self.cluster_id,
                                                     self.pseudo_path])

    def test_create_multiple_exports(self):
        #Export-1 with default values
        self._create_default_export()
        #Export-2 with r only
        self._create_export(export_id='2', extra_cmd=[self.pseudo_path+'1', '--readonly'])
        #Export-3 for subvolume with r only
        self._cmd('fs', 'subvolume', 'create', self.fs_name, 'sub_vol')
        fs_path = self._cmd('fs', 'subvolume', 'getpath', self.fs_name, 'sub_vol')
        self._create_export(export_id='3', extra_cmd=[self.pseudo_path+'2', '--readonly', fs_path.strip()])
        #Export-4 for subvolume
        self._create_export(export_id='4', extra_cmd=[self.pseudo_path+'3', fs_path.strip()])
        self._test_delete_cluster()
        self._check_export_obj_deleted(conf_obj=True)
