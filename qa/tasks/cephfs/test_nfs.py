import errno
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

    def _check_auth_ls(self, export_id=1, check_in=False):
        '''
        Tests export user id creation or deletion.
        :param export_id: Denotes export number
        :param check_in: Check specified export id
        '''
        output = self._cmd('auth', 'ls')
        if check_in:
            self.assertIn(f'client.{self.cluster_id}{export_id}', output)
        else:
            self.assertNotIn(f'client-{self.cluster_id}', output)

    def _test_idempotency(self, cmd_func, cmd_args):
        '''
        Test idempotency of commands. It first runs the TestNFS test method
        for a command and then checks the result of command run again. TestNFS
        test method has required checks to verify that command works.
        :param cmd_func: TestNFS method
        :param cmd_args: nfs command arguments to be run
        '''
        cmd_func()
        ret = self.mgr_cluster.mon_manager.raw_cluster_cmd_result(*cmd_args)
        if ret != 0:
            self.fail("Idempotency test failed")

    def _test_create_cluster(self):
        '''
        Test single nfs cluster deployment.
        '''
        # Disable any running nfs ganesha daemon
        self._check_nfs_server_status()
        self._nfs_cmd('cluster', 'create', self.export_type, self.cluster_id)
        # Wait for few seconds as ganesha daemon take few seconds to be deployed
        time.sleep(8)
        orch_output = self._check_nfs_status()
        expected_status = '1/1'
        # Check for expected status and daemon name (nfs.ganesha-<cluster_id>)
        if self.expected_name not in orch_output or expected_status not in orch_output:
            self.fail("NFS Ganesha cluster could not be deployed")

    def _test_delete_cluster(self):
        '''
        Test deletion of a single nfs cluster.
        '''
        self._nfs_cmd('cluster', 'delete', self.cluster_id)
        expected_output = "No services reported\n"
        # Wait for few seconds as ganesha daemon takes few seconds to be deleted
        wait_time = 10
        while wait_time <= 60:
            time.sleep(wait_time)
            orch_output = self._check_nfs_status()
            if expected_output == orch_output:
                return
            wait_time += 10
        self.fail("NFS Ganesha cluster could not be deleted")

    def _test_list_cluster(self, empty=False):
        '''
        Test listing of deployed nfs clusters. If nfs cluster is deployed then
        it checks for expected cluster id. Otherwise checks nothing is listed.
        :param empty: If true it denotes no cluster is deployed.
        '''
        if empty:
            cluster_id = ''
        else:
            cluster_id = self.cluster_id
        nfs_output = self._nfs_cmd('cluster', 'ls')
        self.assertEqual(cluster_id, nfs_output.strip())

    def _create_export(self, export_id, create_fs=False, extra_cmd=None):
        '''
        Test creation of a single export.
        :param export_id: Denotes export number
        :param create_fs: If false filesytem exists. Otherwise create it.
        :param extra_cmd: List of extra arguments for creating export.
        '''
        if create_fs:
            self._cmd('fs', 'volume', 'create', self.fs_name)
        export_cmd = ['nfs', 'export', 'create', 'cephfs', self.fs_name, self.cluster_id]
        if isinstance(extra_cmd, list):
            export_cmd.extend(extra_cmd)
        else:
            export_cmd.append(self.pseudo_path)
        # Runs the nfs export create command
        self._cmd(*export_cmd)
        # Check if user id for export is created
        self._check_auth_ls(export_id, check_in=True)
        res = self._sys_cmd(['rados', '-p', 'nfs-ganesha', '-N', self.cluster_id, 'get',
                             f'export-{export_id}', '-'])
        # Check if export object is created
        if res == b'':
            self.fail("Export cannot be created")

    def _create_default_export(self):
        '''
        Deploy a single nfs cluster and create export with default options.
        '''
        self._test_create_cluster()
        self._create_export(export_id='1', create_fs=True)

    def _delete_export(self):
        '''
        Delete an export.
        '''
        self._nfs_cmd('export', 'delete', self.cluster_id, self.pseudo_path)
        self._check_auth_ls()

    def _test_list_export(self):
        '''
        Test listing of created exports.
        '''
        nfs_output = json.loads(self._nfs_cmd('export', 'ls', self.cluster_id))
        self.assertIn(self.pseudo_path, nfs_output)

    def _check_export_obj_deleted(self, conf_obj=False):
        '''
        Test if export or config object are deleted successfully.
        :param conf_obj: It denotes config object needs to be checked
        '''
        rados_obj_ls = self._sys_cmd(['rados', '-p', 'nfs-ganesha', '-N', self.cluster_id, 'ls'])

        if b'export-' in rados_obj_ls or (conf_obj and b'conf-nfs' in rados_obj_ls):
            self.fail("Delete export failed")

    def test_create_and_delete_cluster(self):
        '''
        Test successful creation and deletion of the nfs cluster.
        '''
        self._test_create_cluster()
        self._test_list_cluster()
        self._test_delete_cluster()
        # List clusters again to ensure no cluster is shown
        self._test_list_cluster(empty=True)

    def test_create_delete_cluster_idempotency(self):
        '''
        Test idempotency of cluster create and delete commands.
        '''
        self._test_idempotency(self._test_create_cluster, ['nfs', 'cluster', 'create', self.export_type,
                                                           self.cluster_id])
        self._test_idempotency(self._test_delete_cluster, ['nfs', 'cluster', 'delete', self.cluster_id])

    def test_create_cluster_with_invalid_cluster_id(self):
        '''
        Test nfs cluster deployment failure with invalid cluster id.
        '''
        try:
            invalid_cluster_id = '/cluster_test'  # Only [A-Za-z0-9-_.] chars are valid
            self._nfs_cmd('cluster', 'create', self.export_type, invalid_cluster_id)
            self.fail(f"Cluster successfully created with invalid cluster id {invalid_cluster_id}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.EINVAL:
                raise

    def test_create_cluster_with_invalid_export_type(self):
        '''
        Test nfs cluster deployment failure with invalid export type.
        '''
        try:
            invalid_export_type = 'rgw'  # Only cephfs is valid
            self._nfs_cmd('cluster', 'create', invalid_export_type, self.cluster_id)
            self.fail(f"Cluster successfully created with invalid export type {invalid_export_type}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.EINVAL:
                raise

    def test_export_create_and_delete(self):
        '''
        Test successful creation and deletion of the cephfs export.
        '''
        self._create_default_export()
        self._delete_export()
        # Check if rados export object is deleted
        self._check_export_obj_deleted()
        self._test_delete_cluster()

    def test_create_delete_export_idempotency(self):
        '''
        Test idempotency of export create and delete commands.
        '''
        self._test_idempotency(self._create_default_export, ['nfs', 'export', 'create', 'cephfs',
                                                             self.fs_name, self.cluster_id,
                                                             self.pseudo_path])
        self._test_idempotency(self._delete_export, ['nfs', 'export', 'delete', self.cluster_id,
                                                     self.pseudo_path])

    def test_create_multiple_exports(self):
        '''
        Test creating multiple exports with different access type and path.
        '''
        #Export-1 with default values (access type = rw and path = '\')
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
        # Check if rados ganesha conf object is deleted
        self._check_export_obj_deleted(conf_obj=True)
        self._check_auth_ls()

    def test_exports_on_mgr_restart(self):
        '''
        Test export availability on restarting mgr.
        '''
        self._create_default_export()
        # unload and load module will restart the mgr
        self._unload_module("cephadm")
        self._load_module("cephadm")
        self._orch_cmd("set", "backend", "cephadm")
        # Checks if created export is listed
        self._test_list_export()
        self._delete_export()

    def test_export_create_with_non_existing_fsname(self):
        '''
        Test creating export with non-existing filesystem.
        '''
        try:
            fs_name = 'nfs-test'
            self._test_create_cluster()
            self._nfs_cmd('export', 'create', 'cephfs', fs_name, self.cluster_id, self.pseudo_path)
            self.fail(f"Export created with non-existing filesystem {fs_name}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.ENOENT:
                raise
        finally:
            self._test_delete_cluster()

    def test_export_create_with_non_existing_clusterid(self):
        '''
        Test creating cephfs export with non-existing nfs cluster.
        '''
        try:
            cluster_id = 'invalidtest'
            self._nfs_cmd('export', 'create', 'cephfs', self.fs_name, cluster_id, self.pseudo_path)
            self.fail(f"Export created with non-existing cluster id {cluster_id}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.ENOENT:
                raise
