# NOTE: these tests are not yet compatible with vstart_runner.py.
import errno
import json
import time
import logging
from io import BytesIO, StringIO

from tasks.mgr.mgr_test_case import MgrTestCase
from teuthology import contextutil
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

NFS_POOL_NAME = '.nfs'  # should match mgr_module.py

# TODO Add test for cluster update when ganesha can be deployed on multiple ports.
class TestNFS(MgrTestCase):
    def _cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(*args)

    def _nfs_cmd(self, *args):
        return self._cmd("nfs", *args)

    def _nfs_complete_cmd(self, cmd):
        return self.mgr_cluster.mon_manager.run_cluster_cmd(args=f"nfs {cmd}",
                                                            stdout=StringIO(),
                                                            stderr=StringIO(),
                                                            check_status=False)

    def _orch_cmd(self, *args):
        return self._cmd("orch", *args)

    def _sys_cmd(self, cmd):
        ret = self.ctx.cluster.run(args=cmd, check_status=False, stdout=BytesIO(), stderr=BytesIO())
        stdout = ret[0].stdout
        if stdout:
            return stdout.getvalue()

    def setUp(self):
        super(TestNFS, self).setUp()
        self._load_module('nfs')
        self.cluster_id = "test"
        self.export_type = "cephfs"
        self.pseudo_path = "/cephfs"
        self.path = "/"
        self.fs_name = "nfs-cephfs"
        self.expected_name = "nfs.test"
        self.sample_export = {
         "export_id": 1,
         "path": self.path,
         "cluster_id": self.cluster_id,
         "pseudo": self.pseudo_path,
         "access_type": "RW",
         "squash": "none",
         "security_label": True,
         "protocols": [
           4
         ],
         "transports": [
           "TCP"
         ],
         "fsal": {
           "name": "CEPH",
           "user_id": "nfs.test.1",
           "fs_name": self.fs_name,
         },
         "clients": []
        }

    def _check_nfs_server_status(self):
        res = self._sys_cmd(['sudo', 'systemctl', 'status', 'nfs-server'])
        if isinstance(res, bytes) and b'Active: active' in res:
            self._disable_nfs()

    def _disable_nfs(self):
        log.info("Disabling NFS")
        self._sys_cmd(['sudo', 'systemctl', 'disable', 'nfs-server', '--now'])

    def _fetch_nfs_daemons_details(self, enable_json=False):
        args = ('ps', f'--service_name={self.expected_name}')
        if enable_json:
            args = (*args, '--format=json')
        return self._orch_cmd(*args)

    def _check_nfs_cluster_event(self, expected_event):
        '''
        Check whether an event occured during the lifetime of the NFS service
        :param expected_event: event that was expected to occur
        '''
        event_occurred = False
        # Wait few seconds for NFS daemons' status to be updated
        with contextutil.safe_while(sleep=10, tries=18, _raise=False) as proceed:
            while not event_occurred and proceed():
                daemons_details = json.loads(
                    self._fetch_nfs_daemons_details(enable_json=True))
                log.info('daemons details %s', daemons_details)
                # 'events' key may not exist in the daemon description
                # after a mgr fail over and could take some time to appear
                # (it's populated on first daemon event)
                if 'events' not in daemons_details[0]:
                    continue
                for event in daemons_details[0]['events']:
                    log.info('daemon event %s', event)
                    if expected_event in event:
                        event_occurred = True
                        break
        return event_occurred

    def _check_nfs_cluster_status(self, expected_status, fail_msg):
        '''
        Check the current status of the NFS service
        :param expected_status: Status to be verified
        :param fail_msg: Message to be printed if test failed
        '''
        # Wait for a minute as ganesha daemon takes some time to be
        # deleted/created
        with contextutil.safe_while(sleep=6, tries=10, _raise=False) as proceed:
            while proceed():
                if expected_status in self._fetch_nfs_daemons_details():
                    return
        self.fail(fail_msg)

    def _check_auth_ls(self, export_id=1, check_in=False):
        '''
        Tests export user id creation or deletion.
        :param export_id: Denotes export number
        :param check_in: Check specified export id
        '''
        output = self._cmd('auth', 'ls')
        client_id = f'client.nfs.{self.cluster_id}'
        if check_in:
            self.assertIn(f'{client_id}.{export_id}', output)
        else:
            self.assertNotIn(f'{client_id}.{export_id}', output)

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
        with contextutil.safe_while(sleep=4, tries=10) as proceed:
            while proceed():
                try:
                    # Disable any running nfs ganesha daemon
                    self._check_nfs_server_status()
                    cluster_create = self._nfs_complete_cmd(
                        f'cluster create {self.cluster_id}')
                    if cluster_create.stderr and 'cluster already exists' \
                            in cluster_create.stderr.getvalue():
                        self._test_delete_cluster()
                        continue
                    # Check for expected status and daemon name
                    # (nfs.<cluster_id>)
                    self._check_nfs_cluster_status(
                        'running', 'NFS Ganesha cluster deployment failed')
                    break
                except (AssertionError, CommandFailedError) as e:
                    log.warning(f'{e}, retrying')

    def _test_delete_cluster(self):
        '''
        Test deletion of a single nfs cluster.
        '''
        self._nfs_cmd('cluster', 'rm', self.cluster_id)
        self._check_nfs_cluster_status('No daemons reported',
                                       'NFS Ganesha cluster could not be deleted')

    def _test_list_cluster(self, empty=False):
        '''
        Test listing of deployed nfs clusters. If nfs cluster is deployed then
        it checks for expected cluster id. Otherwise checks nothing is listed.
        :param empty: If true it denotes no cluster is deployed.
        '''
        nfs_output = self._nfs_cmd('cluster', 'ls')
        jdata = json.loads(nfs_output)
        if empty:
            self.assertEqual(len(jdata), 0)
        else:
            cluster_id = self.cluster_id
            self.assertEqual([cluster_id], jdata)

    def _create_export(self, export_id, create_fs=False, extra_cmd=None):
        '''
        Test creation of a single export.
        :param export_id: Denotes export number
        :param create_fs: If false filesytem exists. Otherwise create it.
        :param extra_cmd: List of extra arguments for creating export.
        '''
        if create_fs:
            self._cmd('fs', 'volume', 'create', self.fs_name)
            with contextutil.safe_while(sleep=5, tries=30) as proceed:
                while proceed():
                    output = self._cmd(
                        'orch', 'ls', '-f', 'json',
                        '--service-name', f'mds.{self.fs_name}'
                    )
                    j = json.loads(output)
                    if j[0]['status']['running']:
                        break
        export_cmd = ['nfs', 'export', 'create', 'cephfs',
                      '--fsname', self.fs_name, '--cluster-id', self.cluster_id]
        if isinstance(extra_cmd, list):
            export_cmd.extend(extra_cmd)
        else:
            export_cmd.extend(['--pseudo-path', self.pseudo_path])
        # Runs the nfs export create command
        self._cmd(*export_cmd)
        # Check if user id for export is created
        self._check_auth_ls(export_id, check_in=True)
        res = self._sys_cmd(['rados', '-p', NFS_POOL_NAME, '-N', self.cluster_id, 'get',
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
        self._nfs_cmd('export', 'rm', self.cluster_id, self.pseudo_path)
        self._check_auth_ls()

    def _test_list_export(self):
        '''
        Test listing of created exports.
        '''
        nfs_output = json.loads(self._nfs_cmd('export', 'ls', self.cluster_id))
        self.assertIn(self.pseudo_path, nfs_output)

    def _test_list_detailed(self, sub_vol_path):
        '''
        Test listing of created exports with detailed option.
        :param sub_vol_path: Denotes path of subvolume
        '''
        nfs_output = json.loads(self._nfs_cmd('export', 'ls', self.cluster_id, '--detailed'))
        # Export-1 with default values (access type = rw and path = '\')
        self.assertDictEqual(self.sample_export, nfs_output[0])
        # Export-2 with r only
        self.sample_export['export_id'] = 2
        self.sample_export['pseudo'] = self.pseudo_path + '1'
        self.sample_export['access_type'] = 'RO'
        self.sample_export['fsal']['user_id'] = f'{self.expected_name}.2'
        self.assertDictEqual(self.sample_export, nfs_output[1])
        # Export-3 for subvolume with r only
        self.sample_export['export_id'] = 3
        self.sample_export['path'] = sub_vol_path
        self.sample_export['pseudo'] = self.pseudo_path + '2'
        self.sample_export['fsal']['user_id'] = f'{self.expected_name}.3'
        self.assertDictEqual(self.sample_export, nfs_output[2])
        # Export-4 for subvolume
        self.sample_export['export_id'] = 4
        self.sample_export['pseudo'] = self.pseudo_path + '3'
        self.sample_export['access_type'] = 'RW'
        self.sample_export['fsal']['user_id'] = f'{self.expected_name}.4'
        self.assertDictEqual(self.sample_export, nfs_output[3])

    def _get_export(self):
        '''
        Returns export block in json format
        '''
        return json.loads(self._nfs_cmd('export', 'info', self.cluster_id, self.pseudo_path))

    def _test_get_export(self):
        '''
        Test fetching of created export.
        '''
        nfs_output = self._get_export()
        self.assertDictEqual(self.sample_export, nfs_output)

    def _check_export_obj_deleted(self, conf_obj=False):
        '''
        Test if export or config object are deleted successfully.
        :param conf_obj: It denotes config object needs to be checked
        '''
        rados_obj_ls = self._sys_cmd(['rados', '-p', NFS_POOL_NAME, '-N', self.cluster_id, 'ls'])

        if b'export-' in rados_obj_ls or (conf_obj and b'conf-nfs' in rados_obj_ls):
            self.fail("Delete export failed")

    def _get_port_ip_info(self):
        '''
        Return port and ip for a cluster
        '''
        #{'test': {'backend': [{'hostname': 'smithi068', 'ip': '172.21.15.68',
        #'port': 2049}]}}
        with contextutil.safe_while(sleep=5, tries=6) as proceed:
            while proceed():
                try:
                    info_output = json.loads(
                        self._nfs_cmd('cluster', 'info',
                                      self.cluster_id))['test']['backend'][0]
                    return info_output["port"], info_output["ip"]
                except (IndexError, CommandFailedError) as e:
                    if 'list index out of range' in str(e):
                        log.warning('no port and/or ip found, retrying')
                    else:
                        log.warning(f'{e}, retrying')

    def _test_mnt(self, pseudo_path, port, ip, check=True):
        '''
        Test mounting of created exports
        :param pseudo_path: It is the pseudo root name
        :param port: Port of deployed nfs cluster
        :param ip: IP of deployed nfs cluster
        :param check: It denotes if i/o testing needs to be done
        '''
        tries = 3
        while True:
            try:
                self.ctx.cluster.run(
                    args=['sudo', 'mount', '-t', 'nfs', '-o', f'port={port}',
                          f'{ip}:{pseudo_path}', '/mnt'])
                break
            except CommandFailedError as e:
                if tries:
                    tries -= 1
                    time.sleep(2)
                    continue
                # Check if mount failed only when non existing pseudo path is passed
                if not check and e.exitstatus == 32:
                    return
                raise

        self.ctx.cluster.run(args=['sudo', 'chmod', '1777', '/mnt'])

        try:
            self.ctx.cluster.run(args=['touch', '/mnt/test'])
            out_mnt = self._sys_cmd(['ls', '/mnt'])
            self.assertEqual(out_mnt,  b'test\n')
        finally:
            self.ctx.cluster.run(args=['sudo', 'umount', '/mnt'])

    def _write_to_read_only_export(self, pseudo_path, port, ip):
        '''
        Check if write to read only export fails
        '''
        try:
            self._test_mnt(pseudo_path, port, ip)
        except CommandFailedError as e:
            # Write to cephfs export should fail for test to pass
            self.assertEqual(
                e.exitstatus, errno.EPERM,
                'invalid error code on trying to write to read-only export')
        else:
            self.fail('expected write to a read-only export to fail')

    def _create_cluster_with_fs(self, fs_name, mnt_pt=None):
        """
        create a cluster along with fs and mount it to the path supplied
        :param fs_name: name of CephFS volume to be created
        :param mnt_pt: mount fs to the path
        """
        self._test_create_cluster()
        self._cmd('fs', 'volume', 'create', fs_name)
        with contextutil.safe_while(sleep=5, tries=30) as proceed:
            while proceed():
                output = self._cmd(
                    'orch', 'ls', '-f', 'json',
                    '--service-name', f'mds.{fs_name}'
                )
                j = json.loads(output)
                if j[0]['status']['running']:
                    break
        if mnt_pt:
            with contextutil.safe_while(sleep=3, tries=3) as proceed:
                while proceed():
                    try:
                        self.ctx.cluster.run(args=['sudo', 'ceph-fuse', mnt_pt])
                        break
                    except CommandFailedError as e:
                        log.warning(f'{e}, retrying')
            self.ctx.cluster.run(args=['sudo', 'chmod', '1777', mnt_pt])

    def _delete_cluster_with_fs(self, fs_name, mnt_pt=None, mode=None):
        """
        delete cluster along with fs and unmount it from the path supplied
        :param fs_name: name of CephFS volume to be deleted
        :param mnt_pt: unmount fs from the path
        :param mode: revert to this mode
        """
        if mnt_pt:
            self.ctx.cluster.run(args=['sudo', 'umount', mnt_pt])
            if mode:
                if isinstance(mode, bytes):
                    mode = mode.decode().strip()
                self.ctx.cluster.run(args=['sudo', 'chmod', mode, mnt_pt])
        self._cmd('fs', 'volume', 'rm', fs_name, '--yes-i-really-mean-it')
        self._test_delete_cluster()

    def _nfs_export_apply(self, cluster, exports, raise_on_error=False):
        return self.ctx.cluster.run(args=['ceph', 'nfs', 'export', 'apply',
                                          cluster, '-i', '-'],
                                    check_status=raise_on_error,
                                    stdin=json.dumps(exports),
                                    stdout=StringIO(), stderr=StringIO())

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
        self._test_idempotency(self._test_create_cluster, ['nfs', 'cluster', 'create', self.cluster_id])
        self._test_idempotency(self._test_delete_cluster, ['nfs', 'cluster', 'rm', self.cluster_id])

    def test_create_cluster_with_invalid_cluster_id(self):
        '''
        Test nfs cluster deployment failure with invalid cluster id.
        '''
        try:
            invalid_cluster_id = '/cluster_test'  # Only [A-Za-z0-9-_.] chars are valid
            self._nfs_cmd('cluster', 'create', invalid_cluster_id)
            self.fail(f"Cluster successfully created with invalid cluster id {invalid_cluster_id}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.EINVAL:
                raise

    def test_create_and_delete_export(self):
        '''
        Test successful creation and deletion of the cephfs export.
        '''
        self._create_default_export()
        self._test_get_export()
        port, ip = self._get_port_ip_info()
        self._test_mnt(self.pseudo_path, port, ip)
        self._delete_export()
        # Check if rados export object is deleted
        self._check_export_obj_deleted()
        self._test_mnt(self.pseudo_path, port, ip, False)
        self._test_delete_cluster()

    def test_create_delete_export_idempotency(self):
        '''
        Test idempotency of export create and delete commands.
        '''
        self._test_idempotency(self._create_default_export, [
            'nfs', 'export', 'create', 'cephfs',
            '--fsname', self.fs_name, '--cluster-id', self.cluster_id,
            '--pseudo-path', self.pseudo_path])
        self._test_idempotency(self._delete_export, ['nfs', 'export', 'rm', self.cluster_id,
                                                     self.pseudo_path])
        self._test_delete_cluster()

    def test_create_multiple_exports(self):
        '''
        Test creating multiple exports with different access type and path.
        '''
        # Export-1 with default values (access type = rw and path = '\')
        self._create_default_export()
        # Export-2 with r only
        self._create_export(export_id='2',
                            extra_cmd=['--pseudo-path', self.pseudo_path+'1', '--readonly'])
        # Export-3 for subvolume with r only
        self._cmd('fs', 'subvolume', 'create', self.fs_name, 'sub_vol')
        fs_path = self._cmd('fs', 'subvolume', 'getpath', self.fs_name, 'sub_vol').strip()
        self._create_export(export_id='3',
                            extra_cmd=['--pseudo-path', self.pseudo_path+'2', '--readonly',
                                       '--path', fs_path])
        # Export-4 for subvolume
        self._create_export(export_id='4',
                            extra_cmd=['--pseudo-path', self.pseudo_path+'3',
                                       '--path', fs_path])
        # Check if exports gets listed
        self._test_list_detailed(fs_path)
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
        # Check if ganesha daemon is running
        self._check_nfs_cluster_status('running', 'Failed to redeploy NFS Ganesha cluster')
        # Checks if created export is listed
        self._test_list_export()
        port, ip = self._get_port_ip_info()
        self._test_mnt(self.pseudo_path, port, ip)
        self._delete_export()
        self._test_delete_cluster()

    def test_export_create_with_non_existing_fsname(self):
        '''
        Test creating export with non-existing filesystem.
        '''
        try:
            fs_name = 'nfs-test'
            self._test_create_cluster()
            self._nfs_cmd('export', 'create', 'cephfs',
                          '--fsname', fs_name, '--cluster-id', self.cluster_id,
                          '--pseudo-path', self.pseudo_path)
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
            self._nfs_cmd('export', 'create', 'cephfs', '--fsname', self.fs_name,
                          '--cluster-id', cluster_id, '--pseudo-path', self.pseudo_path)
            self.fail(f"Export created with non-existing cluster id {cluster_id}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.ENOENT:
                raise

    def test_export_create_with_relative_pseudo_path_and_root_directory(self):
        '''
        Test creating cephfs export with relative or '/' pseudo path.
        '''
        def check_pseudo_path(pseudo_path):
            try:
                self._nfs_cmd('export', 'create', 'cephfs', '--fsname', self.fs_name,
                              '--cluster-id', self.cluster_id,
                              '--pseudo-path', pseudo_path)
                self.fail(f"Export created for {pseudo_path}")
            except CommandFailedError as e:
                # Command should fail for test to pass
                if e.exitstatus != errno.EINVAL:
                    raise

        self._test_create_cluster()
        self._cmd('fs', 'volume', 'create', self.fs_name)
        check_pseudo_path('invalidpath')
        check_pseudo_path('/')
        check_pseudo_path('//')
        self._cmd('fs', 'volume', 'rm', self.fs_name, '--yes-i-really-mean-it')
        self._test_delete_cluster()

    def test_write_to_read_only_export(self):
        '''
        Test write to readonly export.
        '''
        self._test_create_cluster()
        self._create_export(export_id='1', create_fs=True,
                            extra_cmd=['--pseudo-path', self.pseudo_path, '--readonly'])
        port, ip = self._get_port_ip_info()
        self._check_nfs_cluster_status('running', 'NFS Ganesha cluster restart failed')
        self._write_to_read_only_export(self.pseudo_path, port, ip)
        self._test_delete_cluster()

    def test_cluster_info(self):
        '''
        Test cluster info outputs correct ip and hostname
        '''
        self._test_create_cluster()
        info_output = json.loads(self._nfs_cmd('cluster', 'info', self.cluster_id))
        print(f'info {info_output}')
        info_ip = info_output[self.cluster_id].get('backend', [])[0].pop("ip")
        host_details = {
            self.cluster_id: {
                'backend': [
                    {
                        "hostname": self._sys_cmd(['hostname']).decode("utf-8").strip(),
                        "port": 2049
                    }
                ],
                "virtual_ip": None,
            }
        }
        host_ip = self._sys_cmd(['hostname', '-I']).decode("utf-8").split()
        print(f'host_ip is {host_ip}, info_ip is {info_ip}')
        self.assertDictEqual(info_output, host_details)
        self.assertTrue(info_ip in host_ip)
        self._test_delete_cluster()

    def test_cluster_set_reset_user_config(self):
        '''
        Test cluster is created using user config and reverts back to default
        config on reset.
        '''
        self._test_create_cluster()

        pool = NFS_POOL_NAME
        user_id = 'test'
        fs_name = 'user_test_fs'
        pseudo_path = '/ceph'
        self._cmd('fs', 'volume', 'create', fs_name)
        time.sleep(20)
        key = self._cmd('auth', 'get-or-create-key', f'client.{user_id}', 'mon',
            'allow r', 'osd',
            f'allow rw pool={pool} namespace={self.cluster_id}, allow rw tag cephfs data={fs_name}',
            'mds', f'allow rw path={self.path}').strip()
        config = f""" LOG {{
        Default_log_level = FULL_DEBUG;
        }}

        EXPORT {{
	        Export_Id = 100;
	        Transports = TCP;
	        Path = /;
	        Pseudo = {pseudo_path};
	        Protocols = 4;
	        Access_Type = RW;
	        Attr_Expiration_Time = 0;
	        Squash = None;
	        FSAL {{
	              Name = CEPH;
                      Filesystem = {fs_name};
                      User_Id = {user_id};
                      Secret_Access_Key = '{key}';
	        }}
        }}"""
        port, ip = self._get_port_ip_info()
        self.ctx.cluster.run(args=['ceph', 'nfs', 'cluster', 'config',
            'set', self.cluster_id, '-i', '-'], stdin=config)
        time.sleep(30)
        res = self._sys_cmd(['rados', '-p', pool, '-N', self.cluster_id, 'get',
                             f'userconf-nfs.{user_id}', '-'])
        self.assertEqual(config, res.decode('utf-8'))
        self._test_mnt(pseudo_path, port, ip)
        self._nfs_cmd('cluster', 'config', 'reset', self.cluster_id)
        rados_obj_ls = self._sys_cmd(['rados', '-p', NFS_POOL_NAME, '-N', self.cluster_id, 'ls'])
        if b'conf-nfs' not in rados_obj_ls and b'userconf-nfs' in rados_obj_ls:
            self.fail("User config not deleted")
        time.sleep(30)
        self._test_mnt(pseudo_path, port, ip, False)
        self._cmd('fs', 'volume', 'rm', fs_name, '--yes-i-really-mean-it')
        self._test_delete_cluster()

    def test_cluster_set_user_config_with_non_existing_clusterid(self):
        '''
        Test setting user config for non-existing nfs cluster.
        '''
        cluster_id = 'invalidtest'
        with contextutil.safe_while(sleep=3, tries=3) as proceed:
            while proceed():
                try:
                    self.ctx.cluster.run(args=['ceph', 'nfs', 'cluster',
                                               'config', 'set', cluster_id,
                                               '-i', '-'], stdin='testing')
                    self.fail(f"User config set for non-existing cluster"
                              f"{cluster_id}")
                except CommandFailedError as e:
                    # Command should fail for test to pass
                    if e.exitstatus == errno.ENOENT:
                        break
                    log.warning('exitstatus != ENOENT, retrying')

    def test_cluster_reset_user_config_with_non_existing_clusterid(self):
        '''
        Test resetting user config for non-existing nfs cluster.
        '''
        try:
            cluster_id = 'invalidtest'
            self._nfs_cmd('cluster', 'config', 'reset', cluster_id)
            self.fail(f"User config reset for non-existing cluster {cluster_id}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.ENOENT:
                raise

    def test_create_export_via_apply(self):
        '''
        Test creation of export via apply
        '''
        self._test_create_cluster()
        self.ctx.cluster.run(args=['ceph', 'nfs', 'export', 'apply',
                                   self.cluster_id, '-i', '-'],
                             stdin=json.dumps({
                                 "path": "/",
                                 "pseudo": "/cephfs",
                                 "squash": "none",
                                 "access_type": "rw",
                                 "protocols": [4],
                                 "fsal": {
                                     "name": "CEPH",
                                     "fs_name": self.fs_name
                                 }
                             }))
        port, ip = self._get_port_ip_info()
        self._test_mnt(self.pseudo_path, port, ip)
        self._check_nfs_cluster_status(
            'running', 'NFS Ganesha cluster not running after new export was applied')
        self._test_delete_cluster()

    def test_update_export(self):
        '''
        Test update of export's pseudo path and access type from rw to ro
        '''
        self._create_default_export()
        port, ip = self._get_port_ip_info()
        self._test_mnt(self.pseudo_path, port, ip)
        export_block = self._get_export()
        new_pseudo_path = '/testing'
        export_block['pseudo'] = new_pseudo_path
        export_block['access_type'] = 'RO'
        self.ctx.cluster.run(args=['ceph', 'nfs', 'export', 'apply',
                                   self.cluster_id, '-i', '-'],
                             stdin=json.dumps(export_block))
        if not self._check_nfs_cluster_event('restart'):
            self.fail("updating export's pseudo path should trigger restart of NFS service")
        self._check_nfs_cluster_status('running', 'NFS Ganesha cluster not running after restart')
        self._write_to_read_only_export(new_pseudo_path, port, ip)
        self._test_delete_cluster()

    def test_update_export_ro_to_rw(self):
        '''
        Test update of export's access level from ro to rw
        '''
        self._test_create_cluster()
        self._create_export(
            export_id='1', create_fs=True,
            extra_cmd=['--pseudo-path', self.pseudo_path, '--readonly'])
        port, ip = self._get_port_ip_info()
        self._write_to_read_only_export(self.pseudo_path, port, ip)
        export_block = self._get_export()
        export_block['access_type'] = 'RW'
        self.ctx.cluster.run(
            args=['ceph', 'nfs', 'export', 'apply', self.cluster_id, '-i', '-'],
            stdin=json.dumps(export_block))
        if self._check_nfs_cluster_event('restart'):
            self.fail("update of export's access type should not trigger NFS service restart")
        self._test_mnt(self.pseudo_path, port, ip)
        self._test_delete_cluster()

    def test_update_export_with_invalid_values(self):
        '''
        Test update of export with invalid values
        '''
        self._create_default_export()
        export_block = self._get_export()

        def update_with_invalid_values(key, value, fsal=False):
            export_block_new = dict(export_block)
            if fsal:
                export_block_new['fsal'] = dict(export_block['fsal'])
                export_block_new['fsal'][key] = value
            else:
                export_block_new[key] = value
            try:
                self.ctx.cluster.run(args=['ceph', 'nfs', 'export', 'apply',
                                           self.cluster_id, '-i', '-'],
                        stdin=json.dumps(export_block_new))
            except CommandFailedError:
                pass

        update_with_invalid_values('export_id', 9)
        update_with_invalid_values('cluster_id', 'testing_new')
        update_with_invalid_values('pseudo', 'test_relpath')
        update_with_invalid_values('access_type', 'W')
        update_with_invalid_values('squash', 'no_squash')
        update_with_invalid_values('security_label', 'invalid')
        update_with_invalid_values('protocols', [2])
        update_with_invalid_values('transports', ['UD'])
        update_with_invalid_values('name', 'RGW', True)
        update_with_invalid_values('user_id', 'testing_export', True)
        update_with_invalid_values('fs_name', 'b', True)
        self._test_delete_cluster()

    def test_cmds_without_reqd_args(self):
        '''
        Test that cmd fails on not passing required arguments
        '''
        def exec_cmd_invalid(*cmd):
            try:
                self._nfs_cmd(*cmd)
                self.fail(f"nfs {cmd} command executed successfully without required arguments")
            except CommandFailedError as e:
                # Command should fail for test to pass
                if e.exitstatus != errno.EINVAL:
                    raise

        exec_cmd_invalid('cluster', 'create')
        exec_cmd_invalid('cluster', 'delete')
        exec_cmd_invalid('cluster', 'config', 'set')
        exec_cmd_invalid('cluster', 'config', 'reset')
        exec_cmd_invalid('export', 'create', 'cephfs')
        exec_cmd_invalid('export', 'create', 'cephfs', 'clusterid')
        exec_cmd_invalid('export', 'create', 'cephfs', 'clusterid', 'a_fs')
        exec_cmd_invalid('export', 'ls')
        exec_cmd_invalid('export', 'delete')
        exec_cmd_invalid('export', 'delete', 'clusterid')
        exec_cmd_invalid('export', 'info')
        exec_cmd_invalid('export', 'info', 'clusterid')
        exec_cmd_invalid('export', 'apply')

    def test_non_existent_cluster(self):
        """
        Test that cluster info doesn't throw junk data for non-existent cluster
        """
        cluster_ls = self._nfs_cmd('cluster', 'ls')
        self.assertNotIn('foo', cluster_ls, 'cluster foo exists')
        try:
            self._nfs_cmd('cluster', 'info', 'foo')
            self.fail("nfs cluster info foo returned successfully for non-existent cluster")
        except CommandFailedError as e:
            if e.exitstatus != errno.ENOENT:
                raise

    def test_nfs_export_with_invalid_path(self):
        """
        Test that nfs exports can't be created with invalid path
        """
        mnt_pt = '/mnt'
        preserve_mode = self._sys_cmd(['stat', '-c', '%a', mnt_pt])
        self._create_cluster_with_fs(self.fs_name, mnt_pt)
        try:
            self._create_export(export_id='123',
                                extra_cmd=['--pseudo-path', self.pseudo_path,
                                           '--path', '/non_existent_dir'])
        except CommandFailedError as e:
            if e.exitstatus != errno.ENOENT:
                raise
        self._delete_cluster_with_fs(self.fs_name, mnt_pt, preserve_mode)

    def test_nfs_export_creation_at_filepath(self):
        """
        Test that nfs exports can't be created at a filepath
        """
        mnt_pt = '/mnt'
        preserve_mode = self._sys_cmd(['stat', '-c', '%a', mnt_pt])
        self._create_cluster_with_fs(self.fs_name, mnt_pt)
        self.ctx.cluster.run(args=['touch', f'{mnt_pt}/testfile'])
        try:
            self._create_export(export_id='123', extra_cmd=['--pseudo-path',
                                                            self.pseudo_path,
                                                            '--path',
                                                            '/testfile'])
        except CommandFailedError as e:
            if e.exitstatus != errno.ENOTDIR:
                raise
        self.ctx.cluster.run(args=['rm', '-rf', '/mnt/testfile'])
        self._delete_cluster_with_fs(self.fs_name, mnt_pt, preserve_mode)

    def test_nfs_export_creation_at_symlink(self):
        """
        Test that nfs exports can't be created at a symlink path
        """
        mnt_pt = '/mnt'
        preserve_mode = self._sys_cmd(['stat', '-c', '%a', mnt_pt])
        self._create_cluster_with_fs(self.fs_name, mnt_pt)
        self.ctx.cluster.run(args=['mkdir', f'{mnt_pt}/testdir'])
        self.ctx.cluster.run(args=['ln', '-s', f'{mnt_pt}/testdir',
                                   f'{mnt_pt}/testdir_symlink'])
        try:
            self._create_export(export_id='123',
                                extra_cmd=['--pseudo-path',
                                           self.pseudo_path,
                                           '--path',
                                           '/testdir_symlink'])
        except CommandFailedError as e:
            if e.exitstatus != errno.ENOTDIR:
                raise
        self.ctx.cluster.run(args=['rm', '-rf', f'{mnt_pt}/*'])
        self._delete_cluster_with_fs(self.fs_name, mnt_pt, preserve_mode)

    def test_nfs_export_apply_multiple_exports(self):
        """
        Test multiple export creation/update with multiple
        export blocks provided in the json/conf file using:
        ceph nfs export apply <nfs_cluster> -i <{conf/json}_file>, and check
        1) if there are multiple failure:
        -> Return the EIO and error status to CLI (along with JSON output
           containing status of every export).
        2) if there is single failure:
        -> Return the respective errno and error status to CLI (along with
           JSON output containing status of every export).
        """

        mnt_pt = self._sys_cmd(['mktemp', '-d']).decode().strip()
        self._create_cluster_with_fs(self.fs_name, mnt_pt)
        try:
            self.ctx.cluster.run(args=['mkdir', f'{mnt_pt}/testdir1'])
            self.ctx.cluster.run(args=['mkdir', f'{mnt_pt}/testdir2'])
            self.ctx.cluster.run(args=['mkdir', f'{mnt_pt}/testdir3'])
            self._create_export(export_id='1',
                                extra_cmd=['--pseudo-path', self.pseudo_path,
                                           '--path', '/testdir1'])
            self._create_export(export_id='2',
                                extra_cmd=['--pseudo-path',
                                           self.pseudo_path+'2',
                                           '--path', '/testdir2'])
            exports = [
                {
                    "export_id": 11,  # export_id change not allowed
                    "path": "/testdir1",
                    "pseudo": self.pseudo_path,
                    "squash": "none",
                    "access_type": "rw",
                    "protocols": [4],
                    "fsal": {
                        "name": "CEPH",
                        "user_id": "nfs.test.1",
                        "fs_name": self.fs_name
                    }
                },
                {
                    "export_id": 2,
                    "path": "/testdir2",
                    "pseudo": self.pseudo_path+'2',
                    "squash": "none",
                    "access_type": "rw",
                    "protocols": [4],
                    "fsal": {
                        "name": "CEPH",
                        "user_id": "nfs.test.2",
                        "fs_name": "invalid_fs_name"  # invalid fs
                    }
                },
                {   # no error, export creation should succeed
                    "export_id": 3,
                    "path": "/testdir3",
                    "pseudo": self.pseudo_path+'3',
                    "squash": "none",
                    "access_type": "rw",
                    "protocols": [4],
                    "fsal": {
                        "name": "CEPH",
                        "user_id": "nfs.test.3",
                        "fs_name": self.fs_name
                    }
                }
            ]

            # multiple failures
            ret = self._nfs_export_apply(self.cluster_id, exports)
            self.assertEqual(ret[0].returncode, errno.EIO)
            self.assertIn("2 export blocks (at index 1, 2) failed to be "
                          "created/updated", ret[0].stderr.getvalue())

            # single failure
            exports[1]["fsal"]["fs_name"] = self.fs_name  # correct the fs
            ret = self._nfs_export_apply(self.cluster_id, exports)
            self.assertEqual(ret[0].returncode, errno.EINVAL)
            self.assertIn("Export ID changed, Cannot update export for "
                          "export block at index 1", ret[0].stderr.getvalue())
        finally:
            self._delete_cluster_with_fs(self.fs_name, mnt_pt)
            self.ctx.cluster.run(args=['rm', '-rf', f'{mnt_pt}'])

    def test_nfs_export_apply_single_export(self):
        """
        Test that when single export creation/update fails with multiple
        export blocks provided in the json/conf file using:
        ceph nfs export apply <nfs_cluster> -i <{conf/json}_file>, it
        returns the respective errno and error status to CLI (along with
        JSON output containing status of every export).
        """

        mnt_pt = self._sys_cmd(['mktemp', '-d']).decode().strip()
        self._create_cluster_with_fs(self.fs_name, mnt_pt)
        try:
            self.ctx.cluster.run(args=['mkdir', f'{mnt_pt}/testdir1'])
            self._create_export(export_id='1',
                                extra_cmd=['--pseudo-path', self.pseudo_path,
                                           '--path', '/testdir1'])
            export = {
                "export_id": 1,
                "path": "/testdir1",
                "pseudo": self.pseudo_path,
                "squash": "none",
                "access_type": "rw",
                "protocols": [4],
                "fsal": {
                    "name": "CEPH",
                    "user_id": "nfs.test.1",
                    "fs_name": "invalid_fs_name"  # invalid fs
                }
            }
            ret = self._nfs_export_apply(self.cluster_id, export)
            self.assertEqual(ret[0].returncode, errno.ENOENT)
            self.assertIn("filesystem invalid_fs_name not found for "
                          "export block at index 1", ret[0].stderr.getvalue())
        finally:
            self._delete_cluster_with_fs(self.fs_name, mnt_pt)
            self.ctx.cluster.run(args=['rm', '-rf', f'{mnt_pt}'])

    def test_nfs_export_apply_json_output_states(self):
        """
        If export creation/update is done using:
        ceph nfs export apply <nfs_cluster> -i <{conf/json}_file> then the
        "status" field in the json output maybe added, updated, error or
        warning. Test different scenarios to make sure these states are
        in the json output as expected.
        """

        mnt_pt = self._sys_cmd(['mktemp', '-d']).decode().strip()
        self._create_cluster_with_fs(self.fs_name, mnt_pt)
        try:
            self.ctx.cluster.run(args=['mkdir', f'{mnt_pt}/testdir1'])
            self.ctx.cluster.run(args=['mkdir', f'{mnt_pt}/testdir2'])
            self.ctx.cluster.run(args=['mkdir', f'{mnt_pt}/testdir3'])
            self._create_export(export_id='1',
                                extra_cmd=['--pseudo-path', self.pseudo_path,
                                           '--path', '/testdir1'])
            exports = [
                {   # change pseudo, state should be "updated"
                    "export_id": 1,
                    "path": "/testdir1",
                    "pseudo": self.pseudo_path+'1',
                    "squash": "none",
                    "access_type": "rw",
                    "protocols": [4],
                    "fsal": {
                        "name": "CEPH",
                        "user_id": "nfs.test.1",
                        "fs_name": self.fs_name
                    }
                },
                {   # a new export, state should be "added"
                    "export_id": 2,
                    "path": "/testdir2",
                    "pseudo": self.pseudo_path+'2',
                    "squash": "none",
                    "access_type": "rw",
                    "protocols": [4],
                    "fsal": {
                        "name": "CEPH",
                        "user_id": "nfs.test.2",
                        "fs_name": self.fs_name
                    }
                },
                {   # error in export block, state should be "error" since the
                    # fs_name is invalid
                    "export_id": 3,
                    "path": "/testdir3",
                    "pseudo": self.pseudo_path+'3',
                    "squash": "none",
                    "access_type": "RW",
                    "protocols": [4],
                    "fsal": {
                        "name": "CEPH",
                        "user_id": "nfs.test.3",
                        "fs_name": "invalid_fs_name"
                    }
                }
            ]
            ret = self._nfs_export_apply(self.cluster_id, exports)
            json_output = json.loads(ret[0].stdout.getvalue().strip())
            self.assertEqual(len(json_output), 3)
            self.assertEqual(json_output[0]["state"], "updated")
            self.assertEqual(json_output[1]["state"], "added")
            self.assertEqual(json_output[2]["state"], "error")
        finally:
            self._delete_cluster_with_fs(self.fs_name, mnt_pt)
            self.ctx.cluster.run(args=['rm', '-rf', f'{mnt_pt}'])
