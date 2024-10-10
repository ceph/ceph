import json
import errno
import logging
import random
import time
import uuid

from shlex import split as shlex_split

from io import StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

class TestPartitioner(CephFSTestCase):
    MDSS_REQUIRED = 3
    CLIENTS_REQUIRED = 2
    TEST_VOLUME_PREFIX = "volume"
    TEST_SUBVOLUME_PREFIX="subvolume"

    def setUp(self):
        super(TestPartitioner, self).setUp()
        self.fs_name = self.fs.name
        self.fs_id = self.fs.id
        self.subvolume_start = random.randint(1, (1<<20))
        self.disable_partitioner_module()
        self.enable_partitioner_module()
        self.subvolume_path = []
        self.subvolume_name = []

        for mount in self.mounts:
            if mount.is_mounted():
                mount.umount_wait()

        for mount in self.mounts:
            log.error(dir(mount))
            log.error(mount)
            log.info('create subvolume')
            subvolume, subvol_path = self.create_subvolume()
            self.subvolume_name.append(subvolume)
            self.subvolume_path.append(subvol_path)
            log.info('mount')
            mount.mount_wait(cephfs_mntpt=subvol_path)

        log.info('set max_mds to 3')
        self.fs.set_max_mds(3)
        self.wait_until_equal(lambda: len(self.fs.get_active_names()), 3, 30,
                              reject_fn=lambda v: v > 3 or v < 1)

    def tearDown(self):
        for mount in self.mounts:
            if mount.is_mounted():
                log.info(f'umounting {mount.client_id}')
                mount.umount_wait()

        for subvolume in self.subvolume_name:
            log.info(f'deleting subvolume {subvolume}')
            self.delete_subvolume(subvolume)

        self.disable_partitioner_module()
        super(TestPartitioner, self).tearDown()

    def _generate_random_volume_name(self, count=1):
        n = self.volume_start
        volumes = [f"{TestPartitioner.TEST_VOLUME_PREFIX}_{i:016}" for i in range(n, n+count)]
        self.volume_start += count
        return volumes[0] if count == 1 else volumes

    def _generate_random_subvolume_name(self, count=1):
        n = self.subvolume_start
        subvolumes = [f"{TestPartitioner.TEST_SUBVOLUME_PREFIX}_{i:016}" for i in range(n, n+count)]
        self.subvolume_start += count
        return subvolumes[0] if count == 1 else subvolumes

    def _fs_cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", *args)

    def create_subvolume(self):
        subvolume = self._generate_random_subvolume_name()

        try:
            self._fs_cmd("subvolume", "create", self.fs_name, subvolume)
        except CommandFailedError as ce:
            raise RuntimeError(f'subvolume {subvolume} creation failed error {ce.exitstatus}')

        subvolpath = self._fs_cmd("subvolume", "getpath", self.fs_name, subvolume)
        subvolpath = subvolpath.rstrip()
        return subvolume, subvolpath

    def delete_subvolume(self, subvolume):
        try:
            self._fs_cmd("subvolume", "rm", self.fs_name, subvolume)
        except CommandFailedError as ce:
            raise RuntimeError(f'subvolume {subvolume} creation failed error {ce.exitstatus}')

    def run_cluster_cmd(self, cmd):
        if isinstance(cmd, str):
            cmd = shlex_split(cmd)

        kwargs = {
                   'args' : cmd,
                   'stdout': StringIO(), 
                   'stderr': StringIO()
                 }
        res = self.fs.mon_manager.run_cluster_cmd(**kwargs)
        return (res.exitstatus, res.stdout.getvalue().strip(), res.stderr.getvalue().strip())

    def enable_partitioner_module(self):
        cmd = "mgr module enable mds_partitioner"
        self.run_cluster_cmd(cmd)
        cmd = "mgr module enable stats"
        self.run_cluster_cmd(cmd)

    def disable_partitioner_module(self):
        cmd = "mgr module disable mds_partitioner"
        self.run_cluster_cmd(cmd)
        cmd ="mgr module disable stats"
        self.run_cluster_cmd(cmd)

    def show_workload_policy(self, fs_name, policy_name, output_format='json'):
        cmd = f"mds_partitioner workload_policy show {fs_name} {policy_name} {output_format}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        if output_format == 'json':
            dict_data = json.loads(retval[1])
            self.assertEquals(dict_data['policy_name'], policy_name)
            return dict_data
        else:
            with self.assertRaises(json.decoder.JSONDecodeError):
                dict_data = json.loads(retval[1])
            self.assertEquals(policy_name in retval[1], True)
            return retval

    def setup_workload_policy(self, fs_name, policy_name, output_format='json'):
        cmd = f"mds_partitioner workload_policy create {fs_name} {policy_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        self.show_workload_policy(fs_name, policy_name, output_format)

        cmd = f"mds_partitioner workload_policy list {fs_name} {output_format}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)
        if output_format == 'json':
            dict_data = json.loads(retval[1])
            self.assertTrue(policy_name in dict_data)
            self.assertEquals(dict_data[policy_name]['policy_name'], policy_name)
        else:
            with self.assertRaises(json.decoder.JSONDecodeError):
                dict_data = json.loads(retval[1])
            self.assertEquals(policy_name in retval[1], True)

        cmd = f"mds_partitioner workload_policy activate {fs_name} {policy_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        cmd = f"mds_partitioner workload_policy set max_mds {fs_name} {policy_name} -1"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        for subvol_path in self.subvolume_path:
            cmd = f"mds_partitioner workload_policy dir_path add {fs_name} {policy_name} {subvol_path}"
            retval = self.run_cluster_cmd(cmd)
            self.assertEquals(retval[0], 0)

        cmd = f"mds_partitioner workload_policy dir_path list {fs_name} {policy_name} {output_format}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        if output_format == 'json':
            dict_data = json.loads(retval[1])
            for subvol_path in self.subvolume_path:
                self.assertEquals(subvol_path in dict_data, True)
        else:
            with self.assertRaises(json.decoder.JSONDecodeError):
                dict_data = json.loads(retval[1])
            for subvol_path in self.subvolume_path:
                self.assertEquals(subvol_path in retval[1], True)

        for subvol_path in self.subvolume_path:
            cmd = f"mds_partitioner workload_policy dir_path rm {fs_name} {policy_name} {subvol_path}"
            retval = self.run_cluster_cmd(cmd)

        for subvol_path in self.subvolume_path:
            cmd = f"mds_partitioner workload_policy dir_path add {fs_name} {policy_name} {subvol_path}"
            retval = self.run_cluster_cmd(cmd)

    def remove_workload_policy_at_startup(self, fs_name):
        cmd = f"mds_partitioner workload_policy remove-all {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)
        dict_data = json.loads(retval[1])
        self.assertTrue('removed_policies' in dict_data)
        self.assertTrue('unremoved_policies' in dict_data)

    def cleanup_workload_policy(self, fs_name, policy_name):
        cmd = f"mds_partitioner workload_policy deactivate {fs_name} {policy_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        cmd = f"mds_partitioner workload_policy remove {fs_name} {policy_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        cmd = f"mds_partitioner workload_policy remove-all {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)
        dict_data = json.loads(retval[1])
        self.assertTrue('removed_policies' in dict_data)
        self.assertTrue('unremoved_policies' in dict_data)

    def show_analyzer_status(self, fs_name, policy_name,  output_format='json'):
        cmd = f"mds_partitioner analyzer status {fs_name} {output_format}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        if output_format == 'json':
            dict_data = json.loads(retval[1])
            self.assertTrue(dict_data['policy_name'] == policy_name)
        else:
            with self.assertRaises(json.decoder.JSONDecodeError):
                dict_data = json.loads(retval[1])
            self.assertEquals('policy_name' in retval[1], True)

    def run_analyzer(self, fs_name, policy_name, create_file=True, timeout=0):
        cmd = f"mds_partitioner analyzer start {fs_name} {policy_name} 3600 10"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        log.info('analyzer status')
        self.show_analyzer_status(fs_name, policy_name)

        log.info('analyzer stop')
        cmd = f"mds_partitioner analyzer stop {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        log.info('analyzer start')
        cmd = f"mds_partitioner analyzer start {fs_name} {policy_name} 3600 10"
        retval = self.run_cluster_cmd(cmd)
        log.info(f'retval {retval[0]}')
        self.assertEquals(retval[0], 0)

        elapsed_time = 0
        sleep_time = 5
        timedout = False
        move_count = 0
        while True:
            if create_file:
                for mount in self.mounts:
                    for i in range(20 * random.randint(1, 10)):
                        mount.create_file(filename=str(uuid.uuid4()))

            dict_data = self.show_workload_policy(fs_name, policy_name)

            log.info(dict_data)
            move_count = 0
            for k, subtree in dict_data['workload_table'].items():
                if subtree['state'] == 'NEED_TO_MOVE':
                    move_count += 1

            if dict_data['workload_table_ready'] == 'True' and \
                    move_count > 0:
                log.info('workload table ready')
                break
            else:
                log.info('workload table NOT ready')

            time.sleep(sleep_time)
            elapsed_time += sleep_time
            if timeout and elapsed_time > timeout:
                timedout = True
                break

        cmd = f"mds_partitioner analyzer stop {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        dict_data = self.show_workload_policy(fs_name, policy_name, 'json')
        if timeout == 0:
            self.assertTrue(dict_data['workload_table_ready'] == 'True')

        return timedout, move_count

    def show_mover_status(self, fs_name, output_format='json'):
        cmd = f"mds_partitioner mover status {fs_name} {output_format}"
        retval = self.run_cluster_cmd(cmd)
        if output_format == 'json':
            dict_data = json.loads(retval[1])
            return dict_data
        else:
            with self.assertRaises(json.decoder.JSONDecodeError):
                dict_data = json.loads(retval[1])
            return retval[1]

    def run_mover(self, fs_name, policy_name):
        cmd = f"mds_partitioner mover start {fs_name} {policy_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        while True:
            dict_data = self.show_mover_status(fs_name, 'json')
            log.info(dict_data)
            if dict_data['total_jobs'] > 0 and  \
                dict_data['total_jobs'] == dict_data['processed_jobs']:
                break
            time.sleep(5)

        cmd = f"mds_partitioner mover stop {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        while True:
            dict_data = self.show_mover_status(fs_name, 'json')
            log.info(dict_data)
            if dict_data['status'] == 'DONE':
                break
            time.sleep(5)

    def run_scheduler(self, fs_name, policy_name, workload=True, analyzer_period=60, scheduler_period=3600):
        cmd = f"mds_partitioner scheduler start {fs_name} {policy_name} {analyzer_period} {scheduler_period}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        start_time = time.time()
        while True:
            if workload:
                for mount in self.mounts:
                    for i in range(10):
                        mount.create_file(filename=str(uuid.uuid4()))

            cmd = f"mds_partitioner scheduler status {fs_name}"
            retval = self.run_cluster_cmd(cmd)
            self.assertEquals(retval[0], 0)

            dict_data = json.loads(retval[1])
            log.info(dict_data)
            elapsed_time = time.time() - start_time

            policy_data = self.show_workload_policy(fs_name, policy_name)

            cond =  dict_data['state'] in ['RUNNING_ANALYZER', 'RUNNING_MOVER', 'WAIT']
            log.info(f'cond {cond} workload {workload}')
            if cond:
                if workload:
                    if policy_data['workload_table_ready'] == 'True':
                        break
                else:
                    break

            self.assertLess(elapsed_time, 300)
            time.sleep(5)

        log.info('stop')
        cmd = f"mds_partitioner scheduler stop {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        start_time = time.time()
        while True:
            cmd = f"mds_partitioner scheduler status {fs_name}"
            retval = self.run_cluster_cmd(cmd)
            self.assertEquals(retval[0], 0)

            dict_data = json.loads(retval[1])
            if dict_data['state'] == 'STOP':
                break
            log.info('2')
            log.info(dict_data)
            elapsed_time = time.time() - start_time
            self.assertLess(elapsed_time, 300)
            time.sleep(5)

    def test_enable_partitioner_commands(self):
        fs_name = self.fs_name
        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        cmd = f"mds_partitioner status {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        status = json.loads(retval[1])
        self.assertEquals(status['fs_name'], fs_name)
        self.assertEquals(status['enable'], True)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        cmd = f"mds_partitioner status {fs_name}"
        retval= self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)
        self.assertEquals(json.loads(retval[1]), {})

    def test_workload_policy_commands(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        # setup workload policy
        self.setup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner workload_policy save {fs_name} {policy_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # cleanup workload policy
        self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_workload_policy_recovery_workload(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        # setup workload policy
        self.setup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner workload_policy save {fs_name} {policy_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        dict_data = self.show_workload_policy(fs_name, policy_name)
        log.info("original data")
        log.info(dict_data)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        for _ in range(2):
            log.info("enable fs")
            cmd = f"mds_partitioner enable {fs_name}"
            retval = self.run_cluster_cmd(cmd)
            self.assertEquals(retval[0], 0)

            dict_data = self.show_workload_policy(fs_name, policy_name)
            log.info("recovered data")
            log.info(dict_data)
            self.assertEquals(dict_data['policy_name'], policy_name)
            self.assertEquals(sorted(dict_data['subtree_list']), sorted(self.subvolume_path))

            cmd = f"mds_partitioner workload_policy activate {fs_name} {policy_name}"
            retval = self.run_cluster_cmd(cmd)
            self.assertEquals(retval[0], 0)

            # run analyzer
            self.run_analyzer(fs_name, policy_name)

            # run mover
            self.run_mover(fs_name, policy_name)

            dict_data = self.show_workload_policy(fs_name, policy_name)
            log.info("last-1 data")
            log.info(dict_data)

            log.info("save")
            cmd = f"mds_partitioner workload_policy save {fs_name} {policy_name}"
            retval = self.run_cluster_cmd(cmd)
            self.assertEquals(retval[0], 0)

            log.info("disable fs")
            cmd = f"mds_partitioner disable {fs_name}"
            retval = self.run_cluster_cmd(cmd)
            self.assertEquals(retval[0], 0)

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # cleanup workload policy
        self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_workload_policy_dirpath_wildcard(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        cmd = f"mds_partitioner workload_policy create {fs_name} {policy_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        cmd = f"mds_partitioner workload_policy activate {fs_name} {policy_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        subvol_path = '/volumes/_nogroup/*'

        cmd = f"mds_partitioner workload_policy dir_path add {fs_name} {policy_name} {subvol_path}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        policy_data = self.show_workload_policy(fs_name, policy_name)

        self.assertEquals(policy_data['subtree_list'][0], subvol_path)
        self.assertEquals(len(policy_data['subtree_list']), 1)
        self.assertEquals(len(policy_data['subtree_list_scan']), 2)

        # run analyzer
        self.run_analyzer(fs_name, policy_name)

        # run mover
        self.run_mover(fs_name, policy_name)

        cmd = f"mds_partitioner workload_policy dir_path rm {fs_name} {policy_name} {subvol_path}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        policy_data = self.show_workload_policy(fs_name, policy_name)
        self.assertEquals(len(policy_data['subtree_list']), 0)
        self.assertEquals(len(policy_data['subtree_list_scan']), 0)

        # cleanup workload policy
        self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_analyzer_mover_commands(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        # setup workload policy
        self.setup_workload_policy(fs_name, policy_name)

        # run analyzer
        self.run_analyzer(fs_name, policy_name)

        # run mover
        self.run_mover(fs_name, policy_name)

        # cleanup workload policy
        self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_scheduler(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        # setup workload policy
        self.setup_workload_policy(fs_name, policy_name)

        # run scheduler
        self.run_scheduler(fs_name, policy_name)

        # cleanup workload policy
        self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_scheduler_noworkload(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        # setup workload policy
        self.setup_workload_policy(fs_name, policy_name)

        # run scheduler
        self.run_scheduler(fs_name, policy_name, workload=False)

        # cleanup workload policy
        self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_mix_auto_and_manual_mode(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        # setup workload policy
        self.setup_workload_policy(fs_name, policy_name)

        modes = ['auto', 'manual']

        for _ in range(2):
            for mode in modes:
                if mode == 'auto':
                    # run scheduler 
                    self.run_scheduler(fs_name, policy_name)
                else:
                    # run analyzer
                    self.run_analyzer(fs_name, policy_name)
                    # run mover
                    self.run_mover(fs_name, policy_name)

        # cleanup workload policy
        self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_output_format(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name} "
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        for output_format in ['json', 'prettytable']:
            cmd = f"mds_partitioner status {fs_name} {output_format}"
            retval = self.run_cluster_cmd(cmd)
            self.assertEquals(retval[0], 0)

            if output_format == 'json':
                dict_data = json.loads(retval[1])
                self.assertEquals(dict_data['enable'], True)
            else:
                with self.assertRaises(json.decoder.JSONDecodeError):
                    dict_data = json.loads(retval[1])
                self.assertEquals('enable' in retval[1], True)

        policy_name = 'test_policy'

        self.remove_workload_policy_at_startup(fs_name)
        self.setup_workload_policy(fs_name, policy_name, 'prettytable')

        self.cleanup_workload_policy(fs_name, policy_name)
        self.setup_workload_policy(fs_name, policy_name, 'json')

        cmd = f"mds_partitioner analyzer start {fs_name} {policy_name} 3600 10"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        self.show_analyzer_status(fs_name, policy_name, output_format='prettytable')
        self.show_analyzer_status(fs_name, policy_name, output_format='json')

        cmd = f"mds_partitioner analyzer stop {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # run mover
        self.show_mover_status(fs_name, 'json')
        self.show_mover_status(fs_name, 'prettytable')

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_partition_mode(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        for partition_mode in ['pin', 'pin_bal_rank_mask', 'pin_dir_bal_mask']:
            log.info(f'test partition mode {partition_mode}')
            # setup workload policy
            self.setup_workload_policy(fs_name, policy_name)

            cmd = f"mds_partitioner workload_policy set partition_mode {fs_name} {policy_name} {partition_mode}"
            retval = self.run_cluster_cmd(cmd)
            self.assertEquals(retval[0], 0)

            # run analyzer
            self.run_analyzer(fs_name, policy_name)

            # run mover
            self.run_mover(fs_name, policy_name)

            # cleanup workload policy
            self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_heavy_rentries_threshold(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        for heavy_rentries_threshold in [1, 100000]:
            for partition_mode in ['pin', 'pin_bal_rank_mask', 'pin_dir_bal_mask']:
                log.info(f'test partition mode {partition_mode} heavy_rentries_threshold {heavy_rentries_threshold}')
                # setup workload policy
                self.setup_workload_policy(fs_name, policy_name)

                cmd = f"mds_partitioner workload_policy set partition_mode {fs_name} {policy_name} {partition_mode}"
                retval = self.run_cluster_cmd(cmd)
                self.assertEquals(retval[0], 0)

                self.config_set('mgr', 'mgr/mds_partitioner/heavy_rentries_threshold', heavy_rentries_threshold)

                # run analyzer
                self.run_analyzer(fs_name, policy_name)

                # run mover
                self.run_mover(fs_name, policy_name)

                # cleanup workload policy
                self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_heavy_rentries_threshold_noworkload(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        for heavy_rentries_threshold in [1, 100000]:
            for partition_mode in ['pin', 'pin_bal_rank_mask', 'pin_dir_bal_mask']:
                log.info(f'test partition mode {partition_mode} heavy_rentries_threshold {heavy_rentries_threshold}')
                # setup workload policy
                self.setup_workload_policy(fs_name, policy_name)

                cmd = f"mds_partitioner workload_policy set partition_mode {fs_name} {policy_name} {partition_mode}"
                retval = self.run_cluster_cmd(cmd)
                self.assertEquals(retval[0], 0)

                self.config_set('mgr', 'mgr/mds_partitioner/heavy_rentries_threshold', heavy_rentries_threshold)

                # run analyzer
                self.run_analyzer(fs_name, policy_name, create_file=False, timeout=15)

                # run mover
                try:
                    self.run_mover(fs_name, policy_name)
                except CommandFailedError as ce:
                    self.assertEqual(ce.exitstatus, errno.EINVAL, "expected EINVAL")
                #else:
                #    self.fail("expected start mover to fail")

                # cleanup workload policy
                self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_history_basic(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        # setup workload policy
        self.setup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner workload_policy history rm-all {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        num_history = 2
        for _ in range(num_history):
            # run analyzer
            _, move_count = self.run_analyzer(fs_name, policy_name, timeout=30)
            # run mover
            if move_count:
                self.run_mover(fs_name, policy_name)

        cmd = f"mds_partitioner workload_policy history list {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)
        history_list = json.loads(retval[1])
        log.info(f'history_list {history_list}')
        self.assertEquals(len(history_list), num_history)

        for history_id in history_list:
            self.assertGreater(len(history_id), 0)
            cmd = f"mds_partitioner workload_policy history rm {fs_name} {history_id}"
            retval = self.run_cluster_cmd(cmd)
            self.assertEquals(retval[0], 0)

        cmd = f"mds_partitioner workload_policy history list {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)
        history_list = json.loads(retval[1])
        self.assertEquals(len(history_list), 0)

        num_history = 2
        for _ in range(num_history):
            # run analyzer
            _, move_count = self.run_analyzer(fs_name, policy_name, timeout=30)
            # run mover
            if move_count:
                self.run_mover(fs_name, policy_name)

        cmd = f"mds_partitioner workload_policy history rm-all {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        cmd = f"mds_partitioner workload_policy history list {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)
        history_list = json.loads(retval[1])
        self.assertEquals(len(history_list), 0)

        # cleanup workload policy
        self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_history_recovery(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        # setup workload policy
        self.setup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner workload_policy history rm-all {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        num_history = 2
        for _ in range(num_history):
            # run analyzer
            _, move_count = self.run_analyzer(fs_name, policy_name, timeout=30)
            # run mover
            if move_count:
                self.run_mover(fs_name, policy_name)

        cmd = f"mds_partitioner workload_policy history list {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)
        history_list = json.loads(retval[1])
        self.assertEquals(len(history_list), num_history)

        log.info("disable fs")
        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        log.info("enable fs")
        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        self.assertEquals(len(history_list), num_history)

        cmd = f"mds_partitioner workload_policy history rm-all {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # cleanup workload policy
        self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_history_output_format(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        # setup workload policy
        self.setup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner workload_policy history rm-all {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        num_history = 2
        for _ in range(num_history):
            # run analyzer
            _, move_count = self.run_analyzer(fs_name, policy_name, timeout=30)
            # run mover
            if move_count:
                self.run_mover(fs_name, policy_name)

        for output_format in ["json", "pretty_table"]:
            cmd = f"mds_partitioner workload_policy history list {fs_name} {output_format}"
            retval = self.run_cluster_cmd(cmd)
            self.assertEquals(retval[0], 0)

            if output_format == "json":
                history_list = json.loads(retval[1])
                self.assertEquals(len(history_list), num_history)
                log.info(history_list)
            else:
                with self.assertRaises(json.decoder.JSONDecodeError):
                    json.loads(retval[1])
                log.info(retval[1])

        cmd = f"mds_partitioner workload_policy history list {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)
        history_list = json.loads(retval[1])

        for output_format in ["json", "pretty_table"]:
            for history_id in history_list:
                cmd = f"mds_partitioner workload_policy history show {fs_name} {history_id} {output_format}"
                retval = self.run_cluster_cmd(cmd)
                self.assertEquals(retval[0], 0)

                if output_format == "json":
                    history_data = json.loads(retval[1])
                else:
                    with self.assertRaises(json.decoder.JSONDecodeError):
                        history_data = json.loads(retval[1])
                    history_data = retval[1]

                log.info('history_data')
                log.info(f'{history_data}')

        cmd = f"mds_partitioner workload_policy history rm-all {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # cleanup workload policy
        self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

    def test_history_adjust_size(self):
        fs_name = self.fs_name
        policy_name = 'test_policy'

        cmd = f"mds_partitioner enable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # remove policies for clean start
        self.remove_workload_policy_at_startup(fs_name)

        # setup workload policy
        self.setup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner workload_policy history rm-all {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        num_history = 4
        for _ in range(num_history):
            # run analyzer
            _, move_count = self.run_analyzer(fs_name, policy_name, timeout=60)
            # run mover
            if move_count:
                self.run_mover(fs_name, policy_name)

            cmd = f"mds_partitioner workload_policy history freeze {fs_name}"
            retval = self.run_cluster_cmd(cmd)
            self.assertEquals(retval[0], 0)
            log.info(retval[1])

        cmd = f"mds_partitioner workload_policy history list {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)
        history_list = json.loads(retval[1])
        log.info(history_list)
        self.assertEquals(len(history_list), num_history)

        num_history = 2
        self.config_set('mgr', 'mgr/mds_partitioner/history_max', num_history)

        cmd = f"mds_partitioner workload_policy history list {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)
        history_list = json.loads(retval[1])
        self.assertEquals(len(history_list), num_history)

        cmd = f"mds_partitioner workload_policy history rm-all {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)

        # cleanup workload policy
        self.cleanup_workload_policy(fs_name, policy_name)

        cmd = f"mds_partitioner disable {fs_name}"
        retval = self.run_cluster_cmd(cmd)
        self.assertEquals(retval[0], 0)
