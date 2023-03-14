import os
import json
import time
import random
import logging
import errno

from teuthology.contextutil import safe_while, MaxWhileTries
from teuthology.exceptions import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class TestMDSMetrics(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 3

    TEST_DIR_PERFIX = "test_mds_metrics"

    def setUp(self):
        super(TestMDSMetrics, self).setUp()
        self._start_with_single_active_mds()
        self._enable_mgr_stats_plugin()

    def tearDown(self):
        self._disable_mgr_stats_plugin()
        super(TestMDSMetrics, self).tearDown()

    def _start_with_single_active_mds(self):
        curr_max_mds = self.fs.get_var('max_mds')
        if curr_max_mds > 1:
            self.fs.shrink(1)

    def verify_mds_metrics(self, active_mds_count=1, client_count=1, ranks=[], mul_fs=[]):
        def verify_metrics_cbk(metrics):
            mds_metrics = metrics['metrics']
            if not len(mds_metrics) == active_mds_count + 1: # n active mdss + delayed set
                return False
            fs_status = self.fs.status()
            nonlocal ranks, mul_fs
            if not ranks:
                if not mul_fs:
                    mul_fs = [self.fs.id]
                for filesystem in mul_fs:
                    ranks = set([info['rank'] for info in fs_status.get_ranks(filesystem)])
            for rank in ranks:
                r = mds_metrics.get("mds.{}".format(rank), None)
                if not r or not len(mds_metrics['delayed_ranks']) == 0:
                    return False
            for item in mul_fs:
                key = fs_status.get_fsmap(item)['mdsmap']['fs_name']
                global_metrics = metrics['global_metrics'].get(key, {})
                client_metadata = metrics['client_metadata'].get(key, {})
                if not len(global_metrics) >= client_count or not len(client_metadata) >= client_count:
                    return False
            return True
        return verify_metrics_cbk

    def _fs_perf_stats(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "perf", "stats", *args)

    def _enable_mgr_stats_plugin(self):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "enable", "stats")

    def _disable_mgr_stats_plugin(self):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "disable", "stats")

    def _spread_directory_on_all_ranks(self, fscid):
        fs_status = self.fs.status()
        ranks = set([info['rank'] for info in fs_status.get_ranks(fscid)])
        # create a per-rank pinned directory
        for rank in ranks:
            dirname = "{0}_{1}".format(TestMDSMetrics.TEST_DIR_PERFIX, rank)
            self.mount_a.run_shell(["mkdir", dirname])
            self.mount_a.setfattr(dirname, "ceph.dir.pin", str(rank))
            log.info("pinning directory {0} to rank {1}".format(dirname, rank))
            for i in range(16):
                filename = "{0}.{1}".format("test", i)
                self.mount_a.write_n_mb(os.path.join(dirname, filename), 1)

    def _do_spread_io(self, fscid):
        # spread readdir I/O
        self.mount_b.run_shell(["find", "."])

    def _do_spread_io_all_clients(self, fscid):
        # spread readdir I/O
        self.mount_a.run_shell(["find", "."])
        self.mount_b.run_shell(["find", "."])

    def _cleanup_test_dirs(self):
        dirnames = self.mount_a.run_shell(["ls"]).stdout.getvalue()
        for dirname in dirnames.split("\n"):
            if dirname.startswith(TestMDSMetrics.TEST_DIR_PERFIX):
                log.info("cleaning directory {}".format(dirname))
                self.mount_a.run_shell(["rm", "-rf", dirname])

    def _get_metrics(self, verifier_callback, trials, *args):
        metrics = None
        done = False
        with safe_while(sleep=1, tries=trials, action='wait for metrics') as proceed:
            while proceed():
                metrics = json.loads(self._fs_perf_stats(*args))
                done = verifier_callback(metrics)
                if done:
                    break
        return done, metrics

    def _setup_fs(self, fs_name):
        fs_a = self.mds_cluster.newfs(name=fs_name)

        self.mds_cluster.mds_restart()

        # Wait for filesystem to go healthy
        fs_a.wait_for_daemons()

        # Reconfigure client auth caps
        for mount in self.mounts:
            self.get_ceph_cmd_result(
                'auth', 'caps', f"client.{mount.client_id}",
                'mds', 'allow',
                'mon', 'allow r',
                'osd', f'allow rw pool={fs_a.get_data_pool_name()}')

        return fs_a

    # basic check to verify if we get back metrics from each active mds rank

    def test_metrics_from_rank(self):
        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

    def test_metrics_post_client_disconnection(self):
        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        self.mount_a.umount_wait()

        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=TestMDSMetrics.CLIENTS_REQUIRED - 1), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

    def test_metrics_mds_grow(self):
        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        # grow the mds cluster
        self.fs.grow(2)

        fscid = self.fs.id
        # spread directory per rank
        self._spread_directory_on_all_ranks(fscid)

        # spread some I/O
        self._do_spread_io(fscid)

        # wait a bit for mgr to get updated metrics
        time.sleep(5)

        # validate
        valid, metrics = self._get_metrics(self.verify_mds_metrics(
            active_mds_count=2, client_count=TestMDSMetrics.CLIENTS_REQUIRED) , 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        # cleanup test directories
        self._cleanup_test_dirs()

    def test_metrics_mds_grow_and_shrink(self):
        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        # grow the mds cluster
        self.fs.grow(2)

        fscid = self.fs.id
        # spread directory per rank
        self._spread_directory_on_all_ranks(fscid)

        # spread some I/O
        self._do_spread_io(fscid)

        # wait a bit for mgr to get updated metrics
        time.sleep(5)

        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(active_mds_count=2, client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        # shrink mds cluster
        self.fs.shrink(1)

        # wait a bit for mgr to get updated metrics
        time.sleep(5)

        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        # cleanup test directories
        self._cleanup_test_dirs()

    def test_delayed_metrics(self):
        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        # grow the mds cluster
        self.fs.grow(2)

        fscid = self.fs.id
        # spread directory per rank
        self._spread_directory_on_all_ranks(fscid)

        # spread some I/O
        self._do_spread_io(fscid)

        # wait a bit for mgr to get updated metrics
        time.sleep(5)

        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(active_mds_count=2, client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        # do not give this mds any chance
        delayed_rank = 1
        mds_id_rank0 = self.fs.get_rank(rank=0)['name']
        mds_id_rank1 = self.fs.get_rank(rank=1)['name']

        self.fs.set_inter_mds_block(True, mds_id_rank0, mds_id_rank1)

        def verify_delayed_metrics(metrics):
            mds_metrics = metrics['metrics']
            r = mds_metrics.get("mds.{}".format(delayed_rank), None)
            if not r or not delayed_rank in mds_metrics['delayed_ranks']:
                return False
            return True
        # validate
        valid, metrics = self._get_metrics(verify_delayed_metrics, 30)
        log.debug("metrics={0}".format(metrics))

        self.assertTrue(valid)
        self.fs.set_inter_mds_block(False, mds_id_rank0, mds_id_rank1)

        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(active_mds_count=2, client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        # cleanup test directories
        self._cleanup_test_dirs()

    def test_query_mds_filter(self):
        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        # grow the mds cluster
        self.fs.grow(2)

        fscid = self.fs.id
        # spread directory per rank
        self._spread_directory_on_all_ranks(fscid)

        # spread some I/O
        self._do_spread_io(fscid)

        # wait a bit for mgr to get updated metrics
        time.sleep(5)

        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(active_mds_count=2, client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        filtered_mds = 1
        def verify_filtered_mds_rank_metrics(metrics):
        # checks if the metrics has only client_metadata and
        # global_metrics filtered using --mds_rank=1
            global_metrics = metrics['global_metrics'].get(self.fs.name, {})
            client_metadata = metrics['client_metadata'].get(self.fs.name, {})
            mds_metrics = metrics['metrics']
            if len(mds_metrics) != 2 or f"mds.{filtered_mds}" not in mds_metrics:
                return False
            if len(global_metrics) > TestMDSMetrics.CLIENTS_REQUIRED or\
                    len(client_metadata) > TestMDSMetrics.CLIENTS_REQUIRED:
                return False
            if len(set(global_metrics) - set(mds_metrics[f"mds.{filtered_mds}"])) or\
                    len(set(client_metadata) - set(mds_metrics[f"mds.{filtered_mds}"])):
                return False
            return True
        # initiate a new query with `--mds_rank` filter and validate if
        # we get metrics *only* from that mds.
        valid, metrics = self._get_metrics(verify_filtered_mds_rank_metrics, 30,
                                           f'--mds_rank={filtered_mds}')
        log.debug(f"metrics={metrics}")
        self.assertTrue(valid, "Incorrect 'ceph fs perf stats' output"
                        f" with filter '--mds_rank={filtered_mds}'")

    def test_query_client_filter(self):
        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        mds_metrics = metrics['metrics']
        # pick an random client
        client = random.choice(list(mds_metrics['mds.0'].keys()))
        # could have used regex to extract client id
        client_id = (client.split(' ')[0]).split('.')[-1]

        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=1), 30, '--client_id={}'.format(client_id))
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

    def test_query_client_ip_filter(self):
        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        client_matadata = metrics['client_metadata'][self.fs.name]
        # pick an random client
        client = random.choice(list(client_matadata.keys()))
        # get IP of client to use in filter
        client_ip = client_matadata[client]['IP']

        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=1), 30, '--client_ip={}'.format(client_ip))
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        # verify IP from output with filter IP
        for i in metrics['client_metadata'][self.fs.name]:
            self.assertEqual(client_ip, metrics['client_metadata'][self.fs.name][i]['IP'])

    def test_query_mds_and_client_filter(self):
        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        # grow the mds cluster
        self.fs.grow(2)

        fscid = self.fs.id
        # spread directory per rank
        self._spread_directory_on_all_ranks(fscid)

        # spread some I/O
        self._do_spread_io_all_clients(fscid)

        # wait a bit for mgr to get updated metrics
        time.sleep(5)

        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(active_mds_count=2, client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

        mds_metrics = metrics['metrics']

        # pick an random client
        client = random.choice(list(mds_metrics['mds.1'].keys()))
        # could have used regex to extract client id
        client_id = (client.split(' ')[0]).split('.')[-1]
        filtered_mds = 1
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=1, ranks=[filtered_mds]),
            30, '--mds_rank={}'.format(filtered_mds), '--client_id={}'.format(client_id))
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

    def test_for_invalid_mds_rank(self):
        invalid_mds_rank = "1,"
        # try, 'fs perf stat' command with invalid mds_rank
        try:
            self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "perf", "stats", "--mds_rank", invalid_mds_rank)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise
        else:
            raise RuntimeError("expected the 'fs perf stat' command to fail for invalid mds_rank")

    def test_for_invalid_client_id(self):
        invalid_client_id = "abcd"
        # try, 'fs perf stat' command with invalid client_id
        try:
            self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "perf", "stats", "--client_id", invalid_client_id)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise
        else:
            raise RuntimeError("expected the 'fs perf stat' command to fail for invalid client_id")

    def test_for_invalid_client_ip(self):
        invalid_client_ip = "1.2.3"
        # try, 'fs perf stat' command with invalid client_ip
        try:
            self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "perf", "stats", "--client_ip", invalid_client_ip)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise
        else:
            raise RuntimeError("expected the 'fs perf stat' command to fail for invalid client_ip")

    def test_perf_stats_stale_metrics(self):
        """
        That `ceph fs perf stats` doesn't output stale metrics after the rank0 MDS failover
        """
        # validate
        valid, metrics = self._get_metrics(self.verify_mds_metrics(
            active_mds_count=1, client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
        log.debug(f'metrics={metrics}')
        self.assertTrue(valid)

        # mount_a and mount_b are the clients mounted for TestMDSMetrics. So get their
        # entries from the global_metrics.
        client_a_name = f'client.{self.mount_a.get_global_id()}'
        client_b_name = f'client.{self.mount_b.get_global_id()}'

        global_metrics = metrics['global_metrics']
        client_a_metrics = global_metrics[self.fs.name][client_a_name]
        client_b_metrics = global_metrics[self.fs.name][client_b_name]

        # fail rank0 mds
        self.fs.rank_fail(rank=0)

        # Wait for rank0 up:active state
        self.fs.wait_for_state('up:active', rank=0, timeout=30)

        fscid = self.fs.id

        # spread directory per rank
        self._spread_directory_on_all_ranks(fscid)

        # spread some I/O
        self._do_spread_io_all_clients(fscid)

        # wait a bit for mgr to get updated metrics
        time.sleep(5)

        # validate
        try:
            valid, metrics_new = self._get_metrics(self.verify_mds_metrics(
                active_mds_count=1, client_count=TestMDSMetrics.CLIENTS_REQUIRED), 30)
            log.debug(f'metrics={metrics_new}')
            self.assertTrue(valid)

            client_metadata = metrics_new['client_metadata']
            client_a_metadata = client_metadata.get(self.fs.name, {}).get(client_a_name, {})
            client_b_metadata = client_metadata.get(self.fs.name, {}).get(client_b_name, {})

            global_metrics = metrics_new['global_metrics']
            client_a_metrics_new = global_metrics.get(self.fs.name, {}).get(client_a_name, {})
            client_b_metrics_new = global_metrics.get(self.fs.name, {}).get(client_b_name, {})

            # the metrics should be different for the test to succeed.
            self.assertTrue(client_a_metadata and client_b_metadata and
                            client_a_metrics_new and client_b_metrics_new and
                            (client_a_metrics_new != client_a_metrics) and
                            (client_b_metrics_new != client_b_metrics),
                            "Invalid 'ceph fs perf stats' metrics after rank0 mds failover")
        except MaxWhileTries:
            raise RuntimeError("Failed to fetch 'ceph fs perf stats' metrics")
        finally:
            # cleanup test directories
            self._cleanup_test_dirs()

    def test_client_metrics_and_metadata(self):
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()
        self.fs.delete_all_filesystems()

        self.mds_cluster.mon_manager.raw_cluster_cmd("fs", "flag", "set",
            "enable_multiple", "true", "--yes-i-really-mean-it")

        # creating filesystem
        fs_a = self._setup_fs(fs_name="fs1")

        # Mount a client on fs_a
        self.mount_a.mount_wait(cephfs_name=fs_a.name)
        self.mount_a.write_n_mb("pad.bin", 1)
        self.mount_a.write_n_mb("test.bin", 2)
        self.mount_a.path_to_ino("test.bin")
        self.mount_a.create_files()

        # creating another filesystem
        fs_b = self._setup_fs(fs_name="fs2")

        # Mount a client on fs_b
        self.mount_b.mount_wait(cephfs_name=fs_b.name)
        self.mount_b.write_n_mb("test.bin", 1)
        self.mount_b.path_to_ino("test.bin")
        self.mount_b.create_files()

        fscid_list = [fs_a.id, fs_b.id]

        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=1, mul_fs=fscid_list), 30)
        log.debug(f"metrics={metrics}")
        self.assertTrue(valid)

        client_metadata_a = metrics['client_metadata']['fs1']
        client_metadata_b = metrics['client_metadata']['fs2']

        for i in client_metadata_a:
            if not (client_metadata_a[i]['hostname']):
                raise RuntimeError("hostname of fs1 not found!")
            if not (client_metadata_a[i]['valid_metrics']):
                raise RuntimeError("valid_metrics of fs1 not found!")

        for i in client_metadata_b:
            if not (client_metadata_b[i]['hostname']):
                raise RuntimeError("hostname of fs2 not found!")
            if not (client_metadata_b[i]['valid_metrics']):
                raise RuntimeError("valid_metrics of fs2 not found!")

    def test_non_existing_mds_rank(self):
        def verify_filtered_metrics(metrics):
        # checks if the metrics has non empty client_metadata and global_metrics
            if metrics['client_metadata'].get(self.fs.name, {})\
              or metrics['global_metrics'].get(self.fs.name, {}):
                return True
            return False

        try:
            # validate
            filter_rank = random.randint(1, 10)
            valid, metrics = self._get_metrics(verify_filtered_metrics, 30,
                                               '--mds_rank={}'.format(filter_rank))
            log.info(f'metrics={metrics}')
            self.assertFalse(valid, "Fetched 'ceph fs perf stats' metrics using nonexistent MDS rank")
        except MaxWhileTries:
            # success
            pass

    def test_perf_stats_stale_metrics_with_multiple_filesystem(self):
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        self.mds_cluster.mon_manager.raw_cluster_cmd("fs", "flag", "set",
                    "enable_multiple", "true", "--yes-i-really-mean-it")

        # creating filesystem
        fs_b = self._setup_fs(fs_name="fs2")

        # Mount a client on fs_b
        self.mount_b.mount_wait(cephfs_name=fs_b.name)
        self.mount_b.write_n_mb("test.bin", 1)
        self.mount_b.path_to_ino("test.bin")
        self.mount_b.create_files()

        # creating another filesystem
        fs_a = self._setup_fs(fs_name="fs1")

        # Mount a client on fs_a
        self.mount_a.mount_wait(cephfs_name=fs_a.name)
        self.mount_a.write_n_mb("pad.bin", 1)
        self.mount_a.write_n_mb("test.bin", 2)
        self.mount_a.path_to_ino("test.bin")
        self.mount_a.create_files()

        # validate
        valid, metrics = self._get_metrics(
            self.verify_mds_metrics(client_count=1, mul_fs=[fs_a.id, fs_b.id]), 30)
        log.debug(f"metrics={metrics}")
        self.assertTrue(valid)

        # get mounted client's entries from the global_metrics.
        client_a_name = f'client.{self.mount_a.get_global_id()}'

        global_metrics = metrics['global_metrics']
        client_a_metrics = global_metrics.get("fs1", {}).get(client_a_name, {})

        # fail active mds of fs_a
        fs_a_mds = fs_a.get_active_names()[0]
        self.mds_cluster.mds_fail(fs_a_mds)
        fs_a.wait_for_state('up:active', rank=0, timeout=30)

        # spread directory per rank
        self._spread_directory_on_all_ranks(fs_a.id)

        # spread some I/O
        self._do_spread_io_all_clients(fs_a.id)

        # wait a bit for mgr to get updated metrics
        time.sleep(5)

        # validate
        try:
            valid, metrics_new = self._get_metrics(
                self.verify_mds_metrics(client_count=1, mul_fs=[fs_a.id, fs_b.id]), 30)
            log.debug(f'metrics={metrics_new}')
            self.assertTrue(valid)

            client_metadata = metrics_new['client_metadata']
            client_a_metadata = client_metadata.get("fs1", {}).get(client_a_name, {})

            global_metrics = metrics_new['global_metrics']
            client_a_metrics_new = global_metrics.get("fs1", {}).get(client_a_name, {})

            # the metrics should be different for the test to succeed.
            self.assertTrue(client_a_metadata and client_a_metrics_new
                            and (client_a_metrics_new != client_a_metrics),
                            "Invalid 'ceph fs perf stats' metrics after"
                            f" rank0 mds of {fs_a.name} failover")
        except MaxWhileTries:
            raise RuntimeError("Failed to fetch `ceph fs perf stats` metrics")
        finally:
            # cleanup test directories
            self._cleanup_test_dirs()

