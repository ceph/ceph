import errno
import json
import logging
import uuid
from io import StringIO
from os.path import join as os_path_join
import re
from time import sleep

from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while

from tasks.cephfs.cephfs_test_case import CephFSTestCase, classhook
from tasks.cephfs.filesystem import FileLayout, FSMissing
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.caps_helper import (CapTester, gen_mon_cap_str,
                                      gen_osd_cap_str, gen_mds_cap_str)

log = logging.getLogger(__name__)
MDS_RESTART_GRACE = 60

class TestLabeledPerfCounters(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 1

    def _get_counters_for(self, filesystem, client_id):
        dump = self.fs.rank_tell(["counter", "dump"])
        per_client_metrics_key = f'mds_client_metrics-{filesystem}'
        counters = [c["counters"] for \
                    c in dump[per_client_metrics_key] if c["labels"]["client"] == client_id]
        return counters[0]

    def test_per_client_labeled_perf_counters_on_client_disconnect(self):
        """
        That the per-client labelled metrics are unavailable during client disconnect
        """
        mount_a_id = f'client.{self.mount_a.get_global_id()}'
        self.mount_a.teardown()
        with safe_while(sleep=1, tries=30, action=f'wait for counters - {mount_a_id}') as proceed:
            while proceed():
                dump = self.fs.rank_tell(["counter", "dump"])
                per_client_metrics_key = f"mds_client_metrics-{dump['mds_client_metrics'][0]['labels']['fs_name']}"
                clients = [c["labels"]["client"] for c in dump.get(per_client_metrics_key, {})]
                if clients and mount_a_id not in clients:
                    # success, no metrics.
                    return True

    def test_per_client_labeled_perf_counters_on_client_reconnect(self):
        """
        That the per-client labelled metrics are generated during client reconnect
        """
        # fail active mds and wait for reconnect
        mds = self.fs.get_active_names()[0]
        self.mds_cluster.mds_fail(mds)
        self.fs.wait_for_state('up:active', rank=0, timeout=MDS_RESTART_GRACE)
        mount_a_id = f'client.{self.mount_a.get_global_id()}'
        mount_b_id = f'client.{self.mount_b.get_global_id()}'
        fs_suffix = ""

        with safe_while(sleep=1, tries=30, action='wait for counters') as proceed:
            while proceed():
                dump = self.fs.rank_tell(["counter", "dump"])
                fs_suffix = dump['mds_client_metrics'][0]['labels']['fs_name']
                per_client_metrics_key = f"mds_client_metrics-{fs_suffix}"
                clients = [c["labels"]["client"] for c in dump.get(per_client_metrics_key, {})]
                if mount_a_id in clients and mount_b_id in clients:
                    # success, got metrics.
                    break # break to continue the test

        # Post reconnecting, validate the io perf counters
        # write workload
        self.mount_a.create_n_files("test_dir/test_file", 1000, sync=True)
        with safe_while(sleep=1, tries=30, action=f'wait for counters - {mount_a_id}') as proceed:
            while proceed():
                counters_dump_a = self._get_counters_for(fs_suffix, mount_a_id)
                if counters_dump_a["total_write_ops"] > 0 and counters_dump_a["total_write_size"] > 0 and \
                   counters_dump_a["avg_write_latency"] >= 0 and counters_dump_a["avg_metadata_latency"] >= 0 and  \
                   counters_dump_a["opened_files"] >= 0 and counters_dump_a["opened_inodes"] > 0 and \
                   counters_dump_a["cap_hits"] > 0 and counters_dump_a["dentry_lease_hits"] > 0 and \
                   counters_dump_a["pinned_icaps"] > 0:
                    break # break to continue the test

        # read from the other client
        for i in range(100):
            self.mount_b.open_background(basename=f'test_dir/test_file_{i}', write=False)
        with safe_while(sleep=1, tries=30, action=f'wait for counters - {mount_b_id}') as proceed:
            while proceed():
                counters_dump_b = self._get_counters_for(fs_suffix, mount_b_id)
                if counters_dump_b["total_read_ops"] >= 0 and counters_dump_b["total_read_size"] >= 0 and \
                   counters_dump_b["avg_read_latency"] >= 0 and counters_dump_b["avg_metadata_latency"] >= 0 and \
                   counters_dump_b["opened_files"] >= 0 and counters_dump_b["opened_inodes"] >= 0 and \
                   counters_dump_b["cap_hits"] > 0 and counters_dump_a["dentry_lease_hits"] > 0 and \
                   counters_dump_b["pinned_icaps"] > 0:
                    break # break to continue the test
        self.mount_a.teardown()
        self.mount_b.teardown()

    def test_per_client_labeled_perf_counters_io(self):
        """
        That the per-client labelled perf counters depict the clients performing IO.
        """
        # sleep a bit so that we get updated clients...
        sleep(10)

        # lookout for clients...
        dump = self.fs.rank_tell(["counter", "dump"])

        fs_suffix = dump["mds_client_metrics"][0]["labels"]["fs_name"]
        self.assertGreaterEqual(dump["mds_client_metrics"][0]["counters"]["num_clients"], 2)

        per_client_metrics_key = f'mds_client_metrics-{fs_suffix}'
        mount_a_id = f'client.{self.mount_a.get_global_id()}'
        mount_b_id = f'client.{self.mount_b.get_global_id()}'

        clients = [c["labels"]["client"] for c in dump[per_client_metrics_key]]
        self.assertIn(mount_a_id, clients)
        self.assertIn(mount_b_id, clients)

        # write workload
        self.mount_a.create_n_files("test_dir/test_file", 1000, sync=True)
        with safe_while(sleep=1, tries=30, action=f'wait for counters - {mount_a_id}') as proceed:
            while proceed():
                counters_dump_a = self._get_counters_for(fs_suffix, mount_a_id)
                if counters_dump_a["total_write_ops"] > 0 and counters_dump_a["total_write_size"] > 0 and \
                   counters_dump_a["avg_write_latency"] >= 0 and counters_dump_a["avg_metadata_latency"] >= 0 and  \
                   counters_dump_a["opened_files"] >= 0 and counters_dump_a["opened_inodes"] > 0 and \
                   counters_dump_a["cap_hits"] > 0 and counters_dump_a["dentry_lease_hits"] > 0 and \
                   counters_dump_a["pinned_icaps"] > 0:
                    break # break to continue the test

        # read from the other client
        for i in range(100):
            self.mount_b.open_background(basename=f'test_dir/test_file_{i}', write=False)
        with safe_while(sleep=1, tries=30, action=f'wait for counters - {mount_b_id}') as proceed:
            while proceed():
                counters_dump_b = self._get_counters_for(fs_suffix, mount_b_id)
                if counters_dump_b["total_read_ops"] >= 0 and counters_dump_b["total_read_size"] >= 0 and \
                   counters_dump_b["avg_read_latency"] >= 0 and counters_dump_b["avg_metadata_latency"] >= 0 and \
                   counters_dump_b["opened_files"] >= 0 and counters_dump_b["opened_inodes"] >= 0 and \
                   counters_dump_b["cap_hits"] > 0 and counters_dump_a["dentry_lease_hits"] > 0 and \
                   counters_dump_b["pinned_icaps"] > 0:
                    break # break to continue the test
        self.mount_a.teardown()
        self.mount_b.teardown()

class TestAdminCommands(CephFSTestCase):
    """
    Tests for administration command.
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def check_pool_application_metadata_key_value(self, pool, app, key, value):
        output = self.get_ceph_cmd_stdout(
            'osd', 'pool', 'application', 'get', pool, app, key)
        self.assertEqual(str(output.strip()), value)

    def setup_ec_pools(self, n, metadata=True, overwrites=True):
        if metadata:
            self.run_ceph_cmd('osd', 'pool', 'create', n+"-meta", "8")
        cmd = ['osd', 'erasure-code-profile', 'set', n+"-profile", "m=2", "k=2", "crush-failure-domain=osd"]
        self.run_ceph_cmd(cmd)
        self.run_ceph_cmd('osd', 'pool', 'create', n+"-data", "8", "erasure", n+"-profile")
        if overwrites:
            self.run_ceph_cmd('osd', 'pool', 'set', n+"-data", 'allow_ec_overwrites', 'true')

    def gen_health_warn_mds_cache_oversized(self):
        health_warn = 'MDS_CACHE_OVERSIZED'

        self.config_set('mds', 'mds_cache_memory_limit', '1K')
        self.config_set('mds', 'mds_health_cache_threshold', '1.00000')
        self.mount_a.open_n_background('.', 400)

        self.wait_for_health(health_warn, 30)

    def gen_health_warn_mds_trim(self):
        health_warn = 'MDS_TRIM'

        # for generating health warning MDS_TRIM
        self.config_set('mds', 'mds_debug_subtrees', 'true')
        # this will really really slow the trimming, so that MDS_TRIM stays
        # for longer.
        self.config_set('mds', 'mds_log_trim_decay_rate', '60')
        self.config_set('mds', 'mds_log_trim_threshold', '1')
        self.mount_a.open_n_background('.', 400)

        self.wait_for_health(health_warn, 30)


class TestMdsLastSeen(CephFSTestCase):
    """
    Tests for `mds last-seen` command.
    """

    MDSS_REQUIRED = 2

    def test_in_text(self):
        """
        That `mds last-seen` returns 0 for an MDS currently in the map.
        """

        status = self.fs.status()
        r0 = self.fs.get_rank(0, status=status)
        s = self.get_ceph_cmd_stdout("mds", "last-seen", r0['name'])
        seconds = int(re.match(r"^(\d+)s$", s).group(1))
        self.assertEqual(seconds, 0)

    def test_in_json(self):
        """
        That `mds last-seen` returns 0 for an MDS currently in the map.
        """

        status = self.fs.status()
        r0 = self.fs.get_rank(0, status=status)
        s = self.get_ceph_cmd_stdout("--format=json", "mds", "last-seen", r0['name'])
        J = json.loads(s)
        seconds = int(re.match(r"^(\d+)s$", J['last-seen']).group(1))
        self.assertEqual(seconds, 0)

    def test_unknown(self):
        """
        That `mds last-seen` returns ENOENT for an mds not in recent maps.
        """

        try:
            self.get_ceph_cmd_stdout("--format=json", "mds", "last-seen", 'foo')
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("non-existent mds should fail ENOENT")

    def test_standby(self):
        """
        That `mds last-seen` returns 0 for a standby.
        """

        status = self.fs.status()
        for info in status.get_standbys():
            s = self.get_ceph_cmd_stdout("--format=json", "mds", "last-seen", info['name'])
            J = json.loads(s)
            seconds = int(re.match(r"^(\d+)s$", J['last-seen']).group(1))
            self.assertEqual(seconds, 0)

    def test_stopped(self):
        """
        That `mds last-seen` returns >0 for mds that is stopped.
        """

        status = self.fs.status()
        r0 = self.fs.get_rank(0, status=status)
        self.fs.mds_stop(mds_id=r0['name'])
        self.fs.rank_fail()
        sleep(2)
        with safe_while(sleep=1, tries=self.fs.beacon_timeout, action='wait for last-seen >0') as proceed:
            while proceed():
                s = self.get_ceph_cmd_stdout("--format=json", "mds", "last-seen", r0['name'])
                J = json.loads(s)
                seconds = int(re.match(r"^(\d+)s$", J['last-seen']).group(1))
                if seconds == 0:
                    continue
                self.assertGreater(seconds, 1)
                break

    def test_gc(self):
        """
        That historical mds information is eventually garbage collected.
        """

        prune_time = 20
        sleep_time = 2
        self.config_set('mon', 'mon_fsmap_prune_threshold', prune_time)
        status = self.fs.status()
        r0 = self.fs.get_rank(0, status=status)
        self.fs.mds_stop(mds_id=r0['name'])
        self.fs.rank_fail()
        last = 0
        for i in range(prune_time):
            sleep(sleep_time) # we will sleep twice prune_time
            try:
                s = self.get_ceph_cmd_stdout("--format=json", "mds", "last-seen", r0['name'])
                J = json.loads(s)
                seconds = int(re.match(r"^(\d+)s$", J['last-seen']).group(1))
                self.assertGreater(seconds, last)
                log.debug("last_seen: %ds", seconds)
                last = seconds
            except CommandFailedError as e:
                self.assertEqual(e.exitstatus, errno.ENOENT)
                self.assertGreaterEqual(last + sleep_time + 1, prune_time) # rounding error add 1
                return
        self.fail("map was no garbage collected as expected")

@classhook('_add_valid_tell')
class TestValidTell(TestAdminCommands):
    @classmethod
    def _add_valid_tell(cls):
        tells = [
          ['cache', 'status'],
          ['damage', 'ls'],
          ['dump_blocked_ops'],
          ['dump_blocked_ops_count'],
          ['dump_historic_ops'],
          ['dump_historic_ops_by_duration'],
          ['dump_mempools'],
          ['dump_ops_in_flight'],
          ['flush', 'journal'],
          ['get', 'subtrees'],
          ['ops', 'locks'],
          ['ops'],
          ['status'],
          ['version'],
        ]
        def test(c):
            def f(self):
                J = self.fs.rank_tell(c)
                json.dumps(J)
                log.debug("dumped:\n%s", str(J))
            return f
        for c in tells:
            setattr(cls, 'test_valid_' + '_'.join(c), test(c))

class TestFsStatus(TestAdminCommands):
    """
    Test "ceph fs status subcommand.
    """

    MDSS_REQUIRED = 3

    def test_fs_status(self):
        """
        That `ceph fs status` command functions.
        """

        s = self.get_ceph_cmd_stdout("fs", "status")
        self.assertTrue("active" in s)

        mdsmap = json.loads(self.get_ceph_cmd_stdout("fs", "status", "--format=json-pretty"))["mdsmap"]
        self.assertEqual(mdsmap[0]["state"], "active")

        mdsmap = json.loads(self.get_ceph_cmd_stdout("fs", "status", "--format=json"))["mdsmap"]
        self.assertEqual(mdsmap[0]["state"], "active")

    def test_fs_status_standby_replay(self):
        """
        That `ceph fs status` command functions.
        """

        self.fs.set_allow_standby_replay(True)

        s = self.get_ceph_cmd_stdout("fs", "status")
        self.assertTrue("active" in s)
        self.assertTrue("standby-replay" in s)
        self.assertTrue("0-s" in s)
        self.assertTrue("standby" in s)

        mdsmap = json.loads(self.get_ceph_cmd_stdout("fs", "status", "--format=json-pretty"))["mdsmap"]
        self.assertEqual(mdsmap[0]["state"], "active")
        self.assertEqual(mdsmap[1]["state"], "standby-replay")
        self.assertEqual(mdsmap[1]["rank"], "0-s")
        self.assertEqual(mdsmap[2]["state"], "standby")

        mdsmap = json.loads(self.get_ceph_cmd_stdout("fs", "status", "--format=json"))["mdsmap"]
        self.assertEqual(mdsmap[0]["state"], "active")
        self.assertEqual(mdsmap[1]["state"], "standby-replay")
        self.assertEqual(mdsmap[1]["rank"], "0-s")
        self.assertEqual(mdsmap[2]["state"], "standby")


class TestAddDataPool(TestAdminCommands):
    """
    Test "ceph fs add_data_pool" subcommand.
    """

    def test_add_data_pool_root(self):
        """
        That a new data pool can be added and used for the root directory.
        """

        p = self.fs.add_data_pool("foo")
        self.fs.set_dir_layout(self.mount_a, ".", FileLayout(pool=p))

    def test_add_data_pool_application_metadata(self):
        """
        That the application metadata set on a newly added data pool is as expected.
        """
        pool_name = "foo"
        mon_cmd = self.get_ceph_cmd_stdout
        mon_cmd('osd', 'pool', 'create', pool_name, '--pg_num_min',
                str(self.fs.pg_num_min))
        # Check whether https://tracker.ceph.com/issues/43061 is fixed
        mon_cmd('osd', 'pool', 'application', 'enable', pool_name, 'cephfs')
        self.fs.add_data_pool(pool_name, create=False)
        self.check_pool_application_metadata_key_value(
            pool_name, 'cephfs', 'data', self.fs.name)

    def test_add_data_pool_subdir(self):
        """
        That a new data pool can be added and used for a sub-directory.
        """

        p = self.fs.add_data_pool("foo")
        self.mount_a.run_shell("mkdir subdir")
        self.fs.set_dir_layout(self.mount_a, "subdir", FileLayout(pool=p))

    def test_add_data_pool_non_alphamueric_name_as_subdir(self):
        """
        That a new data pool with non-alphanumeric name can be added and used for a sub-directory.
        """
        p = self.fs.add_data_pool("I-am-data_pool00.")
        self.mount_a.run_shell("mkdir subdir")
        self.fs.set_dir_layout(self.mount_a, "subdir", FileLayout(pool=p))

    def test_add_data_pool_ec(self):
        """
        That a new EC data pool can be added.
        """

        n = "test_add_data_pool_ec"
        self.setup_ec_pools(n, metadata=False)
        self.fs.add_data_pool(n+"-data", create=False)

    def test_add_already_in_use_data_pool(self):
        """
        That command try to add data pool which is already in use with another fs.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        # create second data pool, metadata pool and add with filesystem
        second_fs = "second_fs"
        second_metadata_pool = "second_metadata_pool"
        second_data_pool = "second_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', second_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', second_data_pool)
        self.run_ceph_cmd('fs', 'new', second_fs, second_metadata_pool, second_data_pool)

        # try to add 'first_data_pool' with 'second_fs'
        # Expecting EINVAL exit status because 'first_data_pool' is already in use with 'first_fs'
        try:
            self.run_ceph_cmd('fs', 'add_data_pool', second_fs, first_data_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because data pool is already in use as data pool for first_fs")

    def test_add_already_in_use_metadata_pool(self):
        """
        That command try to add metadata pool which is already in use with another fs.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        # create second data pool, metadata pool and add with filesystem
        second_fs = "second_fs"
        second_metadata_pool = "second_metadata_pool"
        second_data_pool = "second_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', second_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', second_data_pool)
        self.run_ceph_cmd('fs', 'new', second_fs, second_metadata_pool, second_data_pool)

        # try to add 'second_metadata_pool' with 'first_fs' as a data pool
        # Expecting EINVAL exit status because 'second_metadata_pool'
        # is already in use with 'second_fs' as a metadata pool
        try:
            self.run_ceph_cmd('fs', 'add_data_pool', first_fs, second_metadata_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because data pool is already in use as metadata pool for 'second_fs'")

class TestFsNew(TestAdminCommands):
    """
    Test "ceph fs new" subcommand.
    """
    MDSS_REQUIRED = 3

    def test_fsnames_can_only_by_goodchars(self):
        n = 'test_fsnames_can_only_by_goodchars'
        metapoolname, datapoolname = n+'-testmetapool', n+'-testdatapool'
        badname = n+'badname@#'

        self.run_ceph_cmd('osd', 'pool', 'create', n+metapoolname)
        self.run_ceph_cmd('osd', 'pool', 'create', n+datapoolname)

        # test that fsname not with "goodchars" fails
        args = ['fs', 'new', badname, metapoolname, datapoolname]
        proc = self.run_ceph_cmd(args=args, stderr=StringIO(),
                                 check_status=False)
        self.assertIn('invalid chars', proc.stderr.getvalue().lower())

        self.run_ceph_cmd('osd', 'pool', 'rm', metapoolname,
                          metapoolname,
                          '--yes-i-really-really-mean-it-not-faking')
        self.run_ceph_cmd('osd', 'pool', 'rm', datapoolname,
                          datapoolname,
                          '--yes-i-really-really-mean-it-not-faking')

    def test_new_default_ec(self):
        """
        That a new file system warns/fails with an EC default data pool.
        """

        self.mount_a.umount_wait(require_clean=True)
        self.mds_cluster.delete_all_filesystems()
        n = "test_new_default_ec"
        self.setup_ec_pools(n)
        try:
            self.run_ceph_cmd('fs', 'new', n, n+"-meta", n+"-data")
        except CommandFailedError as e:
            if e.exitstatus == 22:
                pass
            else:
                raise
        else:
            raise RuntimeError("expected failure")

    def test_new_default_ec_force(self):
        """
        That a new file system succeeds with an EC default data pool with --force.
        """

        self.mount_a.umount_wait(require_clean=True)
        self.mds_cluster.delete_all_filesystems()
        n = "test_new_default_ec_force"
        self.setup_ec_pools(n)
        self.run_ceph_cmd('fs', 'new', n, n+"-meta", n+"-data", "--force")

    def test_new_default_ec_no_overwrite(self):
        """
        That a new file system fails with an EC default data pool without overwrite.
        """

        self.mount_a.umount_wait(require_clean=True)
        self.mds_cluster.delete_all_filesystems()
        n = "test_new_default_ec_no_overwrite"
        self.setup_ec_pools(n, overwrites=False)
        try:
            self.run_ceph_cmd('fs', 'new', n, n+"-meta", n+"-data")
        except CommandFailedError as e:
            if e.exitstatus == 22:
                pass
            else:
                raise
        else:
            raise RuntimeError("expected failure")
        # and even with --force !
        try:
            self.run_ceph_cmd('fs', 'new', n, n+"-meta", n+"-data", "--force")
        except CommandFailedError as e:
            if e.exitstatus == 22:
                pass
            else:
                raise
        else:
            raise RuntimeError("expected failure")

    def test_fs_new_pool_application_metadata(self):
        """
        That the application metadata set on the pools of a newly created filesystem are as expected.
        """
        self.mount_a.umount_wait(require_clean=True)
        self.mds_cluster.delete_all_filesystems()
        fs_name = "test_fs_new_pool_application"
        keys = ['metadata', 'data']
        pool_names = [fs_name+'-'+key for key in keys]
        mon_cmd = self.get_ceph_cmd_stdout
        for p in pool_names:
            mon_cmd('osd', 'pool', 'create', p, '--pg_num_min', str(self.fs.pg_num_min))
            mon_cmd('osd', 'pool', 'application', 'enable', p, 'cephfs')
        mon_cmd('fs', 'new', fs_name, pool_names[0], pool_names[1])
        for i in range(2):
            self.check_pool_application_metadata_key_value(
                pool_names[i], 'cephfs', keys[i], fs_name)

    def test_fs_new_with_specific_id(self):
        """
        That a file system can be created with a specific ID.
        """
        fs_name = "test_fs_specific_id"
        fscid = 100
        keys = ['metadata', 'data']
        pool_names = [fs_name+'-'+key for key in keys]
        for p in pool_names:
            self.run_ceph_cmd(f'osd pool create {p}')
        self.run_ceph_cmd(f'fs new {fs_name} {pool_names[0]} {pool_names[1]} --fscid  {fscid} --force')
        self.fs.status().get_fsmap(fscid)
        for i in range(2):
            self.check_pool_application_metadata_key_value(pool_names[i], 'cephfs', keys[i], fs_name)

    def test_fs_new_with_specific_id_idempotency(self):
        """
        That command to create file system with specific ID is idempotent.
        """
        fs_name = "test_fs_specific_id"
        fscid = 100
        keys = ['metadata', 'data']
        pool_names = [fs_name+'-'+key for key in keys]
        for p in pool_names:
            self.run_ceph_cmd(f'osd pool create {p}')
        self.run_ceph_cmd(f'fs new {fs_name} {pool_names[0]} {pool_names[1]} --fscid  {fscid} --force')
        self.run_ceph_cmd(f'fs new {fs_name} {pool_names[0]} {pool_names[1]} --fscid  {fscid} --force')
        self.fs.status().get_fsmap(fscid)

    def test_fs_new_with_specific_id_fails_without_force_flag(self):
        """
        That command to create file system with specific ID fails without '--force' flag.
        """
        fs_name = "test_fs_specific_id"
        fscid = 100
        keys = ['metadata', 'data']
        pool_names = [fs_name+'-'+key for key in keys]
        for p in pool_names:
            self.run_ceph_cmd(f'osd pool create {p}')
        try:
            self.run_ceph_cmd(f'fs new {fs_name} {pool_names[0]} {pool_names[1]} --fscid  {fscid}')
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                "invalid error code on creating a file system with specifc ID without --force flag")
        else:
            self.fail("expected creating file system with specific ID without '--force' flag to fail")

    def test_fs_new_with_specific_id_fails_already_in_use(self):
        """
        That creating file system with ID already in use fails.
        """
        fs_name = "test_fs_specific_id"
        # file system ID already in use
        fscid =  self.fs.status().map['filesystems'][0]['id']
        keys = ['metadata', 'data']
        pool_names = [fs_name+'-'+key for key in keys]
        for p in pool_names:
            self.run_ceph_cmd(f'osd pool create {p}')
        try:
            self.run_ceph_cmd(f'fs new {fs_name} {pool_names[0]} {pool_names[1]} --fscid  {fscid} --force')
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                "invalid error code on creating a file system with specifc ID that is already in use")
        else:
            self.fail("expected creating file system with ID already in use to fail")

    def test_fs_new_metadata_pool_already_in_use(self):
        """
        That creating file system with metadata pool already in use.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        second_fs = "second_fs"
        second_data_pool = "second_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', second_data_pool)

        # try to create new fs 'second_fs' with following configuration
        # metadata pool -> 'first_metadata_pool'
        # data pool -> 'second_data_pool'
        # Expecting EINVAL exit status because 'first_metadata_pool'
        # is already in use with 'first_fs'
        try:
            self.run_ceph_cmd('fs', 'new', second_fs, first_metadata_pool, second_data_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because metadata  pool is already in use for 'first_fs'")

    def test_fs_new_data_pool_already_in_use(self):
        """
        That creating file system with data pool already in use.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        second_fs = "second_fs"
        second_metadata_pool = "second_metadata_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', second_metadata_pool)

        # try to create new fs 'second_fs' with following configuration
        # metadata pool -> 'second_metadata_pool'
        # data pool -> 'first_data_pool'
        # Expecting EINVAL exit status because 'first_data_pool'
        # is already in use with 'first_fs'
        try:
            self.run_ceph_cmd('fs', 'new', second_fs, second_metadata_pool, first_data_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because data pool is already in use for 'first_fs'")

    def test_fs_new_metadata_and_data_pool_in_use_by_another_same_fs(self):
        """
        That creating file system with metadata and data pool which is already in use by another same fs.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        second_fs = "second_fs"

        # try to create new fs 'second_fs' with following configuration
        # metadata pool -> 'first_metadata_pool'
        # data pool -> 'first_data_pool'
        # Expecting EINVAL exit status because 'first_metadata_pool' and 'first_data_pool'
        # is already in use with 'first_fs'
        try:
            self.run_ceph_cmd('fs', 'new', second_fs, first_metadata_pool, first_data_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because metadata and data pool is already in use for 'first_fs'")

    def test_fs_new_metadata_and_data_pool_in_use_by_different_fs(self):
        """
        That creating file system with metadata and data pool which is already in use by different fs.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        # create second data pool, metadata pool and add with filesystem
        second_fs = "second_fs"
        second_metadata_pool = "second_metadata_pool"
        second_data_pool = "second_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', second_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', second_data_pool)
        self.run_ceph_cmd('fs', 'new', second_fs, second_metadata_pool, second_data_pool)

        third_fs = "third_fs"

        # try to create new fs 'third_fs' with following configuration
        # metadata pool -> 'first_metadata_pool'
        # data pool -> 'second_data_pool'
        # Expecting EINVAL exit status because 'first_metadata_pool' and 'second_data_pool'
        # is already in use with 'first_fs' and 'second_fs'
        try:
            self.run_ceph_cmd('fs', 'new', third_fs, first_metadata_pool, second_data_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because metadata and data pool is already in use for 'first_fs' and 'second_fs'")

    def test_fs_new_interchange_already_in_use_metadata_and_data_pool_of_same_fs(self):
        """
        That creating file system with interchanging metadata and data pool which is already in use by same fs.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        second_fs = "second_fs"

        # try to create new fs 'second_fs' with following configuration
        # metadata pool -> 'first_data_pool' (already used as data pool for 'first_fs')
        # data pool -> 'first_metadata_pool' (already used as metadata pool for 'first_fs')
        # Expecting EINVAL exit status because 'first_data_pool' and 'first_metadata_pool'
        # is already in use with 'first_fs'
        try:
            self.run_ceph_cmd('fs', 'new', second_fs, first_data_pool, first_metadata_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because metadata and data pool is already in use for 'first_fs'")

    def test_fs_new_interchange_already_in_use_metadata_and_data_pool_of_different_fs(self):
        """
        That creating file system with interchanging metadata and data pool which is already in use by defferent fs.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        # create second data pool, metadata pool and add with filesystem
        second_fs = "second_fs"
        second_metadata_pool = "second_metadata_pool"
        second_data_pool = "second_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', second_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', second_data_pool)
        self.run_ceph_cmd('fs', 'new', second_fs, second_metadata_pool, second_data_pool)

        third_fs = "third_fs"

        # try to create new fs 'third_fs' with following configuration
        # metadata pool -> 'first_data_pool' (already used as data pool for 'first_fs')
        # data pool -> 'second_metadata_pool' (already used as metadata pool for 'second_fs')
        # Expecting EINVAL exit status because 'first_data_pool' and 'second_metadata_pool'
        # is already in use with 'first_fs' and 'second_fs'
        try:
            self.run_ceph_cmd('fs', 'new', third_fs, first_data_pool, second_metadata_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because metadata and data pool is already in use for 'first_fs' and 'second_fs'")

    def test_fs_new_metadata_pool_already_in_use_with_rbd(self):
        """
        That creating new file system with metadata pool already used by rbd.
        """

        # create pool and initialise with rbd
        new_pool = "new_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', new_pool)
        self.ctx.cluster.run(args=['rbd', 'pool', 'init', new_pool])

        new_fs = "new_fs"
        new_data_pool = "new_data_pool"

        self.run_ceph_cmd('osd', 'pool', 'create', new_data_pool)

        # try to create new fs 'new_fs' with following configuration
        # metadata pool -> 'new_pool' (already used by rbd app)
        # data pool -> 'new_data_pool'
        # Expecting EINVAL exit status because 'new_pool' is already in use with 'rbd' app
        try:
            self.run_ceph_cmd('fs', 'new', new_fs, new_pool, new_data_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because metadata pool is already in use for rbd")

    def test_fs_new_data_pool_already_in_use_with_rbd(self):
        """
        That creating new file system with data pool already used by rbd.
        """

        # create pool and initialise with rbd
        new_pool = "new_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', new_pool)
        self.ctx.cluster.run(args=['rbd', 'pool', 'init', new_pool])

        new_fs = "new_fs"
        new_metadata_pool = "new_metadata_pool"

        self.run_ceph_cmd('osd', 'pool', 'create', new_metadata_pool)

        # try to create new fs 'new_fs' with following configuration
        # metadata pool -> 'new_metadata_pool'
        # data pool -> 'new_pool' (already used by rbd app)
        # Expecting EINVAL exit status because 'new_pool' is already in use with 'rbd' app
        try:
            self.run_ceph_cmd('fs', 'new', new_fs, new_metadata_pool, new_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because data pool is already in use for rbd")

class TestRenameCommand(TestAdminCommands):
    """
    Tests for rename command.
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 2

    def test_fs_rename(self):
        """
        That the file system can be renamed, and the application metadata set on its pools are as expected.
        """
        # Renaming the file system breaks this mount as the client uses
        # file system specific authorization. The client cannot read
        # or write even if the client's cephx ID caps are updated to access
        # the new file system name without the client being unmounted and
        # re-mounted.
        self.mount_a.umount_wait(require_clean=True)
        orig_fs_name = self.fs.name
        new_fs_name = 'new_cephfs'
        client_id = 'test_new_cephfs'

        self.run_ceph_cmd(f'fs fail {self.fs.name}')
        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session true')
        sleep(5)

        self.run_ceph_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')

        self.run_ceph_cmd(f'fs set {new_fs_name} joinable true')
        self.run_ceph_cmd(f'fs set {new_fs_name} refuse_client_session false')
        self.fs.wait_for_daemons()

        # authorize a cephx ID access to the renamed file system.
        # use the ID to write to the file system.
        self.fs.name = new_fs_name
        keyring = self.fs.authorize(client_id, ('/', 'rw'))
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=client_id,
                             client_keyring_path=keyring_path,
                             cephfs_mntpt='/',
                             cephfs_name=self.fs.name)
        filedata, filename = 'some data on fs', 'file_on_fs'
        filepath = os_path_join(self.mount_a.hostfs_mntpt, filename)
        self.mount_a.write_file(filepath, filedata)
        self.check_pool_application_metadata_key_value(
            self.fs.get_data_pool_name(), 'cephfs', 'data', new_fs_name)
        self.check_pool_application_metadata_key_value(
            self.fs.get_metadata_pool_name(), 'cephfs', 'metadata', new_fs_name)

        # cleanup
        self.mount_a.umount_wait()
        self.run_ceph_cmd(f'auth rm client.{client_id}')

    def test_fs_rename_idempotency(self):
        """
        That the file system rename operation is idempotent.
        """
        # Renaming the file system breaks this mount as the client uses
        # file system specific authorization.
        self.mount_a.umount_wait(require_clean=True)
        orig_fs_name = self.fs.name
        new_fs_name = 'new_cephfs'

        self.run_ceph_cmd(f'fs fail {self.fs.name}')
        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session true')
        sleep(5)

        self.run_ceph_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')
        self.run_ceph_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')

        self.run_ceph_cmd(f'fs set {new_fs_name} joinable true')
        self.run_ceph_cmd(f'fs set {new_fs_name} refuse_client_session false')
        self.fs.wait_for_daemons()

        # original file system name does not appear in `fs ls` command
        self.assertFalse(self.fs.exists())
        self.fs.name = new_fs_name
        self.assertTrue(self.fs.exists())

    def test_fs_rename_fs_new_fails_with_old_fsname_existing_pools(self):
        """
        That after renaming a file system, creating a file system with
        old name and existing FS pools fails.
        """
        # Renaming the file system breaks this mount as the client uses
        # file system specific authorization.
        self.mount_a.umount_wait(require_clean=True)
        orig_fs_name = self.fs.name
        new_fs_name = 'new_cephfs'
        data_pool = self.fs.get_data_pool_name()
        metadata_pool = self.fs.get_metadata_pool_name()

        self.run_ceph_cmd(f'fs fail {self.fs.name}')
        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session true')
        sleep(5)

        self.run_ceph_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')

        self.run_ceph_cmd(f'fs set {new_fs_name} joinable true')
        self.run_ceph_cmd(f'fs set {new_fs_name} refuse_client_session false')
        self.fs.wait_for_daemons()

        try:
            self.run_ceph_cmd(f"fs new {orig_fs_name} {metadata_pool} {data_pool}")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                "invalid error code on creating a new file system with old "
                "name and existing pools.")
        else:
            self.fail("expected creating new file system with old name and "
                      "existing pools to fail.")

        try:
            self.run_ceph_cmd(f"fs new {orig_fs_name} {metadata_pool} {data_pool} --force")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                "invalid error code on creating a new file system with old "
                "name, existing pools and --force flag.")
        else:
            self.fail("expected creating new file system with old name, "
                      "existing pools, and --force flag to fail.")

        try:
            self.run_ceph_cmd(f"fs new {orig_fs_name} {metadata_pool} {data_pool} "
                                 "--allow-dangerous-metadata-overlay")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                "invalid error code on creating a new file system with old name, "
                "existing pools and --allow-dangerous-metadata-overlay flag.")
        else:
            self.fail("expected creating new file system with old name, "
                      "existing pools, and --allow-dangerous-metadata-overlay flag to fail.")

    def test_fs_rename_fails_without_yes_i_really_mean_it_flag(self):
        """
        That renaming a file system without '--yes-i-really-mean-it' flag fails.
        """
        # Failing the file system breaks this mount
        self.mount_a.umount_wait(require_clean=True)

        self.run_ceph_cmd(f'fs fail {self.fs.name}')
        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session true')
        sleep(5)

        try:
            self.run_ceph_cmd(f"fs rename {self.fs.name} new_fs")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM,
                "invalid error code on renaming a file system without the  "
                "'--yes-i-really-mean-it' flag")
        else:
            self.fail("expected renaming of file system without the "
                      "'--yes-i-really-mean-it' flag to fail ")

        self.run_ceph_cmd(f'fs set {self.fs.name} joinable true')
        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session false')
        self.fs.wait_for_daemons()

    def test_fs_rename_fails_for_non_existent_fs(self):
        """
        That renaming a non-existent file system fails.
        """
        # Failing the file system breaks this mount
        self.mount_a.umount_wait(require_clean=True)

        self.run_ceph_cmd(f'fs fail {self.fs.name}')
        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session true')
        sleep(5)

        try:
            self.run_ceph_cmd("fs rename non_existent_fs new_fs --yes-i-really-mean-it")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on renaming a non-existent fs")
        else:
            self.fail("expected renaming of a non-existent file system to fail")
        self.run_ceph_cmd(f'fs set {self.fs.name} joinable true')
        self.fs.wait_for_daemons()
        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session false')

    def test_fs_rename_fails_new_name_already_in_use(self):
        """
        That renaming a file system fails if the new name refers to an existing file system.
        """
        self.fs2 = self.mds_cluster.newfs(name='cephfs2', create=True)

        # let's unmount the client before failing the FS
        self.mount_a.umount_wait(require_clean=True)

        self.run_ceph_cmd(f'fs fail {self.fs.name}')
        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session true')
        sleep(5)

        try:
            self.run_ceph_cmd(f"fs rename {self.fs.name} {self.fs2.name} --yes-i-really-mean-it")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                             "invalid error code on renaming to a fs name that is already in use")
        else:
            self.fail("expected renaming to a new file system name that is already in use to fail.")

        self.run_ceph_cmd(f'fs set {self.fs.name} joinable true')
        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session false')
        self.fs.wait_for_daemons()

    def test_fs_rename_fails_with_mirroring_enabled(self):
        """
        That renaming a file system fails if mirroring is enabled on it.
        """
        orig_fs_name = self.fs.name
        new_fs_name = 'new_cephfs'

        # let's unmount the client before failing the FS
        self.mount_a.umount_wait(require_clean=True)

        self.run_ceph_cmd(f'fs mirror enable {orig_fs_name}')
        self.run_ceph_cmd(f'fs fail {self.fs.name}')
        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session true')
        sleep(5)

        try:
            self.run_ceph_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM, "invalid error code on renaming a mirrored file system")
        else:
            self.fail("expected renaming of a mirrored file system to fail")

        self.run_ceph_cmd(f'fs mirror disable {orig_fs_name}')
        self.run_ceph_cmd(f'fs set {self.fs.name} joinable true')
        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session false')
        self.fs.wait_for_daemons()

    def test_rename_when_fs_is_online(self):
        '''
        Test that the command "ceph fs swap" command fails when first of the
        two of FSs isn't failed/down.
        '''
        client_id = 'test_new_cephfs'
        new_fs_name = 'new_cephfs'

        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session true')

        self.negtest_ceph_cmd(
            args=(f'fs rename {self.fs.name} {new_fs_name} '
                   '--yes-i-really-mean-it'),
            errmsgs=(f"CephFS '{self.fs.name}' is not offline. Before "
                      "renaming a CephFS, it must be marked as down. See "
                      "`ceph fs fail`."),
            retval=errno.EPERM)

        self.run_ceph_cmd(f'fs set {self.fs.name} refuse_client_session false')

        self.fs.getinfo()
        keyring = self.fs.authorize(client_id, ('/', 'rw'))
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=client_id,
                             client_keyring_path=keyring_path,
                             cephfs_mntpt='/',
                             cephfs_name=self.fs.name)

        self.check_pool_application_metadata_key_value(
            self.fs.get_data_pool_name(), 'cephfs', 'data', self.fs.name)
        self.check_pool_application_metadata_key_value(
            self.fs.get_metadata_pool_name(), 'cephfs', 'metadata',
            self.fs.name)

    def test_rename_when_clients_not_refused(self):
        '''
        Test that "ceph fs rename" fails when client_refuse_session is not
        set.
        '''
        self.mount_a.umount_wait(require_clean=True)

        self.run_ceph_cmd(f'fs fail {self.fs.name}')

        self.negtest_ceph_cmd(
            args=f"fs rename {self.fs.name} new_fs --yes-i-really-mean-it",
            errmsgs=(f"CephFS '{self.fs.name}' doesn't refuse clients. "
                      "Before renaming a CephFS, flag "
                      "'refuse_client_session' must be set. See "
                      "`ceph fs set`."),
            retval=errno.EPERM)

        self.run_ceph_cmd(f'fs fail {self.fs.name}')


class TestDump(CephFSTestCase):
    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 1

    def test_fs_dump_epoch(self):
        """
        That dumping a specific epoch works.
        """

        status1 = self.fs.status()
        status2 = self.fs.status(epoch=status1["epoch"]-1)
        self.assertEqual(status1["epoch"], status2["epoch"]+1)

    def test_fsmap_trim(self):
        """
        That the fsmap is trimmed normally.
        """

        paxos_service_trim_min = 25
        self.config_set('mon', 'paxos_service_trim_min', paxos_service_trim_min)
        mon_max_mdsmap_epochs = 20
        self.config_set('mon', 'mon_max_mdsmap_epochs', mon_max_mdsmap_epochs)

        status = self.fs.status()
        epoch = status["epoch"]

        # for N mutations
        mutations = paxos_service_trim_min + mon_max_mdsmap_epochs
        b = False
        for i in range(mutations):
            self.fs.set_joinable(b)
            b = not b

        sleep(10) # for tick/compaction

        try:
            self.fs.status(epoch=epoch)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT, "invalid error code when trying to fetch FSMap that was trimmed")
        else:
            self.fail("trimming did not occur as expected")

    def test_fsmap_force_trim(self):
        """
        That the fsmap is trimmed forcefully.
        """

        status = self.fs.status()
        epoch = status["epoch"]

        paxos_service_trim_min = 1
        self.config_set('mon', 'paxos_service_trim_min', paxos_service_trim_min)
        mon_mds_force_trim_to = epoch+1
        self.config_set('mon', 'mon_mds_force_trim_to', mon_mds_force_trim_to)

        # force a new fsmap
        self.fs.set_joinable(False)
        sleep(10) # for tick/compaction

        status = self.fs.status()
        log.debug(f"new epoch is {status['epoch']}")
        self.fs.status(epoch=epoch+1) # epoch+1 is not trimmed, may not == status["epoch"]

        try:
            self.fs.status(epoch=epoch)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT, "invalid error code when trying to fetch FSMap that was trimmed")
        else:
            self.fail("trimming did not occur as expected")


class TestRequiredClientFeatures(CephFSTestCase):
    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 1

    def test_required_client_features(self):
        """
        That `ceph fs required_client_features` command functions.
        """

        def is_required(index):
            out = self.get_ceph_cmd_stdout('fs', 'get', self.fs.name, '--format=json-pretty')
            features = json.loads(out)['mdsmap']['required_client_features']
            if "feature_{0}".format(index) in features:
                return True;
            return False;

        features = json.loads(self.get_ceph_cmd_stdout('fs', 'feature', 'ls', '--format=json-pretty'))
        self.assertGreater(len(features), 0);

        for f in features:
            self.fs.required_client_features('rm', str(f['index']))

        for f in features:
            index = f['index']
            feature = f['name']
            if feature == 'reserved':
                feature = str(index)

            if index % 3 == 0:
                continue;
            self.fs.required_client_features('add', feature)
            self.assertTrue(is_required(index))

            if index % 2 == 0:
                continue;
            self.fs.required_client_features('rm', feature)
            self.assertFalse(is_required(index))

    def test_required_client_feature_add_reserved(self):
        """
        That `ceph fs required_client_features X add reserved` fails.
        """

        p = self.fs.required_client_features('add', 'reserved', check_status=False, stderr=StringIO())
        self.assertIn('Invalid feature name', p.stderr.getvalue())

    def test_required_client_feature_rm_reserved(self):
        """
        That `ceph fs required_client_features X rm reserved` fails.
        """

        p = self.fs.required_client_features('rm', 'reserved', check_status=False, stderr=StringIO())
        self.assertIn('Invalid feature name', p.stderr.getvalue())

    def test_required_client_feature_add_reserved_bit(self):
        """
        That `ceph fs required_client_features X add <reserved_bit>` passes.
        """

        p = self.fs.required_client_features('add', '1', stderr=StringIO())
        self.assertIn("added feature 'reserved' to required_client_features", p.stderr.getvalue())

    def test_required_client_feature_rm_reserved_bit(self):
        """
        That `ceph fs required_client_features X rm <reserved_bit>` passes.
        """

        self.fs.required_client_features('add', '1')
        p = self.fs.required_client_features('rm', '1', stderr=StringIO())
        self.assertIn("removed feature 'reserved' from required_client_features", p.stderr.getvalue())

class TestCompatCommands(CephFSTestCase):
    """
    """

    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 3

    def test_add_compat(self):
        """
        Test adding a compat.
        """

        self.fs.fail()
        self.fs.add_compat(63, 'placeholder')
        mdsmap = self.fs.get_mds_map()
        self.assertIn("feature_63", mdsmap['compat']['compat'])

    def test_add_incompat(self):
        """
        Test adding an incompat.
        """

        self.fs.fail()
        self.fs.add_incompat(63, 'placeholder')
        mdsmap = self.fs.get_mds_map()
        log.info(f"{mdsmap}")
        self.assertIn("feature_63", mdsmap['compat']['incompat'])

    def test_rm_compat(self):
        """
        Test removing a compat.
        """

        self.fs.fail()
        self.fs.add_compat(63, 'placeholder')
        self.fs.rm_compat(63)
        mdsmap = self.fs.get_mds_map()
        self.assertNotIn("feature_63", mdsmap['compat']['compat'])

    def test_rm_incompat(self):
        """
        Test removing an incompat.
        """

        self.fs.fail()
        self.fs.add_incompat(63, 'placeholder')
        self.fs.rm_incompat(63)
        mdsmap = self.fs.get_mds_map()
        self.assertNotIn("feature_63", mdsmap['compat']['incompat'])

    def test_standby_compat(self):
        """
        That adding a compat does not prevent standbys from joining.
        """

        self.fs.fail()
        self.fs.add_compat(63, "placeholder")
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        mdsmap = self.fs.get_mds_map()
        self.assertIn("feature_63", mdsmap['compat']['compat'])

    def test_standby_incompat_reject(self):
        """
        That adding an incompat feature prevents incompatible daemons from joining.
        """

        self.fs.fail()
        self.fs.add_incompat(63, "placeholder")
        self.fs.set_joinable()
        try:
            self.fs.wait_for_daemons(timeout=60)
        except RuntimeError as e:
            if "Timed out waiting for MDS daemons to become healthy" in str(e):
                pass
            else:
                raise
        else:
            self.fail()

    def test_standby_incompat_upgrade(self):
        """
        That an MDS can upgrade the compat of a fs.
        """

        self.fs.fail()
        self.fs.rm_incompat(1)
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        mdsmap = self.fs.get_mds_map()
        self.assertIn("feature_1", mdsmap['compat']['incompat'])

    def test_standby_replay_not_upgradeable(self):
        """
        That the mons will not upgrade the MDSMap compat if standby-replay is
        enabled.
        """

        self.fs.fail()
        self.fs.rm_incompat(1)
        self.fs.set_allow_standby_replay(True)
        self.fs.set_joinable()
        try:
            self.fs.wait_for_daemons(timeout=60)
        except RuntimeError as e:
            if "Timed out waiting for MDS daemons to become healthy" in str(e):
                pass
            else:
                raise
        else:
            self.fail()

    def test_standby_incompat_reject_multifs(self):
        """
        Like test_standby_incompat_reject but with a second fs.
        """

        fs2 = self.mds_cluster.newfs(name="cephfs2", create=True)
        fs2.fail()
        fs2.add_incompat(63, 'placeholder')
        fs2.set_joinable()
        try:
            fs2.wait_for_daemons(timeout=60)
        except RuntimeError as e:
            if "Timed out waiting for MDS daemons to become healthy" in str(e):
                pass
            else:
                raise
        else:
            self.fail()
        # did self.fs lose MDS or standbys suicide?
        self.fs.wait_for_daemons()
        mdsmap = fs2.get_mds_map()
        self.assertIn("feature_63", mdsmap['compat']['incompat'])

class TestConfigCommands(CephFSTestCase):
    """
    Test that daemons and clients respond to the otherwise rarely-used
    runtime config modification operations.
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def test_ceph_config_show(self):
        """
        That I can successfully show MDS configuration.
        """

        names = self.fs.get_rank_names()
        for n in names:
            s = self.get_ceph_cmd_stdout("config", "show", "mds."+n)
            self.assertTrue("NAME" in s)
            self.assertTrue("mon_host" in s)


    def test_client_config(self):
        """
        That I can successfully issue asok "config set" commands

        :return:
        """

        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Test only applies to FUSE clients")

        test_key = "client_cache_size"
        test_val = "123"
        self.mount_a.admin_socket(['config', 'set', test_key, test_val])
        out = self.mount_a.admin_socket(['config', 'get', test_key])
        self.assertEqual(out[test_key], test_val)


    def test_mds_config_asok(self):
        test_key = "mds_max_purge_ops"
        test_val = "123"
        self.fs.mds_asok(['config', 'set', test_key, test_val])
        out = self.fs.mds_asok(['config', 'get', test_key])
        self.assertEqual(out[test_key], test_val)

    def test_mds_dump_cache_asok(self):
        cache_file = "cache_file"
        timeout = "1"
        self.fs.rank_asok(['dump', 'cache', cache_file, timeout])

    def test_mds_config_tell(self):
        test_key = "mds_max_purge_ops"
        test_val = "123"

        self.fs.rank_tell(['injectargs', "--{0}={1}".format(test_key, test_val)])

        # Read it back with asok because there is no `tell` equivalent
        out = self.fs.rank_tell(['config', 'get', test_key])
        self.assertEqual(out[test_key], test_val)


class TestMirroringCommands(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def _enable_mirroring(self, fs_name):
        self.run_ceph_cmd("fs", "mirror", "enable", fs_name)

    def _disable_mirroring(self, fs_name):
        self.run_ceph_cmd("fs", "mirror", "disable", fs_name)

    def _add_peer(self, fs_name, peer_spec, remote_fs_name):
        peer_uuid = str(uuid.uuid4())
        self.run_ceph_cmd("fs", "mirror", "peer_add", fs_name, peer_uuid, peer_spec, remote_fs_name)

    def _remove_peer(self, fs_name, peer_uuid):
        self.run_ceph_cmd("fs", "mirror", "peer_remove", fs_name, peer_uuid)

    def _verify_mirroring(self, fs_name, flag_str):
        status = self.fs.status()
        fs_map = status.get_fsmap_byname(fs_name)
        if flag_str == 'enabled':
            self.assertTrue('mirror_info' in fs_map)
        elif flag_str == 'disabled':
            self.assertTrue('mirror_info' not in fs_map)
        else:
            raise RuntimeError(f'invalid flag_str {flag_str}')

    def _get_peer_uuid(self, fs_name, peer_spec):
        status = self.fs.status()
        fs_map = status.get_fsmap_byname(fs_name)
        mirror_info = fs_map.get('mirror_info', None)
        self.assertTrue(mirror_info is not None)
        for peer_uuid, remote in mirror_info['peers'].items():
            client_name = remote['remote']['client_name']
            cluster_name = remote['remote']['cluster_name']
            spec = f'{client_name}@{cluster_name}'
            if spec == peer_spec:
                return peer_uuid
        return None

    def test_mirroring_command(self):
        """basic mirroring command test -- enable, disable mirroring on a
        filesystem"""
        self._enable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "enabled")
        self._disable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "disabled")

    def test_mirroring_peer_commands(self):
        """test adding and removing peers to a mirror enabled filesystem"""
        self._enable_mirroring(self.fs.name)
        self._add_peer(self.fs.name, "client.site-b@site-b", "fs_b")
        self._add_peer(self.fs.name, "client.site-c@site-c", "fs_c")
        self._verify_mirroring(self.fs.name, "enabled")
        uuid_peer_b = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        uuid_peer_c = self._get_peer_uuid(self.fs.name, "client.site-c@site-c")
        self.assertTrue(uuid_peer_b is not None)
        self.assertTrue(uuid_peer_c is not None)
        self._remove_peer(self.fs.name, uuid_peer_b)
        self._remove_peer(self.fs.name, uuid_peer_c)
        self._disable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "disabled")

    def test_mirroring_command_idempotency(self):
        """test to check idempotency of mirroring family of commands """
        self._enable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "enabled")
        self._enable_mirroring(self.fs.name)
        # add peer
        self._add_peer(self.fs.name, "client.site-b@site-b", "fs_b")
        uuid_peer_b1 = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        self.assertTrue(uuid_peer_b1 is not None)
        # adding the peer again should be idempotent
        self._add_peer(self.fs.name, "client.site-b@site-b", "fs_b")
        uuid_peer_b2 = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        self.assertTrue(uuid_peer_b2 is not None)
        self.assertTrue(uuid_peer_b1 == uuid_peer_b2)
        # remove peer
        self._remove_peer(self.fs.name, uuid_peer_b1)
        uuid_peer_b3 = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        self.assertTrue(uuid_peer_b3 is None)
        # removing the peer again should be idempotent
        self._remove_peer(self.fs.name, uuid_peer_b1)
        self._disable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "disabled")
        self._disable_mirroring(self.fs.name)

    def test_mirroring_disable_with_peers(self):
        """test disabling mirroring for a filesystem with active peers"""
        self._enable_mirroring(self.fs.name)
        self._add_peer(self.fs.name, "client.site-b@site-b", "fs_b")
        self._verify_mirroring(self.fs.name, "enabled")
        uuid_peer_b = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        self.assertTrue(uuid_peer_b is not None)
        self._disable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "disabled")
        # enable mirroring to check old peers
        self._enable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "enabled")
        # peer should be gone
        uuid_peer_b = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        self.assertTrue(uuid_peer_b is None)
        self._disable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "disabled")

    def test_mirroring_with_filesystem_reset(self):
        """test to verify mirroring state post filesystem reset"""
        self._enable_mirroring(self.fs.name)
        self._add_peer(self.fs.name, "client.site-b@site-b", "fs_b")
        self._verify_mirroring(self.fs.name, "enabled")
        uuid_peer_b = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        self.assertTrue(uuid_peer_b is not None)
        # reset filesystem
        self.fs.fail()
        self.fs.reset()
        self.fs.wait_for_daemons()
        self._verify_mirroring(self.fs.name, "disabled")


class TestFsAuthorize(CephFSTestCase):
    client_id = 'testuser'
    client_name = 'client.' + client_id
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 3

    def test_single_path_r(self):
        PERM = 'r'
        FS_AUTH_CAPS = (('/', PERM),)
        self.captester = CapTester(self.mount_a, '/')
        keyring = self.fs.authorize(self.client_id, FS_AUTH_CAPS)

        self._remount(keyring)
        self.captester.run_cap_tests(self.fs, self.client_id, PERM)

    def test_single_path_rw(self):
        PERM = 'rw'
        FS_AUTH_CAPS = (('/', PERM),)
        self.captester = CapTester(self.mount_a, '/')
        keyring = self.fs.authorize(self.client_id, FS_AUTH_CAPS)

        self._remount(keyring)
        self.captester.run_cap_tests(self.fs, self.client_id, PERM)

    def test_single_path_rootsquash(self):
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("only FUSE client has CEPHFS_FEATURE_MDS_AUTH_CAPS "
                          "needed to enforce root_squash MDS caps")

        PERM = 'rw'
        FS_AUTH_CAPS = (('/', PERM, 'root_squash'),)
        self.captester = CapTester(self.mount_a, '/')
        keyring = self.fs.authorize(self.client_id, FS_AUTH_CAPS)

        self._remount(keyring)
        # testing MDS caps...
        # Since root_squash is set in client caps, client can read but not
        # write even thought access level is set to "rw".
        self.captester.conduct_pos_test_for_read_caps()
        self.captester.conduct_pos_test_for_open_caps()
        self.captester.conduct_neg_test_for_write_caps(sudo_write=True)
        self.captester.conduct_neg_test_for_chown_caps()
        self.captester.conduct_neg_test_for_truncate_caps()

    def test_multifs_single_path_rootsquash(self):
        """
        Test root_squash with multi fs
        """
        self.skipTest('this test is broken ATM, see: '
                      'https://tracker.ceph.com/issues/66076.')

        self.fs1 = self.fs
        self.fs2 = self.mds_cluster.newfs('testcephfs2')
        self.mount_b.remount(cephfs_name=self.fs2.name)
        self.captester1 = CapTester(self.mount_a)
        self.captester2 = CapTester(self.mount_b)

        PERM = 'rw'
        FS_AUTH_CAPS = (('/', PERM, 'root_squash'),)

        self.fs1.authorize(self.client_id, FS_AUTH_CAPS)
        self.fs2.authorize(self.client_id, FS_AUTH_CAPS)
        keyring = self.fs.mon_manager.get_keyring(self.client_id)

        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=self.client_id,
                             client_keyring_path=keyring_path)
        # testing MDS caps...
        # Since root_squash is set in client caps, client can read but not
        # write even though access level is set to "rw" on both fses
        self.captester1.conduct_pos_test_for_read_caps()
        self.captester1.conduct_pos_test_for_open_caps()
        self.captester1.conduct_neg_test_for_write_caps(sudo_write=True)
        self.captester1.conduct_neg_test_for_chown_caps()
        self.captester1.conduct_neg_test_for_truncate_caps()

        keyring_path = self.mount_b.client_remote.mktemp(data=keyring)
        self.mount_b.remount(client_id=self.client_id,
                             client_keyring_path=keyring_path)
        self.captester2.conduct_pos_test_for_read_caps()
        self.captester2.conduct_pos_test_for_open_caps()
        self.captester2.conduct_neg_test_for_write_caps(sudo_write=True)
        self.captester2.conduct_neg_test_for_chown_caps()
        self.captester2.conduct_neg_test_for_truncate_caps()

    def test_multifs_rootsquash_nofeature(self):
        """
        That having root_squash on one fs doesn't prevent access to others.
        """

        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("only FUSE client has CEPHFS_FEATURE_MDS_AUTH_CAPS "
                          "needed to enforce root_squash MDS caps")

        self.fs1 = self.fs
        self.fs2 = self.mds_cluster.newfs('testcephfs2')

        self.mount_a.umount_wait()

        # Authorize client to fs1
        FS_AUTH_CAPS = (('/', 'rw'),)
        self.fs1.authorize(self.client_id, FS_AUTH_CAPS)

        FS_AUTH_CAPS = (('/', 'rw', 'root_squash'),)
        keyring = self.fs2.authorize(self.client_id, FS_AUTH_CAPS)

        CEPHFS_FEATURE_MDS_AUTH_CAPS_CHECK = 21
        # all but CEPHFS_FEATURE_MDS_AUTH_CAPS_CHECK
        features = ",".join([str(i) for i in range(CEPHFS_FEATURE_MDS_AUTH_CAPS_CHECK)])
        mntargs = [f"--client_debug_inject_features={features}"]

        # should succeed
        with self.assert_cluster_log("report clients with broken root_squash", present=False):
            keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
            self.mount_a.remount(client_id=self.client_id, client_keyring_path=keyring_path, mntargs=mntargs, cephfs_name=self.fs1.name)

        captester = CapTester(self.mount_a, '/')
        captester.conduct_pos_test_for_read_caps()
        captester.conduct_pos_test_for_open_caps()

    def test_rootsquash_nofeature(self):
        """
        That having root_squash on an fs without the feature bit raises a HEALTH_ERR warning.
        """

        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("only FUSE client has CEPHFS_FEATURE_MDS_AUTH_CAPS "
                          "needed to enforce root_squash MDS caps")

        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        FS_AUTH_CAPS = (('/', 'rw', 'root_squash'),)
        keyring = self.fs.authorize(self.client_id, FS_AUTH_CAPS)

        CEPHFS_FEATURE_MDS_AUTH_CAPS_CHECK = 21
        # all but CEPHFS_FEATURE_MDS_AUTH_CAPS_CHECK
        features = ",".join([str(i) for i in range(CEPHFS_FEATURE_MDS_AUTH_CAPS_CHECK)])
        mntargs = [f"--client_debug_inject_features={features}"]

        # should succeed
        with self.assert_cluster_log("with broken root_squash implementation"):
            keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
            self.mount_a.remount(client_id=self.client_id, client_keyring_path=keyring_path, mntargs=mntargs, cephfs_name=self.fs.name)
            self.wait_for_health("MDS_CLIENTS_BROKEN_ROOTSQUASH", 60)
            self.assertFalse(self.mount_a.is_blocked())

        self.mount_a.umount_wait()
        self.wait_for_health_clear(60)

    def test_rootsquash_nofeature_evict(self):
        """
        That having root_squash on an fs without the feature bit can be evicted.
        """

        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("only FUSE client has CEPHFS_FEATURE_MDS_AUTH_CAPS "
                          "needed to enforce root_squash MDS caps")

        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        FS_AUTH_CAPS = (('/', 'rw', 'root_squash'),)
        keyring = self.fs.authorize(self.client_id, FS_AUTH_CAPS)

        CEPHFS_FEATURE_MDS_AUTH_CAPS_CHECK = 21
        # all but CEPHFS_FEATURE_MDS_AUTH_CAPS_CHECK
        features = ",".join([str(i) for i in range(CEPHFS_FEATURE_MDS_AUTH_CAPS_CHECK)])
        mntargs = [f"--client_debug_inject_features={features}"]

        # should succeed
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=self.client_id, client_keyring_path=keyring_path, mntargs=mntargs, cephfs_name=self.fs.name)
        self.wait_for_health("MDS_CLIENTS_BROKEN_ROOTSQUASH", 60)

        self.fs.required_client_features("add", "client_mds_auth_caps")
        self.wait_for_health_clear(60)
        self.assertTrue(self.mount_a.is_blocked())


    def test_single_path_rootsquash_issue_56067(self):
        """
        That a FS client using root squash MDS caps allows non-root user to write data
        to a file. And after client remount, the non-root user can read the data that
        was previously written by it. https://tracker.ceph.com/issues/56067
        """
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("only FUSE client has CEPHFS_FEATURE_MDS_AUTH_CAPS "
                          "needed to enforce root_squash MDS caps")

        keyring = self.fs.authorize(self.client_id, ('/', 'rw', 'root_squash'))
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=self.client_id,
                             client_keyring_path=keyring_path,
                             cephfs_mntpt='/')
        filedata, filename = 'some data on fs 1', 'file_on_fs1'
        filepath = os_path_join(self.mount_a.hostfs_mntpt, filename)
        self.mount_a.write_file(filepath, filedata)

        self.mount_a.remount(client_id=self.client_id,
                             client_keyring_path=keyring_path,
                             cephfs_mntpt='/')
        if filepath.find(self.mount_a.hostfs_mntpt) != -1:
            contents = self.mount_a.read_file(filepath)
            self.assertEqual(filedata, contents)

    def test_single_path_authorize_on_nonalphanumeric_fsname(self):
        """
        That fs authorize command works on filesystems with names having [_.-]
        characters
        """
        self.mount_a.umount_wait(require_clean=True)
        # let's unmount both client before deleting the FS
        self.mount_b.umount_wait(require_clean=True)
        self.mds_cluster.delete_all_filesystems()
        fs_name = "cephfs-_."
        self.fs = self.mds_cluster.newfs(name=fs_name)
        self.fs.wait_for_daemons()
        self.run_ceph_cmd(f'auth caps client.{self.mount_a.client_id} '
                          f'mon "allow r" '
                          f'osd "allow rw pool={self.fs.get_data_pool_name()}" '
                          f'mds allow')
        self.mount_a.remount(cephfs_name=self.fs.name)
        PERM = 'rw'
        FS_AUTH_CAPS = (('/', PERM),)
        self.captester = CapTester(self.mount_a, '/')
        keyring = self.fs.authorize(self.client_id, FS_AUTH_CAPS)

        self._remount(keyring)
        self.captester.run_mds_cap_tests(PERM)

    def test_fs_read_and_single_path_rw(self):
        """
        Tests the file creation using 'touch' cmd on a specific path
        which has 'rw' caps and 'r' caps on the rest of the fs.

        The mds auth caps with 'rw' caps on a specific path and 'r' caps
        on the rest of the fs has an issue. The file creation using 'touch'
        cmd on the fuse client used to fail while doing setattr.
        Please see https://tracker.ceph.com/issues/67212

        The new file creation test using 'touch' cmd is added to
        'MdsCapTester.run_mds_cap_tests' which eventually gets
        called by '_remount_and_run_tests'
        """
        FS_AUTH_CAPS = (('/', 'r'), ('/dir2', 'rw'))
        self.mount_a.run_shell('mkdir -p ./dir2')
        self.captesters = (CapTester(self.mount_a, '/'),
                           CapTester(self.mount_a, '/dir2'))
        keyring = self.fs.authorize(self.client_id, FS_AUTH_CAPS)

        self._remount_and_run_tests(FS_AUTH_CAPS, keyring)

    def test_multiple_path_r(self):
        PERM = 'r'
        FS_AUTH_CAPS = (('/dir1/dir12', PERM), ('/dir2/dir22', PERM))
        for c in FS_AUTH_CAPS:
            self.mount_a.run_shell(f'mkdir -p .{c[0]}')
        self.captesters = (CapTester(self.mount_a, '/dir1/dir12'),
                           CapTester(self.mount_a, '/dir2/dir22'))
        keyring = self.fs.authorize(self.client_id, FS_AUTH_CAPS)

        self._remount_and_run_tests(FS_AUTH_CAPS, keyring)

    def test_multiple_path_rw(self):
        PERM = 'rw'
        FS_AUTH_CAPS = (('/dir1/dir12', PERM), ('/dir2/dir22', PERM))
        for c in FS_AUTH_CAPS:
            self.mount_a.run_shell(f'mkdir -p .{c[0]}')
        self.captesters = (CapTester(self.mount_a, '/dir1/dir12'),
                           CapTester(self.mount_a, '/dir2/dir22'))
        keyring = self.fs.authorize(self.client_id, FS_AUTH_CAPS)

        self._remount_and_run_tests(FS_AUTH_CAPS, keyring)

    def test_when_MDS_caps_needs_update_but_others_dont(self):
        '''
        Test that the command "ceph fs authorize" successfully updates MDS
        caps even when MON and OSD caps don't need an update.

        Tests: https://tracker.ceph.com/issues/64182

        In this test we run -

            ceph fs authorize cephfs1 client.x / rw
            ceph fs authorize cephfs2 client.x / rw
            ceph fs authorize cephfs2 client.x /dir1 rw

        The first command will create the keyring by adding the MDS cap for
        root path & MON and OSD caps with name of the FS name (say "cephfs1").
        Second command will update the all of client's caps -- MON, OSD and
        MDS caps to add caps for 2nd CephFS. The third command doesn't need
        to add MON and OSD caps since cap for "cephfs2" has been granted
        already. Thus, third command only need to update the MDS cap, thus
        testing the bug under consideration here.
        '''
        PERM = 'rw'
        DIR = 'dir1'

        self.fs2 = self.mds_cluster.newfs(name='cephfs2', create=True)
        self.mount_b.remount(cephfs_name=self.fs2.name)
        self.mount_b.run_shell(f'mkdir {DIR}')
        self.captesters = (CapTester(self.mount_a, '/'),
                           CapTester(self.mount_b, '/'),
                           CapTester(self.mount_b, f'/{DIR}'))

        FS_AUTH_CAPS = (('/', PERM), ('/', PERM), (DIR, PERM))
        keyring = self.fs.authorize(self.client_id, FS_AUTH_CAPS[0])
        keyring = self.fs2.authorize(self.client_id, FS_AUTH_CAPS[1])

        # if the following block of code pass it implies that
        # https://tracker.ceph.com/issues/64182 has been fixed
        keyring = self.fs2.authorize(self.client_id, FS_AUTH_CAPS[2])
        mdscaps = ('caps mds = "'
                   f'allow {PERM} fsname={self.fs.name}, '
                   f'allow {PERM} fsname={self.fs2.name}, '
                   f'allow {PERM} fsname={self.fs2.name} path={DIR}')
        self.assertIn(mdscaps, keyring)

        self._remount_and_run_tests_for_cap(
            FS_AUTH_CAPS[0], self.captesters[0], self.fs.name, self.mount_a,
            keyring)
        self._remount_and_run_tests_for_cap(
            FS_AUTH_CAPS[1], self.captesters[1], self.fs2.name, self.mount_b,
            keyring)
        self._remount_and_run_tests_for_cap(
            FS_AUTH_CAPS[2], self.captesters[2], self.fs2.name, self.mount_b,
            keyring)

    def test_adding_multiple_caps(self):
        '''
        Test that the command "ceph fs authorize" is successful in updating
        the entity's caps when multiple caps are passed to it in one go.

        Tests: https://tracker.ceph.com/issues/64127
        Tests: https://tracker.ceph.com/issues/64417
        '''
        DIR = 'dir1'
        self.mount_a.run_shell(f'mkdir {DIR}')
        self.captesters = (CapTester(self.mount_a, '/'),
                           CapTester(self.mount_a, f'/{DIR}'))
        self.fs.authorize(self.client_id, ('/', 'rw'))

        FS_AUTH_CAPS = ('/', 'rw', 'root_squash'), (f'/{DIR}', 'rw' )
        # The fact that following line passes means
        # https://tracker.ceph.com/issues/64127 has been fixed
        keyring = self.fs.authorize(self.client_id, FS_AUTH_CAPS)

        # The fact that following lines passes means
        # https://tracker.ceph.com/issues/64417 has been fixed.
        mdscaps = (f'allow rw fsname={self.fs.name} root_squash, '
                   f'allow rw fsname={self.fs.name} path={DIR}')
        self.assertIn(mdscaps, keyring)

        self._remount_and_run_tests(FS_AUTH_CAPS, keyring)

    def _remount_and_run_tests(self, fs_auth_caps, keyring):
        '''
        This method is specifically designed to meet needs of most of the
        test case in this class. Following are assumptions made here -

        1. CephFS to be mounted is self.fs
        2. Mount object to be used is self.mount_a
        3. Keyring file will be created on the host specified in self.mount_a.
        4. CephFS dir that will serve as root is PATH component of particular
           cap from FS_AUTH_CAPS.
        '''
        for i, c in enumerate(fs_auth_caps):
            self.assertIn(i, (0, 1))
            PATH = c[0]
            PERM = c[1]
            self._remount(keyring, PATH)
            # actual tests...
            self.captesters[i].run_cap_tests(self.fs, self.client_id, PERM,
                                             PATH)

    def _remount(self, keyring, path='/'):
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=self.client_id,
                             client_keyring_path=keyring_path,
                             cephfs_mntpt=path)

    def _remount_and_run_tests_for_cap(self, cap, captester, fsname, mount,
                                       keyring):
        PATH = cap[0]
        PERM = cap[1]

        cephfs_mntpt = os_path_join('/', PATH)
        keyring_path = mount.client_remote.mktemp(data=keyring)
        mount.remount(client_id=self.client_id, cephfs_mntpt=cephfs_mntpt,
                      cephfs_name=fsname, client_keyring_path=keyring_path)

        captester.run_cap_tests(self.fs, self.client_id, PERM, PATH)

    def tearDown(self):
        self.mount_a.umount_wait()
        self.run_ceph_cmd(f'auth rm {self.client_name}')

        super(type(self), self).tearDown()


class TestFsAuthorizeUpdate(CephFSTestCase):
    client_id = 'testuser'
    client_name = f'client.{client_id}'
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 3

    ######################################
    # cases where "fs authorize" adds caps
    ######################################

    def test_add_caps_for_another_FS(self):
        """
        Test that "ceph fs authorize" adds caps for a second FS to a keyring
        that already had caps for first FS.
        """
        self.fs1 = self.fs
        self.fs2 = self.mds_cluster.newfs('testcephfs2')
        self.mount_b.remount(cephfs_name=self.fs2.name)
        self.captesters = (CapTester(self.mount_a), CapTester(self.mount_b))
        self.fs1.authorize(self.client_id, ('/', 'rw'))

        ########### testing begins here.
        TEST_PERM = 'rw'
        self.fs2.authorize(self.client_id, ('/', TEST_PERM,))
        keyring = self.fs.mon_manager.get_keyring(self.client_id)
        moncap = gen_mon_cap_str((('r', self.fs1.name),
                                  ('r', self.fs2.name)))
        osdcap = gen_osd_cap_str(((TEST_PERM, self.fs1.name),
                                  (TEST_PERM, self.fs2.name)))
        mdscap = gen_mds_cap_str(((TEST_PERM, self.fs1.name),
                                  (TEST_PERM, self.fs2.name)))
        for cap in (moncap, osdcap, mdscap):
            self.assertIn(cap, keyring)
        self._remount(self.mount_a, self.fs1.name, keyring)
        self._remount(self.mount_b, self.fs2.name, keyring)
        self.captesters[0].run_cap_tests(self.fs1, self.client_id, TEST_PERM)
        self.captesters[0].run_cap_tests(self.fs1, self.client_id, TEST_PERM)

    def test_add_caps_for_same_FS_diff_path(self):
        '''
        Test that "ceph fs authorze" grants a new cap when it is run for a
        second time for same client and same FS but with a differnt paths.

        In other words, running following two commands -
        $ ceph fs authorize a client.x1 /dir1 rw
        $ ceph fs authorize a client.x1 /dir2 rw

        Should produce following MDS caps -
        caps mon = "allow r fsname=a"
        caps osd = "allow rw tag cephfs data=a"
        caps mds = "allow rw fsname=a path=dir1, allow rw fsname=a path=dir2"
        '''
        PERM, PATH1, PATH2 = 'rw', 'dir1', 'dir2'
        self.mount_a.run_shell('mkdir dir1 dir2')
        self.captester_a = CapTester(self.mount_a, PATH1)
        self.captester_b = CapTester(self.mount_b, PATH2)
        self.fs.authorize(self.client_id, ('/', PERM))

        ########## testing begins here.
        self.fs.authorize(self.client_id, (PATH1, PERM))
        keyring = self.fs.mon_manager.get_keyring(self.client_id)
        moncap = gen_mon_cap_str((('r', self.fs.name),))
        osdcap = gen_osd_cap_str(((PERM, self.fs.name),))
        mdscap = gen_mds_cap_str(((PERM, self.fs.name, PATH1),))
        for cap in (moncap, osdcap, mdscap):
            self.assertIn(cap, keyring)
        self._remount(self.mount_a, self.fs.name, keyring, PATH1)
        self.captester_a.run_cap_tests(self.fs, self.client_id, PERM, PATH1)

        ########### testing once more
        self.fs.authorize(self.client_id, (PATH2, PERM))
        keyring = self.fs.mon_manager.get_keyring(self.client_id)
        moncap = gen_mon_cap_str((('r', self.fs.name),))
        osdcap = gen_osd_cap_str(((PERM, self.fs.name),))
        mdscap = gen_mds_cap_str(((PERM, self.fs.name, PATH1),
                                  (PERM, self.fs.name, PATH2)))
        for cap in (moncap, osdcap, mdscap):
            self.assertIn(cap, keyring)
        self._remount(self.mount_b, self.fs.name, keyring, PATH2)
        self.captester_b.run_cap_tests(self.fs, self.client_id, PERM, PATH2)

    def test_add_caps_for_client_with_no_caps(self):
        """
        Test that "ceph fs authorize" adds caps to the keyring when the
        entity already has a keyring but it contains no caps.
        """
        TEST_PERM = 'rw'
        self.captester = CapTester(self.mount_a)
        self.run_ceph_cmd(f'auth add {self.client_name}')

        ######## testing begins here.
        self.fs.authorize(self.client_id, ('/', TEST_PERM))
        keyring = self.fs.mon_manager.get_keyring(self.client_id)
        moncap = gen_mon_cap_str((('r', self.fs.name,),))
        osdcap = gen_osd_cap_str(((TEST_PERM, self.fs.name),))
        mdscap = gen_mds_cap_str(((TEST_PERM, self.fs.name),))
        for cap in (moncap, osdcap, mdscap):
            self.assertIn(cap, keyring)
        self._remount(self.mount_a, self.fs.name, keyring)
        self.captester.run_cap_tests(self.fs, self.client_id, TEST_PERM)


    #########################################
    # cases where "fs authorize" changes caps
    #########################################

    def test_change_perms(self):
        """
        Test that "ceph fs authorize" updates the caps for a FS when the caps
        for that FS were already present in that keyring.
        """
        OLD_PERM = 'rw'
        NEW_PERM = 'r'
        self.captester = CapTester(self.mount_a)

        self.fs.authorize(self.client_id, ('/', OLD_PERM))
        ########### testing begins here
        self.fs.authorize(self.client_id, ('/', NEW_PERM))
        keyring = self.fs.mon_manager.get_keyring(self.client_id)
        moncap = gen_mon_cap_str((('r', self.fs.name,),))
        osdcap = gen_osd_cap_str(((NEW_PERM, self.fs.name),))
        mdscap = gen_mds_cap_str(((NEW_PERM, self.fs.name),))
        for cap in (moncap, osdcap, mdscap):
            self.assertIn(cap, keyring)
        self._remount(self.mount_a, self.fs.name, keyring)
        self.captester.run_cap_tests(self.fs, self.client_id, NEW_PERM)

    ################################################
    # Cases where fs authorize maintains idempotency
    ################################################

    def test_idem_caps_passed_same_as_current_caps(self):
        """
        Test that "ceph fs authorize" exits with the keyring on stdout and the
        expected error message on stderr when caps supplied to the subcommand
        are already present in the entity's keyring.
        """
        PERM = 'rw'
        self.captester = CapTester(self.mount_a)
        self.fs.authorize(self.client_id, ('/', PERM))
        keyring1 = self.fs.mon_manager.get_keyring(self.client_id)

        ############# testing begins here.
        proc = self.fs.mon_manager.run_cluster_cmd(
            args=f'fs authorize {self.fs.name} {self.client_name} / {PERM}',
            stdout=StringIO(), stderr=StringIO())
        errmsg = proc.stderr.getvalue()
        self.assertIn(f'no update for caps of {self.client_name}', errmsg)

        keyring2 = self.fs.mon_manager.get_keyring(self.client_id)
        self.assertIn(keyring1, keyring2)

        self._remount(self.mount_a, self.fs.name, keyring2)
        self.captester.run_cap_tests(self.fs, self.client_id, PERM)

    def test_idem_unaffected_root_squash(self):
        """
        Test that "root_squash" is not deleted from MDS caps when user runs
        "fs authorize" a second time passing same FS name and path but not
        with "root_squash"
        In other words,
        $ ceph fs authorize a client.x / rw root_squash
        [client.x]
        key = AQCvsyVkiDJZBBAAn1ClsPKvTfrCkUs01Eh8og==
        $ ceph auth get client.x
        [client.x]
                key = AQD61CVkcA1QCRAAd0XYqPbHvcc+lpUAuc6Vcw==
                caps mds = "allow rw fsname=a root_squash"
                caps mon = "allow r fsname=a"
                caps osd = "allow rw tag cephfs data=a"
        $ ceph fs authorize a client.x / rw
        $ ceph auth get client.x
        [client.x]
                key = AQD61CVkcA1QCRAAd0XYqPbHvcc+lpUAuc6Vcw==
                caps mds = "allow rw fsname=a root_squash"
                caps mon = "allow r fsname=a"
                caps osd = "allow rw tag cephfs data=a"
        """
        PERM, PATH = 'rw', 'dir1'
        self.mount_a.run_shell(f'mkdir {PATH}')
        self.captester = CapTester(self.mount_a, PATH)
        self.fs.authorize(self.client_id, (PATH, PERM, 'root_squash'))

        ############# testing begins here.
        self.fs.authorize(self.client_id, (PATH, PERM))
        keyring = self.fs.mon_manager.get_keyring(self.client_id)
        moncap = gen_mon_cap_str((('r', self.fs.name,),))
        osdcap = gen_osd_cap_str(((PERM, self.fs.name),))
        mdscap = gen_mds_cap_str(((PERM, self.fs.name, PATH, True),))
        for cap in (moncap, osdcap, mdscap):
            self.assertIn(cap, keyring)

    def _get_uid(self):
        return self.mount_a.client_remote.run(
            args='id -u', stdout=StringIO()).stdout.getvalue().strip()

    def _get_gid(self):
        return self.mount_a.client_remote.run(
            args='id -g', stdout=StringIO()).stdout.getvalue().strip()

    def test_idem_unaffected_uid(self):
        '''
        1. Create a client with caps that has FS name and UID in it.
        2. Run "ceph fs authorize" command for that client with same FS name.
        3. Test that UID (as well as any other part of cap) is unaffected,
           i.e. same as before.
        '''
        PERM, UID = 'rw', self._get_uid()
        self.captester = CapTester(self.mount_a)
        moncap = gen_mon_cap_str((('r', self.fs.name,),))
        osdcap = gen_osd_cap_str(((PERM, self.fs.name),))
        mdscap = f'allow rw uid={UID}'
        self.fs.mon_manager.run_cluster_cmd(
            args=(f'auth add {self.client_name} mon "{moncap}" '
                  f'osd "{osdcap}" mds "{mdscap}"'))

        ############# testing begins here.
        self.fs.authorize(self.client_id, ('/', PERM))
        keyring = self.fs.mon_manager.get_keyring(self.client_id)
        for cap in (moncap, osdcap, mdscap):
            self.assertIn(cap, keyring)
        self._remount(self.mount_a, self.fs.name, keyring)
        self.captester.run_cap_tests(self.fs, self.client_id, PERM)

    def test_idem_unaffected_gids(self):
        '''
        1. Create a client with caps that has FS name, UID and GID in it.
        2. Run "ceph fs authorize" command for that client with same FS name.
        3. Test that GID (as well as any other part of cap) is unaffected,
           i.e. same as before.
        '''
        PERM, UID, GID = 'rw', self._get_uid(), self._get_gid()
        self.captester = CapTester(self.mount_a)
        moncap = gen_mon_cap_str((('r', self.fs.name),))
        osdcap = gen_osd_cap_str(((PERM, self.fs.name),))
        mdscap = f'allow rw uid={UID} gids={GID}'
        self.fs.mon_manager.run_cluster_cmd(
            args=(f'auth add {self.client_name} mon "{moncap}" '
                  f'osd "{osdcap}" mds "{mdscap}"'))

        ############# testing begins here.
        self.fs.authorize(self.client_id, ('/', PERM))
        keyring = self.fs.mon_manager.get_keyring(self.client_id)
        for cap in (moncap, osdcap, mdscap):
            self.assertIn(cap, keyring)
        self._remount(self.mount_a, self.fs.name, keyring)
        self.captester.run_cap_tests(self.fs, self.client_id, PERM)

    def test_idem_unaffected_gids_multiple(self):
        '''
        1. Create client with caps with FS name, UID & multiple GIDs in it.
        2. Run "ceph fs authorize" command for that client with same FS name.
        3. Test that multiple GIDs (as well as any other part of cap) is
           unaffected, i.e. same as before.
        '''
        PERM, UID = 'rw', self._get_uid()
        gids = [int(self._get_gid()), 1001, 1002]
        # Apparently code for MDS caps always arranges GID in ascending
        # order. Let's sort so that latter cap string matches with keyring.
        gids.sort()
        gids = f'{gids[0]},{gids[1]},{gids[2]}'
        self.captester = CapTester(self.mount_a)
        moncap = gen_mon_cap_str((('r', self.fs.name),))
        osdcap = gen_osd_cap_str(((PERM, self.fs.name),))
        mdscap = f'allow rw uid={UID} gids={gids}'
        self.fs.mon_manager.run_cluster_cmd(
            args=(f'auth add {self.client_name} mon "{moncap}" '
                  f'osd "{osdcap}" mds "{mdscap}"'))

        ############# testing begins here.
        self.fs.authorize(self.client_id, ('/', PERM))
        keyring = self.fs.mon_manager.get_keyring(self.client_id)
        for cap in (moncap, osdcap, mdscap):
            self.assertIn(cap, keyring)
        self._remount(self.mount_a, self.fs.name, keyring)
        self.captester.run_cap_tests(self.fs, self.client_id, PERM)

    def _remount(self, mount_x, fsname, keyring, cephfs_mntpt='/'):
        if len(cephfs_mntpt) > 1 and cephfs_mntpt[0] != '/':
            cephfs_mntpt = '/' + cephfs_mntpt
        keyring_path = mount_x.client_remote.mktemp(data=keyring)
        mount_x.remount(client_id=self.client_id,
                        client_keyring_path=keyring_path,
                        cephfs_mntpt=cephfs_mntpt, cephfs_name=fsname)


class TestAdminCommandIdempotency(CephFSTestCase):
    """
    Tests for administration command idempotency.
    """

    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 1

    def test_rm_idempotency(self):
        """
        That a removing a fs twice is idempotent.
        """

        data_pools = self.fs.get_data_pool_names(refresh=True)
        self.fs.fail()
        self.fs.rm()
        try:
            self.fs.get_mds_map()
        except FSMissing:
            pass
        else:
            self.fail("get_mds_map should raise")
        p = self.fs.rm()
        self.assertIn("does not exist", p.stderr.getvalue())
        self.fs.remove_pools(data_pools)


class TestAdminCommandDumpTree(CephFSTestCase):
    """
    Tests for administration command subtrees.
    """

    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 1

    def test_dump_subtrees(self):
        """
        Dump all the subtrees to make sure the MDS daemon won't crash.
        """

        subtrees = self.fs.mds_asok(['get', 'subtrees'])
        log.info(f"dumping {len(subtrees)} subtrees:")
        for subtree in subtrees:
            log.info(f"  subtree: '{subtree['dir']['path']}'")
            self.fs.mds_asok(['dump', 'tree', subtree['dir']['path']])

        log.info("dumping 2 special subtrees:")
        log.info("  subtree: '/'")
        self.fs.mds_asok(['dump', 'tree', '/'])
        log.info("  subtree: '~mdsdir'")
        self.fs.mds_asok(['dump', 'tree', '~mdsdir'])

class TestAdminCommandDumpLoads(CephFSTestCase):
    """
    Tests for administration command dump loads.
    """

    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 1

    def test_dump_loads(self):
        """
        make sure depth limit param is considered when dump loads for a MDS daemon.
        """

        log.info("dumping loads")
        loads = self.fs.mds_asok(['dump', 'loads', '1'])
        self.assertIsNotNone(loads)
        self.assertIn("dirfrags", loads)
        for d in loads["dirfrags"]:
            self.assertLessEqual(d["path"].count("/"), 1)

class TestFsBalRankMask(CephFSTestCase):
    """
    Tests ceph fs set <fs_name> bal_rank_mask
    """

    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 2

    def test_bal_rank_mask(self):
        """
        check whether a specified bal_rank_mask value is valid or not.
        """
        bal_rank_mask = '0x0'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = '0'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = '-1'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = 'all'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = '0x1'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = '1'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = 'f0'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = 'ab'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = '0xfff0'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        MAX_MDS = 256
        bal_rank_mask = '0x' + 'f' * int(MAX_MDS / 4)
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = ''
        log.info("set bal_rank_mask to empty string")
        try:
            self.fs.set_bal_rank_mask(bal_rank_mask)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)

        bal_rank_mask = '0x1' + 'f' * int(MAX_MDS / 4)
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        try:
            self.fs.set_bal_rank_mask(bal_rank_mask)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)


class TestPermErrMsg(CephFSTestCase):

    CLIENT_NAME = 'client.testuser'
    FS1_NAME, FS2_NAME, FS3_NAME = 'abcd', 'efgh', 'ijkl'

    EXPECTED_ERRNO = 22
    EXPECTED_ERRMSG = ("Permission flags in MDS capability string must be '*' "
                       "or 'all' or must start with 'r'")

    MONCAP = f'allow r fsname={FS1_NAME}'
    OSDCAP = f'allow rw tag cephfs data={FS1_NAME}'
    MDSCAPS = [
        'allow w',
        f'allow w fsname={FS1_NAME}',

        f'allow rw fsname={FS1_NAME}, allow w fsname={FS2_NAME}',
        f'allow w fsname={FS1_NAME}, allow rw fsname={FS2_NAME}',
        f'allow w fsname={FS1_NAME}, allow w fsname={FS2_NAME}',

        (f'allow rw fsname={FS1_NAME}, allow rw fsname={FS2_NAME}, allow '
         f'w fsname={FS3_NAME}'),

        # without space after comma
        f'allow rw fsname={FS1_NAME},allow w fsname={FS2_NAME}',


        'allow wr',
        f'allow wr fsname={FS1_NAME}',

        f'allow rw fsname={FS1_NAME}, allow wr fsname={FS2_NAME}',
        f'allow wr fsname={FS1_NAME}, allow rw fsname={FS2_NAME}',
        f'allow wr fsname={FS1_NAME}, allow wr fsname={FS2_NAME}',

        (f'allow rw fsname={FS1_NAME}, allow rw fsname={FS2_NAME}, allow '
         f'wr fsname={FS3_NAME}'),

        # without space after comma
        f'allow rw fsname={FS1_NAME},allow wr fsname={FS2_NAME}']

    def _negtestcmd(self, SUBCMD, MDSCAP):
        return self.negtest_ceph_cmd(
            args=(f'{SUBCMD} {self.CLIENT_NAME} '
                  f'mon "{self.MONCAP}" osd "{self.OSDCAP}" mds "{MDSCAP}"'),
            retval=self.EXPECTED_ERRNO, errmsgs=self.EXPECTED_ERRMSG)

    def test_auth_add(self):
        for mdscap in self.MDSCAPS:
            self._negtestcmd('auth add', mdscap)

    def test_auth_caps(self):
        for mdscap in self.MDSCAPS:
            self.fs.mon_manager.run_cluster_cmd(
                args=f'auth add {self.CLIENT_NAME}')

            self._negtestcmd('auth caps', mdscap)

            self.fs.mon_manager.run_cluster_cmd(
                args=f'auth rm {self.CLIENT_NAME}')

    def test_auth_get_or_create(self):
        for mdscap in self.MDSCAPS:
            self._negtestcmd('auth get-or-create', mdscap)

    def test_auth_get_or_create_key(self):
        for mdscap in self.MDSCAPS:
            self._negtestcmd('auth get-or-create-key', mdscap)

    def test_fs_authorize(self):
        for wrong_perm in ('w', 'wr'):
            self.negtest_ceph_cmd(
                args=(f'fs authorize {self.fs.name} {self.CLIENT_NAME} / '
                      f'{wrong_perm}'), retval=self.EXPECTED_ERRNO,
                errmsgs=self.EXPECTED_ERRMSG)


class TestFSFail(TestAdminCommands):

    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 1

    def test_with_health_warn_cache_oversized(self):
        '''
        Test that, when health warning MDS_CACHE_OVERSIZE is present for an
        MDS, command "ceph fs fail" fails without confirmation flag and passes
        when confirmation flag is passed.
        '''
        health_warn = 'MDS_CACHE_OVERSIZED'
        self.gen_health_warn_mds_cache_oversized()

        # actual testing begins now.
        self.negtest_ceph_cmd(args=f'fs fail {self.fs.name}',
                              retval=1, errmsgs=health_warn)
        self.run_ceph_cmd(f'fs fail {self.fs.name} --yes-i-really-mean-it')

        # Bring and wait for MDS to be up since it is needed for unmounting
        # of CephFS in CephFSTestCase.tearDown() to be successful.
        self.fs.set_joinable()
        self.fs.wait_for_daemons()

    def test_with_health_warn_trim(self):
        '''
        Test that, when health warning MDS_TRIM is present for an MDS, command
        "ceph fs fail" fails without confirmation flag and passes when
        confirmation flag is passed.
        '''
        health_warn = 'MDS_TRIM'
        self.gen_health_warn_mds_trim()

        # actual testing begins now.
        self.negtest_ceph_cmd(args=f'fs fail {self.fs.name}',
                              retval=1, errmsgs=health_warn)
        self.run_ceph_cmd(f'fs fail {self.fs.name} --yes-i-really-mean-it')

        # Bring and wait for MDS to be up since it is needed for unmounting
        # of CephFS in CephFSTestCase.tearDown() to be successful.
        self.fs.set_joinable()
        self.fs.wait_for_daemons()

    def test_with_health_warn_with_2_active_MDSs(self):
        '''
        Test that, when a CephFS has 2 active MDSs and one of them have either
        health warning MDS_TRIM or MDS_CACHE_OVERSIZE, running "ceph fs fail"
        fails without confirmation flag and passes when confirmation flag is
        passed.
        '''
        health_warn = 'MDS_CACHE_OVERSIZED'
        self.fs.set_max_mds(2)
        self.gen_health_warn_mds_cache_oversized()

        # actual testing begins now.
        self.negtest_ceph_cmd(args=f'fs fail {self.fs.name}',
                              retval=1, errmsgs=health_warn)
        self.run_ceph_cmd(f'fs fail {self.fs.name} --yes-i-really-mean-it')

        # Bring and wait for MDS to be up since it is needed for unmounting
        # of CephFS in CephFSTestCase.tearDown() to be successful.
        self.fs.set_joinable()
        self.fs.wait_for_daemons()


class TestMDSFail(TestAdminCommands):

    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 1

    def test_with_health_warn_cache_oversized(self):
        '''
        Test that, when health warning MDS_CACHE_OVERSIZE is present for an
        MDS, command "ceph mds fail" fails without confirmation flag and
        passes when confirmation flag is passed.
        '''
        health_warn = 'MDS_CACHE_OVERSIZED'
        self.gen_health_warn_mds_cache_oversized()

        # actual testing begins now.
        active_mds_id = self.fs.get_active_names()[0]
        self.negtest_ceph_cmd(args=f'mds fail {active_mds_id}',
                              retval=1, errmsgs=health_warn)
        self.run_ceph_cmd(f'mds fail {active_mds_id} --yes-i-really-mean-it')

    def test_with_health_warn_trim(self):
        '''
        Test that, when health warning MDS_TRIM is present for an MDS, command
        "ceph mds fail" fails without confirmation flag and passes when
        confirmation is passed.
        '''
        health_warn = 'MDS_TRIM'
        self.gen_health_warn_mds_trim()

        # actual testing begins now...
        active_mds_id = self.fs.get_active_names()[0]
        self.negtest_ceph_cmd(args=f'mds fail {active_mds_id}',
                              retval=1, errmsgs=health_warn)
        self.run_ceph_cmd(f'mds fail {active_mds_id} --yes-i-really-mean-it')

    def _get_unhealthy_mds_id(self, health_warn):
        '''
        Return MDS ID for which health warning in "health_warn" has been
        generated.
        '''
        health_report = json.loads(self.get_ceph_cmd_stdout('health detail '
                                                            '--format json'))
        # variable "msg" should hold string something like this -
        # 'mds.b(mds.0): Behind on trimming (865/10) max_segments: 10,
        # num_segments: 86
        msg = health_report['checks'][health_warn]['detail'][0]['message']
        mds_id = msg.split('(')[0]
        mds_id = mds_id.replace('mds.', '')
        return mds_id

    def test_with_health_warn_with_2_active_MDSs(self):
        '''
        Test when a CephFS has 2 active MDSs and one of them have either
        health warning MDS_TRIM or MDS_CACHE_OVERSIZE, running "ceph mds fail"
        fails for both MDSs without confirmation flag and passes for both when
        confirmation flag is passed.
        '''
        health_warn = 'MDS_CACHE_OVERSIZED'
        self.fs.set_max_mds(2)
        self.gen_health_warn_mds_cache_oversized()
        mds1_id, mds2_id = self.fs.get_active_names()

        # MDS ID for which health warning has been generated.
        hw_mds_id = self._get_unhealthy_mds_id(health_warn)
        if mds1_id == hw_mds_id:
            non_hw_mds_id = mds2_id
        elif mds2_id == hw_mds_id:
            non_hw_mds_id = mds1_id
        else:
            raise RuntimeError('There are only 2 MDSs right now but apparently'
                               'health warning was raised for an MDS other '
                               'than these two. This is definitely an error.')

        # actual testing begins now...
        self.negtest_ceph_cmd(args=f'mds fail {non_hw_mds_id}', retval=1,
                              errmsgs=health_warn)
        self.negtest_ceph_cmd(args=f'mds fail {hw_mds_id}', retval=1,
                              errmsgs=health_warn)
        self.run_ceph_cmd(f'mds fail {mds1_id} --yes-i-really-mean-it')
        self.run_ceph_cmd(f'mds fail {mds2_id} --yes-i-really-mean-it')


class TestFSSetMaxMDS(TestAdminCommands):

    def test_when_unhealthy_without_confirm(self):
        '''
        Test that command "ceph fs set <fsname> max_mds <num>" without the
        confirmation flag (--yes-i-really-mean-it) fails when cluster is
        unhealthy.
        '''
        self.gen_health_warn_mds_cache_oversized()

        with self.assertRaises(CommandFailedError) as cfe:
            self.fs.set_max_mds(2, confirm=False)
        self.assertEqual(cfe.exception.exitstatus, errno.EPERM)

    def test_when_unhealthy_with_confirm(self):
        '''
        Test that command "ceph fs set <fsname> max_mds <num>
        --yes-i-really-mean-it" runs successfully when cluster is unhealthy.
        '''
        self.gen_health_warn_mds_cache_oversized()

        self.fs.set_max_mds(2, confirm=True)
        self.assertEqual(self.fs.get_var('max_mds'), 2)

    def test_when_mds_trim_without_confirm(self):
        '''
        Test that command "ceph fs set <fsname> max_mds <num>" without the
        confirmation flag (--yes-i-really-mean-it) fails when cluster has
        MDS_TRIM health warning.
        '''
        self.gen_health_warn_mds_trim()

        with self.assertRaises(CommandFailedError) as cfe:
            self.fs.set_max_mds(2, confirm=False)
        self.assertEqual(cfe.exception.exitstatus, errno.EPERM)

    def test_when_mds_trim_when_with_confirm(self):
        '''
        Test that command "ceph fs set <fsname> max_mds <num>
        --yes-i-really-mean-it" runs successfully when cluster has MDS_TRIM
        health warning.
        '''
        self.gen_health_warn_mds_trim()

        self.fs.set_max_mds(2, confirm=True)
        self.assertEqual(self.fs.get_var('max_mds'), 2)

    def test_when_healthy_with_confirm(self):
        '''
        Test that command "ceph fs set <fsname> max_mds <num>
        --yes-i-really-mean-it" runs successfully also when cluster is
        healthy.
        '''
        self.fs.set_max_mds(2, confirm=True)
        self.assertEqual(self.fs.get_var('max_mds'), 2)


class TestToggleVolumes(CephFSTestCase):
    '''
    Contains code for enabling/disabling mgr/volumes plugin.
    '''

    VOL_MOD_NAME = 'volumes'
    CONFIRM = '--yes-i-really-mean-it'

    def tearDown(self):
        '''
        Ensure that the volumes plugin is enabled after the test has finished
        running since not doing so might affect tearDown() of CephFSTestCase or
        other superclasses.
        '''
        json_output = self.get_ceph_cmd_stdout('mgr module ls --format json')
        json_output = json.loads(json_output)

        if 'volumes' in json_output['force_disabled_modules']:
            self.run_ceph_cmd(f'mgr module enable {self.VOL_MOD_NAME}')

        super(TestToggleVolumes, self).tearDown()

    def test_force_disable_with_confirmation(self):
        '''
        Test that running "ceph mgr module force disable volumes
        --yes-i-really-mean-it" successfully disables volumes plugin.

        Also test "ceph mgr module ls" output after this.
        '''
        self.run_ceph_cmd(f'mgr module force disable {self.VOL_MOD_NAME} '
                          f'{self.CONFIRM}')

        json_output = self.get_ceph_cmd_stdout('mgr module ls --format json')
        json_output = json.loads(json_output)

        self.assertIn(self.VOL_MOD_NAME, json_output['always_on_modules'])
        self.assertIn(self.VOL_MOD_NAME, json_output['force_disabled_modules'])

        self.assertNotIn(self.VOL_MOD_NAME, json_output['enabled_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['disabled_modules'])

    def test_force_disable_fails_without_confirmation(self):
        '''
        Test that running "ceph mgr module force disable volumes" fails with
        EPERM when confirmation flag is not passed along.

        Also test that output of this command suggests user to pass
        --yes-i-really-mean-it.
        '''
        proc = self.run_ceph_cmd(
            f'mgr module force disable {self.VOL_MOD_NAME}',
            stderr=StringIO(), check_status=False)

        self.assertEqual(proc.returncode, errno.EPERM)

        proc_stderr = proc.stderr.getvalue()
        self.assertIn('EPERM', proc_stderr)
        # ensure that the confirmation flag was recommended
        self.assertIn(self.CONFIRM, proc_stderr)

    def test_force_disable_idempotency(self):
        '''
        Test that running "ceph mgr module force disable volumes" passes when
        volumes plugin was already force disabled.
        '''
        self.run_ceph_cmd(f'mgr module force disable {self.VOL_MOD_NAME} '
                          f'{self.CONFIRM}')
        sleep(5)

        json_output = self.get_ceph_cmd_stdout('mgr module ls --format '
                                              'json-pretty')
        json_output = json.loads(json_output)

        self.assertIn(self.VOL_MOD_NAME, json_output['always_on_modules'])
        self.assertIn(self.VOL_MOD_NAME, json_output['force_disabled_modules'])

        self.assertNotIn(self.VOL_MOD_NAME, json_output['enabled_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['disabled_modules'])

        # XXX: this this test, running this command 2nd time should pass.
        self.run_ceph_cmd(f'mgr module force disable {self.VOL_MOD_NAME}')

    def test_force_disable_nonexistent_mod(self):
        '''
        Test that passing non-existent name to "ceph mgr module force disable"
        command leads to an error.
        '''
        proc = self.run_ceph_cmd(
            f'mgr module force disable abcd {self.CONFIRM}',
            check_status=False, stderr=StringIO())
        self.assertEqual(proc.returncode, errno.EINVAL)
        self.assertIn('EINVAL', proc.stderr.getvalue())

    def test_force_disable_non_alwayson_mod(self):
        '''
        Test that passing non-existent name to "ceph mgr module force disable"
        command leads to an error.
        '''
        json_output = self.get_ceph_cmd_stdout(
            'mgr module ls --format json-pretty', check_status=False,
            stderr=StringIO())
        output_dict = json.loads(json_output)
        some_non_alwayson_mod = output_dict['enabled_modules'][0]

        proc = self.run_ceph_cmd(
            f'mgr module force disable {some_non_alwayson_mod} {self.CONFIRM}',
            check_status=False, stderr=StringIO())
        self.assertEqual(proc.returncode, errno.EINVAL)
        self.assertIn('EINVAL', proc.stderr.getvalue())

    def test_enabled_by_default(self):
        '''
        Test that volumes plugin is enabled by default and is also reported as
        "always on".
        '''
        json_output = self.get_ceph_cmd_stdout('mgr module ls --format json')
        json_output = json.loads(json_output)

        self.assertIn(self.VOL_MOD_NAME, json_output['always_on_modules'])

        self.assertNotIn(self.VOL_MOD_NAME, json_output['enabled_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['disabled_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['force_disabled_modules'])

    def test_disable_fails(self):
        '''
        Test that running "ceph mgr module disable volumes" fails with EPERM.

        This is expected since volumes is an always-on module and therefore
        it can only be disabled using command "ceph mgr module force disable
        volumes".
        '''
        proc = self.run_ceph_cmd(f'mgr module disable {self.VOL_MOD_NAME}',
                                 stderr=StringIO(), check_status=False)
        self.assertEqual(proc.returncode, errno.EPERM)

        proc_stderr = proc.stderr.getvalue()
        self.assertIn('EPERM', proc_stderr)

    def test_enable_idempotency(self):
        '''
        Test that enabling volumes plugin when it is already enabled doesn't
        exit with non-zero return value.

        Also test that it reports plugin as already enabled.
        '''
        proc = self.run_ceph_cmd(f'mgr module enable {self.VOL_MOD_NAME}',
                                 stderr=StringIO())
        self.assertEqual(proc.returncode, 0)

        proc_stderr = proc.stderr.getvalue()
        self.assertIn('already enabled', proc_stderr)
        self.assertIn('always-on', proc_stderr)

    def test_enable_post_disabling(self):
        '''
        Test that enabling volumes plugin after (force-)disabling it works
        successfully.

        Alo test "ceph mgr module ls" output for volumes plugin afterwards.
        '''
        self.run_ceph_cmd(f'mgr module force disable {self.VOL_MOD_NAME} '
                          f'{self.CONFIRM}')
        # give bit of time for plugin to be disabled.
        sleep(5)

        self.run_ceph_cmd(f'mgr module enable {self.VOL_MOD_NAME}')
        # give bit of time for plugin to be functional again
        sleep(5)
        json_output = self.get_ceph_cmd_stdout('mgr module ls --format json')
        json_output = json.loads(json_output)
        self.assertIn(self.VOL_MOD_NAME, json_output['always_on_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['enabled_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['disabled_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['force_disabled_modules'])

        # plugin is reported properly by "ceph mgr module ls" command, check if
        # it is also working fine.
        self.run_ceph_cmd('fs volume ls')
