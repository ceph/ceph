"""
MDS admin socket scrubbing-related tests.
"""
import logging
import errno
import time
from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while
import os
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class TestScrubControls(CephFSTestCase):
    """
    Test basic scrub control operations such as abort, pause and resume.
    """

    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 1

    def _abort_scrub(self, expected):
        res = self.fs.run_scrub(["abort"])
        self.assertEqual(res['return_code'], expected)
    def _pause_scrub(self, expected):
        res = self.fs.run_scrub(["pause"])
        self.assertEqual(res['return_code'], expected)
    def _resume_scrub(self, expected):
        res = self.fs.run_scrub(["resume"])
        self.assertEqual(res['return_code'], expected)
    def _check_task_status(self, expected_status, timo=120):
        """ check scrub status for current active mds in ceph status """
        with safe_while(sleep=1, tries=120, action='wait for task status') as proceed:
            while proceed():
                active = self.fs.get_active_names()
                log.debug("current active={0}".format(active))
                task_status = self.fs.get_task_status("scrub status")
                try:
                    if task_status[active[0]].startswith(expected_status):
                        return True
                except KeyError:
                    pass

    def _check_task_status_na(self, timo=120):
        """ check absence of scrub status in ceph status """
        with safe_while(sleep=1, tries=120, action='wait for task status') as proceed:
            while proceed():
                active = self.fs.get_active_names()
                log.debug("current active={0}".format(active))
                task_status = self.fs.get_task_status("scrub status")
                if not active[0] in task_status:
                    return True

    def create_scrub_data(self, test_dir):
        for i in range(32):
            dirname = "dir.{0}".format(i)
            dirpath = os.path.join(test_dir, dirname)
            self.mount_a.run_shell_payload(f"""
set -e
mkdir -p {dirpath}
for ((i = 0; i < 32; i++)); do
    dd if=/dev/urandom of={dirpath}/filename.$i bs=1M conv=fdatasync count=1
done
""")

    def test_scrub_abort(self):
        test_dir = "scrub_control_test_path"
        abs_test_path = "/{0}".format(test_dir)

        self.create_scrub_data(test_dir)

        out_json = self.fs.run_scrub(["start", abs_test_path, "recursive"])
        self.assertNotEqual(out_json, None)

        # abort and verify
        self._abort_scrub(0)
        self.fs.wait_until_scrub_complete(sleep=5, timeout=30)

        # sleep enough to fetch updated task status
        checked = self._check_task_status_na()
        self.assertTrue(checked)

    def test_scrub_pause_and_resume(self):
        test_dir = "scrub_control_test_path"
        abs_test_path = "/{0}".format(test_dir)

        log.info("mountpoint: {0}".format(self.mount_a.mountpoint))
        client_path = os.path.join(self.mount_a.mountpoint, test_dir)
        log.info("client_path: {0}".format(client_path))

        self.create_scrub_data(test_dir)

        out_json = self.fs.run_scrub(["start", abs_test_path, "recursive"])
        self.assertNotEqual(out_json, None)

        # pause and verify
        self._pause_scrub(0)
        out_json = self.fs.get_scrub_status()
        self.assertTrue("PAUSED" in out_json['status'])

        checked = self._check_task_status("paused")
        self.assertTrue(checked)

        # resume and verify
        self._resume_scrub(0)
        out_json = self.fs.get_scrub_status()
        self.assertFalse("PAUSED" in out_json['status'])

        checked = self._check_task_status_na()
        self.assertTrue(checked)

    def test_scrub_pause_and_resume_with_abort(self):
        test_dir = "scrub_control_test_path"
        abs_test_path = "/{0}".format(test_dir)

        self.create_scrub_data(test_dir)

        out_json = self.fs.run_scrub(["start", abs_test_path, "recursive"])
        self.assertNotEqual(out_json, None)

        # pause and verify
        self._pause_scrub(0)
        out_json = self.fs.get_scrub_status()
        self.assertTrue("PAUSED" in out_json['status'])

        checked = self._check_task_status("paused")
        self.assertTrue(checked)

        # abort and verify
        self._abort_scrub(0)
        out_json = self.fs.get_scrub_status()
        self.assertTrue("PAUSED" in out_json['status'])
        self.assertTrue("0 inodes" in out_json['status'])

        # scrub status should still be paused...
        checked = self._check_task_status("paused")
        self.assertTrue(checked)

        # resume and verify
        self._resume_scrub(0)
        self.assertTrue(self.fs.wait_until_scrub_complete(sleep=5, timeout=30))

        checked = self._check_task_status_na()
        self.assertTrue(checked)

    def test_scrub_task_status_on_mds_failover(self):
        (original_active, ) = self.fs.get_active_names()
        original_standbys = self.mds_cluster.get_standby_daemons()

        test_dir = "scrub_control_test_path"
        abs_test_path = "/{0}".format(test_dir)

        self.create_scrub_data(test_dir)

        out_json = self.fs.run_scrub(["start", abs_test_path, "recursive"])
        self.assertNotEqual(out_json, None)

        # pause and verify
        self._pause_scrub(0)
        out_json = self.fs.get_scrub_status()
        self.assertTrue("PAUSED" in out_json['status'])

        checked = self._check_task_status("paused")
        self.assertTrue(checked)

        # Kill the rank 0
        self.fs.mds_stop(original_active)

        def promoted():
            active = self.fs.get_active_names()
            return active and active[0] in original_standbys

        log.info("Waiting for promotion of one of the original standbys {0}".format(
            original_standbys))
        self.wait_until_true(promoted, timeout=self.fs.beacon_timeout)

        self._check_task_status_na()

class TestScrubChecks(CephFSTestCase):
    """
    Run flush and scrub commands on the specified files in the filesystem. This
    task will run through a sequence of operations, but it is not comprehensive
    on its own -- it doesn't manipulate the mds cache state to test on both
    in- and out-of-memory parts of the hierarchy. So it's designed to be run
    multiple times within a single test run, so that the test can manipulate
    memory state.

    Usage:
    mds_scrub_checks:
      mds_rank: 0
      path: path/to/test/dir
      client: 0
      run_seq: [0-9]+

    Increment the run_seq on subsequent invocations within a single test run;
    it uses that value to generate unique folder and file names.
    """

    MDSS_REQUIRED = 1
    CLIENTS_REQUIRED = 1
    def get_dsplits(self, dir_ino):
        return self.fs.rank_asok(['dump', 'inode', str(dir_ino)])['dirfragtree']['splits']

    def test_scrub_checks(self):
        self._checks(0)
        self._checks(1)

    def _checks(self, run_seq):
        mds_rank = 0
        test_dir = "scrub_test_path"

        abs_test_path = "/{0}".format(test_dir)

        log.info("mountpoint: {0}".format(self.mount_a.mountpoint))
        client_path = os.path.join(self.mount_a.mountpoint, test_dir)
        log.info("client_path: {0}".format(client_path))

        log.info("Cloning repo into place")
        repo_path = TestScrubChecks.clone_repo(self.mount_a, client_path)

        log.info("Initiating mds_scrub_checks on mds.{id_} test_path {path}, run_seq {seq}".format(
            id_=mds_rank, path=abs_test_path, seq=run_seq)
        )


        success_validator = lambda j, r: self.json_validator(j, r, "return_code", 0)

        nep = "{test_path}/i/dont/exist".format(test_path=abs_test_path)
        self.tell_command(mds_rank, "flush_path {nep}".format(nep=nep),
                          lambda j, r: self.json_validator(j, r, "return_code", -errno.ENOENT))
        self.tell_command(mds_rank, "scrub start {nep}".format(nep=nep),
                          lambda j, r: self.json_validator(j, r, "return_code", -errno.ENOENT))

        test_repo_path = "{test_path}/ceph-qa-suite".format(test_path=abs_test_path)
        dirpath = "{repo_path}/suites".format(repo_path=test_repo_path)

        if run_seq == 0:
            log.info("First run: flushing {dirpath}".format(dirpath=dirpath))
            command = "flush_path {dirpath}".format(dirpath=dirpath)
            self.tell_command(mds_rank, command, success_validator)
        command = "scrub start {dirpath}".format(dirpath=dirpath)
        self.tell_command(mds_rank, command, success_validator)

        filepath = "{repo_path}/suites/fs/verify/validater/valgrind.yaml".format(
            repo_path=test_repo_path)
        if run_seq == 0:
            log.info("First run: flushing {filepath}".format(filepath=filepath))
            command = "flush_path {filepath}".format(filepath=filepath)
            self.tell_command(mds_rank, command, success_validator)
        command = "scrub start {filepath}".format(filepath=filepath)
        self.tell_command(mds_rank, command, success_validator)

        if run_seq == 0:
            log.info("First run: flushing base dir /")
            command = "flush_path /"
            self.tell_command(mds_rank, command, success_validator)
        command = "scrub start /"
        self.tell_command(mds_rank, command, success_validator)

        new_dir = "{repo_path}/new_dir_{i}".format(repo_path=repo_path, i=run_seq)
        test_new_dir = "{repo_path}/new_dir_{i}".format(repo_path=test_repo_path,
                                                        i=run_seq)
        self.mount_a.run_shell(["mkdir", new_dir])
        command = "flush_path {dir}".format(dir=test_new_dir)
        self.tell_command(mds_rank, command, success_validator)

        new_file = "{repo_path}/new_file_{i}".format(repo_path=repo_path,
                                                     i=run_seq)
        test_new_file = "{repo_path}/new_file_{i}".format(repo_path=test_repo_path,
                                                          i=run_seq)
        self.mount_a.write_n_mb(new_file, 1)

        command = "flush_path {file}".format(file=test_new_file)
        self.tell_command(mds_rank, command, success_validator)

        # check that scrub fails on errors
        ino = self.mount_a.path_to_ino(new_file)
        rados_obj_name = "{ino:x}.00000000".format(ino=ino)
        command = "scrub start {file}".format(file=test_new_file)

        def _check_and_clear_damage(ino, dtype):
            all_damage = self.fs.rank_tell(["damage", "ls"], rank=mds_rank)
            damage = [d for d in all_damage if d['ino'] == ino and d['damage_type'] == dtype]
            for d in damage:
                self.run_ceph_cmd(
                    'tell', f'mds.{self.fs.get_active_names()[mds_rank]}',
                    "damage", "rm", str(d['id']))
            return len(damage) > 0

        # Missing parent xattr
        self.assertFalse(_check_and_clear_damage(ino, "backtrace"));
        self.fs.rados(["rmxattr", rados_obj_name, "parent"], pool=self.fs.get_data_pool_name())
        self.tell_command(mds_rank, command, success_validator)
        self.fs.wait_until_scrub_complete(sleep=5, timeout=30)
        self.assertTrue(_check_and_clear_damage(ino, "backtrace"));

        command = "flush_path /"
        self.tell_command(mds_rank, command, success_validator)

    def scrub_with_stray_evaluation(self, fs, mnt, path, flag, files=2000,
                                    _hard_links=3):
        fs.set_allow_new_snaps(True)

        test_dir = "stray_eval_dir"
        mnt.run_shell(["mkdir", test_dir])
        client_path = os.path.join(mnt.mountpoint, test_dir)
        mnt.create_n_files(fs_path=f"{test_dir}/file", count=files,
                           hard_links=_hard_links)
        mnt.run_shell(["mkdir", f"{client_path}/.snap/snap1-{test_dir}"])
        mnt.run_shell(f"find {client_path}/ -type f -delete")
        mnt.run_shell(["rmdir", f"{client_path}/.snap/snap1-{test_dir}"])
        perf_dump = fs.rank_tell(["perf", "dump"], rank=0)
        self.assertNotEqual(perf_dump.get('mds_cache').get('num_strays'),
                            0, "mdcache.num_strays is zero")

        log.info(
            f"num of strays: {perf_dump.get('mds_cache').get('num_strays')}")

        out_json = fs.run_scrub(["start", path, flag])
        self.assertNotEqual(out_json, None)
        self.assertEqual(out_json["return_code"], 0)

        self.assertEqual(
            fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)

        perf_dump = fs.rank_tell(["perf", "dump"], rank=0)
        self.assertEqual(int(perf_dump.get('mds_cache').get('num_strays')),
                         0, "mdcache.num_strays is non-zero")

    def test_scrub_repair(self):
        mds_rank = 0
        test_dir = "scrub_repair_path"

        self.mount_a.run_shell(["mkdir", test_dir])
        self.mount_a.run_shell(["touch", "{0}/file".format(test_dir)])
        dir_objname = "{:x}.00000000".format(self.mount_a.path_to_ino(test_dir))

        self.mount_a.umount_wait()

        # flush journal entries to dirfrag objects, and expire journal
        self.fs.mds_asok(['flush', 'journal'])
        self.fs.mds_stop()

        # remove the dentry from dirfrag, cause incorrect fragstat/rstat
        self.fs.radosm(["rmomapkey", dir_objname, "file_head"])

        self.fs.mds_fail_restart()
        self.fs.wait_for_daemons()

        self.mount_a.mount_wait()

        # fragstat indicates the directory is not empty, rmdir should fail
        with self.assertRaises(CommandFailedError) as ar:
            self.mount_a.run_shell(["rmdir", test_dir])
        self.assertEqual(ar.exception.exitstatus, 1)

        self.tell_command(mds_rank, "scrub start /{0} repair".format(test_dir),
                          lambda j, r: self.json_validator(j, r, "return_code", 0))

        # wait a few second for background repair
        time.sleep(10)

        # fragstat should be fixed
        self.mount_a.run_shell(["rmdir", test_dir])

    def test_scrub_merge_dirfrags(self):
        """
        That a directory is merged during scrub.
        """

        test_path = "testdir"
        abs_test_path = f"/{test_path}"
        split_size = 20
        merge_size = 5
        split_bits = 1
        self.config_set('mds', 'mds_bal_split_size', split_size)
        self.config_set('mds', 'mds_bal_merge_size', merge_size)
        self.config_set('mds', 'mds_bal_split_bits', split_bits)

        self.mount_a.run_shell(["mkdir", test_path])
        dir_ino=self.mount_a.path_to_ino(test_path)

        self.assertEqual(len(self.get_dsplits(dir_ino)), 0)
        self.mount_a.create_n_files(f"{test_path}/file", split_size * 2)

        self.mount_a.umount_wait()

        self.fs.flush()
        self.fs.mds_fail_restart()
        self.fs.wait_for_daemons()

        split_size = 100
        merge_size = 30
        self.config_set('mds', 'mds_bal_split_size', split_size)
        self.config_set('mds', 'mds_bal_merge_size', merge_size)

        #Assert to ensure split is present
        self.assertGreater(len(self.get_dsplits(dir_ino)), 0)
        out_json = self.fs.run_scrub(["start", abs_test_path, "recursive"])
        self.assertNotEqual(out_json, None)

        #Wait until no splits to confirm merge by scrub
        self.wait_until_true(
            lambda: len(self.get_dsplits(dir_ino)) == 0,
            timeout=30
        )

    def test_scrub_split_dirfrags(self):
        """
        That a directory is split during scrub.
        """

        test_path = "testdir"
        abs_test_path = f"/{test_path}"
        split_size = 20
        merge_size = 5
        split_bits = 1

        self.mount_a.run_shell(["mkdir", test_path])
        dir_ino=self.mount_a.path_to_ino(test_path)

        self.assertEqual(len(self.get_dsplits(dir_ino)), 0)
        self.mount_a.create_n_files(f"{test_path}/file", split_size + 1)

        self.mount_a.umount_wait()

        self.fs.flush()
        self.fs.mds_fail_restart()
        self.fs.wait_for_daemons()

        self.config_set('mds', 'mds_bal_split_size', split_size)
        self.config_set('mds', 'mds_bal_merge_size', merge_size)
        self.config_set('mds', 'mds_bal_split_bits', split_bits)

        #Assert to ensure no splits are present
        self.assertEqual(len(self.get_dsplits(dir_ino)), 0)
        out_json = self.fs.run_scrub(["start", abs_test_path, "recursive"])
        self.assertNotEqual(out_json, None)

        #Wait until split is present to confirm split by scrub
        self.wait_until_true(
            lambda: len(self.get_dsplits(dir_ino)) > 0,
            timeout=30
        )

    def test_stray_evaluation_with_scrub(self):
        """
        test that scrub can iterate over ~mdsdir and evaluate strays
        """
        self.scrub_with_stray_evaluation(self.fs, self.mount_a, "~mdsdir",
                                         "recursive")

    def test_flag_scrub_mdsdir(self):
        """
        test flag scrub_mdsdir
        """
        self.scrub_with_stray_evaluation(self.fs, self.mount_a, "/",
                                         "recursive,scrub_mdsdir")

    @staticmethod
    def json_validator(json_out, rc, element, expected_value):
        if rc != 0:
            return False, "asok command returned error {rc}".format(rc=rc)
        element_value = json_out.get(element)
        if element_value != expected_value:
            return False, "unexpectedly got {jv} instead of {ev}!".format(
                jv=element_value, ev=expected_value)
        return True, "Succeeded"

    def tell_command(self, mds_rank, command, validator):
        log.info("Running command '{command}'".format(command=command))

        command_list = command.split()
        jout = self.fs.rank_tell(command_list, rank=mds_rank, check_status=False)

        log.info("command '{command}' returned '{jout}'".format(
                     command=command, jout=jout))

        success, errstring = validator(jout, 0)
        if not success:
            raise AsokCommandFailedError(command, 0, jout, errstring)
        return jout

    @staticmethod
    def clone_repo(client_mount, path):
        repo = "ceph-qa-suite"
        repo_path = os.path.join(path, repo)
        client_mount.run_shell(["mkdir", "-p", path])

        try:
            client_mount.stat(repo_path)
        except CommandFailedError:
            client_mount.run_shell([
                "git", "clone", '--branch', 'giant',
                "http://github.com/ceph/{repo}".format(repo=repo),
                "{path}/{repo}".format(path=path, repo=repo)
            ])

        return repo_path


class AsokCommandFailedError(Exception):
    """
    Exception thrown when we get an unexpected response
    on an admin socket command
    """

    def __init__(self, command, rc, json_out, errstring):
        self.command = command
        self.rc = rc
        self.json = json_out
        self.errstring = errstring

    def __str__(self):
        return "Admin socket: {command} failed with rc={rc} json output={json}, because '{es}'".format(
            command=self.command, rc=self.rc, json=self.json, es=self.errstring)
