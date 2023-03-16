from io import BytesIO, StringIO
import json
import logging
import errno
import re
import time
from teuthology.contextutil import MaxWhileTries
from teuthology.exceptions import CommandFailedError
from teuthology.orchestra.run import wait
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase, for_teuthology

DAMAGED_ON_START = "damaged_on_start"
DAMAGED_ON_LS = "damaged_on_ls"
CRASHED = "server crashed"
NO_DAMAGE = "no damage"
READONLY = "readonly"
FAILED_CLIENT = "client failed"
FAILED_SERVER = "server failed"

# An EIO in response to a stat from the client
EIO_ON_LS = "eio"

# An EIO, but nothing in damage table (not ever what we expect)
EIO_NO_DAMAGE = "eio without damage entry"


log = logging.getLogger(__name__)


class TestDamage(CephFSTestCase):
    def _simple_workload_write(self):
        self.mount_a.run_shell(["mkdir", "subdir"])
        self.mount_a.write_n_mb("subdir/sixmegs", 6)
        return self.mount_a.stat("subdir/sixmegs")

    def is_marked_damaged(self, rank):
        mds_map = self.fs.get_mds_map()
        return rank in mds_map['damaged']

    @for_teuthology #459s
    def test_object_deletion(self):
        """
        That the MDS has a clean 'damaged' response to loss of any single metadata object
        """

        self._simple_workload_write()

        # Hmm, actually it would be nice to permute whether the metadata pool
        # state contains sessions or not, but for the moment close this session
        # to avoid waiting through reconnect on every MDS start.
        self.mount_a.umount_wait()
        for mds_name in self.fs.get_active_names():
            self.fs.mds_asok(["flush", "journal"], mds_name)

        self.fs.fail()

        serialized = self.fs.radosmo(['export', '-'])

        def is_ignored(obj_id, dentry=None):
            """
            A filter to avoid redundantly mutating many similar objects (e.g.
            stray dirfrags) or similar dentries (e.g. stray dir dentries)
            """
            if re.match("60.\.00000000", obj_id) and obj_id != "600.00000000":
                return True

            if dentry and obj_id == "100.00000000":
                if re.match("stray.+_head", dentry) and dentry != "stray0_head":
                    return True

            return False

        def get_path(obj_id, dentry=None):
            """
            What filesystem path does this object or dentry correspond to?   i.e.
            what should I poke to see EIO after damaging it?
            """

            if obj_id == "1.00000000" and dentry == "subdir_head":
                return "./subdir"
            elif obj_id == "10000000000.00000000" and dentry == "sixmegs_head":
                return "./subdir/sixmegs"

            # None means ls will do an "ls -R" in hope of seeing some errors
            return None

        objects = self.fs.radosmo(["ls"], stdout=StringIO()).strip().split("\n")
        objects = [o for o in objects if not is_ignored(o)]

        # Find all objects with an OMAP header
        omap_header_objs = []
        for o in objects:
            header = self.fs.radosmo(["getomapheader", o], stdout=StringIO())
            # The rados CLI wraps the header output in a hex-printed style
            header_bytes = int(re.match("header \((.+) bytes\)", header).group(1))
            if header_bytes > 0:
                omap_header_objs.append(o)

        # Find all OMAP key/vals
        omap_keys = []
        for o in objects:
            keys_str = self.fs.radosmo(["listomapkeys", o], stdout=StringIO())
            if keys_str:
                for key in keys_str.strip().split("\n"):
                    if not is_ignored(o, key):
                        omap_keys.append((o, key))

        # Find objects that have data in their bodies
        data_objects = []
        for obj_id in objects:
            stat_out = self.fs.radosmo(["stat", obj_id], stdout=StringIO())
            size = int(re.match(".+, size (.+)$", stat_out).group(1))
            if size > 0:
                data_objects.append(obj_id)

        # Define the various forms of damage we will inflict
        class MetadataMutation(object):
            def __init__(self, obj_id_, desc_, mutate_fn_, expectation_, ls_path=None):
                self.obj_id = obj_id_
                self.desc = desc_
                self.mutate_fn = mutate_fn_
                self.expectation = expectation_
                if ls_path is None:
                    self.ls_path = "."
                else:
                    self.ls_path = ls_path

            def __eq__(self, other):
                return self.desc == other.desc

            def __hash__(self):
                return hash(self.desc)

        junk = "deadbeef" * 10
        mutations = []

        # Removals
        for o in objects:
            if o in [
                # JournalPointers are auto-replaced if missing (same path as upgrade)
                "400.00000000",
                # Missing dirfrags for non-system dirs result in empty directory
                "10000000000.00000000",
                # PurgeQueue is auto-created if not found on startup
                "500.00000000",
                # open file table is auto-created if not found on startup
                "mds0_openfiles.0"
            ]:
                expectation = NO_DAMAGE
            else:
                expectation = DAMAGED_ON_START

            log.info("Expectation on rm '{0}' will be '{1}'".format(
                o, expectation
            ))

            mutations.append(MetadataMutation(
                o,
                "Delete {0}".format(o),
                lambda o=o: self.fs.radosm(["rm", o]),
                expectation
            ))

        # Blatant corruptions
        for obj_id in data_objects:
            if obj_id == "500.00000000":
                # purge queue corruption results in read-only FS
                mutations.append(MetadataMutation(
                    obj_id,
                    "Corrupt {0}".format(obj_id),
                    lambda o=obj_id: self.fs.radosm(["put", o, "-"], stdin=StringIO(junk)),
                    READONLY
                ))
            else:
                mutations.append(MetadataMutation(
                    obj_id,
                    "Corrupt {0}".format(obj_id),
                    lambda o=obj_id: self.fs.radosm(["put", o, "-"], stdin=StringIO(junk)),
                    DAMAGED_ON_START
                ))

        # Truncations
        for o in data_objects:
            if o == "500.00000000":
                # The PurgeQueue is allowed to be empty: Journaler interprets
                # an empty header object as an empty journal.
                expectation = NO_DAMAGE
            else:
                expectation = DAMAGED_ON_START

            mutations.append(
                MetadataMutation(
                    o,
                    "Truncate {0}".format(o),
                    lambda o=o: self.fs.radosm(["truncate", o, "0"]),
                    expectation
            ))

        # OMAP value corruptions
        for o, k in omap_keys:
            if o.startswith("100."):
                # Anything in rank 0's 'mydir'
                expectation = DAMAGED_ON_START
            else:
                expectation = EIO_ON_LS

            mutations.append(
                MetadataMutation(
                    o,
                    "Corrupt omap key {0}:{1}".format(o, k),
                    lambda o=o,k=k: self.fs.radosm(["setomapval", o, k, junk]),
                    expectation,
                    get_path(o, k)
                )
            )

        # OMAP header corruptions
        for o in omap_header_objs:
            if re.match("60.\.00000000", o) \
                    or o in ["1.00000000", "100.00000000", "mds0_sessionmap"]:
                expectation = DAMAGED_ON_START
            else:
                expectation = NO_DAMAGE

            log.info("Expectation on corrupt header '{0}' will be '{1}'".format(
                o, expectation
            ))

            mutations.append(
                MetadataMutation(
                    o,
                    "Corrupt omap header on {0}".format(o),
                    lambda o=o: self.fs.radosm(["setomapheader", o, junk]),
                    expectation
                )
            )

        results = {}

        for mutation in mutations:
            log.info("Applying mutation '{0}'".format(mutation.desc))

            # Reset MDS state
            self.mount_a.umount_wait(force=True)
            self.fs.fail()
            self.run_ceph_cmd('mds', 'repaired', '0')

            # Reset RADOS pool state
            self.fs.radosm(['import', '-'], stdin=BytesIO(serialized))

            # Inject the mutation
            mutation.mutate_fn()

            # Try starting the MDS
            self.fs.set_joinable()

            # How long we'll wait between starting a daemon and expecting
            # it to make it through startup, and potentially declare itself
            # damaged to the mon cluster.
            startup_timeout = 60

            if mutation.expectation not in (EIO_ON_LS, DAMAGED_ON_LS, NO_DAMAGE):
                if mutation.expectation == DAMAGED_ON_START:
                    # The MDS may pass through active before making it to damaged
                    try:
                        self.wait_until_true(lambda: self.is_marked_damaged(0), startup_timeout)
                    except RuntimeError:
                        pass

                # Wait for MDS to either come up or go into damaged state
                try:
                    self.wait_until_true(lambda: self.is_marked_damaged(0) or self.fs.are_daemons_healthy(), startup_timeout)
                except RuntimeError:
                    crashed = False
                    # Didn't make it to healthy or damaged, did it crash?
                    for daemon_id, daemon in self.fs.mds_daemons.items():
                        if daemon.proc and daemon.proc.finished:
                            crashed = True
                            log.error("Daemon {0} crashed!".format(daemon_id))
                            daemon.proc = None  # So that subsequent stop() doesn't raise error
                    if not crashed:
                        # Didn't go health, didn't go damaged, didn't crash, so what?
                        raise
                    else:
                        log.info("Result: Mutation '{0}' led to crash".format(mutation.desc))
                        results[mutation] = CRASHED
                        continue
                if self.is_marked_damaged(0):
                    log.info("Result: Mutation '{0}' led to DAMAGED state".format(mutation.desc))
                    results[mutation] = DAMAGED_ON_START
                    continue
                else:
                    log.info("Mutation '{0}' did not prevent MDS startup, attempting ls...".format(mutation.desc))
            else:
                try:
                    self.wait_until_true(self.fs.are_daemons_healthy, 60)
                except RuntimeError:
                    log.info("Result: Mutation '{0}' should have left us healthy, actually not.".format(mutation.desc))
                    if self.is_marked_damaged(0):
                        results[mutation] = DAMAGED_ON_START
                    else:
                        results[mutation] = FAILED_SERVER
                    continue
                log.info("Daemons came up after mutation '{0}', proceeding to ls".format(mutation.desc))

            # MDS is up, should go damaged on ls or client mount
            self.mount_a.mount_wait()
            if mutation.ls_path == ".":
                proc = self.mount_a.run_shell(["ls", "-R", mutation.ls_path], wait=False)
            else:
                proc = self.mount_a.stat(mutation.ls_path, wait=False)

            if mutation.expectation == DAMAGED_ON_LS:
                try:
                    self.wait_until_true(lambda: self.is_marked_damaged(0), 60)
                    log.info("Result: Mutation '{0}' led to DAMAGED state after ls".format(mutation.desc))
                    results[mutation] = DAMAGED_ON_LS
                except RuntimeError:
                    if self.fs.are_daemons_healthy():
                        log.error("Result: Failed to go damaged on mutation '{0}', actually went active".format(
                            mutation.desc))
                        results[mutation] = NO_DAMAGE
                    else:
                        log.error("Result: Failed to go damaged on mutation '{0}'".format(mutation.desc))
                        results[mutation] = FAILED_SERVER
            elif mutation.expectation == READONLY:
                proc = self.mount_a.run_shell(["mkdir", "foo"], wait=False)
                try:
                    proc.wait()
                except CommandFailedError:
                    stderr = proc.stderr.getvalue()
                    log.info(stderr)
                    if "Read-only file system".lower() in stderr.lower():
                        pass
                    else:
                        raise
            else:
                try:
                    wait([proc], 20)
                    log.info("Result: Mutation '{0}' did not caused DAMAGED state".format(mutation.desc))
                    results[mutation] = NO_DAMAGE
                except MaxWhileTries:
                    log.info("Result: Failed to complete client IO on mutation '{0}'".format(mutation.desc))
                    results[mutation] = FAILED_CLIENT
                except CommandFailedError as e:
                    if e.exitstatus == errno.EIO:
                        log.info("Result: EIO on client")
                        results[mutation] = EIO_ON_LS
                    else:
                        log.info("Result: unexpected error {0} on client".format(e))
                        results[mutation] = FAILED_CLIENT

            if mutation.expectation == EIO_ON_LS:
                # EIOs mean something handled by DamageTable: assert that it has
                # been populated
                damage = json.loads(
                    self.get_ceph_cmd_stdout(
                        'tell', f'mds.{self.fs.get_active_names()[0]}',
                        "damage", "ls", '--format=json-pretty'))
                if len(damage) == 0:
                    results[mutation] = EIO_NO_DAMAGE

        failures = [(mutation, result) for (mutation, result) in results.items() if mutation.expectation != result]
        if failures:
            log.error("{0} mutations had unexpected outcomes:".format(len(failures)))
            for mutation, result in failures:
                log.error("  Expected '{0}' actually '{1}' from '{2}'".format(
                    mutation.expectation, result, mutation.desc
                ))
            raise RuntimeError("{0} mutations had unexpected outcomes".format(len(failures)))
        else:
            log.info("All {0} mutations had expected outcomes".format(len(mutations)))

    def test_damaged_dentry(self):
        # Damage to dentrys is interesting because it leaves the
        # directory's `complete` flag in a subtle state where
        # we have marked the dir complete in order that folks
        # can access it, but in actual fact there is a dentry
        # missing
        self.mount_a.run_shell(["mkdir", "subdir/"])

        self.mount_a.run_shell(["touch", "subdir/file_undamaged"])
        self.mount_a.run_shell(["touch", "subdir/file_to_be_damaged"])

        subdir_ino = self.mount_a.path_to_ino("subdir")

        self.mount_a.umount_wait()
        for mds_name in self.fs.get_active_names():
            self.fs.mds_asok(["flush", "journal"], mds_name)

        self.fs.fail()

        # Corrupt a dentry
        junk = "deadbeef" * 10
        dirfrag_obj = "{0:x}.00000000".format(subdir_ino)
        self.fs.radosm(["setomapval", dirfrag_obj, "file_to_be_damaged_head", junk])

        # Start up and try to list it
        self.fs.set_joinable()
        self.fs.wait_for_daemons()

        self.mount_a.mount_wait()
        dentries = self.mount_a.ls("subdir/")

        # The damaged guy should have disappeared
        self.assertEqual(dentries, ["file_undamaged"])

        # I should get ENOENT if I try and read it normally, because
        # the dir is considered complete
        try:
            self.mount_a.stat("subdir/file_to_be_damaged", wait=True)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            raise AssertionError("Expected ENOENT")

        # The fact that there is damaged should have bee recorded
        damage = json.loads(
            self.get_ceph_cmd_stdout(
                'tell', f'mds.{self.fs.get_active_names()[0]}',
                "damage", "ls", '--format=json-pretty'))
        self.assertEqual(len(damage), 1)
        damage_id = damage[0]['id']

        # If I try to create a dentry with the same name as the damaged guy
        # then that should be forbidden
        try:
            self.mount_a.touch("subdir/file_to_be_damaged")
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EIO)
        else:
            raise AssertionError("Expected EIO")

        # Attempting that touch will clear the client's complete flag, now
        # when I stat it I'll get EIO instead of ENOENT
        try:
            self.mount_a.stat("subdir/file_to_be_damaged", wait=True)
        except CommandFailedError as e:
            if isinstance(self.mount_a, FuseMount):
                self.assertEqual(e.exitstatus, errno.EIO)
            else:
                # Old kernel client handles this case differently
                self.assertIn(e.exitstatus, [errno.ENOENT, errno.EIO])
        else:
            raise AssertionError("Expected EIO")

        nfiles = self.mount_a.getfattr("./subdir", "ceph.dir.files")
        self.assertEqual(nfiles, "2")

        self.mount_a.umount_wait()

        # Now repair the stats
        scrub_json = self.fs.run_scrub(["start", "/subdir", "repair"])
        log.info(json.dumps(scrub_json, indent=2))

        self.assertNotEqual(scrub_json, None)
        self.assertEqual(scrub_json["return_code"], 0)
        self.assertEqual(self.fs.wait_until_scrub_complete(tag=scrub_json["scrub_tag"]), True)

        # Check that the file count is now correct
        self.mount_a.mount_wait()
        nfiles = self.mount_a.getfattr("./subdir", "ceph.dir.files")
        self.assertEqual(nfiles, "1")

        # Clean up the omap object
        self.fs.radosm(["setomapval", dirfrag_obj, "file_to_be_damaged_head", junk])

        # Clean up the damagetable entry
        self.run_ceph_cmd(
            'tell', f'mds.{self.fs.get_active_names()[0]}',
            "damage", "rm", f"{damage_id}")

        # Now I should be able to create a file with the same name as the
        # damaged guy if I want.
        self.mount_a.touch("subdir/file_to_be_damaged")

    def test_open_ino_errors(self):
        """
        That errors encountered during opening inos are properly propagated
        """

        self.mount_a.run_shell(["mkdir", "dir1"])
        self.mount_a.run_shell(["touch", "dir1/file1"])
        self.mount_a.run_shell(["mkdir", "dir2"])
        self.mount_a.run_shell(["touch", "dir2/file2"])
        self.mount_a.run_shell(["mkdir", "testdir"])
        self.mount_a.run_shell(["ln", "dir1/file1", "testdir/hardlink1"])
        self.mount_a.run_shell(["ln", "dir2/file2", "testdir/hardlink2"])

        file1_ino = self.mount_a.path_to_ino("dir1/file1")
        file2_ino = self.mount_a.path_to_ino("dir2/file2")
        dir2_ino = self.mount_a.path_to_ino("dir2")

        # Ensure everything is written to backing store
        self.mount_a.umount_wait()
        self.fs.mds_asok(["flush", "journal"])

        # Drop everything from the MDS cache
        self.fs.fail()
        self.fs.journal_tool(['journal', 'reset'], 0)
        self.fs.set_joinable()
        self.fs.wait_for_daemons()

        self.mount_a.mount_wait()

        # Case 1: un-decodeable backtrace

        # Validate that the backtrace is present and decodable
        self.fs.read_backtrace(file1_ino)
        # Go corrupt the backtrace of alpha/target (used for resolving
        # bravo/hardlink).
        self.fs._write_data_xattr(file1_ino, "parent", "rhubarb")

        # Check that touching the hardlink gives EIO
        ran = self.mount_a.run_shell(["stat", "testdir/hardlink1"], wait=False)
        try:
            ran.wait()
        except CommandFailedError:
            self.assertTrue("Input/output error" in ran.stderr.getvalue())

        # Check that an entry is created in the damage table
        damage = json.loads(
            self.get_ceph_cmd_stdout(
                'tell', 'mds.{0}'.format(self.fs.get_active_names()[0]),
                "damage", "ls", '--format=json-pretty'))
        self.assertEqual(len(damage), 1)
        self.assertEqual(damage[0]['damage_type'], "backtrace")
        self.assertEqual(damage[0]['ino'], file1_ino)

        self.run_ceph_cmd(
            'tell', 'mds.{0}'.format(self.fs.get_active_names()[0]),
            "damage", "rm", str(damage[0]['id']))


        # Case 2: missing dirfrag for the target inode

        self.fs.radosm(["rm", "{0:x}.00000000".format(dir2_ino)])

        # Check that touching the hardlink gives EIO
        ran = self.mount_a.run_shell(["stat", "testdir/hardlink2"], wait=False)
        try:
            ran.wait()
        except CommandFailedError:
            self.assertTrue("Input/output error" in ran.stderr.getvalue())

        # Check that an entry is created in the damage table
        damage = json.loads(
            self.get_ceph_cmd_stdout(
                'tell', 'mds.{0}'.format(self.fs.get_active_names()[0]),
                "damage", "ls", '--format=json-pretty'))
        self.assertEqual(len(damage), 2)
        if damage[0]['damage_type'] == "backtrace" :
            self.assertEqual(damage[0]['ino'], file2_ino)
            self.assertEqual(damage[1]['damage_type'], "dir_frag")
            self.assertEqual(damage[1]['ino'], dir2_ino)
        else:
            self.assertEqual(damage[0]['damage_type'], "dir_frag")
            self.assertEqual(damage[0]['ino'], dir2_ino)
            self.assertEqual(damage[1]['damage_type'], "backtrace")
            self.assertEqual(damage[1]['ino'], file2_ino)

        for entry in damage:
            self.run_ceph_cmd(
                'tell', 'mds.{0}'.format(self.fs.get_active_names()[0]),
                "damage", "rm", str(entry['id']))

    def test_dentry_first_existing(self):
        """
        That the MDS won't abort when the dentry is already known to be damaged.
        """

        def verify_corrupt():
            info = self.fs.read_cache("/a", 0)
            log.debug('%s', info)
            self.assertEqual(len(info), 1)
            dirfrags = info[0]['dirfrags']
            self.assertEqual(len(dirfrags), 1)
            dentries = dirfrags[0]['dentries']
            self.assertEqual([dn['path'] for dn in dentries if dn['is_primary']], ['a/c'])
            self.assertEqual(dentries[0]['snap_first'], 18446744073709551606) # SNAP_HEAD

        self.mount_a.run_shell_payload("mkdir -p a/b")
        self.fs.flush()
        self.config_set("mds", "mds_abort_on_newly_corrupt_dentry", False)
        self.config_set("mds", "mds_inject_rename_corrupt_dentry_first", "1.0")
        time.sleep(5) # for conf to percolate
        self.mount_a.run_shell_payload("mv a/b a/c; sync .")
        self.mount_a.umount()
        verify_corrupt()
        self.fs.fail()
        self.config_rm("mds", "mds_inject_rename_corrupt_dentry_first")
        self.config_set("mds", "mds_abort_on_newly_corrupt_dentry", False)
        self.fs.set_joinable()
        status = self.fs.status()
        self.fs.flush()
        self.assertFalse(self.fs.status().hadfailover(status))
        verify_corrupt()

    def test_dentry_first_preflush(self):
        """
        That the MDS won't write a dentry with new damage to CDentry::first
        to the journal.
        """

        rank0 = self.fs.get_rank()
        self.fs.rank_freeze(True, rank=0)
        self.mount_a.run_shell_payload("mkdir -p a/{b,c}/d")
        self.fs.flush()
        self.config_set("mds", "mds_inject_rename_corrupt_dentry_first", "1.0")
        time.sleep(5) # for conf to percolate
        with self.assert_cluster_log("MDS abort because newly corrupt dentry"):
            p = self.mount_a.run_shell_payload("timeout 60 mv a/b a/z", wait=False)
            self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(), timeout=self.fs.beacon_timeout)
        self.config_rm("mds", "mds_inject_rename_corrupt_dentry_first")
        self.fs.rank_freeze(False, rank=0)
        self.delete_mds_coredump(rank0['name'])
        self.fs.mds_restart(rank0['name'])
        self.fs.wait_for_daemons()
        p.wait()
        self.mount_a.run_shell_payload("stat a/ && find a/")
        self.fs.flush()

    def test_dentry_first_precommit(self):
        """
        That the MDS won't write a dentry with new damage to CDentry::first
        to the directory object.
        """

        fscid = self.fs.id
        self.mount_a.run_shell_payload("mkdir -p a/{b,c}/d; sync .")
        self.mount_a.umount() # allow immediate scatter write back
        self.fs.flush()
        # now just twiddle some inode metadata on a regular file
        self.mount_a.mount_wait()
        self.mount_a.run_shell_payload("chmod 711 a/b/d; sync .")
        self.mount_a.umount() # avoid journaling session related things
        # okay, now cause the dentry to get damaged after loading from the journal
        self.fs.fail()
        self.config_set("mds", "mds_inject_journal_corrupt_dentry_first", "1.0")
        time.sleep(5) # for conf to percolate
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        rank0 = self.fs.get_rank()
        self.fs.rank_freeze(True, rank=0)
        # so now we want to trigger commit but this will crash, so:
        with self.assert_cluster_log("MDS abort because newly corrupt dentry"):
            c = ['--connect-timeout=60', 'tell', f"mds.{fscid}:0", "flush", "journal"]
            p = self.ceph_cluster.mon_manager.run_cluster_cmd(args=c, wait=False, timeoutcmd=30)
            self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(), timeout=self.fs.beacon_timeout)
        self.config_rm("mds", "mds_inject_journal_corrupt_dentry_first")
        self.fs.rank_freeze(False, rank=0)
        self.delete_mds_coredump(rank0['name'])
        self.fs.mds_restart(rank0['name'])
        self.fs.wait_for_daemons()
        try:
            p.wait()
        except CommandFailedError as e:
            print(e)
        else:
            self.fail("flush journal should fail!")
        self.mount_a.mount_wait()
        self.mount_a.run_shell_payload("stat a/ && find a/")
        self.fs.flush()
