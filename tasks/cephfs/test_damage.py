

import logging
import re
from teuthology.contextutil import MaxWhileTries

from teuthology.orchestra.run import wait
from tasks.cephfs.cephfs_test_case import CephFSTestCase

DAMAGED_ON_START = "damaged_on_start"
DAMAGED_ON_LS = "damaged_on_ls"
CRASHED = "server crashed"
NO_DAMAGE = "no damage"
FAILED_CLIENT = "client failed"
FAILED_SERVER = "server failed"


log = logging.getLogger(__name__)


class TestDamage(CephFSTestCase):
    def _simple_workload_write(self):
        self.mount_a.run_shell(["mkdir", "subdir"])
        self.mount_a.write_n_mb("subdir/sixmegs", 6)
        return self.mount_a.stat("subdir/sixmegs")

    def is_marked_damaged(self, rank):
        mds_map = self.fs.get_mds_map()
        return rank in mds_map['damaged']

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

        self.fs.mds_stop()
        self.fs.mds_fail()

        self.fs.rados(['export', '/tmp/metadata.bin'])

        def is_ignored(obj_id):
            """
            A filter to avoid redundantly mutating many similar objects (e.g.
            stray dirfrags)
            """
            if re.match("60.\.00000000", obj_id) and obj_id != "600.00000000":
                return True

            return False

        objects = self.fs.rados(["ls"]).split("\n")
        objects = [o for o in objects if not is_ignored(o)]

        # Find all objects with an OMAP header
        omap_header_objs = []
        for o in objects:
            header = self.fs.rados(["getomapheader", o])
            # The rados CLI wraps the header output in a hex-printed style
            header_bytes = int(re.match("header \((.+) bytes\)", header).group(1))
            if header_bytes > 0:
                omap_header_objs.append(o)

        # Find all OMAP key/vals
        omap_keys = []
        for o in objects:
            keys_str = self.fs.rados(["listomapkeys", o])
            if keys_str:
                for key in keys_str.split("\n"):
                    omap_keys.append((o, key))

        # Find objects that have data in their bodies
        data_objects = []
        for obj_id in objects:
            stat_out = self.fs.rados(["stat", obj_id])
            size = int(re.match(".+, size (.+)$", stat_out).group(1))
            if size > 0:
                data_objects.append(obj_id)

        # Define the various forms of damage we will inflict
        class MetadataMutation(object):
            def __init__(self, obj_id_, desc_, mutate_fn_, expectation_):
                self.obj_id = obj_id_
                self.desc = desc_
                self.mutate_fn = mutate_fn_
                self.expectation = expectation_

            def __eq__(self, other):
                return self.desc == other.desc

            def __hash__(self):
                return hash(self.desc)

        # Removals
        mutations = []
        for obj_id in objects:
            if obj_id in [
                "400.00000000",
                "100.00000000",
                "10000000000.00000000",
                "1.00000000"
            ]:
                expectation = NO_DAMAGE
            else:
                expectation = DAMAGED_ON_START

            log.info("Expectation on rm '{0}' will be '{1}'".format(
                obj_id, expectation
            ))

            mutations.append(MetadataMutation(
                obj_id,
                "Delete {0}".format(obj_id),
                lambda o=obj_id: self.fs.rados(["rm", o]),
                expectation
            ))

        junk = "deadbeef" * 10

        # Blatant corruptions
        mutations.extend([
            MetadataMutation(
                o,
                "Corrupt {0}".format(o),
                lambda o=o: self.fs.rados(["put", o, "-"], stdin_data=junk),
                DAMAGED_ON_START
            ) for o in data_objects
        ])

        # Truncations
        mutations.extend([
            MetadataMutation(
                o,
                "Truncate {0}".format(o),
                lambda o=o: self.fs.rados(["truncate", o, "0"]),
                DAMAGED_ON_START
            ) for o in data_objects
        ])

        # OMAP value corruptions
        for o, k in omap_keys:
            if o.startswith("1.") or o.startswith("100."):
                expectation = DAMAGED_ON_START
            else:
                expectation = DAMAGED_ON_LS

            mutations.append(
                MetadataMutation(
                    o,
                    "Corrupt omap key {0}:{1}".format(o, k),
                    lambda o=o: self.fs.rados(["setomapval", o, k, junk]),
                    expectation
                )
            )

        # OMAP header corruptions
        for obj_id in omap_header_objs:
            if obj_id == "mds0_sessionmap" or re.match("60.\.00000000", obj_id):
                expectation = DAMAGED_ON_START
            else:
                expectation = NO_DAMAGE

            log.info("Expectation on corrupt header '{0}' will be '{1}'".format(
                obj_id, expectation
            ))

            mutations.append(
                MetadataMutation(
                    obj_id,
                    "Corrupt omap header on {0}".format(obj_id),
                    lambda o=obj_id: self.fs.rados(["setomapheader", o, junk]),
                    expectation
                )
            )

        results = {}

        for mutation in mutations:
            log.info("Applying mutation '{0}'".format(mutation.desc))

            # Reset MDS state
            self.mount_a.umount_wait(force=True)
            self.fs.mds_stop()
            self.fs.mds_fail()
            self.fs.mon_manager.raw_cluster_cmd('mds', 'repaired', '0')

            # Reset RADOS pool state
            self.fs.rados(['import', '/tmp/metadata.bin'])

            # Inject the mutation
            mutation.mutate_fn()

            # Try starting the MDS
            self.fs.mds_restart()

            if mutation.expectation not in (DAMAGED_ON_LS, NO_DAMAGE):
                # Wait for MDS to either come up or go into damaged state
                try:
                    self.wait_until_true(lambda: self.is_marked_damaged(0) or self.fs.are_daemons_healthy(), 60)
                except RuntimeError:
                    crashed = False
                    # Didn't make it to healthy or damaged, did it crash?
                    for daemon_id, daemon in self.fs.mds_daemons.items():
                        if daemon.proc.finished:
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
            self.mount_a.mount()
            self.mount_a.wait_until_mounted()
            proc = self.mount_a.run_shell(["ls", "-R"], wait=False)

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

            else:
                try:
                    wait([proc], 20)
                    log.info("Result: As expected, mutation '{0}' did not caused DAMAGED state".format(mutation.desc))
                    results[mutation] = NO_DAMAGE
                except MaxWhileTries:
                    log.info("Result: Failed to complete client IO on mutation '{0}'".format(mutation.desc))
                    results[mutation] = FAILED_CLIENT

        failures = [(mutation, result) for (mutation, result) in results.items() if mutation.expectation != result]
        if failures:
            log.error("{0} mutations had unexpected outcomes:".format(len(failures)))
            for mutation, result in failures:
                log.error("  Expected '{0}' actually '{1}' from '{2}'".format(
                    mutation.expectation, result, mutation.desc
                ))
            raise RuntimeError("{0} mutations had unexpected outcomes".format(len(failures)))
        else:
            log.info("All mutations had expected outcomes")
