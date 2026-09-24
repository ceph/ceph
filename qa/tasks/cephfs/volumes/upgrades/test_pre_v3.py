from errno import *
from os import urandom
from os.path import join, basename, dirname
from logging import getLogger
from textwrap import dedent
from json import loads

from tasks.cephfs.test_volumes import VolumesHelper

from teuthology.exceptions import CommandFailedError


log = getLogger(__name__)


class TestUpgrades(VolumesHelper):
    '''
    Tests related subvolume upgrade.
    '''

    def test_subvolume_upgrade_legacy_to_v1(self):
        """
        poor man's upgrade test -- rather than going through a full upgrade cycle,
        emulate subvolumes by going through the wormhole and verify if they are
        accessible.
        further ensure that a legacy volume is not updated to v2.
        """
        subvolume1, subvolume2 = self._gen_subvol_name(2)
        group = self._gen_subvol_grp_name()

        # emulate a old-fashioned subvolume -- one in the default group and
        # the other in a custom group
        createpath1 = join(".", "volumes", "_nogroup", subvolume1)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath1], omit_sudo=False)

        # create group
        createpath2 = join(".", "volumes", group, subvolume2)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath2], omit_sudo=False)

        # this would auto-upgrade on access without anyone noticing
        subvolpath1 = self._fs_cmd("subvolume", "getpath", self.volname, subvolume1)
        self.assertNotEqual(subvolpath1, None)
        subvolpath1 = subvolpath1.rstrip() # remove "/" prefix and any trailing newline

        subvolpath2 = self._fs_cmd("subvolume", "getpath", self.volname, subvolume2, group)
        self.assertNotEqual(subvolpath2, None)
        subvolpath2 = subvolpath2.rstrip() # remove "/" prefix and any trailing newline

        # and... the subvolume path returned should be what we created behind the scene
        self.assertEqual(createpath1[1:], subvolpath1)
        self.assertEqual(createpath2[1:], subvolpath2)

        # ensure metadata file is in legacy location, with required version v1
        self._assert_meta_location_and_version(self.volname, subvolume1, version=1, legacy=True)
        self._assert_meta_location_and_version(self.volname, subvolume2, subvol_group=group, version=1, legacy=True)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def _test_subvolume_no_upgrade_v1_sanity(self):
        """
        poor man's upgrade test -- theme continues...

        This test is to ensure v1 subvolumes are retained as is, due to a snapshot being present, and runs through
        a series of operations on the v1 subvolume to ensure they work as expected.
        """
        subvol_md = ["atime", "bytes_pcent", "bytes_quota", "bytes_used", "created_at", "ctime",
                     "data_pool", "gid", "mode", "mon_addrs", "mtime", "path", "pool_namespace",
                     "type", "uid", "features", "state"]
        snap_md = ["created_at", "data_pool", "has_pending_clones"]

        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone1, clone2 = self._gen_subvol_clone_name(2)
        mode = "777"
        uid  = "1000"
        gid  = "1000"

        # emulate a v1 subvolume -- in the default group
        subvolume_path = self._create_v1_subvolume(subvolume)

        # getpath
        subvolpath = self._get_subvolume_path(self.volname, subvolume)
        self.assertEqual(subvolpath, subvolume_path)

        # ls
        subvolumes = loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumes), 1, "subvolume ls count mismatch, expected '1', found {0}".format(len(subvolumes)))
        self.assertEqual(subvolumes[0]['name'], subvolume,
                         "subvolume name mismatch in ls output, expected '{0}', found '{1}'".format(subvolume, subvolumes[0]['name']))

        # info
        subvol_info = loads(self._get_subvolume_info(self.volname, subvolume))
        for md in subvol_md:
            self.assertIn(md, subvol_info, "'{0}' key not present in metadata of subvolume".format(md))

        self.assertEqual(subvol_info["state"], "complete",
                         msg="expected state to be 'complete', found '{0}".format(subvol_info["state"]))
        self.assertEqual(len(subvol_info["features"]), 2,
                         msg="expected 1 feature, found '{0}' ({1})".format(len(subvol_info["features"]), subvol_info["features"]))
        for feature in ['snapshot-clone', 'snapshot-autoprotect']:
            self.assertIn(feature, subvol_info["features"], msg="expected feature '{0}' in subvolume".format(feature))

        # resize
        nsize = self.DEFAULT_FILE_SIZE*1024*1024*10
        self._fs_cmd("subvolume", "resize", self.volname, subvolume, str(nsize))
        subvol_info = loads(self._get_subvolume_info(self.volname, subvolume))
        for md in subvol_md:
            self.assertIn(md, subvol_info, "'{0}' key not present in metadata of subvolume".format(md))
        self.assertEqual(subvol_info["bytes_quota"], nsize, "bytes_quota should be set to '{0}'".format(nsize))

        # create (idempotent) (change some attrs, to ensure attrs are preserved from the snapshot on clone)
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode", mode, "--uid", uid, "--gid", gid)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=8)

        # snap-create
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # check clone status
        self._wait_for_clone_to_complete(clone1)

        # ensure clone is v2
        self._assert_meta_location_and_version(self.volname, clone1, version=2)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone1, source_version=1)

        # clone (older snapshot)
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, 'fake', clone2)

        # check clone status
        self._wait_for_clone_to_complete(clone2)

        # ensure clone is v2
        self._assert_meta_location_and_version(self.volname, clone2, version=2)

        # verify clone
        # TODO: rentries will mismatch till this is fixed https://tracker.ceph.com/issues/46747
        #self._verify_clone(subvolume, 'fake', clone2, source_version=1)

        # snap-info
        snap_info = loads(self._get_subvolume_snapshot_info(self.volname, subvolume, snapshot))
        for md in snap_md:
            self.assertIn(md, snap_info, "'{0}' key not present in metadata of snapshot".format(md))
        self.assertEqual(snap_info["has_pending_clones"], "no")

        # snap-ls
        subvol_snapshots = loads(self._fs_cmd('subvolume', 'snapshot', 'ls', self.volname, subvolume))
        self.assertEqual(len(subvol_snapshots), 2, "subvolume ls count mismatch, expected 2', found {0}".format(len(subvol_snapshots)))
        snapshotnames = [snapshot['name'] for snapshot in subvol_snapshots]
        for name in [snapshot, 'fake']:
            self.assertIn(name, snapshotnames, msg="expected snapshot '{0}' in subvolume snapshot ls".format(name))

        # snap-rm
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, "fake")

        # ensure volume is still at version 1
        self._assert_meta_location_and_version(self.volname, subvolume, version=1)

        # rm
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone1)
        self._fs_cmd("subvolume", "rm", self.volname, clone2)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_no_upgrade_v1_to_v2(self):
        """
        poor man's upgrade test -- theme continues...
        ensure v1 to v2 upgrades are not done automatically due to various states of v1
        """
        subvolume1, subvolume2, subvolume3 = self._gen_subvol_name(3)
        group = self._gen_subvol_grp_name()

        # emulate a v1 subvolume -- in the default group
        subvol1_path = self._create_v1_subvolume(subvolume1)

        # emulate a v1 subvolume -- in a custom group
        subvol2_path = self._create_v1_subvolume(subvolume2, subvol_group=group)

        # emulate a v1 subvolume -- in a clone pending state
        self._create_v1_subvolume(subvolume3, subvol_type='clone', has_snapshot=False, state='pending')

        # this would attempt auto-upgrade on access, but fail to do so as snapshots exist
        subvolpath1 = self._get_subvolume_path(self.volname, subvolume1)
        self.assertEqual(subvolpath1, subvol1_path)

        subvolpath2 = self._get_subvolume_path(self.volname, subvolume2, group_name=group)
        self.assertEqual(subvolpath2, subvol2_path)

        # this would attempt auto-upgrade on access, but fail to do so as volume is not complete
        # use clone status, as only certain operations are allowed in pending state
        status = loads(self._fs_cmd("clone", "status", self.volname, subvolume3))
        self.assertEqual(status["status"]["state"], "pending")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume1, "fake")
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume2, "fake", group)

        # ensure metadata file is in v1 location, with version retained as v1
        self._assert_meta_location_and_version(self.volname, subvolume1, version=1)
        self._assert_meta_location_and_version(self.volname, subvolume2, subvol_group=group, version=1)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, group)
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume3)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, EAGAIN, "invalid error code on rm of subvolume undergoing clone")
        else:
            self.fail("expected rm of subvolume undergoing clone to fail")

        # ensure metadata file is in v1 location, with version retained as v1
        self._assert_meta_location_and_version(self.volname, subvolume3, version=1)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume3, "--force")

        # verify list subvolumes returns an empty list
        subvolumels = loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_upgrade_v1_to_v2(self):
        """
        poor man's upgrade test -- theme continues...
        ensure v1 to v2 upgrades work
        """
        subvolume1, subvolume2 = self._gen_subvol_name(2)
        group = self._gen_subvol_grp_name()

        # emulate a v1 subvolume -- in the default group
        subvol1_path = self._create_v1_subvolume(subvolume1, has_snapshot=False)

        # emulate a v1 subvolume -- in a custom group
        subvol2_path = self._create_v1_subvolume(subvolume2, subvol_group=group, has_snapshot=False)

        # this would attempt auto-upgrade on access
        subvolpath1 = self._get_subvolume_path(self.volname, subvolume1)
        self.assertEqual(subvolpath1, subvol1_path)

        subvolpath2 = self._get_subvolume_path(self.volname, subvolume2, group_name=group)
        self.assertEqual(subvolpath2, subvol2_path)

        # ensure metadata file is in v2 location, with version retained as v2
        self._assert_meta_location_and_version(self.volname, subvolume1, version=2)
        self._assert_meta_location_and_version(self.volname, subvolume2, subvol_group=group, version=2)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def _test_malicious_metafile_on_legacy_to_v1_upgrade(self):
        """
        Validate handcrafted .meta file on legacy subvol root doesn't break the system
        on legacy subvol upgrade to v1
        poor man's upgrade test -- theme continues...
        """
        subvol1, subvol2 = self._gen_subvol_name(2)

        # emulate a old-fashioned subvolume in the default group
        createpath1 = join(".", "volumes", "_nogroup", subvol1)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath1], omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath1, 'ceph.dir.layout.pool', default_pool, sudo=True)

        # create v2 subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvol2)

        # Create malicious .meta file in legacy subvolume root. Copy v2 subvolume
        # .meta into legacy subvol1's root
        subvol2_metapath = join(".", "volumes", "_nogroup", subvol2, ".meta")
        self.mount_a.run_shell(['sudo', 'cp', subvol2_metapath, createpath1], omit_sudo=False)

        # Upgrade legacy subvol1 to v1
        subvolpath1 = self._fs_cmd("subvolume", "getpath", self.volname, subvol1)
        self.assertNotEqual(subvolpath1, None)
        subvolpath1 = subvolpath1.rstrip()

        # the subvolume path returned should not be of subvol2 from handcrafted
        # .meta file
        self.assertEqual(createpath1[1:], subvolpath1)

        # ensure metadata file is in legacy location, with required version v1
        self._assert_meta_location_and_version(self.volname, subvol1, version=1, legacy=True)

        # Authorize alice authID read-write access to subvol1. Verify it authorizes subvol1 path and not subvol2
        # path whose '.meta' file is copied to subvol1 root
        authid1 = "alice"
        self._fs_cmd("subvolume", "authorize", self.volname, subvol1, authid1)

        # Validate that the mds path added is of subvol1 and not of subvol2
        out = loads(self.get_ceph_cmd_stdout("auth", "get", "client.alice", "--format=json-pretty"))
        self.assertEqual("client.alice", out[0]["entity"])
        self.assertEqual("allow rw path={0}".format(createpath1[1:]), out[0]["caps"]["mds"])

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvol1)
        self._fs_cmd("subvolume", "rm", self.volname, subvol2)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def _test_binary_metafile_on_legacy_to_v1_upgrade(self):
        """
        Validate binary .meta file on legacy subvol root doesn't break the system
        on legacy subvol upgrade to v1
        poor man's upgrade test -- theme continues...
        """
        subvol = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # emulate a old-fashioned subvolume -- in a custom group
        createpath = join(".", "volumes", group, subvol)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath, 'ceph.dir.layout.pool', default_pool, sudo=True)

        # Create unparseable binary .meta file on legacy subvol's root
        meta_contents = urandom(4096)
        meta_filepath = join(self.mount_a.mountpoint, createpath, ".meta")
        self.mount_a.client_remote.write_file(meta_filepath, meta_contents, sudo=True)

        # Upgrade legacy subvol to v1
        subvolpath = self._fs_cmd("subvolume", "getpath", self.volname, subvol, group)
        self.assertNotEqual(subvolpath, None)
        subvolpath = subvolpath.rstrip()

        # The legacy subvolume path should be returned for subvol.
        # Should ignore unparseable binary .meta file in subvol's root
        self.assertEqual(createpath[1:], subvolpath)

        # ensure metadata file is in legacy location, with required version v1
        self._assert_meta_location_and_version(self.volname, subvol, subvol_group=group, version=1, legacy=True)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvol, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def _test_unparseable_metafile_on_legacy_to_v1_upgrade(self):
        """
        Validate unparseable text .meta file on legacy subvol root doesn't break the system
        on legacy subvol upgrade to v1
        poor man's upgrade test -- theme continues...
        """
        subvol = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # emulate a old-fashioned subvolume -- in a custom group
        createpath = join(".", "volumes", group, subvol)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath, 'ceph.dir.layout.pool', default_pool, sudo=True)

        # Create unparseable text .meta file on legacy subvol's root
        meta_contents = "unparseable config\nfile ...\nunparseable config\nfile ...\n"
        meta_filepath = join(self.mount_a.mountpoint, createpath, ".meta")
        self.mount_a.client_remote.write_file(meta_filepath, meta_contents, sudo=True)

        # Upgrade legacy subvol to v1
        subvolpath = self._fs_cmd("subvolume", "getpath", self.volname, subvol, group)
        self.assertNotEqual(subvolpath, None)
        subvolpath = subvolpath.rstrip()

        # The legacy subvolume path should be returned for subvol.
        # Should ignore unparseable binary .meta file in subvol's root
        self.assertEqual(createpath[1:], subvolpath)

        # ensure metadata file is in legacy location, with required version v1
        self._assert_meta_location_and_version(self.volname, subvol, subvol_group=group, version=1, legacy=True)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvol, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)
