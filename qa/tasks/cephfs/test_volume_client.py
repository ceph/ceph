import json
import logging
import time
import os
from textwrap import dedent
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


class TestVolumeClient(CephFSTestCase):
    #
    # TODO: Test that VolumeClient can recover from partial auth updates.
    #

    # One for looking at the global filesystem, one for being
    # the VolumeClient, two for mounting the created shares
    CLIENTS_REQUIRED = 4

    def _volume_client_python(self, client, script, vol_prefix=None, ns_prefix=None):
        # Can't dedent this *and* the script we pass in, because they might have different
        # levels of indentation to begin with, so leave this string zero-indented
        if vol_prefix:
            vol_prefix = "\"" + vol_prefix + "\""
        if ns_prefix:
            ns_prefix = "\"" + ns_prefix + "\""
        return client.run_python("""
from ceph_volume_client import CephFSVolumeClient, VolumePath
import logging
log = logging.getLogger("ceph_volume_client")
log.addHandler(logging.StreamHandler())
log.setLevel(logging.DEBUG)
vc = CephFSVolumeClient("manila", "{conf_path}", "ceph", {vol_prefix}, {ns_prefix})
vc.connect()
{payload}
vc.disconnect()
        """.format(payload=script, conf_path=client.config_path, vol_prefix=vol_prefix, ns_prefix=ns_prefix))

    def _sudo_write_file(self, remote, path, data):
        """
        Write data to a remote file as super user

        :param remote: Remote site.
        :param path: Path on the remote being written to.
        :param data: Data to be written.

        Both perms and owner are passed directly to chmod.
        """
        remote.run(
            args=[
                'sudo',
                'python',
                '-c',
                'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
                path,
            ],
            stdin=data,
        )

    def _configure_vc_auth(self, mount, id_name):
        """
        Set up auth credentials for the VolumeClient user
        """
        out = self.fs.mon_manager.raw_cluster_cmd(
            "auth", "get-or-create", "client.{name}".format(name=id_name),
            "mds", "allow *",
            "osd", "allow rw",
            "mon", "allow *"
        )
        mount.client_id = id_name
        self._sudo_write_file(mount.client_remote, mount.get_keyring_path(), out)
        self.set_conf("client.{name}".format(name=id_name), "keyring", mount.get_keyring_path())

    def _configure_guest_auth(self, volumeclient_mount, guest_mount,
                              guest_entity, mount_path,
                              namespace_prefix=None, readonly=False,
                              tenant_id=None):
        """
        Set up auth credentials for the guest client to mount a volume.

        :param volumeclient_mount: mount used as the handle for driving
                                   volumeclient.
        :param guest_mount: mount used by the guest client.
        :param guest_entity: auth ID used by the guest client.
        :param mount_path: path of the volume.
        :param namespace_prefix: name prefix of the RADOS namespace, which
                                 is used for the volume's layout.
        :param readonly: defaults to False. If set to 'True' only read-only
                         mount access is granted to the guest.
        :param tenant_id: (OpenStack) tenant ID of the guest client.
        """

        head, volume_id = os.path.split(mount_path)
        head, group_id = os.path.split(head)
        head, volume_prefix = os.path.split(head)
        volume_prefix = "/" + volume_prefix

        # Authorize the guest client's auth ID to mount the volume.
        key = self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            auth_result = vc.authorize(vp, "{guest_entity}", readonly={readonly},
                                       tenant_id="{tenant_id}")
            print auth_result['auth_key']
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity=guest_entity,
            readonly=readonly,
            tenant_id=tenant_id)), volume_prefix, namespace_prefix
        )

        # CephFSVolumeClient's authorize() does not return the secret
        # key to a caller who isn't multi-tenant aware. Explicitly
        # query the key for such a client.
        if not tenant_id:
            key = self.fs.mon_manager.raw_cluster_cmd(
            "auth", "get-key", "client.{name}".format(name=guest_entity),
            )

        # The guest auth ID should exist.
        existing_ids = [a['entity'] for a in self.auth_list()]
        self.assertIn("client.{0}".format(guest_entity), existing_ids)

        # Create keyring file for the guest client.
        keyring_txt = dedent("""
        [client.{guest_entity}]
            key = {key}

        """.format(
            guest_entity=guest_entity,
            key=key
        ))
        guest_mount.client_id = guest_entity
        self._sudo_write_file(guest_mount.client_remote,
                              guest_mount.get_keyring_path(),
                              keyring_txt)

        # Add a guest client section to the ceph config file.
        self.set_conf("client.{0}".format(guest_entity), "client quota", "True")
        self.set_conf("client.{0}".format(guest_entity), "debug client", "20")
        self.set_conf("client.{0}".format(guest_entity), "debug objecter", "20")
        self.set_conf("client.{0}".format(guest_entity),
                      "keyring", guest_mount.get_keyring_path())

    def test_default_prefix(self):
        group_id = "grpid"
        volume_id = "volid"
        DEFAULT_VOL_PREFIX = "volumes"
        DEFAULT_NS_PREFIX = "fsvolumens_"

        self.mount_b.umount_wait()
        self._configure_vc_auth(self.mount_b, "manila")

        #create a volume with default prefix
        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 10, data_isolated=True)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        # The dir should be created
        self.mount_a.stat(os.path.join(DEFAULT_VOL_PREFIX, group_id, volume_id))

        #namespace should be set
        ns_in_attr = self.mount_a.getfattr(os.path.join(DEFAULT_VOL_PREFIX, group_id, volume_id), "ceph.dir.layout.pool_namespace")
        namespace = "{0}{1}".format(DEFAULT_NS_PREFIX, volume_id)
        self.assertEqual(namespace, ns_in_attr)


    def test_lifecycle(self):
        """
        General smoke test for create, extend, destroy
        """

        # I'm going to use mount_c later as a guest for mounting the created
        # shares
        self.mounts[2].umount()

        # I'm going to leave mount_b unmounted and just use it as a handle for
        # driving volumeclient.  It's a little hacky but we don't have a more
        # general concept for librados/libcephfs clients as opposed to full
        # blown mounting clients.
        self.mount_b.umount_wait()
        self._configure_vc_auth(self.mount_b, "manila")

        guest_entity = "guest"
        group_id = "grpid"
        volume_id = "volid"

        volume_prefix = "/myprefix"
        namespace_prefix = "mynsprefix_"

        # Create a 100MB volume
        volume_size = 100
        mount_path = self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            create_result = vc.create_volume(vp, 1024*1024*{volume_size})
            print create_result['mount_path']
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            volume_size=volume_size
        )), volume_prefix, namespace_prefix)

        # The dir should be created
        self.mount_a.stat(os.path.join("myprefix", group_id, volume_id))

        # Authorize and configure credentials for the guest to mount the
        # the volume.
        self._configure_guest_auth(self.mount_b, self.mounts[2], guest_entity,
                                   mount_path, namespace_prefix)
        self.mounts[2].mount(mount_path=mount_path)

        # df should see volume size, same as the quota set on volume's dir
        self.assertEqual(self.mounts[2].df()['total'],
                         volume_size * 1024 * 1024)
        self.assertEqual(
                self.mount_a.getfattr(
                    os.path.join(volume_prefix.strip("/"), group_id, volume_id),
                    "ceph.quota.max_bytes"),
                "%s" % (volume_size * 1024 * 1024))

        # df granularity is 4MB block so have to write at least that much
        data_bin_mb = 4
        self.mounts[2].write_n_mb("data.bin", data_bin_mb)

        # Write something outside volume to check this space usage is
        # not reported in the volume's DF.
        other_bin_mb = 6
        self.mount_a.write_n_mb("other.bin", other_bin_mb)

        # global: df should see all the writes (data + other).  This is a >
        # rather than a == because the global spaced used includes all pools
        self.assertGreater(self.mount_a.df()['used'],
                           (data_bin_mb + other_bin_mb) * 1024 * 1024)

        # Hack: do a metadata IO to kick rstats
        self.mounts[2].run_shell(["touch", "foo"])

        # volume: df should see the data_bin_mb consumed from quota, same
        # as the rbytes for the volume's dir
        self.wait_until_equal(
                lambda: self.mounts[2].df()['used'],
                data_bin_mb * 1024 * 1024, timeout=60)
        self.wait_until_equal(
                lambda: self.mount_a.getfattr(
                    os.path.join(volume_prefix.strip("/"), group_id, volume_id),
                    "ceph.dir.rbytes"),
                "%s" % (data_bin_mb * 1024 * 1024), timeout=60)

        # sync so that file data are persist to rados
        self.mounts[2].run_shell(["sync"])

        # Our data should stay in particular rados namespace
        pool_name = self.mount_a.getfattr(os.path.join("myprefix", group_id, volume_id), "ceph.dir.layout.pool")
        namespace = "{0}{1}".format(namespace_prefix, volume_id)
        ns_in_attr = self.mount_a.getfattr(os.path.join("myprefix", group_id, volume_id), "ceph.dir.layout.pool_namespace")
        self.assertEqual(namespace, ns_in_attr)

        objects_in_ns = set(self.fs.rados(["ls"], pool=pool_name, namespace=namespace).split("\n"))
        self.assertNotEqual(objects_in_ns, set())

        # De-authorize the guest
        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity}")
            vc.evict("{guest_entity}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity=guest_entity
        )), volume_prefix, namespace_prefix)

        # Once deauthorized, the client should be unable to do any more metadata ops
        # The way that the client currently behaves here is to block (it acts like
        # it has lost network, because there is nothing to tell it that is messages
        # are being dropped because it's identity is gone)
        background = self.mounts[2].write_n_mb("rogue.bin", 1, wait=False)
        time.sleep(10)  # Approximate check for 'stuck' as 'still running after 10s'
        self.assertFalse(background.finished)

        # After deauthorisation, the client ID should be gone (this was the only
        # volume it was authorised for)
        self.assertNotIn("client.{0}".format(guest_entity), [e['entity'] for e in self.auth_list()])

        # Clean up the dead mount (ceph-fuse's behaviour here is a bit undefined)
        self.mounts[2].kill()
        self.mounts[2].kill_cleanup()
        try:
            background.wait()
        except CommandFailedError:
            # We killed the mount out from under you
            pass

        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
            vc.purge_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )), volume_prefix, namespace_prefix)

    def test_idempotency(self):
        """
        That the volumeclient interface works when calling everything twice
        """
        self.mount_b.umount_wait()
        self._configure_vc_auth(self.mount_b, "manila")

        guest_entity = "guest"
        group_id = "grpid"
        volume_id = "volid"
        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 10)
            vc.create_volume(vp, 10)
            vc.authorize(vp, "{guest_entity}")
            vc.authorize(vp, "{guest_entity}")
            vc.deauthorize(vp, "{guest_entity}")
            vc.deauthorize(vp, "{guest_entity}")
            vc.delete_volume(vp)
            vc.delete_volume(vp)
            vc.purge_volume(vp)
            vc.purge_volume(vp)

            vc.create_volume(vp, 10, data_isolated=True)
            vc.create_volume(vp, 10, data_isolated=True)
            vc.authorize(vp, "{guest_entity}")
            vc.authorize(vp, "{guest_entity}")
            vc.deauthorize(vp, "{guest_entity}")
            vc.deauthorize(vp, "{guest_entity}")
            vc.evict("{guest_entity}")
            vc.evict("{guest_entity}")
            vc.delete_volume(vp, data_isolated=True)
            vc.delete_volume(vp, data_isolated=True)
            vc.purge_volume(vp, data_isolated=True)
            vc.purge_volume(vp, data_isolated=True)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity=guest_entity
        )))

    def test_data_isolated(self):
        """
        That data isolated shares get their own pool
        :return:
        """

        # Because the teuthology config template sets mon_pg_warn_max_per_osd to
        # 10000 (i.e. it just tries to ignore health warnings), reset it to something
        # sane before using volume_client, to avoid creating pools with absurdly large
        # numbers of PGs.
        self.set_conf("global", "mon pg warn max per osd", "300")
        for mon_daemon_state in self.ctx.daemons.iter_daemons_of_role('mon'):
            mon_daemon_state.restart()

        self.mount_b.umount_wait()
        self._configure_vc_auth(self.mount_b, "manila")

        # Calculate how many PGs we'll expect the new volume pool to have
        osd_map = json.loads(self.fs.mon_manager.raw_cluster_cmd('osd', 'dump', '--format=json-pretty'))
        max_per_osd = int(self.fs.get_config('mon_pg_warn_max_per_osd'))
        osd_count = len(osd_map['osds'])
        max_overall = osd_count * max_per_osd

        existing_pg_count = 0
        for p in osd_map['pools']:
            existing_pg_count += p['pg_num']

        expected_pg_num = (max_overall - existing_pg_count) / 10
        log.info("max_per_osd {0}".format(max_per_osd))
        log.info("osd_count {0}".format(osd_count))
        log.info("max_overall {0}".format(max_overall))
        log.info("existing_pg_count {0}".format(existing_pg_count))
        log.info("expected_pg_num {0}".format(expected_pg_num))

        pools_a = json.loads(self.fs.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['pools']

        group_id = "grpid"
        volume_id = "volid"
        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 10, data_isolated=True)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        pools_b = json.loads(self.fs.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['pools']

        # Should have created one new pool
        new_pools = set(p['pool_name'] for p in pools_b) - set([p['pool_name'] for p in pools_a])
        self.assertEqual(len(new_pools), 1)

        # It should have followed the heuristic for PG count
        # (this is an overly strict test condition, so we may want to remove
        #  it at some point as/when the logic gets fancier)
        created_pg_num = self.fs.mon_manager.get_pool_property(list(new_pools)[0], "pg_num")
        self.assertEqual(expected_pg_num, created_pg_num)

    def test_15303(self):
        """
        Reproducer for #15303 "Client holds incorrect complete flag on dir
        after losing caps" (http://tracker.ceph.com/issues/15303)
        """
        for m in self.mounts:
            m.umount_wait()

        # Create a dir on mount A
        self.mount_a.mount()
        self.mount_a.run_shell(["mkdir", "parent1"])
        self.mount_a.run_shell(["mkdir", "parent2"])
        self.mount_a.run_shell(["mkdir", "parent1/mydir"])

        # Put some files in it from mount B
        self.mount_b.mount()
        self.mount_b.run_shell(["touch", "parent1/mydir/afile"])
        self.mount_b.umount_wait()

        # List the dir's contents on mount A
        self.assertListEqual(self.mount_a.ls("parent1/mydir"),
                             ["afile"])

    def test_evict_client(self):
        """
        That a volume client can be evicted based on its auth ID and the volume
        path it has mounted.
        """

        # mounts[1] would be used as handle for driving VolumeClient. mounts[2]
        # and mounts[3] would be used as guests to mount the volumes/shares.

        for i in range(1, 4):
            self.mounts[i].umount_wait()

        volumeclient_mount = self.mounts[1]
        self._configure_vc_auth(volumeclient_mount, "manila")
        guest_mounts = (self.mounts[2], self.mounts[3])

        guest_entity = "guest"
        group_id = "grpid"
        mount_paths = []
        volume_ids = []

        # Create two volumes. Authorize 'guest' auth ID to mount the two
        # volumes. Mount the two volumes. Write data to the volumes.
        for i in range(2):
            # Create volume.
            volume_ids.append("volid_{0}".format(str(i)))
            mount_paths.append(
                self._volume_client_python(volumeclient_mount, dedent("""
                    vp = VolumePath("{group_id}", "{volume_id}")
                    create_result = vc.create_volume(vp, 10 * 1024 * 1024)
                    print create_result['mount_path']
                """.format(
                    group_id=group_id,
                    volume_id=volume_ids[i]
            ))))

            # Authorize 'guest' auth ID to mount the volume.
            self._configure_guest_auth(volumeclient_mount, guest_mounts[i],
                                       guest_entity, mount_paths[i])

            # Mount the volume.
            guest_mounts[i].mountpoint_dir_name = 'mnt.{id}.{suffix}'.format(
                id=guest_entity, suffix=str(i))
            guest_mounts[i].mount(mount_path=mount_paths[i])
            guest_mounts[i].write_n_mb("data.bin", 1)


        # Evict client, guest_mounts[0], using auth ID 'guest' and has mounted
        # one volume.
        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.evict("{guest_entity}", volume_path=vp)
        """.format(
            group_id=group_id,
            volume_id=volume_ids[0],
            guest_entity=guest_entity
        )))

        # Evicted guest client, guest_mounts[0], should not be able to do
        # anymore metadata ops. It behaves as if it has lost network
        # connection.
        background = guest_mounts[0].write_n_mb("rogue.bin", 1, wait=False)
        # Approximate check for 'stuck' as 'still running after 10s'.
        time.sleep(10)
        self.assertFalse(background.finished)

        # Guest client, guest_mounts[1], using the same auth ID 'guest', but
        # has mounted the other volume, should be able to use its volume
        # unaffected.
        guest_mounts[1].write_n_mb("data.bin.1", 1)

        # Cleanup.
        for i in range(2):
            self._volume_client_python(volumeclient_mount, dedent("""
                vp = VolumePath("{group_id}", "{volume_id}")
                vc.deauthorize(vp, "{guest_entity}")
                vc.delete_volume(vp)
                vc.purge_volume(vp)
            """.format(
                group_id=group_id,
                volume_id=volume_ids[i],
                guest_entity=guest_entity
            )))

        # We must hard-umount the one that we evicted
        guest_mounts[0].umount_wait(force=True)

    def test_purge(self):
        """
        Reproducer for #15266, exception trying to purge volumes that
        contain non-ascii filenames.

        Additionally test any other purge corner cases here.
        """
        # I'm going to leave mount_b unmounted and just use it as a handle for
        # driving volumeclient.  It's a little hacky but we don't have a more
        # general concept for librados/libcephfs clients as opposed to full
        # blown mounting clients.
        self.mount_b.umount_wait()
        self._configure_vc_auth(self.mount_b, "manila")

        group_id = "grpid"
        # Use a unicode volume ID (like Manila), to reproduce #15266
        volume_id = u"volid"

        # Create
        mount_path = self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", u"{volume_id}")
            create_result = vc.create_volume(vp, 10)
            print create_result['mount_path']
        """.format(
            group_id=group_id,
            volume_id=volume_id
        )))

        # Strip leading "/"
        mount_path = mount_path[1:]

        # A file with non-ascii characters
        self.mount_a.run_shell(["touch", os.path.join(mount_path, u"b\u00F6b")])

        # A file with no permissions to do anything
        self.mount_a.run_shell(["touch", os.path.join(mount_path, "noperms")])
        self.mount_a.run_shell(["chmod", "0000", os.path.join(mount_path, "noperms")])

        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", u"{volume_id}")
            vc.delete_volume(vp)
            vc.purge_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id
        )))

        # Check it's really gone
        self.assertEqual(self.mount_a.ls("volumes/_deleting"), [])
        self.assertEqual(self.mount_a.ls("volumes/"), ["_deleting", group_id])

    def test_readonly_authorization(self):
        """
        That guest clients can be restricted to read-only mounts of volumes.
        """

        volumeclient_mount = self.mounts[1]
        guest_mount = self.mounts[2]
        volumeclient_mount.umount_wait()
        guest_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        guest_entity = "guest"
        group_id = "grpid"
        volume_id = "volid"

        # Create a volume.
        mount_path = self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            create_result = vc.create_volume(vp, 1024*1024*10)
            print create_result['mount_path']
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        # Authorize and configure credentials for the guest to mount the
        # the volume with read-write access.
        self._configure_guest_auth(volumeclient_mount, guest_mount, guest_entity,
                                   mount_path, readonly=False)

        # Mount the volume, and write to it.
        guest_mount.mount(mount_path=mount_path)
        guest_mount.write_n_mb("data.bin", 1)

        # Change the guest auth ID's authorization to read-only mount access.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity=guest_entity
        )))
        self._configure_guest_auth(volumeclient_mount, guest_mount, guest_entity,
                                   mount_path, readonly=True)

        # The effect of the change in access level to read-only is not
        # immediate. The guest sees the change only after a remount of
        # the volume.
        guest_mount.umount_wait()
        guest_mount.mount(mount_path=mount_path)

        # Read existing content of the volume.
        self.assertListEqual(guest_mount.ls(guest_mount.mountpoint), ["data.bin"])
        # Cannot write into read-only volume.
        with self.assertRaises(CommandFailedError):
            guest_mount.write_n_mb("rogue.bin", 1)

    def test_get_authorized_ids(self):
        """
        That for a volume, the authorized IDs and their access levels
        can be obtained using CephFSVolumeClient's get_authorized_ids().
        """
        volumeclient_mount = self.mounts[1]
        volumeclient_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        group_id = "grpid"
        volume_id = "volid"
        guest_entity_1 = "guest1"
        guest_entity_2 = "guest2"

        log.info("print group ID: {0}".format(group_id))

        # Create a volume.
        auths = self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 1024*1024*10)
            auths = vc.get_authorized_ids(vp)
            print auths
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))
        # Check the list of authorized IDs for the volume.
        expected_result = None
        self.assertEqual(str(expected_result), auths)

        # Allow two auth IDs access to the volume.
        auths = self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{guest_entity_1}", readonly=False)
            vc.authorize(vp, "{guest_entity_2}", readonly=True)
            auths = vc.get_authorized_ids(vp)
            print auths
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity_1=guest_entity_1,
            guest_entity_2=guest_entity_2,
        )))
        # Check the list of authorized IDs and their access levels.
        expected_result = [(u'guest1', u'rw'), (u'guest2', u'r')]
        self.assertItemsEqual(str(expected_result), auths)

        # Disallow both the auth IDs' access to the volume.
        auths = self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity_1}")
            vc.deauthorize(vp, "{guest_entity_2}")
            auths = vc.get_authorized_ids(vp)
            print auths
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity_1=guest_entity_1,
            guest_entity_2=guest_entity_2,
        )))
        # Check the list of authorized IDs for the volume.
        expected_result = None
        self.assertItemsEqual(str(expected_result), auths)

    def test_multitenant_volumes(self):
        """
        That volume access can be restricted to a tenant.

        That metadata used to enforce tenant isolation of
        volumes is stored as a two-way mapping between auth
        IDs and volumes that they're authorized to access.
        """
        volumeclient_mount = self.mounts[1]
        volumeclient_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        group_id = "groupid"
        volume_id = "volumeid"

        # Guest clients belonging to different tenants, but using the same
        # auth ID.
        auth_id = "guest"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }
        guestclient_2 = {
            "auth_id": auth_id,
            "tenant_id": "tenant2",
        }

        # Create a volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 1024*1024*10)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        # Check that volume metadata file is created on volume creation.
        vol_metadata_filename = "_{0}:{1}.meta".format(group_id, volume_id)
        self.assertIn(vol_metadata_filename, self.mounts[0].ls("volumes"))

        # Authorize 'guestclient_1', using auth ID 'guest' and belonging to
        # 'tenant1', with 'rw' access to the volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            auth_id=guestclient_1["auth_id"],
            tenant_id=guestclient_1["tenant_id"]
        )))

        # Check that auth metadata file for auth ID 'guest', is
        # created on authorizing 'guest' access to the volume.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, self.mounts[0].ls("volumes"))

        # Verify that the auth metadata file stores the tenant ID that the
        # auth ID belongs to, the auth ID's authorized access levels
        # for different volumes, versioning details, etc.
        expected_auth_metadata = {
            u"version": 1,
            u"compat_version": 1,
            u"dirty": False,
            u"tenant_id": u"tenant1",
            u"volumes": {
                u"groupid/volumeid": {
                    u"dirty": False,
                    u"access_level": u"rw",
                }
            }
        }

        auth_metadata = self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            auth_metadata = vc._auth_metadata_get("{auth_id}")
            print auth_metadata
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            auth_id=guestclient_1["auth_id"],
        )))

        self.assertItemsEqual(str(expected_auth_metadata), auth_metadata)

        # Verify that the volume metadata file stores info about auth IDs
        # and their access levels to the volume, versioning details, etc.
        expected_vol_metadata = {
            u"version": 1,
            u"compat_version": 1,
            u"auths": {
                u"guest": {
                    u"dirty": False,
                    u"access_level": u"rw"
                }
            }
        }

        vol_metadata = self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            volume_metadata = vc._volume_metadata_get(vp)
            print volume_metadata
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))
        self.assertItemsEqual(str(expected_vol_metadata), vol_metadata)

        # Cannot authorize 'guestclient_2' to access the volume.
        # It uses auth ID 'guest', which has already been used by a
        # 'guestclient_1' belonging to an another tenant for accessing
        # the volume.
        with self.assertRaises(CommandFailedError):
            self._volume_client_python(volumeclient_mount, dedent("""
                vp = VolumePath("{group_id}", "{volume_id}")
                vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
            """.format(
                group_id=group_id,
                volume_id=volume_id,
                auth_id=guestclient_2["auth_id"],
                tenant_id=guestclient_2["tenant_id"]
            )))

        # Check that auth metadata file is cleaned up on removing
        # auth ID's only access to a volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity=guestclient_1["auth_id"]
        )))

        self.assertNotIn(auth_metadata_filename, self.mounts[0].ls("volumes"))

        # Check that volume metadata file is cleaned up on volume deletion.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))
        self.assertNotIn(vol_metadata_filename, self.mounts[0].ls("volumes"))

    def test_recover_metadata(self):
        """
        That volume client can recover from partial auth updates using
        metadata files, which store auth info and its update status info.
        """
        volumeclient_mount = self.mounts[1]
        volumeclient_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        group_id = "groupid"
        volume_id = "volumeid"

        guestclient = {
            "auth_id": "guest",
            "tenant_id": "tenant",
        }

        # Create a volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 1024*1024*10)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        # Authorize 'guestclient' access to the volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            auth_id=guestclient["auth_id"],
            tenant_id=guestclient["tenant_id"]
        )))

        # Check that auth metadata file for auth ID 'guest' is created.
        auth_metadata_filename = "${0}.meta".format(guestclient["auth_id"])
        self.assertIn(auth_metadata_filename, self.mounts[0].ls("volumes"))

        # Induce partial auth update state by modifying the auth metadata file,
        # and then run recovery procedure.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            auth_metadata = vc._auth_metadata_get("{auth_id}")
            auth_metadata['dirty'] = True
            vc._auth_metadata_set("{auth_id}", auth_metadata)
            vc.recover()
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            auth_id=guestclient["auth_id"],
        )))
