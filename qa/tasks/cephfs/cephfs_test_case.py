import json
import logging
import os
import re

from tasks.ceph_test_case import CephTestCase

from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

def classhook(m):
    def dec(cls):
        getattr(cls, m)()
        return cls
    return dec

def for_teuthology(f):
    """
    Decorator that adds an "is_for_teuthology" attribute to the wrapped function
    """
    f.is_for_teuthology = True
    return f


def needs_trimming(f):
    """
    Mark fn as requiring a client capable of trimming its cache (i.e. for ceph-fuse
    this means it needs to be able to run as root, currently)
    """
    f.needs_trimming = True
    return f


class MountDetails():

    def __init__(self, mntobj):
        self.client_id = mntobj.client_id
        self.client_keyring_path = mntobj.client_keyring_path
        self.client_remote = mntobj.client_remote
        self.cephfs_name = mntobj.cephfs_name
        self.cephfs_mntpt = mntobj.cephfs_mntpt
        self.hostfs_mntpt = mntobj.hostfs_mntpt

    def restore(self, mntobj):
        mntobj.client_id = self.client_id
        mntobj.client_keyring_path = self.client_keyring_path
        mntobj.client_remote = self.client_remote
        mntobj.cephfs_name = self.cephfs_name
        mntobj.cephfs_mntpt = self.cephfs_mntpt
        mntobj.hostfs_mntpt = self.hostfs_mntpt


class CephFSTestCase(CephTestCase):
    """
    Test case for Ceph FS, requires caller to populate Filesystem and Mounts,
    into the fs, mount_a, mount_b class attributes (setting mount_b is optional)

    Handles resetting the cluster under test between tests.
    """

    # FIXME weird explicit naming
    mount_a = None
    mount_b = None
    recovery_mount = None

    # Declarative test requirements: subclasses should override these to indicate
    # their special needs.  If not met, tests will be skipped.
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1
    REQUIRE_ONE_CLIENT_REMOTE = False

    # Whether to create the default filesystem during setUp
    REQUIRE_FILESYSTEM = True

    # create a backup filesystem if required.
    # required REQUIRE_FILESYSTEM enabled
    REQUIRE_BACKUP_FILESYSTEM = False

    LOAD_SETTINGS = [] # type: ignore

    def _save_mount_details(self):
        """
        XXX: Tests may change details of mount objects, so let's stash them so
        that these details are restored later to ensure smooth setUps and
        tearDowns for upcoming tests.
        """
        self._orig_mount_details = [MountDetails(m) for m in self.mounts]
        log.info(self._orig_mount_details)

    def _remove_blocklist(self):
        # In case anything is in the OSD blocklist list, clear it out.  This is to avoid
        # the OSD map changing in the background (due to blocklist expiry) while tests run.
        try:
            self.run_ceph_cmd("osd blocklist clear")
        except CommandFailedError:
            # Fallback for older Ceph cluster
            try:
                blocklist = json.loads(self.get_ceph_cmd_stdout("osd",
                    "dump", "--format=json-pretty"))['blocklist']
                log.info(f"Removing {len(blocklist)} blocklist entries")
                for addr, blocklisted_at in blocklist.items():
                    self.run_ceph_cmd("osd", "blocklist", "rm", addr)
            except KeyError:
                # Fallback for more older Ceph clusters, who will use 'blacklist' instead.
                blacklist = json.loads(self.get_ceph_cmd_stdout("osd",
                    "dump", "--format=json-pretty"))['blacklist']
                log.info(f"Removing {len(blacklist)} blacklist entries")
                for addr, blocklisted_at in blacklist.items():
                    self.run_ceph_cmd("osd", "blacklist", "rm", addr)

    def setUp(self):
        super(CephFSTestCase, self).setUp()

        self.config_set('mon', 'mon_allow_pool_delete', True)

        if len(self.mds_cluster.mds_ids) < self.MDSS_REQUIRED:
            self.skipTest("Only have {0} MDSs, require {1}".format(
                len(self.mds_cluster.mds_ids), self.MDSS_REQUIRED
            ))

        if len(self.mounts) < self.CLIENTS_REQUIRED:
            self.skipTest("Only have {0} clients, require {1}".format(
                len(self.mounts), self.CLIENTS_REQUIRED
            ))

        if self.REQUIRE_ONE_CLIENT_REMOTE:
            if self.mounts[0].client_remote.hostname in self.mds_cluster.get_mds_hostnames():
                self.skipTest("Require first client to be on separate server from MDSs")

        # Create friendly mount_a, mount_b attrs
        for i in range(0, self.CLIENTS_REQUIRED):
            setattr(self, "mount_{0}".format(chr(ord('a') + i)), self.mounts[i])

        self.mds_cluster.clear_firewall()

        # Unmount all clients, we are about to blow away the filesystem
        for mount in self.mounts:
            if mount.is_mounted():
                mount.umount_wait(force=True)
        self._save_mount_details()

        # To avoid any issues with e.g. unlink bugs, we destroy and recreate
        # the filesystem rather than just doing a rm -rf of files
        self.mds_cluster.delete_all_filesystems()
        self.mds_cluster.mds_restart() # to reset any run-time configs, etc.
        self.fs = None # is now invalid!
        self.backup_fs = None
        self.recovery_fs = None

        self._remove_blocklist()

        client_mount_ids = [m.client_id for m in self.mounts]
        # In case there were any extra auth identities around from a previous
        # test, delete them
        for entry in self.auth_list():
            ent_type, ent_id = entry['entity'].split(".")
            if ent_type == "client" and ent_id not in client_mount_ids and not (ent_id == "admin" or ent_id[:6] == 'mirror'):
                self.run_ceph_cmd("auth", "del", entry['entity'])

        if self.REQUIRE_FILESYSTEM:
            self.fs = self.mds_cluster.newfs(create=True)

            # In case some test messed with auth caps, reset them
            for client_id in client_mount_ids:
                cmd = ['auth', 'caps', f'client.{client_id}', 'mon','allow r',
                       'osd', f'allow rw tag cephfs data={self.fs.name}',
                       'mds', 'allow']

                if self.get_ceph_cmd_result(*cmd) == 0:
                    break

                cmd[1] = 'add'
                if self.get_ceph_cmd_result(*cmd) != 0:
                    raise RuntimeError(f'Failed to create new client {cmd[2]}')

            # wait for ranks to become active
            self.fs.wait_for_daemons()

            # Mount the requested number of clients
            for i in range(0, self.CLIENTS_REQUIRED):
                self.mounts[i].mount_wait()

        if self.REQUIRE_BACKUP_FILESYSTEM:
            if not self.REQUIRE_FILESYSTEM:
                self.skipTest("backup filesystem requires a primary filesystem as well")
            self.run_ceph_cmd('fs', 'flag', 'set', 'enable_multiple', 'true',
                              '--yes-i-really-mean-it')
            self.backup_fs = self.mds_cluster.newfs(name="backup_fs")
            self.backup_fs.wait_for_daemons()

        # Load an config settings of interest
        for setting in self.LOAD_SETTINGS:
            setattr(self, setting, float(self.fs.mds_asok(
                ['config', 'get', setting], mds_id=list(self.mds_cluster.mds_ids)[0]
            )[setting]))

        self.configs_set = set()

    def tearDown(self):
        self.mds_cluster.clear_firewall()
        for m in self.mounts:
            m.teardown()

        # To prevent failover messages during Unwind of ceph task
        self.mds_cluster.delete_all_filesystems()

        for m, md in zip(self.mounts, self._orig_mount_details):
            md.restore(m)

        for subsys, key in self.configs_set:
            self.mds_cluster.clear_ceph_conf(subsys, key)

        return super(CephFSTestCase, self).tearDown()

    def set_conf(self, subsys, key, value):
        self.configs_set.add((subsys, key))
        self.mds_cluster.set_ceph_conf(subsys, key, value)

    def auth_list(self):
        """
        Convenience wrapper on "ceph auth ls"
        """
        return json.loads(self.get_ceph_cmd_stdout("auth", "ls",
            "--format=json-pretty"))['auth_dump']

    def assert_session_count(self, expected, ls_data=None, mds_id=None):
        if ls_data is None:
            ls_data = self.fs.mds_asok(['session', 'ls'], mds_id=mds_id)

        alive_count = len([s for s in ls_data if s['state'] != 'killing'])

        self.assertEqual(expected, alive_count, "Expected {0} sessions, found {1}".format(
            expected, alive_count
        ))

    def assert_session_state(self, client_id,  expected_state):
        self.assertEqual(
            self._session_by_id(
                self.fs.mds_asok(['session', 'ls'])).get(client_id, {'state': None})['state'],
            expected_state)

    def get_session_data(self, client_id):
        return self._session_by_id(client_id)

    def _session_list(self):
        ls_data = self.fs.mds_asok(['session', 'ls'])
        ls_data = [s for s in ls_data if s['state'] not in ['stale', 'closed']]
        return ls_data

    def get_session(self, client_id, session_ls=None):
        if session_ls is None:
            session_ls = self.fs.mds_asok(['session', 'ls'])

        return self._session_by_id(session_ls)[client_id]

    def _session_by_id(self, session_ls):
        return dict([(s['id'], s) for s in session_ls])

    def perf_dump(self, rank=None, status=None):
        return self.fs.rank_asok(['perf', 'dump'], rank=rank, status=status)

    def wait_until_evicted(self, client_id, timeout=30):
        def is_client_evicted():
            ls = self._session_list()
            for s in ls:
                if s['id'] == client_id:
                    return False
            return True
        self.wait_until_true(is_client_evicted, timeout)

    def wait_for_daemon_start(self, daemon_ids=None):
        """
        Wait until all the daemons appear in the FSMap, either assigned
        MDS ranks or in the list of standbys
        """
        def get_daemon_names():
            return [info['name'] for info in self.mds_cluster.status().get_all()]

        if daemon_ids is None:
            daemon_ids = self.mds_cluster.mds_ids

        try:
            self.wait_until_true(
                lambda: set(daemon_ids) & set(get_daemon_names()) == set(daemon_ids),
                timeout=30
            )
        except RuntimeError:
            log.warning("Timeout waiting for daemons {0}, while we have {1}".format(
                daemon_ids, get_daemon_names()
            ))
            raise

    def delete_mds_coredump(self, daemon_id):
        # delete coredump file, otherwise teuthology.internal.coredump will
        # catch it later and treat it as a failure.
        core_pattern = self.mds_cluster.mds_daemons[daemon_id].remote.sh(
            "sudo sysctl -n kernel.core_pattern")
        core_dir = os.path.dirname(core_pattern.strip())
        if core_dir:  # Non-default core_pattern with a directory in it
            # We have seen a core_pattern that looks like it's from teuthology's coredump
            # task, so proceed to clear out the core file
            if core_dir[0] == '|':
                log.info("Piped core dumps to program {0}, skip cleaning".format(core_dir[1:]))
                return;

            log.info("Clearing core from directory: {0}".format(core_dir))

            # Verify that we see the expected single coredump
            ls_output = self.mds_cluster.mds_daemons[daemon_id].remote.sh([
                "cd", core_dir, run.Raw('&&'),
                "sudo", "ls", run.Raw('|'), "sudo", "xargs", "file"
            ])
            cores = [l.partition(":")[0]
                     for l in ls_output.strip().split("\n")
                     if re.match(r'.*ceph-mds.* -i +{0}'.format(daemon_id), l)]

            log.info("Enumerated cores: {0}".format(cores))
            self.assertEqual(len(cores), 1)

            log.info("Found core file {0}, deleting it".format(cores[0]))

            self.mds_cluster.mds_daemons[daemon_id].remote.run(args=[
                "cd", core_dir, run.Raw('&&'), "sudo", "rm", "-f", cores[0]
            ])
        else:
            log.info("No core_pattern directory set, nothing to clear (internal.coredump not enabled?)")

    def _get_subtrees(self, status=None, rank=None, path=None):
        if path is None:
            path = "/"
        try:
            with contextutil.safe_while(sleep=1, tries=3) as proceed:
                while proceed():
                    try:
                        if rank == "all":
                            subtrees = []
                            for r in self.fs.get_ranks(status=status):
                                s = self.fs.rank_asok(["get", "subtrees"], status=status, rank=r['rank'])
                                log.debug(f"{json.dumps(s, indent=2)}")
                                s = filter(lambda s: s['auth_first'] == r['rank'] and s['auth_second'] == -2, s)
                                subtrees += s
                        else:
                            subtrees = self.fs.rank_asok(["get", "subtrees"], status=status, rank=rank)
                        subtrees = filter(lambda s: s['dir']['path'].startswith(path), subtrees)
                        return list(subtrees)
                    except CommandFailedError as e:
                        # Sometimes we get transient errors
                        if e.exitstatus == 22:
                            pass
                        else:
                            raise
        except contextutil.MaxWhileTries as e:
            raise RuntimeError(f"could not get subtree state from rank {rank}") from e

    def _wait_subtrees(self, test, status=None, rank=None, timeout=30, sleep=2, action=None, path=None):
        test = sorted(test)
        try:
            with contextutil.safe_while(sleep=sleep, tries=timeout//sleep) as proceed:
                while proceed():
                    subtrees = self._get_subtrees(status=status, rank=rank, path=path)
                    filtered = sorted([(s['dir']['path'], s['auth_first']) for s in subtrees])
                    log.info("%s =?= %s", filtered, test)
                    if filtered == test:
                        # Confirm export_pin in output is correct:
                        for s in subtrees:
                            if s['export_pin_target'] >= 0:
                                self.assertTrue(s['export_pin_target'] == s['auth_first'])
                        return subtrees
                    if action is not None:
                        action()
        except contextutil.MaxWhileTries as e:
            raise RuntimeError("rank {0} failed to reach desired subtree state".format(rank)) from e

    def _wait_until_scrub_complete(self, path="/", recursive=True, timeout=100):
        out_json = self.fs.run_scrub(["start", path] + ["recursive"] if recursive else [])
        if not self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"],
                                                 sleep=10, timeout=timeout):
            log.info("timed out waiting for scrub to complete")

    def _wait_distributed_subtrees(self, count, status=None, rank=None, path=None):
        try:
            with contextutil.safe_while(sleep=5, tries=20) as proceed:
                while proceed():
                    subtrees = self._get_subtrees(status=status, rank=rank, path=path)
                    dist = list(filter(lambda s: s['distributed_ephemeral_pin'] == True and
                                                 s['auth_first'] == s['export_pin_target'],
                                       subtrees))
                    log.info(f"len={len(dist)}\n{json.dumps(dist, indent=2)}")

                    if len(subtrees) >= count:
                        if len(subtrees) > len(dist):
                            # partial migration
                            continue
                        return subtrees
        except contextutil.MaxWhileTries as e:
            raise RuntimeError("rank {0} failed to reach desired subtree state".format(rank)) from e

    def _wait_random_subtrees(self, count, status=None, rank=None, path=None):
        try:
            with contextutil.safe_while(sleep=5, tries=20) as proceed:
                while proceed():
                    subtrees = self._get_subtrees(status=status, rank=rank, path=path)
                    subtrees = list(filter(lambda s: s['random_ephemeral_pin'] == True and
                                                     s['auth_first'] == s['export_pin_target'],
                                           subtrees))
                    log.info(f"len={len(subtrees)} {subtrees}")
                    if len(subtrees) >= count:
                        return subtrees
        except contextutil.MaxWhileTries as e:
            raise RuntimeError("rank {0} failed to reach desired subtree state".format(rank)) from e

    def create_client(self, client_id, moncap=None, osdcap=None, mdscap=None):
        if not (moncap or osdcap or mdscap):
            if self.fs:
                return self.fs.authorize(client_id, ('/', 'rw'))
            else:
                raise RuntimeError('no caps were passed and the default FS '
                                   'is not created yet to allow client auth '
                                   'for it.')

        cmd = ['auth', 'add', f'client.{client_id}']
        if moncap:
            cmd += ['mon', moncap]
        if osdcap:
            cmd += ['osd', osdcap]
        if mdscap:
            cmd += ['mds', mdscap]

        self.run_ceph_cmd(*cmd)
        return self.get_ceph_cmd_stdout(f'auth get {self.client_name}')
