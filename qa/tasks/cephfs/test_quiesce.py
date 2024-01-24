import re
import json
import logging
import secrets
import unittest
from io import StringIO
import os.path
from time import sleep

from teuthology.contextutil import safe_while

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

INODE_RE = re.compile(r'\[inode 0x([0-9a-fA-F]+)')
FP_RE = re.compile(r'fp=#0x([0-9a-fA-F]+)(\S*)')

# MDS uses linux defines:
S_IFMT   = 0o0170000
S_IFSOCK =  0o140000
S_IFLNK  =  0o120000
S_IFREG  =  0o100000
S_IFBLK  =  0o060000
S_IFDIR  =  0o040000
S_IFCHR  =  0o020000
S_IFIFO  =  0o010000
S_ISUID  =  0o004000
S_ISGID  =  0o002000
S_ISVTX  =  0o001000

class QuiesceTestCase(CephFSTestCase):
    """
    Test case for quiescing subvolumes.
    """

    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 1

    QUIESCE_SUBVOLUME = "subvol_quiesce"

    def setUp(self):
        super().setUp()
        self.run_ceph_cmd(f'fs subvolume create {self.fs.name} {self.QUIESCE_SUBVOLUME} --mode=777')
        p = self.run_ceph_cmd(f'fs subvolume getpath {self.fs.name} {self.QUIESCE_SUBVOLUME}', stdout=StringIO())
        self.mntpnt = p.stdout.getvalue().strip()
        self.subvolume = self.mntpnt

    def tearDown(self):
        # restart fs so quiesce commands clean up and commands are left unkillable
        self.fs.fail()
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        super().tearDown()

    def _configure_subvolume(self):
        for m in self.mounts:
            m.umount_wait()
        for m in self.mounts:
            m.update_attrs(cephfs_mntpt = self.mntpnt)
            m.mount()

    CLIENT_WORKLOAD = """
        set -ex
        pushd `mktemp -d -p .`
        cp -a /usr .
        popd
    """
    def _client_background_workload(self):
       for m in self.mounts:
           p = m.run_shell_payload(self.CLIENT_WORKLOAD, wait=False, stderr=StringIO())
           m.background_procs.append(p)

    def _wait_for_quiesce_complete(self, reqid, rank=0):
        op = None
        try:
            with safe_while(sleep=1, tries=120, action='wait for quiesce completion') as proceed:
                while proceed():
                    op = self.fs.get_op(reqid, rank=rank)
                    log.debug(f"op:\n{op}")
                    self.assertEqual(op['type_data']['op_name'], 'quiesce_path')
                    if op['type_data']['flag_point'] in (self.FP_QUIESCE_COMPLETE, self.FP_QUIESCE_COMPLETE_NON_AUTH_TREE):
                        return op
        except:
            log.info(f"op:\n{op}")
            raise

    FP_QUIESCE_COMPLETE = 'quiesce complete'
    FP_QUIESCE_BLOCKED = 'quiesce blocked'
    FP_QUIESCE_COMPLETE_NON_AUTH = 'quiesce complete for non-auth inode'
    FP_QUIESCE_COMPLETE_NON_AUTH_TREE = 'quiesce complete for non-auth tree'
    def _verify_quiesce(self, rank=0, root=None, splitauth=False):
        if root is None:
            root = self.subvolume

        root_ino = self.fs.read_cache(root, depth=0, rank=rank)[0]['ino']
        ops = self.fs.get_ops(locks=True, rank=rank)
        quiesce_inode_ops = {}
        skipped_nonauth = False

        count_q = 0
        count_qb = 0
        count_qna = 0

        for op in ops['ops']:
            try:
                log.debug(f"op = {op}")
                type_data = op['type_data']
                flag_point = type_data['flag_point']
                op_type = type_data['op_type']
                if op_type == 'client_request' or op_type == 'peer_request':
                    continue
                op_name = type_data['op_name']
                if op_name == "quiesce_path":
                    self.assertIn(flag_point, (self.FP_QUIESCE_COMPLETE, self.FP_QUIESCE_COMPLETE_NON_AUTH_TREE))
                    if flag_point == self.FP_QUIESCE_COMPLETE_NON_AUTH_TREE:
                        skipped_nonauth = True
                elif op_name == "quiesce_inode":
                    # get the inode number
                    op_description = op['description']
                    m = FP_RE.search(op_description)
                    self.assertIsNotNone(m)
                    if len(m.group(2)) == 0:
                        ino = int(m.group(1), 16)
                    else:
                        self.assertEqual(int(m.group(1)), 1)
                        fp = m.group(2)
                        dump = self.fs.read_cache(fp, depth=0, rank=rank)
                        ino = dump[0]['ino']
                    self.assertNotIn(ino, quiesce_inode_ops)

                    self.assertIn(flag_point, (self.FP_QUIESCE_COMPLETE, self.FP_QUIESCE_BLOCKED, self.FP_QUIESCE_COMPLETE_NON_AUTH))

                    locks = type_data['locks']
                    if flag_point == self.FP_QUIESCE_BLOCKED:
                        count_qb += 1
                        self.assertEqual(locks, [])
                    elif flag_point == self.FP_QUIESCE_COMPLETE_NON_AUTH:
                        count_qna += 1
                        #self.assertEqual(len(locks), 1)
                        #lock = locks[0]
                        #lock_type = lock['lock']['type']
                        #self.assertEqual(lock_type, "iauth")
                        #object_string = lock['object_string']
                        #m = INODE_RE.match(object_string)
                        #self.assertIsNotNone(m)
                        #self.assertEqual(ino, int(m.group(1), 16))
                    else:
                        count_q += 1
                        for lock in locks:
                            lock_type = lock['lock']['type']
                            if lock_type.startswith('i'):
                                object_string = lock['object_string']
                                m = INODE_RE.match(object_string)
                                self.assertIsNotNone(m)
                                self.assertEqual(ino, int(m.group(1), 16))
                        self.assertIsNotNone(ino)
                    quiesce_inode_ops[ino] = op
            except:
                log.error(f"op:\n{json.dumps(op, indent=2)}")
                raise

        log.info(f"q = {count_q}; qb = {count_qb}; qna = {count_qna}")

        if skipped_nonauth:
            return

        for ino, op in quiesce_inode_ops.items():
            log.debug(f"{ino}: {op['description']}")

        # now verify all files in cache have an op
        cache = self.fs.read_cache(root, rank=rank)
        visited = set()
        locks_expected = set([
          "iquiesce",
          "isnap",
          "ipolicy",
          "ifile",
          "inest",
          "idft",
          "iauth",
          "ilink",
          "ixattr",
        ])
        for inode in cache:
            ino = inode['ino']
            visited.add(ino)
            mode = inode['mode']
            self.assertIn(ino, quiesce_inode_ops)
            op = quiesce_inode_ops[ino]
            type_data = op['type_data']
            flag_point = type_data['flag_point']
            try:
                locks_seen = set()
                lock_type = None
                op_name = type_data['op_name']
                for lock in op['type_data']['locks']:
                    lock_type = lock['lock']['type']
                    if lock_type == "iquiesce":
                        if ino == root_ino:
                            self.assertEqual(lock['flags'], 1)
                            self.assertEqual(lock['lock']['state'], 'sync')
                        else:
                            self.assertEqual(lock['flags'], 4)
                            self.assertEqual(lock['lock']['state'], 'xlock')
                    elif lock_type == "isnap":
                        self.assertEqual(lock['flags'], 1)
                        self.assertEqual(lock['lock']['state'][:4], 'sync')
                    elif lock_type == "ifile":
                        if (mode & S_IFMT) == S_IFREG:
                            self.assertEqual(lock['flags'], 4)
                            self.assertEqual(lock['lock']['state'], 'xlock')
                        else:
                            self.assertEqual(lock['flags'], 1)
                            self.assertEqual(lock['lock']['state'][:4], 'sync')
                    elif lock_type in ("ipolicy", "inest", "idft", "iauth", "ilink", "ixattr"):
                        self.assertEqual(lock['flags'], 1)
                        self.assertEqual(lock['lock']['state'][:4], 'sync')
                    elif lock_type == "iversion":
                        # only regular files acquire xlock (on ifile)
                        self.assertEqual((mode & S_IFMT), S_IFREG)
                        self.assertEqual(lock['flags'], 2)
                        self.assertEqual(lock['lock']['state'][:4], 'lock')
                    else:
                        # no iflock
                        self.assertFalse(lock_type.startswith("i"))
                    if flag_point == self.FP_QUIESCE_COMPLETE and lock_type.startswith("i"):
                        #if op_name == "quiesce_inode":
                        #    self.assertTrue(lock['object']['is_auth'])
                        locks_seen.add(lock_type)
                try:
                    if flag_point == self.FP_QUIESCE_BLOCKED:
                        self.assertTrue(inode['quiesce_block'])
                        self.assertEqual(set(), locks_seen)
                    elif flag_point == self.FP_QUIESCE_COMPLETE_NON_AUTH:
                        self.assertFalse(inode['quiesce_block'])
                        self.assertEqual(set(), locks_seen)
                    elif (mode & S_IFMT) == S_IFREG:
                        self.assertFalse(inode['quiesce_block'])
                        self.assertEqual(locks_expected.union(set(['iversion'])), locks_seen)
                    else:
                        self.assertFalse(inode['quiesce_block'])
                        self.assertEqual(locks_expected, locks_seen)
                except:
                    log.error(f"{sorted(locks_expected)} != {sorted(locks_seen)}")
                    raise
            except:
                log.error(f"inode:\n{json.dumps(inode, indent=2)}")
                log.error(f"op:\n{json.dumps(op, indent=2)}")
                log.error(f"lock_type: {lock_type}")
                raise
        try:
            self.assertEqual(visited, quiesce_inode_ops.keys())
        except:
            log.error(f"cache:\n{json.dumps(cache, indent=2)}")
            log.error(f"ops:\n{json.dumps(quiesce_inode_ops, indent=2)}")

        # check request/cap count is stopped
        # count inodes under /usr and count subops!

    def reqid_tostr(self, reqid):
        return f"{reqid['entity']['type']}.{reqid['entity']['num']}:{reqid['tid']}"

class TestQuiesce(QuiesceTestCase):
    """
    Single rank functional tests.
    """

    def test_quiesce_path_workload(self):
        """
        That a quiesce op can be created and verified while a workload is running.
        """

        self._configure_subvolume()
        self._client_background_workload()

        sleep(secrets.randbelow(30)+10)

        J = self.fs.rank_tell(["quiesce", "path", self.subvolume])
        reqid = self.reqid_tostr(J['op']['reqid'])
        self._wait_for_quiesce_complete(reqid)

        self._verify_quiesce()

    def test_quiesce_path_snap(self):
        """
        That a snapshot can be taken on a quiesced subvolume.
        """

        self._configure_subvolume()
        self._client_background_workload()

        sleep(secrets.randbelow(30)+10)

        J = self.fs.rank_tell(["quiesce", "path", self.subvolume])
        reqid = self.reqid_tostr(J['op']['reqid'])
        self._wait_for_quiesce_complete(reqid)

        #path = os.path.normpath(os.path.join(self.mntpnt, ".."))
        #p = self.fs.run_client_payload(f"mkdir {path}/.snap/foo && ls {path}/.snap/", stdout=StringIO())
        #p = self.mount_a.run_shell_payload(f"mkdir ../.snap/foo && ls ../.snap/", stdout=StringIO())
        self.run_ceph_cmd(f'fs subvolume snapshot create {self.fs.name} {self.QUIESCE_SUBVOLUME} foo')
        p = self.run_ceph_cmd(f'fs subvolume snapshot ls {self.fs.name} {self.QUIESCE_SUBVOLUME}', stdout=StringIO())
        log.debug(f"{p.stdout.getvalue()}")

    def test_quiesce_path_create(self):
        """
        That a quiesce op can be created and verified.
        """

        J = self.fs.rank_tell(["quiesce", "path", self.subvolume])
        reqid = self.reqid_tostr(J['op']['reqid'])
        self._wait_for_quiesce_complete(reqid)
        self._verify_quiesce()

    def test_quiesce_path_kill(self):
        """
        That killing a quiesce op also kills its subops
        ("quiesce_inode").
        """

        J = self.fs.rank_tell(["quiesce", "path", self.subvolume])
        reqid = self.reqid_tostr(J['op']['reqid'])
        self._wait_for_quiesce_complete(reqid)
        self._verify_quiesce()
        ops = self.fs.get_ops()
        quiesce_inode = 0
        for op in ops['ops']:
            op_name = op['type_data']['op_name']
            if op_name == "quiesce_inode":
                quiesce_inode += 1
        log.debug(f"there are {quiesce_inode} quiesce_path_inode requests")
        self.assertLess(0, quiesce_inode)
        J = self.fs.kill_op(reqid)
        log.debug(f"{J}")
        ops = self.fs.get_ops()
        for op in ops['ops']:
            op_name = op['type_data']['op_name']
            self.assertNotIn(op_name, ('quiesce_path', 'quiesce_inode'))

    def test_quiesce_path_release(self):
        """
        That killing the quiesce op properly releases the subvolume so that
        client IO proceeds.
        """

        self._configure_subvolume()
        self._client_background_workload()

        P = self.fs.rank_tell(["ops"])
        log.debug(f"{P}")

        J = self.fs.rank_tell(["quiesce", "path", self.subvolume])
        reqid = self.reqid_tostr(J['op']['reqid'])
        self._wait_for_quiesce_complete(reqid)

        P = self.fs.rank_tell(["ops"])
        log.debug(f"{P}")

        self.fs.kill_op(reqid)

        P = self.fs.rank_tell(["perf", "dump"])
        log.debug(f"{P}")
        requests = P['mds']['request']
        replies = P['mds']['reply']
        grants = P['mds']['ceph_cap_op_grant']

        sleep(5)

        P = self.fs.rank_tell(["perf", "dump"])
        log.debug(f"{P}")
        self.assertLess(requests, P['mds']['request'])
        self.assertLess(replies, P['mds']['reply'])
        self.assertLess(grants, P['mds']['ceph_cap_op_grant'])

        P = self.fs.rank_tell(["ops"])
        log.debug(f"{P}")

    def test_quiesce_path_link_terminal(self):
        """
        That quiesce on path with an terminal link fails with ENOTDIR even
        pointing to a valid subvolume.
        """

        self._configure_subvolume()

        self.mount_a.run_shell_payload("ln -s ../.. subvol_quiesce")
        path = self.mount_a.cephfs_mntpt + "/subvol_quiesce"

        J = self.fs.rank_tell(["quiesce", "path", path, '--wait'])
        log.debug(f"{J}")
        self.assertEqual(J['op']['result'], -20) # ENOTDIR: the link is not a directory

    def test_quiesce_path_link_intermediate(self):
        """
        That quiesce on path with an intermediate link fails with ENOTDIR.
        """

        self._configure_subvolume()

        self.mount_a.run_shell_payload("ln -s ../../.. _nogroup")
        path = self.mount_a.cephfs_mntpt + "/_nogroup/" + self.QUIESCE_SUBVOLUME

        J = self.fs.rank_tell(["quiesce", "path", path, '--wait'])
        log.debug(f"{J}")
        self.assertEqual(J['op']['result'], -20) # ENOTDIR: path_traverse: the intermediate link is not a directory

    def test_quiesce_path_notsubvol(self):
        """
        That quiesce on a directory under a subvolume is valid.
        """

        self._configure_subvolume()

        self.mount_a.run_shell_payload("mkdir dir")
        path = self.mount_a.cephfs_mntpt + "/dir"

        J = self.fs.rank_tell(["quiesce", "path", path, '--wait'])
        reqid = self.reqid_tostr(J['op']['reqid'])
        self._wait_for_quiesce_complete(reqid)
        self._verify_quiesce(root=path)

    def test_quiesce_path_regfile(self):
        """
        That quiesce on a regular file fails with ENOTDIR.
        """

        self._configure_subvolume()

        self.mount_a.run_shell_payload("touch file")
        path = self.mount_a.cephfs_mntpt + "/file"

        J = self.fs.rank_tell(["quiesce", "path", path, '--wait'])
        log.debug(f"{J}")
        self.assertEqual(J['op']['result'], -20) # ENOTDIR

    def test_quiesce_path_dup(self):
        """
        That two identical quiesce ops will result in one failing with
        EINPROGRESS.
        """

        self._configure_subvolume()

        op1 = self.fs.rank_tell(["quiesce", "path", self.subvolume])['op']
        op1_reqid = self.reqid_tostr(op1['reqid'])
        op2 = self.fs.rank_tell(["quiesce", "path", self.subvolume, '--wait'])['op']
        op1 = self.fs.get_op(op1_reqid)['type_data'] # for possible dup result
        log.debug(f"op1 = {op1}")
        log.debug(f"op2 = {op2}")
        self.assertIn(op1['flag_point'], (self.FP_QUIESCE_COMPLETE, 'cleaned up request'))
        self.assertIn(op2['flag_point'], (self.FP_QUIESCE_COMPLETE, 'cleaned up request'))
        self.assertTrue(op1['result'] == -115 or op2['result'] == -115) # EINPROGRESS

    def test_quiesce_blocked(self):
        """
        That a file with ceph.quiesce.block is not quiesced.
        """

        self._configure_subvolume()

        self.mount_a.run_shell_payload("touch file && setfattr -n ceph.quiesce.block -v 1 file")

        J = self.fs.rank_tell(["quiesce", "path", self.subvolume, '--wait'])
        log.debug(f"{J}")
        self.assertEqual(J['op']['result'], 0)
        self.assertEqual(J['state']['inodes_blocked'], 1)
        self._verify_quiesce(root=self.subvolume)

    def test_quiesce_slow(self):
        """
        That a subvolume is quiesced when artificially slowed down.
        """

        self.config_set('mds', 'mds_cache_quiesce_delay', '2000')
        self._configure_subvolume()
        self._client_background_workload()

        J = self.fs.rank_tell(["quiesce", "path", self.subvolume])
        log.debug(f"{J}")
        reqid = self.reqid_tostr(J['op']['reqid'])
        self._wait_for_quiesce_complete(reqid)
        self._verify_quiesce(root=self.subvolume)

    # TODO test lookup leaf file/dir after quiesce
    # TODO ditto path_traverse

class TestQuiesceMultiRank(QuiesceTestCase):
    """
    Tests for quiescing subvolumes on multiple ranks.
    """

    MDSS_REQUIRED = 2

    CLIENT_WORKLOAD = """
        set -ex
        for ((i = 0; i < 10; ++i)); do
            (
                pushd `mktemp -d -p .`
                touch file
                sleep 5 # for export
                cp -a /usr .
                popd
            ) &
        done
        wait
    """

    def setUp(self):
        super().setUp()
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()
        self.mds_map = self.fs.get_mds_map(status=status)
        self.ranks = list(range(self.mds_map['max_mds']))

    def test_quiesce_path_splitauth(self):
        """
        That quiesce fails (by default) if auth is split on a path.
        """

        self._configure_subvolume()
        self.mount_a.run_shell_payload("setfattr -n ceph.dir.pin.distributed -v 1 .")
        self._client_background_workload()
        self._wait_distributed_subtrees(2*2, rank="all", path=self.mntpnt)

        op = self.fs.rank_tell(["quiesce", "path", self.subvolume, '--wait'], rank=0)['op']
        self.assertEqual(op['result'], -1) # EPERM


    def test_quiesce_path_multirank(self):
        """
        That quiesce may complete with two ranks and a basic workload.
        """

        self._configure_subvolume()
        self.mount_a.run_shell_payload("setfattr -n ceph.dir.pin.distributed -v 1 .")
        self._client_background_workload()
        self._wait_distributed_subtrees(2*2, rank="all", path=self.mntpnt)

        sleep(secrets.randbelow(30)+10)

        p = self.mount_a.run_shell_payload("ls", stdout=StringIO())
        dirs = p.stdout.getvalue().strip().split()

        ops = []
        for d in dirs:
            path = os.path.join(self.mntpnt, d)
            for r in self.ranks:
                op = self.fs.rank_tell(["quiesce", "path", path], rank=r)['op']
                ops.append((r, op, path))
        for rank, op, path in ops:
            reqid = self.reqid_tostr(op['reqid'])
            log.debug(f"waiting for ({rank}, {reqid})")
            op = self._wait_for_quiesce_complete(reqid, rank=rank)
        # FIXME _verify_quiesce needs adjustment for multiple quiesce
        #for rank, op, path in ops:
        #    self._verify_quiesce(root=path, rank=rank)


class TestQuiesceSplitAuth(QuiesceTestCase):
    """
    Tests for quiescing subvolumes on multiple ranks with split auth.
    """

    MDSS_REQUIRED = 2

    CLIENT_WORKLOAD = """
        set -ex
        for ((i = 0; i < 10; ++i)); do
            (
                pushd `mktemp -d -p .`
                touch file
                sleep 5 # for export
                cp -a /usr .
                popd
            ) &
        done
        wait
    """

    def setUp(self):
        super().setUp()
        self.config_set('mds', 'mds_export_ephemeral_random_max', '0.75')
        self.config_set('mds', 'mds_cache_quiesce_splitauth', 'true')
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()
        self.mds_map = self.fs.get_mds_map(status=status)
        self.ranks = list(range(self.mds_map['max_mds']))

    @unittest.skip("splitauth is experimental")
    def test_quiesce_path_multirank_exports(self):
        """
        That quiesce may complete with two ranks and a basic workload.
        """

        self.config_set('mds', 'mds_cache_quiesce_delay', '4000')
        self._configure_subvolume()
        self.mount_a.run_shell_payload("setfattr -n ceph.dir.pin.random -v 0.5 .")
        self._client_background_workload()

        sleep(2)

        op0 = self.fs.rank_tell(["quiesce", "path", self.subvolume], rank=0)['op']
        op1 = self.fs.rank_tell(["quiesce", "path", self.subvolume], rank=1)['op']
        reqid0 = self.reqid_tostr(op0['reqid'])
        reqid1 = self.reqid_tostr(op1['reqid'])
        op0 = self._wait_for_quiesce_complete(reqid0, rank=0)
        op1 = self._wait_for_quiesce_complete(reqid1, rank=1)
        log.debug(f"op0 = {op0}")
        log.debug(f"op1 = {op1}")
        self._verify_quiesce(rank=0)
        self._verify_quiesce(rank=1)
