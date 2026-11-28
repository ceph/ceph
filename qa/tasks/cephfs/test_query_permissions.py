"""
QUERY_PERMISSIONS (MDS + minimal libcephfs qa path).

Exercises the v1 MDS op against a vstart or teuthology cluster via ceph-fuse
and the libcephfs.query_permissions() helper (qa / tests, not a VFS API).

vstart (from build/):

  export PYTHONPATH=../qa:../src/pybind:lib/cython_modules/lib.3
  export LD_LIBRARY_PATH=$PWD/lib
  # Cython cephfs module must match the interpreter (cmake often uses 3.9):
  export CEPH_LIBCEPHFS_PYTHON=python3.9
  python ../qa/tasks/vstart_runner.py --create \\
      tasks.cephfs.test_query_permissions

Run one area only, e.g. TestQueryPermissions.test_not_returned_without_snapshot_flag
"""

import errno
import glob
import json
import logging
import os
import re
import shutil
import stat
import unittest
from io import StringIO
from textwrap import dedent

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

CEPH_QUERY_PERM_READ = 1
CEPH_QUERY_PERM_WRITE = 2
CEPH_QUERY_PERM_NOT_RETURNED = 0
CEPH_QUERY_PERM_NO_ACL = 1
CEPH_QUERY_PERM_ACL = 2
CEPHFS_FEATURE_QUERY_PERMISSIONS = 25

LIBCEPHFS_PYTHON = os.environ.get('CEPH_LIBCEPHFS_PYTHON', 'python3.9')

def _ceph_build_dir():
    if os.path.isfile(os.path.join(os.getcwd(), 'ceph.conf')):
        return os.getcwd()
    return os.environ.get('CEPH_BUILD', os.getcwd())


def _clean_stale_client_asoks():
    """Remove client asok files whose PIDs are gone (speeds vstart fuse setup)."""
    asok_dir = os.path.join(_ceph_build_dir(), 'asok')
    for path in glob.glob(os.path.join(asok_dir, 'client.*.*.asok')):
        m = re.match(r'.*\.(\d+)\.asok$', path)
        if not m:
            continue
        if not os.path.exists(f"/proc/{m.group(1)}"):
            try:
                os.unlink(path)
            except OSError:
                pass


class QueryPermissionsTestBase(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    BASIC_SUBDIR = "query_perm_basic"
    CEPHX_DIR = "qp_cephx_only"
    CEPHX_FILE = "qp_cephx_only/secret"
    ACL_DIR = "qp_acl"

    def setUp(self):
        _clean_stale_client_asoks()
        if self.mount_a is not None:
            self.mount_a.client_config.setdefault('mount_timeout', 120)
        super().setUp()

    def _abs(self, relpath):
        return os.path.join(self.mount_a.hostfs_mntpt, relpath.lstrip("/"))

    def _ceph_path(self, relpath):
        """Path in the CephFS namespace (libcephfs), not the fuse host mount."""
        return '/' + relpath.lstrip('/')

    def _libcephfs_path(self, relpath, mount_root=None):
        """Path for query_permissions when libcephfs uses mount_root."""
        full = self._ceph_path(relpath)
        if mount_root is None:
            mount_root = getattr(self, '_libcephfs_mount_root', None)
        if not mount_root:
            return full
        if isinstance(mount_root, bytes):
            mount_root = mount_root.decode('utf-8')
        root = '/' + mount_root.strip('/')
        if full == root:
            return '/'
        prefix = root + '/'
        if full.startswith(prefix):
            return '/' + full[len(prefix):]
        return full

    @staticmethod
    def _libcephfs_mount_root_kw(mount_root):
        """pybind mount(mount_root=) expects bytes on py3.9 (see cephfs.pyx)."""
        if mount_root is None:
            return ''
        if isinstance(mount_root, str):
            mount_root = mount_root.encode('utf-8')
        return f'mount_root={mount_root!r}'

    @staticmethod
    def _mds_cap_mount_root(mdscap):
        m = re.search(r'\bpath=([^\s]+)', mdscap)
        if not m:
            return None
        root = m.group(1)
        if not root.startswith('/'):
            root = '/' + root
        return root

    def _libcephfs_subprocess_env(self):
        """LD_LIBRARY_PATH must be set before the python process starts."""
        build = _ceph_build_dir()
        env = os.environ.copy()
        lib = os.path.join(build, 'lib')
        prior = env.get('LD_LIBRARY_PATH', '')
        parts = [p for p in prior.split(':') if p]
        if lib not in parts:
            parts.insert(0, lib)
        env['LD_LIBRARY_PATH'] = ':'.join(parts)
        for p in (
            os.path.join(build, 'lib/cython_modules/lib.3'),
            os.path.abspath(os.path.join(build, '..', 'qa')),
            os.path.abspath(os.path.join(build, '..', 'src/pybind')),
        ):
            if not os.path.isdir(p):
                continue
            cur = env.get('PYTHONPATH', '')
            if p not in cur.split(':'):
                env['PYTHONPATH'] = f"{p}:{cur}" if cur else p
        return env, build

    def _libcephfs_client_kw(self):
        """Match fuse ceph-fuse -k / restricted caps when remounted."""
        kw = []
        keyring = getattr(self.mount_a, 'client_keyring_path', None)
        if keyring:
            kw.append(f"conf={{'keyring': {keyring!r}}}")
        client_id = getattr(self.mount_a, 'client_id', None)
        if client_id is not None:
            kw.append(f"auth_id={client_id!r}")
        return ", ".join(kw)

    def _libcephfs_script(self, build, body, mount_root=None, setup_client=True):
        conf = os.path.join(build, 'ceph.conf')
        header = dedent(f"""
            import os
            os.chdir({build!r})
            import cephfs as libcephfs
            """)
        if setup_client:
            mount_kw = self._libcephfs_mount_root_kw(mount_root)
            client_kw = self._libcephfs_client_kw()
            if client_kw:
                ctor = f"libcephfs.LibCephFS(conffile={conf!r}, {client_kw})"
            else:
                ctor = f"libcephfs.LibCephFS(conffile={conf!r})"
            header += dedent(f"""
            fs = {ctor}
            fs.mount({mount_kw})
            """)
        return header + body

    def _run_libcephfs_subprocess(self, body, mount_root=None, umount_fuse=True,
                                  setup_client=True):
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("libcephfs query_permissions requires fuse vstart/teuth mount")
        exe = LIBCEPHFS_PYTHON.split()[0]
        if shutil.which(exe) is None:
            self.skipTest(
                f"{exe} not found; build pybind cephfs for that version "
                f"and set CEPH_LIBCEPHFS_PYTHON (cmake WITH_PYTHON3)")
        env, build = self._libcephfs_subprocess_env()
        py = self._libcephfs_script(build, body, mount_root, setup_client)
        fuse_was = umount_fuse and self.mount_a.is_mounted()
        if fuse_was:
            self.mount_a.umount_wait()
        try:
            proc = self.mount_a.client_remote.run(
                args=['stdin-killer', '--', LIBCEPHFS_PYTHON, '-c', py],
                env=env,
                cwd=build,
                stdout=StringIO(),
                stderr=StringIO(),
                wait=True,
                check_status=False,
            )
        finally:
            if fuse_was:
                self.mount_a.mount_wait()
        stderr = proc.stderr.getvalue().strip()
        if proc.exitstatus != 0:
            if proc.exitstatus == -11:
                self.fail(
                    "libcephfs crashed (SIGSEGV); rebuild client/libcephfs "
                    "and cython_cephfs from the same tree, and avoid two "
                    "client.0 sessions (this test unmounts ceph-fuse first)")
            raise CommandFailedError(
                proc.args, proc.exitstatus, label=stderr or None)
        return proc.stdout.getvalue().strip()

    def _require_libcephfs_bindings(self):
        out = self._run_libcephfs_subprocess(
            "print('ok')", umount_fuse=False, setup_client=False)
        if out != 'ok':
            self.skipTest(f"unexpected libcephfs probe: {out!r}")

    def _libcephfs_query(self, relpath, rights, want_snapshot=False,
                         mount_root=None):
        self._require_libcephfs_bindings()
        if mount_root is None:
            mount_root = getattr(self, '_libcephfs_mount_root', None)
        ceph_path = self._libcephfs_path(relpath, mount_root)
        body = dedent(f"""
            import json
            ans = fs.query_permissions({ceph_path!r}, {rights},
                                       want_snapshot={want_snapshot!r})
            print(json.dumps(ans))
            """)
        out = self._run_libcephfs_subprocess(body, mount_root=mount_root)
        return json.loads(out)

    def _run_libcephfs_py(self, body, mount_root=None):
        self._require_libcephfs_bindings()
        return self._run_libcephfs_subprocess(body, mount_root=mount_root)

    def _remount_with_mds_cap(self, mdscap, mntargs=None, cephfs_mntpt=None):
        self.mount_a.umount_wait()
        self.run_ceph_cmd(
            f"auth caps client.{self.mount_a.client_id} "
            f"mon 'allow r' osd 'allow rw' mds {mdscap!r}"
        )
        keyring = self.fs.get_ceph_cmd_stdout(
            'auth', 'get', f"client.{self.mount_a.client_id}")
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        cephfs_mntpt = cephfs_mntpt or self._mds_cap_mount_root(mdscap)
        self._libcephfs_mount_root = cephfs_mntpt
        kwargs = dict(
            client_id=self.mount_a.client_id,
            client_keyring_path=keyring_path,
        )
        if cephfs_mntpt:
            kwargs["cephfs_mntpt"] = cephfs_mntpt
        if mntargs:
            kwargs["mntargs"] = mntargs
        self.mount_a.remount(**kwargs)

    def _prep_basic_subdir(self):
        self.mount_a.run_shell(["mkdir", "-p", self.BASIC_SUBDIR])
        self.mount_a.run_shell(["chmod", "755", self.BASIC_SUBDIR])

    def _prep_cephx_tree(self):
        self.mount_a.run_shell(["mkdir", "-p", self.CEPHX_DIR])
        self.mount_a.run_shell(["touch", self.CEPHX_FILE])
        self.mount_a.run_shell(["chmod", "0", self.CEPHX_FILE])


class TestQueryPermissions(QueryPermissionsTestBase):
    """
    Single test case class so vstart can run the full module without hitting
    inter-class CephFSTestCase setUp (umount_wait after a skipped ACL setUp).
    """

    def test_not_returned_without_snapshot_flag(self):
        self._prep_basic_subdir()
        ans = self._libcephfs_query(self.BASIC_SUBDIR, CEPH_QUERY_PERM_READ,
                                    want_snapshot=False)
        self.assertEqual(ans["state"], CEPH_QUERY_PERM_NOT_RETURNED)
        self.assertGreater(ans["ino"], 0)

    def test_snapshot_tuple_mode(self):
        self._prep_basic_subdir()
        ans = self._libcephfs_query(self.BASIC_SUBDIR, CEPH_QUERY_PERM_READ,
                                    want_snapshot=True)
        self.assertIn(ans["state"], (CEPH_QUERY_PERM_NO_ACL, CEPH_QUERY_PERM_ACL))
        self.assertEqual(stat.S_IFDIR, stat.S_IFMT(ans["mode"]))
        self.assertGreater(ans["tuple_version"], 0)

    def test_unsupported_without_feature_bit(self):
        self._prep_basic_subdir()
        features = ",".join(str(i) for i in range(CEPHFS_FEATURE_QUERY_PERMISSIONS))
        mntargs = [f"--client_debug_inject_features={features}"]
        self.mount_a.remount(mntargs=mntargs)
        build = _ceph_build_dir()
        conf_path = os.path.join(build, 'ceph.conf')
        inject = {"client_debug_inject_features": features}
        py = dedent(f"""
            fs = libcephfs.LibCephFS(conffile={conf_path!r}, conf={inject!r})
            fs.mount()
            try:
                fs.query_permissions({self._ceph_path(self.BASIC_SUBDIR)!r}, {CEPH_QUERY_PERM_READ})
            except Exception as e:
                import errno
                if getattr(e, 'errno', None) == errno.EOPNOTSUPP:
                    print('eopnotsupp')
                else:
                    print('err', type(e).__name__, getattr(e, 'errno', None))
            """)
        self._require_libcephfs_bindings()
        out = self._run_libcephfs_subprocess(py, setup_client=False)
        self.assertEqual(out, "eopnotsupp")

    def test_read_without_unix_mode_bits(self):
        self._prep_cephx_tree()
        uid = os.getuid()
        mdscap = f"allow r path=/{self.CEPHX_DIR} uid={uid}"
        self._remount_with_mds_cap(mdscap)
        ans = self._libcephfs_query(self.CEPHX_FILE, CEPH_QUERY_PERM_READ,
                                    want_snapshot=True)
        self.assertIn(ans["state"], (CEPH_QUERY_PERM_NO_ACL, CEPH_QUERY_PERM_ACL))
        self.assertEqual(0, ans["mode"] & 0o777)

    def test_eacces_wrong_path(self):
        self._prep_cephx_tree()
        uid = os.getuid()
        self.mount_a.run_shell(['mkdir', '-p', 'other'])
        mdscap = f"allow r path=/other uid={uid}"
        self._remount_with_mds_cap(mdscap)
        # path=/other cannot ceph_mount("/"): the session root is rejected.
        # Mount the granted directory and walk ".." toward the secret. The MDS
        # denies LOOKUPPARENT of that mount point with EACCES.
        ceph_p = f"../{self.CEPHX_FILE}"
        py = dedent(f"""
            try:
                fs.query_permissions({ceph_p!r}, {CEPH_QUERY_PERM_READ},
                                     want_snapshot=True)
                print('unexpected_ok')
            except Exception as e:
                import errno
                if getattr(e, 'errno', None) == errno.EACCES:
                    print('eacces')
                else:
                    print('err', getattr(e, 'errno', None))
            """)
        self.assertEqual(self._run_libcephfs_py(py, mount_root=b"/other"), "eacces")

    def test_write_right_requires_write_cap(self):
        self._prep_cephx_tree()
        uid = os.getuid()
        mdscap = f"allow r path=/{self.CEPHX_DIR} uid={uid}"
        self._remount_with_mds_cap(mdscap)
        mroot = f'/{self.CEPHX_DIR}'.encode()
        ceph_p = self._libcephfs_path(self.CEPHX_FILE, mroot)
        py = dedent(f"""
            try:
                fs.query_permissions({ceph_p!r}, {CEPH_QUERY_PERM_WRITE})
                print('unexpected_ok')
            except Exception as e:
                import errno
                if getattr(e, 'errno', None) == errno.EACCES:
                    print('eacces')
                else:
                    print('err', getattr(e, 'errno', None))
            """)
        self.assertEqual(self._run_libcephfs_py(py, mount_root=mroot), "eacces")

    def test_snapshot_includes_acl(self):
        self.mount_a.run_shell(["mkdir", "-p", self.ACL_DIR])
        uid = os.getuid()
        try:
            self.mount_a.run_shell(
                ["setfacl", "-m", f"u:{uid}:r-x", self.ACL_DIR], check_status=True)
        except Exception:
            self.skipTest("setfacl not available on this mount (use kernel client for ACL)")
        ans = self._libcephfs_query(self.ACL_DIR, CEPH_QUERY_PERM_READ,
                                    want_snapshot=True)
        self.assertEqual(ans["state"], CEPH_QUERY_PERM_ACL)
        self.assertIn("acl", ans)
        self.assertGreater(len(ans["acl"]), 0)


def load_tests(loader, tests, pattern):
    """Stable order (unittest sorts test method names alphabetically)."""
    suite = unittest.TestSuite()
    order = (
        'test_not_returned_without_snapshot_flag',
        'test_snapshot_tuple_mode',
        'test_unsupported_without_feature_bit',
        'test_read_without_unix_mode_bits',
        'test_eacces_wrong_path',
        'test_write_right_requires_write_cap',
        'test_snapshot_includes_acl',
    )
    for name in order:
        suite.addTest(TestQueryPermissions(name))
    return suite