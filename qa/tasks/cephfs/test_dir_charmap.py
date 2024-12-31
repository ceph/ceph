import base64
import json
from logging import getLogger

from teuthology.exceptions import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.mount import NoSuchAttributeError, InvalidArgumentError, DirectoryNotEmptyError

log = getLogger(__name__)

class CharMapMixin:
    def check_cs(self, path, **kwargs):
        what = kwargs
        what.setdefault("casesensitive", True)
        what.setdefault("normalization", "nfd")
        what.setdefault("encoding", "utf8")
        v = self.mount_a.getfattr(path, "ceph.dir.charmap", helpfulexception=True)
        J = json.loads(v)
        log.debug("cs = %s", v)
        self.assertEqual(what, J)

class TestCharMapVxattr(CephFSTestCase, CharMapMixin):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def test_cs_get_charmap_none(self):
        """
        That getvxattr for a charmap fails if not present in Inode.
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        try:
            self.check_cs("foo")
        except NoSuchAttributeError:
            pass
        else:
            self.fail("should raise error")

    def test_cs_get_charmap_set(self):
        """
        That setvxattr fails for charmap.
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        try:
            self.mount_a.setfattr("foo/", "ceph.dir.charmap", "0", helpfulexception=True)
        except InvalidArgumentError:
            pass
        else:
            self.fail("should raise error")

    def test_cs_set_charmap_inherited(self):
        """
        That charmap is inherited.
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.casesensitive", "0")
        self.mount_a.run_shell_payload("mkdir foo/bar")
        self.check_cs("foo/bar/", casesensitive=False)

    def test_cs_get_charmap_none_rm(self):
        """
        That rmvxattr actually removes the metadata from the Inode.
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.casesensitive", "0")
        self.mount_a.removexattr("foo/", "ceph.dir.charmap")
        try:
            self.check_cs("foo")
        except NoSuchAttributeError:
            pass
        else:
            self.fail("should raise error")

    def test_cs_get_charmap_none_dup(self):
        """
        That rmvxattr is idempotent.
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.casesensitive", "0")
        self.mount_a.removexattr("foo/", "ceph.dir.charmap")
        self.mount_a.removexattr("foo/", "ceph.dir.charmap")

    def test_cs_set_encoding_valid(self):
        """
        That we can set ceph.dir.encoding and check it.
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.encoding", "utf16")
        self.check_cs("foo", encoding="utf16")
        self.mount_a.setfattr("foo/", "ceph.dir.encoding", "utf8")
        self.check_cs("foo", encoding="utf8")

    def test_cs_set_encoding_garbage(self):
        """
        That a garbage encoding is accepted but prevents creating any dentries.
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.encoding", "garbage")
        self.check_cs("foo", encoding="garbage")
        try:
            p = self.mount_a.run_shell_payload("mkdir foo/test", wait=False)
            p.wait()
        except CommandFailedError:
            stderr = p.stderr.getvalue()
            self.assertIn("Permission denied", stderr)
        else:
            self.fail("should fail")

    def test_cs_rm_encoding(self):
        """
        That removing the encoding without any other charmappings will restore access.
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.encoding", "garbage")
        self.mount_a.removexattr("foo/", "ceph.dir.encoding")
        self.check_cs("foo")
        self.mount_a.run_shell_payload("mkdir foo/test")

    def test_cs_set_insensitive_valid(self):
        """
        That we can set ceph.dir.casesensitive and check it.
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.casesensitive", "0")
        self.check_cs("foo", casesensitive=False)
        self.mount_a.setfattr("foo/", "ceph.dir.casesensitive", "1")
        self.check_cs("foo")

    def test_cs_set_insensitive_garbage(self):
        """
        That setting ceph.dir.casesensitive to garbage is rejected (should be bool).
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        try:
            self.mount_a.setfattr("foo/", "ceph.dir.casesensitive", "abc", helpfulexception=True)
        except InvalidArgumentError:
            pass
        else:
            self.fail("should fail")
        try:
            self.check_cs("foo")
        except NoSuchAttributeError:
            pass
        else:
            self.fail("should raise error")

    def test_cs_rm_insensitive(self):
        """
        That we can remove ceph.dir.casesensitive and restore the default.
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.casesensitive", "0")
        self.mount_a.removexattr("foo/", "ceph.dir.casesensitive")
        self.check_cs("foo")

    def test_cs_set_normalization(self):
        """
        That we can set ceph.dir.normalization and check it.
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.normalization", "nfc")
        self.check_cs("foo", normalization="nfc")
        self.mount_a.setfattr("foo/", "ceph.dir.normalization", "nfd")
        self.check_cs("foo", normalization="nfd")

    def test_cs_set_normalization_garbage(self):
        """
        That a garbage normalization is accepted but prevents creating any dentries.
        """
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.normalization", "abc")
        self.check_cs("foo", normalization="abc")
        try:
            p = self.mount_a.run_shell_payload("mkdir foo/test", wait=False)
            p.wait()
        except CommandFailedError:
            stderr = p.stderr.getvalue()
            self.assertIn("Permission denied", stderr)
        else:
            self.fail("should fail")

    def test_cs_feature_bit(self):
        """
        That the CEPHFS_FEATURE_CHARMAP feature bit enforces access.
        """

        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.casesensitive", "0")
        self.check_cs("foo", casesensitive=False)

        self.mount_a.run_shell_payload("dd if=/dev/urandom of=foo/Test1 bs=4k count=1")

        CEPHFS_FEATURE_CHARMAP = 22
        # all but CEPHFS_FEATURE_CHARMAP
        features = ",".join([str(i) for i in range(CEPHFS_FEATURE_CHARMAP)])
        mntargs = [f"--client_debug_inject_features={features}"]

        self.mount_a.remount(mntargs=mntargs)

        self.check_cs("foo", casesensitive=False)

        cmds = [
          "mkdir foo/test2",
          "ln -s . foo/test2",
          "ln foo/Test1 foo/test2",
          "dd if=/dev/urandom of=foo/test2 bs=4k count=1",
          "mv foo/Test1 foo/Test2",
        ]
        for cmd in cmds:
            try:
                p = self.mount_a.run_shell_payload(cmd, wait=False)
                p.wait()
            except CommandFailedError:
                stderr = p.stderr.getvalue()
                self.assertIn("Operation not permitted", stderr)
            else:
                self.fail("should fail")

        okay_cmds = [
          "ls foo/",
          "stat foo/test1",
          "rm foo/test1",
        ]
        for cmd in okay_cmds:
            try:
                p = self.mount_a.run_shell_payload(cmd, wait=False)
                p.wait()
            except CommandFailedError:
                stderr = p.stderr.getvalue()
                self.fail("command failed:\n%s", stderr)


    def test_cs_remount(self):
        """
        That a remount continues to see the charmap.
        """

        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.casesensitive", "0")
        self.check_cs("foo", casesensitive=False)

        self.mount_a.umount_wait()
        self.mount_a.mount()

        self.check_cs("foo", casesensitive=False)

    def test_cs_not_empty_set_insensitive(self):
        """
        That setting a charmap fails for a non-empty directory.
        """

        attrs = {
          "ceph.dir.casesensitive": "0",
          "ceph.dir.normalization": "nfc",
          "ceph.dir.encoding": "utf8",
        }

        self.mount_a.run_shell_payload("mkdir -p foo/dir")
        for attr, v in attrs.items():
            try:
                self.mount_a.setfattr("foo/", attr, v, helpfulexception=True)
            except DirectoryNotEmptyError:
                pass
            else:
                self.fail("should fail")
            try:
                self.check_cs("foo")
            except NoSuchAttributeError:
                pass
            else:
                self.fail("should fail")

class TestNormalization(CephFSTestCase, CharMapMixin):
    """
    Test charmap normalization.
    """

    def test_normalization(self):
        """
        That a normalization works for a conventional example.
        """

        dname = "Grüßen"
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.normalization", "nfd") # default

        self.mount_a.run_shell_payload(f"mkdir foo/{dname}")
        c = self.fs.read_cache("foo", depth=0)

        self.assertEqual(len(c), 1)
        frags = c[0]['dirfrags']
        self.assertEqual(len(frags), 1)
        frag = frags[0]
        dentries = frag['dentries']
        self.assertEqual(len(dentries), 1)
        dentry = dentries[0]
        # ü to u + u0308
        self.assertEqual(dentry['path'], "foo/Gru\u0308\u00dfen")
        altn = dentry['alternate_name']
        altn_bin = base64.b64decode(altn)
        expected = bytes([0x47, 0x72, 0xc3, 0xbc, 0xc3, 0x9f, 0x65, 0x6e]) # 8 not 9 chars
        self.assertIn(expected, altn_bin)

class TestEncoding(CephFSTestCase, CharMapMixin):
    """
    Test charmap encoding.
    """

    def test_encoding(self):
        """
        That an encoding-only charmap still normalizes.
        """

        # N.B.: you cannot disable normalization. Setting to empty string
        # restores default.

        dname = "Grüßen"
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.encoding", "utf8")

        self.mount_a.run_shell_payload(f"mkdir foo/{dname}")
        c = self.fs.read_cache("foo", depth=0)

        self.assertEqual(len(c), 1)
        frags = c[0]['dirfrags']
        self.assertEqual(len(frags), 1)
        frag = frags[0]
        dentries = frag['dentries']
        self.assertEqual(len(dentries), 1)
        dentry = dentries[0]
        self.assertEqual(dentry['path'], "foo/Gru\u0308\u00dfen")
        altn = dentry['alternate_name']
        altn_bin = base64.b64decode(altn)
        expected = bytes([0x47, 0x72, 0xc3, 0xbc, 0xc3, 0x9f, 0x65, 0x6e]) # 8 not 9 chars
        self.assertIn(expected, altn_bin)


class TestCaseFolding(CephFSTestCase, CharMapMixin):
    """
    Test charmap case folding.
    """

    def test_casefolding(self):
        """
        That a case folding works for a conventional example.
        """

        dname = "Grüßen"
        self.mount_a.run_shell_payload("mkdir foo/")
        self.mount_a.setfattr("foo/", "ceph.dir.casesensitive", "0")

        self.mount_a.run_shell_payload(f"mkdir foo/{dname}")
        c = self.fs.read_cache("foo", depth=0)

        self.assertEqual(len(c), 1)
        frags = c[0]['dirfrags']
        self.assertEqual(len(frags), 1)
        frag = frags[0]
        dentries = frag['dentries']
        self.assertEqual(len(dentries), 1)
        dentry = dentries[0]
        path = dentry['path'].encode('utf-8')
        # Grüßen to Gru \u0308 ssen
        # foo/gru\u0308ssen
        expected = bytes([0x66, 0x6f, 0x6f, 0x2f, 0x67, 0x72, 0x75, 0xcc, 0x88, 0x73, 0x73, 0x65, 0x6e])
        self.assertEqual(path, expected)
        # Grüßen
        altn = dentry['alternate_name']
        altn_bin = base64.b64decode(altn)
        expected = base64.b64decode("R3LDvMOfZW4=") # 8 chars, not 9
        self.assertIn(expected, altn_bin)
