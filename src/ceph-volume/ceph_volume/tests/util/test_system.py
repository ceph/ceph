import os
import pwd
import getpass
from textwrap import dedent
from ceph_volume.util import system


class TestMkdirP(object):

    def test_existing_dir_does_not_raise_w_chown(self, monkeypatch, tmpdir):
        user = pwd.getpwnam(getpass.getuser())
        uid, gid = user[2], user[3]
        monkeypatch.setattr(system, 'get_ceph_user_ids', lambda: (uid, gid,))
        path = str(tmpdir)
        system.mkdir_p(path)
        assert os.path.isdir(path)

    def test_new_dir_w_chown(self, monkeypatch, tmpdir):
        user = pwd.getpwnam(getpass.getuser())
        uid, gid = user[2], user[3]
        monkeypatch.setattr(system, 'get_ceph_user_ids', lambda: (uid, gid,))
        path = os.path.join(str(tmpdir), 'new')
        system.mkdir_p(path)
        assert os.path.isdir(path)

    def test_existing_dir_does_not_raise_no_chown(self, tmpdir):
        path = str(tmpdir)
        system.mkdir_p(path, chown=False)
        assert os.path.isdir(path)

    def test_new_dir_no_chown(self, tmpdir):
        path = os.path.join(str(tmpdir), 'new')
        system.mkdir_p(path, chown=False)
        assert os.path.isdir(path)


class TestIsMounted(object):

    def test_not_mounted(self, tmpdir, monkeypatch):
        PROCDIR = str(tmpdir)
        proc_path = os.path.join(PROCDIR, 'mounts')
        with open(proc_path, 'w') as f:
            f.write('')
        monkeypatch.setattr(system, 'PROCDIR', PROCDIR)
        assert system.is_mounted('sdb') is False

    def test_is_mounted_(self, tmpdir, monkeypatch):
        PROCDIR = str(tmpdir)
        proc_path = os.path.join(PROCDIR, 'mounts')
        with open(proc_path, 'w') as f:
            f.write(dedent("""nfsd /proc/fs/nfsd nfsd rw,relatime 0 0
                    /dev/sdc2 /boot xfs rw,seclabel,relatime,attr2,inode64,noquota 0 0
                    tmpfs /run/user/1000 tmpfs rw,seclabel,mode=700,uid=1000,gid=1000 0 0"""))
        monkeypatch.setattr(system, 'PROCDIR', PROCDIR)
        monkeypatch.setattr(os.path, 'exists', lambda x: True)
        assert system.is_mounted('/dev/sdc2') is True

    def test_ignores_two_fields(self, tmpdir, monkeypatch):
        PROCDIR = str(tmpdir)
        proc_path = os.path.join(PROCDIR, 'mounts')
        with open(proc_path, 'w') as f:
            f.write(dedent("""nfsd /proc/fs/nfsd nfsd rw,relatime 0 0
                    /dev/sdc2 /boot
                    tmpfs /run/user/1000 tmpfs rw,seclabel,mode=700,uid=1000,gid=1000 0 0"""))
        monkeypatch.setattr(system, 'PROCDIR', PROCDIR)
        monkeypatch.setattr(os.path, 'exists', lambda x: True)
        assert system.is_mounted('/dev/sdc2') is False

    def test_not_mounted_at_destination(self, tmpdir, monkeypatch):
        PROCDIR = str(tmpdir)
        proc_path = os.path.join(PROCDIR, 'mounts')
        with open(proc_path, 'w') as f:
            f.write(dedent("""nfsd /proc/fs/nfsd nfsd rw,relatime 0 0
                    /dev/sdc2 /var/lib/ceph/osd/ceph-9 xfs rw,attr2,inode64,noquota 0 0
                    tmpfs /run/user/1000 tmpfs rw,seclabel,mode=700,uid=1000,gid=1000 0 0"""))
        monkeypatch.setattr(system, 'PROCDIR', PROCDIR)
        monkeypatch.setattr(os.path, 'exists', lambda x: True)
        assert system.is_mounted('/dev/sdc2', '/var/lib/ceph/osd/ceph-0') is False

    def test_is_mounted_at_destination(self, tmpdir, monkeypatch):
        PROCDIR = str(tmpdir)
        proc_path = os.path.join(PROCDIR, 'mounts')
        with open(proc_path, 'w') as f:
            f.write(dedent("""nfsd /proc/fs/nfsd nfsd rw,relatime 0 0
                    /dev/sdc2 /var/lib/ceph/osd/ceph-0 xfs rw,attr2,inode64,noquota 0 0
                    tmpfs /run/user/1000 tmpfs rw,seclabel,mode=700,uid=1000,gid=1000 0 0"""))
        monkeypatch.setattr(system, 'PROCDIR', PROCDIR)
        monkeypatch.setattr(os.path, 'exists', lambda x: True)
        assert system.is_mounted('/dev/sdc2', '/var/lib/ceph/osd/ceph-0') is True
