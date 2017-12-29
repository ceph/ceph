import os
import pwd
import getpass
import pytest
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


@pytest.fixture
def fake_proc(tmpdir, monkeypatch):
    PROCDIR = str(tmpdir)
    proc_path = os.path.join(PROCDIR, 'mounts')
    with open(proc_path, 'w') as f:
        f.write(dedent("""nfsd /proc/fs/nfsd nfsd rw,relatime 0 0
            rootfs / rootfs rw 0 0
            sysfs /sys sysfs rw,seclabel,nosuid,nodev,noexec,relatime 0 0
            proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0
            devtmpfs /dev devtmpfs rw,seclabel,nosuid,size=238292k,nr_inodes=59573,mode=755 0 0
            securityfs /sys/kernel/security securityfs rw,nosuid,nodev,noexec,relatime 0 0
            tmpfs /dev/shm tmpfs rw,seclabel,nosuid,nodev 0 0
            devpts /dev/pts devpts rw,seclabel,nosuid,noexec,relatime,gid=5,mode=620,ptmxmode=000 0 0
            tmpfs /run tmpfs rw,seclabel,nosuid,nodev,mode=755 0 0
            tmpfs /sys/fs/cgroup tmpfs ro,seclabel,nosuid,nodev,noexec,mode=755 0 0
            cgroup /sys/fs/cgroup/systemd cgroup rw,nosuid,nodev,noexec,relatime,xattr,release_agent=/usr/lib/systemd/systemd-cgroups-agent,name=systemd 0 0
            cgroup /sys/fs/cgroup/freezer cgroup rw,nosuid,nodev,noexec,relatime,freezer 0 0
            configfs /sys/kernel/config configfs rw,relatime 0 0
            /dev/mapper/VolGroup00-LogVol00 / xfs rw,seclabel,relatime,attr2,inode64,noquota 0 0
            selinuxfs /sys/fs/selinux selinuxfs rw,relatime 0 0
            debugfs /sys/kernel/debug debugfs rw,relatime 0 0
            hugetlbfs /dev/hugepages hugetlbfs rw,seclabel,relatime 0 0
            mqueue /dev/mqueue mqueue rw,seclabel,relatime 0 0
            sunrpc /far/lib/nfs/rpc_pipefs rpc_pipefs rw,relatime 0 0
            /dev/sde4 /two/field/path
            nfsd /proc/fs/nfsd nfsd rw,relatime 0 0
            /dev/sde2 /boot xfs rw,seclabel,relatime,attr2,inode64,noquota 0 0
            tmpfs /far/lib/ceph/osd/ceph-5 tmpfs rw,seclabel,relatime 0 0
            tmpfs /far/lib/ceph/osd/ceph-7 tmpfs rw,seclabel,relatime 0 0
            /dev/sda1 /far/lib/ceph/osd/ceph-0 xfs rw,seclabel,noatime,attr2,inode64,noquota 0 0
            tmpfs /run/user/1000 tmpfs rw,seclabel,nosuid,nodev,relatime,size=50040k,mode=700,uid=1000,gid=1000 0 0
            /dev/sdc2 /boot xfs rw,seclabel,relatime,attr2,inode64,noquota 0 0
            tmpfs /run/user/1000 tmpfs rw,seclabel,mode=700,uid=1000,gid=1000 0 0"""))
    monkeypatch.setattr(system, 'PROCDIR', PROCDIR)
    monkeypatch.setattr(os.path, 'exists', lambda x: True)


class TestPathIsMounted(object):

    def test_is_mounted(self, fake_proc):
        assert system.path_is_mounted('/boot') is True

    def test_is_not_mounted(self, fake_proc):
        assert system.path_is_mounted('/far/fib/feph') is False

    def test_is_not_mounted_at_destination(self, fake_proc):
        assert system.path_is_mounted('/boot', destination='/dev/sda1') is False

    def test_is_mounted_at_destination(self, fake_proc):
        assert system.path_is_mounted('/boot', destination='/dev/sdc2') is True


class TestDeviceIsMounted(object):

    def test_is_mounted(self, fake_proc):
        assert system.device_is_mounted('/dev/sda1') is True

    def test_path_is_not_device(self, fake_proc):
        assert system.device_is_mounted('/far/lib/ceph/osd/ceph-7') is False

    def test_is_not_mounted_at_destination(self, fake_proc):
        assert system.device_is_mounted('/dev/sda1', destination='/far/lib/ceph/osd/test-1') is False

    def test_is_mounted_at_destination(self, fake_proc):
        assert system.device_is_mounted('/dev/sda1', destination='/far/lib/ceph/osd/ceph-7') is False


class TestGetMounts(object):

    def test_not_mounted(self, tmpdir, monkeypatch):
        PROCDIR = str(tmpdir)
        proc_path = os.path.join(PROCDIR, 'mounts')
        with open(proc_path, 'w') as f:
            f.write('')
        monkeypatch.setattr(system, 'PROCDIR', PROCDIR)
        assert system.get_mounts() == {}

    def test_is_mounted_(self, fake_proc):
        result = system.get_mounts()
        assert result['/dev/sdc2'] == ['/boot']

    def test_ignores_two_fields(self, fake_proc):
        result = system.get_mounts()
        assert result.get('/dev/sde4') is None

    def test_tmpfs_is_reported(self, fake_proc):
        result = system.get_mounts()
        assert result['tmpfs'][0] == '/dev/shm'

    def test_non_skip_devs_arent_reported(self, fake_proc):
        result = system.get_mounts()
        assert result.get('cgroup') is None

    def test_multiple_mounts_are_appended(self, fake_proc):
        result = system.get_mounts()
        assert len(result['tmpfs']) == 7

    def test_nonexistent_devices_are_skipped(self, tmpdir, monkeypatch):
        PROCDIR = str(tmpdir)
        proc_path = os.path.join(PROCDIR, 'mounts')
        with open(proc_path, 'w') as f:
            f.write(dedent("""nfsd /proc/fs/nfsd nfsd rw,relatime 0 0
                    /dev/sda1 /far/lib/ceph/osd/ceph-0 xfs rw,attr2,inode64,noquota 0 0
                    /dev/sda2 /far/lib/ceph/osd/ceph-1 xfs rw,attr2,inode64,noquota 0 0"""))
        monkeypatch.setattr(system, 'PROCDIR', PROCDIR)
        monkeypatch.setattr(os.path, 'exists', lambda x: False if x == '/dev/sda1' else True)
        result = system.get_mounts()
        assert result.get('/dev/sda1') is None


class TestIsBinary(object):

    def test_is_binary(self, tmpfile):
        binary_path = tmpfile(contents='asd\n\nlkjh\x00')
        assert system.is_binary(binary_path)

    def test_is_not_binary(self, tmpfile):
        binary_path = tmpfile(contents='asd\n\nlkjh0')
        assert system.is_binary(binary_path) is False
