import os
import pwd
import getpass
import pytest
from textwrap import dedent
from ceph_volume.util import system
from mock.mock import patch
from ceph_volume.tests.conftest import Factory


@pytest.fixture
def mock_find_executable_on_host(monkeypatch):
    """
    Monkeypatches util.system.find_executable_on_host, so that a caller can add behavior to the response
    """
    def apply(stdout=None, stderr=None, returncode=0):
        stdout_stream = Factory(read=lambda: stdout)
        stderr_stream = Factory(read=lambda: stderr)
        return_value = Factory(
            stdout=stdout_stream,
            stderr=stderr_stream,
            wait=lambda: returncode,
            communicate=lambda x: (stdout, stderr, returncode)
        )

        monkeypatch.setattr(
            'ceph_volume.util.system.subprocess.Popen',
            lambda *a, **kw: return_value)

    return apply

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

    def test_is_realpath_dev_mounted_at_destination(self, fake_proc, monkeypatch):
        monkeypatch.setattr(system.os.path, 'realpath', lambda x: '/dev/sda1' if 'foo' in x else x)
        result = system.device_is_mounted('/dev/maper/foo', destination='/far/lib/ceph/osd/ceph-0')
        assert result is True

    def test_is_realpath_path_mounted_at_destination(self, fake_proc, monkeypatch):
        monkeypatch.setattr(
            system.os.path, 'realpath',
            lambda x: '/far/lib/ceph/osd/ceph-0' if 'symlink' in x else x)
        result = system.device_is_mounted('/dev/sda1', destination='/symlink/lib/ceph/osd/ceph-0')
        assert result is True


class TestGetMounts(object):

    def test_not_mounted(self, tmpdir, monkeypatch):
        PROCDIR = str(tmpdir)
        proc_path = os.path.join(PROCDIR, 'mounts')
        with open(proc_path, 'w') as f:
            f.write('')
        monkeypatch.setattr(system, 'PROCDIR', PROCDIR)
        m = system.Mounts()
        assert m.get_mounts() == {}

    def test_is_mounted_(self, fake_proc):
        m = system.Mounts()
        assert m.get_mounts()['/dev/sdc2'] == ['/boot']

    def test_ignores_two_fields(self, fake_proc):
        m = system.Mounts()
        assert m.get_mounts().get('/dev/sde4') is None

    def test_tmpfs_is_reported(self, fake_proc):
        m = system.Mounts()
        assert m.get_mounts()['tmpfs'][0] == '/dev/shm'

    def test_non_skip_devs_arent_reported(self, fake_proc):
        m = system.Mounts()
        assert m.get_mounts().get('cgroup') is None

    def test_multiple_mounts_are_appended(self, fake_proc):
        m = system.Mounts()
        assert len(m.get_mounts()['tmpfs']) == 7

    def test_nonexistent_devices_are_skipped(self, tmpdir, monkeypatch):
        PROCDIR = str(tmpdir)
        proc_path = os.path.join(PROCDIR, 'mounts')
        with open(proc_path, 'w') as f:
            f.write(dedent("""nfsd /proc/fs/nfsd nfsd rw,relatime 0 0
                    /dev/sda1 /far/lib/ceph/osd/ceph-0 xfs rw,attr2,inode64,noquota 0 0
                    /dev/sda2 /far/lib/ceph/osd/ceph-1 xfs rw,attr2,inode64,noquota 0 0"""))
        monkeypatch.setattr(system, 'PROCDIR', PROCDIR)
        monkeypatch.setattr(os.path, 'exists', lambda x: False if x == '/dev/sda1' else True)
        m = system.Mounts()
        assert m.get_mounts().get('/dev/sda1') is None


class TestIsBinary(object):

    def test_is_binary(self, fake_filesystem):
        binary_path = fake_filesystem.create_file('/tmp/fake-file', contents='asd\n\nlkjh\x00')
        assert system.is_binary(binary_path.path)

    def test_is_not_binary(self, fake_filesystem):
        binary_path = fake_filesystem.create_file('/tmp/fake-file', contents='asd\n\nlkjh0')
        assert system.is_binary(binary_path.path) is False


class TestGetFileContents(object):

    def test_path_does_not_exist(self, tmpdir):
        filepath = os.path.join(str(tmpdir), 'doesnotexist')
        assert system.get_file_contents(filepath, 'default') == 'default'

    def test_path_has_contents(self, fake_filesystem):
        interesting_file = fake_filesystem.create_file('/tmp/fake-file', contents="1")
        result = system.get_file_contents(interesting_file.path)
        assert result == "1"

    def test_path_has_multiline_contents(self, fake_filesystem):
        interesting_file = fake_filesystem.create_file('/tmp/fake-file', contents="0\n1")
        result = system.get_file_contents(interesting_file.path)
        assert result == "0\n1"

    def test_exception_returns_default(self):
        with patch('builtins.open') as mocked_open:
            mocked_open.side_effect = Exception()
            result = system.get_file_contents('/tmp/fake-file')
        assert result == ''


class TestWhich(object):

    def test_executable_exists_but_is_not_file(self, monkeypatch):
        monkeypatch.setattr(system.os.path, 'isfile', lambda x: False)
        monkeypatch.setattr(system.os.path, 'exists', lambda x: True)
        assert system.which('exedir') == 'exedir'

    def test_executable_does_not_exist(self, monkeypatch):
        monkeypatch.setattr(system.os.path, 'isfile', lambda x: False)
        monkeypatch.setattr(system.os.path, 'exists', lambda x: False)
        assert system.which('exedir') == 'exedir'

    def test_executable_exists_as_file(self, monkeypatch):
        monkeypatch.setattr(system.os, 'getenv', lambda x, y: '')
        monkeypatch.setattr(system.os.path, 'isfile', lambda x: x != 'ceph')
        monkeypatch.setattr(system.os.path, 'exists', lambda x: x != 'ceph')
        assert system.which('ceph') == '/usr/local/bin/ceph'

    def test_warnings_when_executable_isnt_matched(self, monkeypatch, capsys):
        monkeypatch.setattr(system.os.path, 'isfile', lambda x: True)
        monkeypatch.setattr(system.os.path, 'exists', lambda x: False)
        system.which('exedir')
        cap = capsys.readouterr()
        assert 'Executable exedir not in PATH' in cap.err

    def test_run_on_host_found(self, mock_find_executable_on_host):
        mock_find_executable_on_host(stdout="/sbin/lvs\n", stderr="some stderr message\n")
        assert system.which('lvs', run_on_host=True) == '/sbin/lvs'

    def test_run_on_host_not_found(self, mock_find_executable_on_host):
        mock_find_executable_on_host(stdout="", stderr="some stderr message\n")
        assert system.which('lvs', run_on_host=True) == 'lvs'

@pytest.fixture
def stub_which(monkeypatch):
    def apply(value='/bin/restorecon'):
        monkeypatch.setattr(system, 'which', lambda x: value)
    return apply


# python2 has no FileNotFoundError
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = OSError


class TestSetContext(object):

    def setup_method(self):
        try:
            os.environ.pop('CEPH_VOLUME_SKIP_RESTORECON')
        except KeyError:
            pass

    @pytest.mark.parametrize('value', ['1', 'True', 'true', 'TRUE', 'yes'])
    def test_set_context_skips(self, stub_call, fake_run, value):
        stub_call(('', '', 0))
        os.environ['CEPH_VOLUME_SKIP_RESTORECON'] = value
        system.set_context('/tmp/foo')
        assert fake_run.calls == []

    @pytest.mark.parametrize('value', ['0', 'False', 'false', 'FALSE', 'no'])
    def test_set_context_doesnt_skip_with_env(self, stub_call, stub_which, fake_run, value):
        stub_call(('', '', 0))
        stub_which()
        os.environ['CEPH_VOLUME_SKIP_RESTORECON'] = value
        system.set_context('/tmp/foo')
        assert len(fake_run.calls)

    def test_set_context_skips_on_executable(self, stub_call, stub_which, fake_run):
        stub_call(('', '', 0))
        stub_which('restorecon')
        system.set_context('/tmp/foo')
        assert fake_run.calls == []

    def test_set_context_no_skip_on_executable(self, stub_call, stub_which, fake_run):
        stub_call(('', '', 0))
        stub_which('/bin/restorecon')
        system.set_context('/tmp/foo')
        assert len(fake_run.calls)

    @patch('ceph_volume.process.call')
    def test_selinuxenabled_doesnt_exist(self, mocked_call, fake_run):
        mocked_call.side_effect = FileNotFoundError()
        system.set_context('/tmp/foo')
        assert fake_run.calls == []

    def test_selinuxenabled_is_not_enabled(self, stub_call, fake_run):
        stub_call(('', '', 1))
        system.set_context('/tmp/foo')
        assert fake_run.calls == []
