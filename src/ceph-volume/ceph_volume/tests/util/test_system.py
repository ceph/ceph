import os
import pwd
import getpass
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

