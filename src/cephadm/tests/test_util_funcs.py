# Tests for various assorted utility functions found within cephadm
#
from unittest import mock

import os

import pytest

from tests.fixtures import with_cephadm_ctx, import_cephadm

_cephadm = import_cephadm()


class TestCopyTree:
    def _copy_tree(self, *args, **kwargs):
        with with_cephadm_ctx([]) as ctx:
            with mock.patch("cephadm.extract_uid_gid") as eug:
                eug.return_value = (os.getuid(), os.getgid())
                _cephadm.copy_tree(ctx, *args, **kwargs)

    def test_one_dir(self, tmp_path):
        """Copy one dir into a non-existing dest dir."""
        src1 = tmp_path / "src1"
        dst = tmp_path / "dst"
        src1.mkdir(parents=True)

        with (src1 / "foo.txt").open("w") as fh:
            fh.write("hello\n")
            fh.write("earth\n")

        assert not (dst / "foo.txt").exists()

        self._copy_tree([src1], dst)
        assert (dst / "foo.txt").exists()

    def test_one_existing_dir(self, tmp_path):
        """Copy one dir into an existing dest dir."""
        src1 = tmp_path / "src1"
        dst = tmp_path / "dst"
        src1.mkdir(parents=True)
        dst.mkdir(parents=True)

        with (src1 / "foo.txt").open("w") as fh:
            fh.write("hello\n")
            fh.write("earth\n")

        assert not (dst / "src1").exists()

        self._copy_tree([src1], dst)
        assert (dst / "src1/foo.txt").exists()

    def test_two_dirs(self, tmp_path):
        """Copy two source directories into an existing dest dir."""
        src1 = tmp_path / "src1"
        src2 = tmp_path / "src2"
        dst = tmp_path / "dst"
        src1.mkdir(parents=True)
        src2.mkdir(parents=True)
        dst.mkdir(parents=True)

        with (src1 / "foo.txt").open("w") as fh:
            fh.write("hello\n")
            fh.write("earth\n")
        with (src2 / "bar.txt").open("w") as fh:
            fh.write("goodbye\n")
            fh.write("mars\n")

        assert not (dst / "src1").exists()
        assert not (dst / "src2").exists()

        self._copy_tree([src1, src2], dst)
        assert (dst / "src1/foo.txt").exists()
        assert (dst / "src2/bar.txt").exists()

    def test_one_dir_set_uid(self, tmp_path):
        """Explicity pass uid/gid values and assert these are passed to chown."""
        # Because this test will often be run by non-root users it is necessary
        # to mock os.chown or we too easily run into perms issues.
        src1 = tmp_path / "src1"
        dst = tmp_path / "dst"
        src1.mkdir(parents=True)

        with (src1 / "foo.txt").open("w") as fh:
            fh.write("hello\n")
            fh.write("earth\n")

        assert not (dst / "foo.txt").exists()

        with mock.patch("os.chown") as _chown:
            _chown.return_value = None
            self._copy_tree([src1], dst, uid=0, gid=0)
            assert len(_chown.mock_calls) >= 2
            for c in _chown.mock_calls:
                assert c.args[1:] == (0, 0)
        assert (dst / "foo.txt").exists()


class TestCopyFiles:
    def _copy_files(self, *args, **kwargs):
        with with_cephadm_ctx([]) as ctx:
            with mock.patch("cephadm.extract_uid_gid") as eug:
                eug.return_value = (os.getuid(), os.getgid())
                _cephadm.copy_files(ctx, *args, **kwargs)

    def test_one_file(self, tmp_path):
        """Copy one file into the dest dir."""
        file1 = tmp_path / "f1.txt"
        dst = tmp_path / "dst"
        dst.mkdir(parents=True)

        with file1.open("w") as fh:
            fh.write("its test time\n")

        self._copy_files([file1], dst)
        assert (dst / "f1.txt").exists()

    def test_one_file_nodest(self, tmp_path):
        """Copy one file to the given destination path."""
        file1 = tmp_path / "f1.txt"
        dst = tmp_path / "dst"

        with file1.open("w") as fh:
            fh.write("its test time\n")

        self._copy_files([file1], dst)
        assert not dst.is_dir()
        assert dst.is_file()
        assert dst.open("r").read() == "its test time\n"

    def test_three_files(self, tmp_path):
        """Copy one file into the dest dir."""
        file1 = tmp_path / "f1.txt"
        file2 = tmp_path / "f2.txt"
        file3 = tmp_path / "f3.txt"
        dst = tmp_path / "dst"
        dst.mkdir(parents=True)

        with file1.open("w") as fh:
            fh.write("its test time\n")
        with file2.open("w") as fh:
            fh.write("f2\n")
        with file3.open("w") as fh:
            fh.write("f3\n")

        self._copy_files([file1, file2, file3], dst)
        assert (dst / "f1.txt").exists()
        assert (dst / "f2.txt").exists()
        assert (dst / "f3.txt").exists()

    def test_three_files_nodest(self, tmp_path):
        """Copy files to dest path (not a dir). This is not a useful operation."""
        file1 = tmp_path / "f1.txt"
        file2 = tmp_path / "f2.txt"
        file3 = tmp_path / "f3.txt"
        dst = tmp_path / "dst"

        with file1.open("w") as fh:
            fh.write("its test time\n")
        with file2.open("w") as fh:
            fh.write("f2\n")
        with file3.open("w") as fh:
            fh.write("f3\n")

        self._copy_files([file1, file2, file3], dst)
        assert not dst.is_dir()
        assert dst.is_file()
        assert dst.open("r").read() == "f3\n"

    def test_one_file_set_uid(self, tmp_path):
        """Explicity pass uid/gid values and assert these are passed to chown."""
        # Because this test will often be run by non-root users it is necessary
        # to mock os.chown or we too easily run into perms issues.
        file1 = tmp_path / "f1.txt"
        dst = tmp_path / "dst"
        dst.mkdir(parents=True)

        with file1.open("w") as fh:
            fh.write("its test time\n")

        assert not (dst / "f1.txt").exists()

        with mock.patch("os.chown") as _chown:
            _chown.return_value = None
            self._copy_files([file1], dst, uid=0, gid=0)
            assert len(_chown.mock_calls) >= 1
            for c in _chown.mock_calls:
                assert c.args[1:] == (0, 0)
        assert (dst / "f1.txt").exists()


class TestMoveFiles:
    def _move_files(self, *args, **kwargs):
        with with_cephadm_ctx([]) as ctx:
            with mock.patch("cephadm.extract_uid_gid") as eug:
                eug.return_value = (os.getuid(), os.getgid())
                _cephadm.move_files(ctx, *args, **kwargs)

    def test_one_file(self, tmp_path):
        """Move a named file to test dest path."""
        file1 = tmp_path / "f1.txt"
        dst = tmp_path / "dst"

        with file1.open("w") as fh:
            fh.write("lets moove\n")

        assert not dst.exists()
        assert file1.is_file()

        self._move_files([file1], dst)
        assert dst.is_file()
        assert not file1.exists()

    def test_one_file_destdir(self, tmp_path):
        """Move a file into an existing dest dir."""
        file1 = tmp_path / "f1.txt"
        dst = tmp_path / "dst"
        dst.mkdir(parents=True)

        with file1.open("w") as fh:
            fh.write("lets moove\n")

        assert not (dst / "f1.txt").exists()
        assert file1.is_file()

        self._move_files([file1], dst)
        assert (dst / "f1.txt").is_file()
        assert not file1.exists()

    def test_one_file_one_link(self, tmp_path):
        """Move a file and a symlink to that file to a dest dir."""
        file1 = tmp_path / "f1.txt"
        link1 = tmp_path / "lnk"
        dst = tmp_path / "dst"
        dst.mkdir(parents=True)

        with file1.open("w") as fh:
            fh.write("lets moove\n")
        os.symlink("f1.txt", link1)

        assert not (dst / "f1.txt").exists()
        assert file1.is_file()
        assert link1.exists()

        self._move_files([file1, link1], dst)
        assert (dst / "f1.txt").is_file()
        assert (dst / "lnk").is_symlink()
        assert not file1.exists()
        assert not link1.exists()
        assert (dst / "f1.txt").open("r").read() == "lets moove\n"
        assert (dst / "lnk").open("r").read() == "lets moove\n"

    def test_one_file_set_uid(self, tmp_path):
        """Explicity pass uid/gid values and assert these are passed to chown."""
        # Because this test will often be run by non-root users it is necessary
        # to mock os.chown or we too easily run into perms issues.
        file1 = tmp_path / "f1.txt"
        dst = tmp_path / "dst"

        with file1.open("w") as fh:
            fh.write("lets moove\n")

        assert not dst.exists()
        assert file1.is_file()

        with mock.patch("os.chown") as _chown:
            _chown.return_value = None
            self._move_files([file1], dst, uid=0, gid=0)
            assert len(_chown.mock_calls) >= 1
            for c in _chown.mock_calls:
                assert c.args[1:] == (0, 0)
        assert dst.is_file()
        assert not file1.exists()


def test_recursive_chown(tmp_path):
    d1 = tmp_path / "dir1"
    d2 = d1 / "dir2"
    f1 = d2 / "file1.txt"
    d2.mkdir(parents=True)

    with f1.open("w") as fh:
        fh.write("low down\n")

    with mock.patch("os.chown") as _chown:
        _chown.return_value = None
        _cephadm.recursive_chown(str(d1), uid=500, gid=500)
    assert len(_chown.mock_calls) == 3
    assert _chown.mock_calls[0].args == (str(d1), 500, 500)
    assert _chown.mock_calls[1].args == (str(d2), 500, 500)
    assert _chown.mock_calls[2].args == (str(f1), 500, 500)
