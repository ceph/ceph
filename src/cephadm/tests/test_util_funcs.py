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
