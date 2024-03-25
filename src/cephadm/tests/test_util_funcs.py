# Tests for various assorted utility functions found within cephadm
#
from unittest import mock

import functools
import io
import os
import sys

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
                assert c == mock.call(mock.ANY, 0, 0)
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
                assert c == mock.call(mock.ANY, 0, 0)
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
                assert c == mock.call(mock.ANY, 0, 0)
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
    assert _chown.mock_calls[0] == mock.call(str(d1), 500, 500)
    assert _chown.mock_calls[1] == mock.call(str(d2), 500, 500)
    assert _chown.mock_calls[2] == mock.call(str(f1), 500, 500)


class TestFindExecutable:
    def test_standard_exe(self):
        # pretty much every system will have `true` on the path. It's a safe choice
        # for the first assertion
        exe = _cephadm.find_executable("true")
        assert exe.endswith("true")

    def test_custom_path(self, tmp_path):
        foo_sh = tmp_path / "foo.sh"
        with open(foo_sh, "w") as fh:
            fh.write("#!/bin/sh\n")
            fh.write("echo foo\n")
        foo_sh.chmod(0o755)

        exe = _cephadm.find_executable(foo_sh)
        assert str(exe) == str(foo_sh)

    def test_no_path(self, monkeypatch):
        monkeypatch.delenv("PATH")
        exe = _cephadm.find_executable("true")
        assert exe.endswith("true")

    def test_no_path_no_confstr(self, monkeypatch):
        def _fail(_):
            raise ValueError("fail")

        monkeypatch.delenv("PATH")
        monkeypatch.setattr("os.confstr", _fail)
        exe = _cephadm.find_executable("true")
        assert exe.endswith("true")

    def test_unset_path(self):
        exe = _cephadm.find_executable("true", path="")
        assert exe is None

    def test_no_such_exe(self):
        exe = _cephadm.find_executable("foo_bar-baz.noway")
        assert exe is None


def test_find_program():
    exe = _cephadm.find_program("true")
    assert exe.endswith("true")

    with pytest.raises(ValueError):
        _cephadm.find_program("foo_bar-baz.noway")


def _mk_fake_call(enabled, active):
    def _fake_call(ctx, cmd, **kwargs):
        if "is-enabled" in cmd:
            if isinstance(enabled, Exception):
                raise enabled
            return enabled
        if "is-active" in cmd:
            if isinstance(active, Exception):
                raise active
            return active
        raise ValueError("should not get here")

    return _fake_call


@pytest.mark.parametrize(
    "enabled_out, active_out, expected",
    [
        (
            # ok, all is well
            ("", "", 0),
            ("active", "", 0),
            (True, "running", True),
        ),
        (
            # disabled, unknown if active
            ("disabled", "", 1),
            ("", "", 0),
            (False, "unknown", True),
        ),
        (
            # is-enabled error (not disabled, unknown if active
            ("bleh", "", 1),
            ("", "", 0),
            (False, "unknown", False),
        ),
        (
            # is-enabled ok, inactive is stopped
            ("", "", 0),
            ("inactive", "", 0),
            (True, "stopped", True),
        ),
        (
            # is-enabled ok, failed is error
            ("", "", 0),
            ("failed", "", 0),
            (True, "error", True),
        ),
        (
            # is-enabled ok, auto-restart is error
            ("", "", 0),
            ("auto-restart", "", 0),
            (True, "error", True),
        ),
        (
            # error exec'ing is-enabled cmd
            ValueError("bonk"),
            ("active", "", 0),
            (False, "running", False),
        ),
        (
            # error exec'ing is-enabled cmd
            ("", "", 0),
            ValueError("blat"),
            (True, "unknown", True),
        ),
    ],
)
def test_check_unit(enabled_out, active_out, expected):
    with with_cephadm_ctx([]) as ctx:
        with mock.patch('cephadmlib.systemd.call') as _call:
            _call.side_effect = _mk_fake_call(
                enabled=enabled_out,
                active=active_out,
            )
            enabled, state, installed = _cephadm.check_unit(ctx, "foobar")
    assert (enabled, state, installed) == expected


class FakeEnabler:
    def __init__(self, should_be_called):
        self._should_be_called = should_be_called
        self._services = []

    def enable_service(self, service):
        self._services.append(service)

    def check_expected(self):
        if not self._should_be_called:
            assert not self._services
            return
        # there are currently seven chron/chrony type services that
        # cephadm looks for. Make sure it probed for each of them
        # or more in case someone adds to the list.
        assert len(self._services) >= 7
        assert "chrony.service" in self._services
        assert "ntp.service" in self._services


@pytest.mark.parametrize(
    "call_fn, enabler, expected",
    [
        # Test that time sync services are not enabled
        (
            _mk_fake_call(
                enabled=("", "", 1),
                active=("", "", 1),
            ),
            None,
            False,
        ),
        # Test that time sync service is enabled
        (
            _mk_fake_call(
                enabled=("", "", 0),
                active=("active", "", 0),
            ),
            None,
            True,
        ),
        # Test that time sync is not enabled, and try to enable them.
        # This one needs to be not running, but installed in order to
        # call the enabler. It should call the enabler with every known
        # service name.
        (
            _mk_fake_call(
                enabled=("disabled", "", 1),
                active=("", "", 1),
            ),
            FakeEnabler(True),
            False,
        ),
        # Test that time sync is enabled, with an enabler passed which
        # will check that the enabler was never called.
        (
            _mk_fake_call(
                enabled=("", "", 0),
                active=("active", "", 0),
            ),
            FakeEnabler(False),
            True,
        ),
    ],
)
def test_check_time_sync(call_fn, enabler, expected):
    """The check_time_sync call actually checks if a time synchronization service
    is enabled. It is also the only consumer of check_units.
    """
    with with_cephadm_ctx([]) as ctx:
        with mock.patch('cephadmlib.systemd.call') as _call:
            _call.side_effect = call_fn
            result = _cephadm.check_time_sync(ctx, enabler=enabler)
            assert result == expected
            if enabler is not None:
                enabler.check_expected()


@pytest.mark.parametrize(
    "content, expected",
    [
        (
            """#JUNK
            FOO=1
            """,
            (None, None, None),
        ),
        (
            """# A sample from a real centos system
NAME="CentOS Stream"
VERSION="8"
ID="centos"
ID_LIKE="rhel fedora"
VERSION_ID="8"
PLATFORM_ID="platform:el8"
PRETTY_NAME="CentOS Stream 8"
ANSI_COLOR="0;31"
CPE_NAME="cpe:/o:centos:centos:8"
HOME_URL="https://centos.org/"
BUG_REPORT_URL="https://bugzilla.redhat.com/"
REDHAT_SUPPORT_PRODUCT="Red Hat Enterprise Linux 8"
REDHAT_SUPPORT_PRODUCT_VERSION="CentOS Stream"
            """,
            ("centos", "8", None),
        ),
        (
            """# Minimal but complete, made up vals
ID="hpec"
VERSION_ID="33"
VERSION_CODENAME="hpec nimda"
            """,
            ("hpec", "33", "hpec nimda"),
        ),
        (
            """# Minimal but complete, no quotes
ID=hpec
VERSION_ID=33
VERSION_CODENAME=hpec nimda
            """,
            ("hpec", "33", "hpec nimda"),
        ),
    ],
)
def test_get_distro(monkeypatch, content, expected):
    from cephadmlib.packagers import get_distro

    def _fake_open(*args, **kwargs):
        return io.StringIO(content)

    monkeypatch.setattr("builtins.open", _fake_open)
    assert get_distro() == expected


class FakeContext:
    """FakeContext is a minimal type for passing as a ctx, when
    with_cephadm_ctx is not appropriate (it enables too many mocks, etc).
    """

    timeout = 30


def _has_non_zero_exit(clog):
    assert any("Non-zero exit" in ll for _, _, ll in clog.record_tuples)


def _has_values_somewhere(clog, values, non_zero=True):
    if non_zero:
        _has_non_zero_exit(clog)
    for value in values:
        assert any(value in ll for _, _, ll in clog.record_tuples)


@pytest.mark.parametrize(
    "pyline, expected, call_kwargs, log_check",
    [
        pytest.param(
            "import time; time.sleep(0.1)",
            ("", "", 0),
            {},
            None,
            id="brief-sleep",
        ),
        pytest.param(
            "import sys; sys.exit(2)",
            ("", "", 2),
            {},
            _has_non_zero_exit,
            id="exit-non-zero",
        ),
        pytest.param(
            "import sys; sys.exit(0)",
            ("", "", 0),
            {"desc": "success"},
            None,
            id="success-with-desc",
        ),
        pytest.param(
            "print('foo'); print('bar')",
            ("foo\nbar\n", "", 0),
            {"desc": "stdout"},
            None,
            id="stdout-print",
        ),
        pytest.param(
            "import sys; sys.stderr.write('la\\nla\\nla\\n')",
            ("", "la\nla\nla\n", 0),
            {"desc": "stderr"},
            None,
            id="stderr-print",
        ),
        pytest.param(
            "for i in range(501): print(i, flush=True)",
            lambda r: r[2] == 0 and r[1] == "" and "500" in r[0].splitlines(),
            {},
            None,
            id="stdout-long",
        ),
        pytest.param(
            "for i in range(1000000): print(i, flush=True)",
            lambda r: r[2] == 0
            and r[1] == ""
            and len(r[0].splitlines()) == 1000000,
            {},
            None,
            id="stdout-very-long",
        ),
        pytest.param(
            "import sys; sys.stderr.write('pow\\noof\\nouch\\n'); sys.exit(1)",
            ("", "pow\noof\nouch\n", 1),
            {"desc": "stderr"},
            functools.partial(
                _has_values_somewhere,
                values=["pow", "oof", "ouch"],
                non_zero=True,
            ),
            id="stderr-logged-non-zero",
        ),
        pytest.param(
            "import time; time.sleep(4)",
            ("", "", 124),
            {"timeout": 1},
            None,
            id="long-sleep",
        ),
        pytest.param(
            "import time\nfor i in range(100):\n\tprint(i, flush=True); time.sleep(0.01)",
            ("", "", 124),
            {"timeout": 0.5},
            None,
            id="slow-print-timeout",
        ),
        # Commands that time out collect no logs, return empty std{out,err} strings
    ],
)
def test_call(caplog, monkeypatch, pyline, expected, call_kwargs, log_check):
    import logging

    caplog.set_level(logging.INFO)
    monkeypatch.setattr("cephadm.logger", logging.getLogger())
    ctx = FakeContext()
    result = _cephadm.call(ctx, [sys.executable, "-c", pyline], **call_kwargs)
    if callable(expected):
        assert expected(result)
    else:
        assert result == expected
    if callable(log_check):
        log_check(caplog)


class TestWriteNew:
    def test_success(self, tmp_path):
        "Test the simple basic feature of writing a file."
        dest = tmp_path / "foo.txt"
        with _cephadm.write_new(dest) as fh:
            fh.write("something\n")
            fh.write("something else\n")

        with open(dest, "r") as fh:
            assert fh.read() == "something\nsomething else\n"

    def test_write_ower_mode(self, tmp_path):
        "Test that the owner and perms options function."
        dest = tmp_path / "foo.txt"

        # if this is test run as non-root, we can't really change ownership
        uid = os.getuid()
        gid = os.getgid()

        with _cephadm.write_new(dest, owner=(uid, gid), perms=0o600) as fh:
            fh.write("xomething\n")
            fh.write("xomething else\n")

        with open(dest, "r") as fh:
            assert fh.read() == "xomething\nxomething else\n"
            sr = os.fstat(fh.fileno())
            assert sr.st_uid == uid
            assert sr.st_gid == gid
            assert (sr.st_mode & 0o777) == 0o600

    def test_encoding(self, tmp_path):
        "Test that the encoding option functions."
        dest = tmp_path / "foo.txt"
        msg = "\u2603\u26C5\n"
        with _cephadm.write_new(dest, encoding='utf-8') as fh:
            fh.write(msg)
        with open(dest, "rb") as fh:
            b1 = fh.read()
            assert b1.decode('utf-8') == msg

        dest = tmp_path / "foo2.txt"
        with _cephadm.write_new(dest, encoding='utf-16le') as fh:
            fh.write(msg)
        with open(dest, "rb") as fh:
            b2 = fh.read()
            assert b2.decode('utf-16le') == msg

        # the binary data should differ due to the different encodings
        assert b1 != b2

    def test_cleanup(self, tmp_path):
        "Test that an exception during write leaves no file behind."
        dest = tmp_path / "foo.txt"
        with pytest.raises(ValueError):
            with _cephadm.write_new(dest) as fh:
                fh.write("hello\n")
                raise ValueError("foo")
                fh.write("world\n")
        assert not dest.exists()
        assert not dest.with_name(dest.name+".new").exists()
        assert list(dest.parent.iterdir()) == []


class CompareContext1:
    cfg_data = {
        "name": "mane",
        "fsid": "foobar",
        "image": "fake.io/noway/nohow:gndn",
        "meta": {
            "fruit": "banana",
            "vegetable": "carrot",
        },
        "params": {
            "osd_fsid": "robble",
            "tcp_ports": [404, 9999],
        },
        "config_blobs": {
            "alpha": {"sloop": "John B"},
            "beta": {"forest": "birch"},
            "gamma": {"forest": "pine"},
        },
    }

    def check(self, ctx):
        assert ctx.name == 'mane'
        assert ctx.fsid == 'foobar'
        assert ctx.image == 'fake.io/noway/nohow:gndn'
        assert ctx.meta_properties == {"fruit": "banana", "vegetable": "carrot"}
        assert ctx.config_blobs == {
            "alpha": {"sloop": "John B"},
            "beta": {"forest": "birch"},
            "gamma": {"forest": "pine"},
        }
        assert ctx.osd_fsid == "robble"
        assert ctx.tcp_ports == [404, 9999]


class CompareContext2:
    cfg_data = {
        "name": "cc2",
        "fsid": "foobar",
        "meta": {
            "fruit": "banana",
            "vegetable": "carrot",
        },
        "params": {},
        "config_blobs": {
            "alpha": {"sloop": "John B"},
            "beta": {"forest": "birch"},
            "gamma": {"forest": "pine"},
        },
    }

    def check(self, ctx):
        assert ctx.name == 'cc2'
        assert ctx.fsid == 'foobar'
        assert ctx.image == 'quay.ceph.io/ceph-ci/ceph:main'
        assert ctx.meta_properties == {"fruit": "banana", "vegetable": "carrot"}
        assert ctx.config_blobs == {
            "alpha": {"sloop": "John B"},
            "beta": {"forest": "birch"},
            "gamma": {"forest": "pine"},
        }
        assert ctx.osd_fsid is None
        assert ctx.tcp_ports is None


@pytest.mark.parametrize(
    "cc",
    [
        CompareContext1(),
        CompareContext2(),
    ],
)
def test_apply_deploy_config_to_ctx(cc, monkeypatch):
    import logging

    monkeypatch.setattr("cephadm.logger", logging.getLogger())
    ctx = FakeContext()
    _cephadm.apply_deploy_config_to_ctx(cc.cfg_data, ctx)
    cc.check(ctx)


def test_daemon_sub_identity_from_sidecar_service():
    from cephadmlib.daemon_identity import DaemonSubIdentity

    dsi = DaemonSubIdentity(
        '244c9842-866b-11ee-80ad-3497f6318048', 'iscsi', 'rab.oof', 'tcmu'
    )
    service_name = dsi.sidecar_service_name
    assert (
        service_name
        == 'ceph-244c9842-866b-11ee-80ad-3497f6318048-sidecar@iscsi.rab.oof:tcmu.service'
    )
    d2, category = DaemonSubIdentity.from_service_name(service_name)
    assert category == 'sidecar'
    assert d2.fsid == '244c9842-866b-11ee-80ad-3497f6318048'
    assert d2.daemon_type == 'iscsi'
    assert d2.daemon_id == 'rab.oof'
    assert d2.subcomponent == 'tcmu'


def test_daemon_sub_identity_from_init_service():
    from cephadmlib.daemon_identity import DaemonIdentity, DaemonSubIdentity

    di = DaemonIdentity(
        '244c9842-866b-11ee-80ad-3497f6318048', 'putrats', 'wow',
    )
    service_name = di.init_service_name
    assert (
        service_name
        == 'ceph-244c9842-866b-11ee-80ad-3497f6318048-init@putrats.wow.service'
    )
    d2, category = DaemonSubIdentity.from_service_name(service_name)
    assert category == 'init'
    assert d2.fsid == '244c9842-866b-11ee-80ad-3497f6318048'
    assert d2.daemon_type == 'putrats'
    assert d2.daemon_id == 'wow'
    assert d2.subcomponent == 'init'


def test_daemon_sub_identity_from_service_invalid():
    from cephadmlib.daemon_identity import DaemonSubIdentity

    service_name = 'ceph-244c9842-866b-11ee-80ad-3497f6318048-morbo@iscsi.rab.oof.tcmu.service'
    with pytest.raises(ValueError):
        DaemonSubIdentity.from_service_name(service_name)

    service_name = 'ceph-244c9842-866b-11ee-80ad-3497f6318048@iscsi.rab.oof.service'
    with pytest.raises(ValueError):
        DaemonSubIdentity.from_service_name(service_name)

    service_name = 'ceph-244c9842-866b-11ee-80ad-3497f6318048-sidecar@foo.bar.baz:acolon:toomany.service'
    with pytest.raises(ValueError):
        DaemonSubIdentity.from_service_name(service_name)

    service_name = 'ceph-244c9842-866b-11ee-80ad-3497f6318048-init@foo.bar.baz:woops.service'
    with pytest.raises(ValueError):
        DaemonSubIdentity.from_service_name(service_name)

    service_name = 'random-task@elsewise.service'
    with pytest.raises(ValueError):
        DaemonSubIdentity.from_service_name(service_name)


def test_daemon_id_systemd_names():
    from cephadmlib.daemon_identity import DaemonIdentity

    di = DaemonIdentity(
        '244c9842-866b-11ee-80ad-3497f6318048', 'test', 'foo.bar'
    )
    assert (
        di.unit_name
        == 'ceph-244c9842-866b-11ee-80ad-3497f6318048@test.foo.bar'
    )
    assert (
        di.service_name
        == 'ceph-244c9842-866b-11ee-80ad-3497f6318048@test.foo.bar.service'
    )
    assert (
        di.init_service_name
        == 'ceph-244c9842-866b-11ee-80ad-3497f6318048-init@test.foo.bar.service'
    )


def test_daemon_sub_id_systemd_names():
    from cephadmlib.daemon_identity import DaemonSubIdentity

    dsi = DaemonSubIdentity(
        '244c9842-866b-11ee-80ad-3497f6318048', 'test', 'foo.bar', 'quux',
    )
    assert (
        dsi.sidecar_service_name
        == 'ceph-244c9842-866b-11ee-80ad-3497f6318048-sidecar@test.foo.bar:quux.service'
    )
    with pytest.raises(ValueError):
        dsi.service_name


@pytest.mark.parametrize(
    "args,new_arg,expected",
    [
        (['--foo=77'], '--bar', ['--foo=77', '--bar']),
        (['--foo=77'], '--foo=12', ['--foo=12']),
        (
            ['--foo=77', '--quux=later', '--range=2-5'],
            '--quux=now',
            ['--foo=77', '--range=2-5', '--quux=now'],
        ),
        (
            ['--foo=77', '--quux', 'later', '--range=2-5'],
            '--quux=now',
            ['--foo=77', '--range=2-5', '--quux=now'],
        ),
        (
            ['--foo=77', '--quux', 'later', '--range=2-5'],
            '--jiffy',
            ['--foo=77', '--quux', 'later', '--range=2-5', '--jiffy'],
        ),
        (
            ['--foo=77', '--quux=buff', '--range=2-5'],
            '--quux',
            ['--foo=77', '--range=2-5', '--quux'],
        ),
    ],
)
def test_replace_container_args(args, new_arg, expected):
    from cephadmlib.container_types import _replace_container_arg

    _args = list(args)  # preserve the input so test input is not mutated
    _replace_container_arg(_args, new_arg)
    assert _args == expected



def test_enable_shared_namespaces():
    from cephadmlib.container_types import enable_shared_namespaces, Namespace

    args = []
    enable_shared_namespaces(args, 'c001d00d', {Namespace.ipc})
    assert args == ['--ipc=container:c001d00d']

    enable_shared_namespaces(
        args, 'c001d00d', [Namespace.uts, Namespace.network]
    )
    assert args == [
        '--ipc=container:c001d00d',
        '--uts=container:c001d00d',
        '--network=container:c001d00d',
    ]

    enable_shared_namespaces(
        args, 'badd33d5', [Namespace.network]
    )
    assert args == [
        '--ipc=container:c001d00d',
        '--uts=container:c001d00d',
        '--network=container:badd33d5',
    ]
