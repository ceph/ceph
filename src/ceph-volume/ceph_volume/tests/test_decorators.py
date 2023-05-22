import os
import pytest
from ceph_volume import exceptions, decorators, terminal


class TestNeedsRoot(object):

    def test_is_root(self, monkeypatch):
        def func():
            return True
        monkeypatch.setattr(decorators.os, 'getuid', lambda: 0)
        assert decorators.needs_root(func)() is True

    def test_is_not_root_env_var_skip_needs_root(self, monkeypatch):
        def func():
            return True
        monkeypatch.setattr(decorators.os, 'getuid', lambda: 123)
        monkeypatch.setattr(decorators.os, 'environ', {'CEPH_VOLUME_SKIP_NEEDS_ROOT': '1'})
        assert decorators.needs_root(func)() is True

    def test_is_not_root(self, monkeypatch):
        def func():
            return True # pragma: no cover
        monkeypatch.setattr(decorators.os, 'getuid', lambda: 20)
        with pytest.raises(exceptions.SuperUserError) as error:
            decorators.needs_root(func)()

        msg = 'This command needs to be executed with sudo or as root'
        assert str(error.value) == msg


class TestExceptionMessage(object):

    def test_has_str_method(self):
        result = decorators.make_exception_message(RuntimeError('an error'))
        expected = "%s %s\n" % (terminal.red_arrow, 'RuntimeError: an error')
        assert result == expected

    def test_has_no_str_method(self):
        class Error(Exception):
            pass
        result = decorators.make_exception_message(Error())
        expected = "%s %s\n" % (terminal.red_arrow, 'Error')
        assert result == expected


class TestCatches(object):

    def teardown_method(self):
        try:
            del(os.environ['CEPH_VOLUME_DEBUG'])
        except KeyError:
            pass

    def test_ceph_volume_debug_enabled(self):
        os.environ['CEPH_VOLUME_DEBUG'] = '1'
        @decorators.catches() # noqa
        def func():
            raise RuntimeError()
        with pytest.raises(RuntimeError):
            func()

    def test_ceph_volume_debug_disabled_no_exit(self, capsys):
        @decorators.catches(exit=False)
        def func():
            raise RuntimeError()
        func()
        stdout, stderr = capsys.readouterr()
        assert 'RuntimeError\n' in stderr

    def test_ceph_volume_debug_exits(self, capsys):
        @decorators.catches()
        def func():
            raise RuntimeError()
        with pytest.raises(SystemExit):
            func()
        stdout, stderr = capsys.readouterr()
        assert 'RuntimeError\n' in stderr
