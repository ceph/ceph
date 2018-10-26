import os
import sys
import pytest
from ceph_volume import main

try:
    from unittest import mock
except ImportError:
    import mock


class TestVolume(object):

    @mock.patch.object(sys, 'argv', [])
    def test_main_spits_help_with_no_arguments(self, capsys):
        main.Volume()
        stdout, stderr = capsys.readouterr()
        assert 'Log Path' in stdout

    @mock.patch.object(sys, 'argv', [])
    def test_warn_about_using_help_for_full_options(self, capsys):
        main.Volume()
        stdout, stderr = capsys.readouterr()
        assert 'See "ceph-volume --help" for full list' in stdout

    @mock.patch.object(sys, 'argv', [])
    def test_environ_vars_show_up(self, capsys):
        os.environ['CEPH_CONF'] = '/opt/ceph.conf'
        main.Volume()
        stdout, stderr = capsys.readouterr()
        assert 'CEPH_CONF' in stdout
        assert '/opt/ceph.conf' in stdout

    @mock.patch.object(sys, 'argv', ['ceph-volume', '--help'])
    def test_flags_are_parsed_with_help(self, capsys):
        with pytest.raises(SystemExit):
            main.Volume()
        stdout, stderr = capsys.readouterr()
        assert '--cluster' in stdout
        assert '--log-path' in stdout

    @mock.patch.object(sys, 'argv', ['ceph-volume', '--cluster', 'barnacle', 'lvm', '--help'])
    def test_log_ignoring_missing_ceph_conf(self, caplog):
        with pytest.raises(SystemExit) as error:
            main.Volume()
        # make sure we aren't causing an actual error
        assert error.value.code == 0
        log = caplog.records[0]
        assert log.message == 'Running command: ceph-volume --cluster barnacle lvm --help'
        assert log.levelname == 'INFO'
        log = caplog.records[1]
        assert log.message == 'ignoring inability to load ceph.conf'
        assert log.levelname == 'ERROR'
