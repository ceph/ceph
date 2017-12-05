import os
import pytest
from ceph_volume import main


class TestVolume(object):

    def test_main_spits_help_with_no_arguments(self, capsys):
        main.Volume(argv=[])
        stdout, stderr = capsys.readouterr()
        assert 'Log Path' in stdout

    def test_warn_about_using_help_for_full_options(self, capsys):
        main.Volume(argv=[])
        stdout, stderr = capsys.readouterr()
        assert 'See "ceph-volume --help" for full list' in stdout

    def test_environ_vars_show_up(self, capsys):
        os.environ['CEPH_CONF'] = '/opt/ceph.conf'
        main.Volume(argv=[])
        stdout, stderr = capsys.readouterr()
        assert 'CEPH_CONF' in stdout
        assert '/opt/ceph.conf' in stdout

    def test_flags_are_parsed_with_help(self, capsys):
        with pytest.raises(SystemExit):
            main.Volume(argv=['ceph-volume', '--help'])
        stdout, stderr = capsys.readouterr()
        assert '--cluster' in stdout
        assert '--log-path' in stdout

    def test_log_ignoring_missing_ceph_conf(self, caplog):
        with pytest.raises(SystemExit) as error:
            main.Volume(argv=['ceph-volume', '--cluster', 'barnacle', 'lvm', '--help'])
        # make sure we aren't causing an actual error
        assert error.value.code == 0
        log = caplog.records[0]
        assert log.message == 'ignoring inability to load ceph.conf'
        assert log.levelname == 'ERROR'
