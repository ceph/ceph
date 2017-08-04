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
