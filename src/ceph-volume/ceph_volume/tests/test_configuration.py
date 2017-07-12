import os
try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO
from textwrap import dedent
import pytest
from ceph_volume import configuration, exceptions


class TestConf(object):

    def setup(self):
        self.conf_file = StringIO(dedent("""
        [foo]
        default = 0
        """))

    def test_get_non_existing_list(self):
        cfg = configuration.Conf()
        cfg.readfp(self.conf_file)
        assert cfg.get_list('global', 'key') == []

    def test_get_non_existing_list_get_default(self):
        cfg = configuration.Conf()
        cfg.readfp(self.conf_file)
        assert cfg.get_list('global', 'key', ['a']) == ['a']

    def test_get_rid_of_comments(self):
        cfg = configuration.Conf()
        conf_file = StringIO(dedent("""
        [foo]
        default = 0  # this is a comment
        """))

        cfg.readfp(conf_file)
        assert cfg.get_list('foo', 'default') == ['0']

    def test_gets_split_on_commas(self):
        cfg = configuration.Conf()
        conf_file = StringIO(dedent("""
        [foo]
        default = 0,1,2,3  # this is a comment
        """))

        cfg.readfp(conf_file)
        assert cfg.get_list('foo', 'default') == ['0', '1', '2', '3']

    def test_spaces_and_tabs_are_ignored(self):
        cfg = configuration.Conf()
        conf_file = StringIO(dedent("""
        [foo]
        default = 0,        1,  2 ,3  # this is a comment
        """))

        cfg.readfp(conf_file)
        assert cfg.get_list('foo', 'default') == ['0', '1', '2', '3']


class TestLoad(object):

    def test_path_does_not_exist(self):
        with pytest.raises(exceptions.ConfigurationError):
            configuration.load('/path/does/not/exist/ceph.con')

    def test_unable_to_read_configuration(self, tmpdir, capsys):
        ceph_conf = os.path.join(str(tmpdir), 'ceph.conf')
        with open(ceph_conf, 'w') as config:
            config.write(']broken] config\n[[')
        configuration.load(ceph_conf)
        stdout, stderr = capsys.readouterr()
        assert 'Unable to read configuration file' in stdout
        assert 'File contains no section headers' in stdout
