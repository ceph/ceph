import os
try:
    from cStringIO import StringIO
except ImportError: # pragma: no cover
    from io import StringIO # pragma: no cover
from textwrap import dedent
import pytest
from ceph_volume import configuration, exceptions

tabbed_conf = """
[global]
            default = 0
            other_h = 1 # comment
            other_c = 1 ; comment
            colon = ;
            hash = #
"""


class TestConf(object):

    def setup(self):
        self.conf_file = StringIO(dedent("""
        [foo]
        default = 0
        """))

    def test_get_non_existing_list(self):
        cfg = configuration.Conf()
        cfg.is_valid = lambda: True
        cfg.readfp(self.conf_file)
        assert cfg.get_list('global', 'key') == []

    def test_get_non_existing_list_get_default(self):
        cfg = configuration.Conf()
        cfg.is_valid = lambda: True
        cfg.readfp(self.conf_file)
        assert cfg.get_list('global', 'key', ['a']) == ['a']

    def test_get_rid_of_comments(self):
        cfg = configuration.Conf()
        cfg.is_valid = lambda: True
        conf_file = StringIO(dedent("""
        [foo]
        default = 0  # this is a comment
        """))

        cfg.readfp(conf_file)
        assert cfg.get_list('foo', 'default') == ['0']

    def test_gets_split_on_commas(self):
        cfg = configuration.Conf()
        cfg.is_valid = lambda: True
        conf_file = StringIO(dedent("""
        [foo]
        default = 0,1,2,3  # this is a comment
        """))

        cfg.readfp(conf_file)
        assert cfg.get_list('foo', 'default') == ['0', '1', '2', '3']

    def test_spaces_and_tabs_are_ignored(self):
        cfg = configuration.Conf()
        cfg.is_valid = lambda: True
        conf_file = StringIO(dedent("""
        [foo]
        default = 0,        1,  2 ,3  # this is a comment
        """))

        cfg.readfp(conf_file)
        assert cfg.get_list('foo', 'default') == ['0', '1', '2', '3']


class TestLoad(object):

    def test_load_from_path(self, tmpdir):
        conf_path = os.path.join(str(tmpdir), 'ceph.conf')
        with open(conf_path, 'w') as conf:
            conf.write(tabbed_conf)
        result = configuration.load(conf_path)
        assert result.get('global', 'default') == '0'

    def test_load_with_colon_comments(self, tmpdir):
        conf_path = os.path.join(str(tmpdir), 'ceph.conf')
        with open(conf_path, 'w') as conf:
            conf.write(tabbed_conf)
        result = configuration.load(conf_path)
        assert result.get('global', 'other_c') == '1'

    def test_load_with_hash_comments(self, tmpdir):
        conf_path = os.path.join(str(tmpdir), 'ceph.conf')
        with open(conf_path, 'w') as conf:
            conf.write(tabbed_conf)
        result = configuration.load(conf_path)
        assert result.get('global', 'other_h') == '1'

    def test_path_does_not_exist(self):
        with pytest.raises(exceptions.ConfigurationError):
            conf = configuration.load('/path/does/not/exist/ceph.con')
            conf.is_valid()

    def test_unable_to_read_configuration(self, tmpdir, capsys):
        ceph_conf = os.path.join(str(tmpdir), 'ceph.conf')
        with open(ceph_conf, 'w') as config:
            config.write(']broken] config\n[[')
        with pytest.raises(RuntimeError):
            configuration.load(ceph_conf)
        stdout, stderr = capsys.readouterr()
        assert 'File contains no section headers' in stdout

    @pytest.mark.parametrize('commented', ['colon','hash'])
    def test_coment_as_a_value(self, tmpdir, commented):
        conf_path = os.path.join(str(tmpdir), 'ceph.conf')
        with open(conf_path, 'w') as conf:
            conf.write(tabbed_conf)
        result = configuration.load(conf_path)
        assert result.get('global', commented) == ''
