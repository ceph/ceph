from .. import config


class TestYamlConfig(object):
    def test_set_multiple(self):
        conf_obj = config.YamlConfig()
        conf_obj.foo = 'foo'
        conf_obj.bar = 'bar'
        assert conf_obj.foo == 'foo'
        assert conf_obj.bar == 'bar'
        assert conf_obj.to_dict()['foo'] == 'foo'

    def test_from_dict(self):
        in_dict = dict(foo='bar')
        conf_obj = config.YamlConfig.from_dict(in_dict)
        assert conf_obj.foo == 'bar'

    def test_to_dict(self):
        in_dict = dict(foo='bar')
        conf_obj = config.YamlConfig.from_dict(in_dict)
        assert conf_obj.to_dict() == in_dict

    def test_from_str(self):
        in_str = "foo: bar"
        conf_obj = config.YamlConfig.from_str(in_str)
        assert conf_obj.foo == 'bar'

    def test_to_str(self):
        in_str = "foo: bar"
        conf_obj = config.YamlConfig.from_str(in_str)
        assert conf_obj.to_str() == in_str

    def test_update(self):
        conf_obj = config.YamlConfig()
        conf_obj.foo = 'foo'
        conf_obj.bar = 'bar'
        conf_obj.update(dict(bar='baz'))
        assert conf_obj.to_dict() == dict(foo='foo', bar='baz')

    def test_defaults(self):
        conf_obj = config.YamlConfig()
        # Save a copy of the original defaults so we can restore them later.
        # Not doing so would break other tests.
        old_defaults = dict(conf_obj.defaults)
        conf_obj.defaults['foo'] = 'bar'
        assert conf_obj.foo == 'bar'
        # restore defaults
        conf_obj.__class__.defaults = old_defaults

    def test_delattr(self):
        conf_obj = config.YamlConfig()
        conf_obj.foo = 'bar'
        assert conf_obj.foo == 'bar'
        del conf_obj.foo
        assert conf_obj.foo is None


class TestTeuthologyConfig(object):
    def test_defaults(self):
        conf_obj = config.TeuthologyConfig()
        conf_obj.defaults['archive_base'] = 'bar'
        assert conf_obj.archive_base == 'bar'

    def test_get_ceph_git_base_default(self):
        conf_obj = config.TeuthologyConfig()
        conf_obj.yaml_path = ''
        conf_obj.load()
        assert conf_obj.ceph_git_base_url == "https://github.com/ceph/"

    def test_set_ceph_git_base_via_private(self):
        conf_obj = config.TeuthologyConfig()
        conf_obj._YamlConfig__conf['ceph_git_base_url'] = \
            "git://ceph.com/"
        assert conf_obj.ceph_git_base_url == "git://ceph.com/"

    def test_set_nonstandard(self):
        conf_obj = config.TeuthologyConfig()
        conf_obj.something = 'something else'
        assert conf_obj.something == 'something else'


class TestJobConfig(object):
    def test_to_str(self):
        in_str = "foo: bar"
        conf_obj = config.JobConfig.from_str(in_str)
        assert conf_obj.to_str() == in_str
