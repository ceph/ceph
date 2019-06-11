import pytest

from teuthology import config


class TestYamlConfig(object):
    def setup(self):
        self.test_class = config.YamlConfig

    def test_set_multiple(self):
        conf_obj = self.test_class()
        conf_obj.foo = 'foo'
        conf_obj.bar = 'bar'
        assert conf_obj.foo == 'foo'
        assert conf_obj.bar == 'bar'
        assert conf_obj.to_dict()['foo'] == 'foo'

    def test_from_dict(self):
        in_dict = dict(foo='bar')
        conf_obj = self.test_class.from_dict(in_dict)
        assert conf_obj.foo == 'bar'

    def test_contains(self):
        in_dict = dict(foo='bar')
        conf_obj = self.test_class.from_dict(in_dict)
        conf_obj.bar = "foo"
        assert "bar" in conf_obj
        assert "foo" in conf_obj
        assert "baz" not in conf_obj

    def test_to_dict(self):
        in_dict = dict(foo='bar')
        conf_obj = self.test_class.from_dict(in_dict)
        assert conf_obj.to_dict() == in_dict

    def test_from_str(self):
        in_str = "foo: bar"
        conf_obj = self.test_class.from_str(in_str)
        assert conf_obj.foo == 'bar'

    def test_to_str(self):
        in_str = "foo: bar"
        conf_obj = self.test_class.from_str(in_str)
        assert conf_obj.to_str() == in_str

    def test_update(self):
        conf_obj = self.test_class(dict())
        conf_obj.foo = 'foo'
        conf_obj.bar = 'bar'
        conf_obj.update(dict(bar='baz'))
        assert conf_obj.foo == 'foo'
        assert conf_obj.bar == 'baz'

    def test_delattr(self):
        conf_obj = self.test_class()
        conf_obj.foo = 'bar'
        assert conf_obj.foo == 'bar'
        del conf_obj.foo
        assert conf_obj.foo is None

    def test_assignment(self):
        conf_obj = self.test_class()
        conf_obj["foo"] = "bar"
        assert conf_obj["foo"] == "bar"
        assert conf_obj.foo == "bar"

    def test_used_with_update(self):
        d = dict()
        conf_obj = self.test_class.from_dict({"foo": "bar"})
        d.update(conf_obj)
        assert d["foo"] == "bar"

    def test_get(self):
        conf_obj = self.test_class()
        assert conf_obj.get('foo') is None
        assert conf_obj.get('foo', 'bar') == 'bar'
        conf_obj.foo = 'baz'
        assert conf_obj.get('foo') == 'baz'


class TestTeuthologyConfig(TestYamlConfig):
    def setup(self):
        self.test_class = config.TeuthologyConfig

    def test_get_ceph_git_base_default(self):
        conf_obj = self.test_class()
        conf_obj.yaml_path = ''
        conf_obj.load()
        assert conf_obj.ceph_git_base_url == "https://github.com/ceph/"

    def test_set_ceph_git_base_via_private(self):
        conf_obj = self.test_class()
        conf_obj._conf['ceph_git_base_url'] = \
            "git://git.ceph.com/"
        assert conf_obj.ceph_git_base_url == "git://git.ceph.com/"

    def test_get_reserve_machines_default(self):
        conf_obj = self.test_class()
        conf_obj.yaml_path = ''
        conf_obj.load()
        assert conf_obj.reserve_machines == 5

    def test_set_reserve_machines_via_private(self):
        conf_obj = self.test_class()
        conf_obj._conf['reserve_machines'] = 2
        assert conf_obj.reserve_machines == 2

    def test_set_nonstandard(self):
        conf_obj = self.test_class()
        conf_obj.something = 'something else'
        assert conf_obj.something == 'something else'


class TestJobConfig(TestYamlConfig):
    def setup(self):
        self.test_class = config.JobConfig


class TestFakeNamespace(TestYamlConfig):
    def setup(self):
        self.test_class = config.FakeNamespace

    def test_docopt_dict(self):
        """
        Tests if a dict in the format that docopt returns can
        be parsed correctly.
        """
        d = {
            "--verbose": True,
            "--an-option": "some_option",
            "<an_arg>": "the_arg",
            "something": "some_thing",
        }
        conf_obj = self.test_class(d)
        assert conf_obj.verbose
        assert conf_obj.an_option == "some_option"
        assert conf_obj.an_arg == "the_arg"
        assert conf_obj.something == "some_thing"

    def test_config(self):
        """
        Tests that a teuthology_config property is automatically added
        to the conf_obj
        """
        conf_obj = self.test_class(dict(foo="bar"))
        assert conf_obj["foo"] == "bar"
        assert conf_obj.foo == "bar"
        assert conf_obj.teuthology_config.get("fake key") is None

    def test_getattr(self):
        conf_obj = self.test_class.from_dict({"foo": "bar"})
        result = getattr(conf_obj, "not_there", "default")
        assert result == "default"
        result = getattr(conf_obj, "foo")
        assert result == "bar"

    def test_none(self):
        conf_obj = self.test_class.from_dict(dict(null=None))
        assert conf_obj.null is None

    def test_delattr(self):
        conf_obj = self.test_class()
        conf_obj.foo = 'bar'
        assert conf_obj.foo == 'bar'
        del conf_obj.foo
        with pytest.raises(AttributeError):
            conf_obj.foo

    def test_to_str(self):
        in_str = "foo: bar"
        conf_obj = self.test_class.from_str(in_str)
        assert conf_obj.to_str() == "{'foo': 'bar'}"

    def test_multiple_access(self):
        """
        Test that config.config and FakeNamespace.teuthology_config reflect
        each others' modifications
        """
        in_str = "foo: bar"
        conf_obj = self.test_class.from_str(in_str)
        assert config.config.get('test_key_1') is None
        assert conf_obj.teuthology_config.get('test_key_1') is None
        config.config.test_key_1 = 'test value'
        assert conf_obj.teuthology_config['test_key_1'] == 'test value'

        assert config.config.get('test_key_2') is None
        assert conf_obj.teuthology_config.get('test_key_2') is None
        conf_obj.teuthology_config['test_key_2'] = 'test value'
        assert config.config['test_key_2'] == 'test value'
