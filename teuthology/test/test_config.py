from .. import config


class TestConfig(object):
    def setup(self):
        pass

    def teardown(self):
        pass

    def test_get_and_set(self):
        conf_obj = config.Config()
        conf_obj.teuthology_yaml = ''
        conf_obj.load_files()
        assert conf_obj.ceph_git_base_url == "https://github.com/ceph/"
        conf_obj._Config__conf['ceph_git_base_url'] = "git://ceph.com/"
        assert conf_obj.ceph_git_base_url == "git://ceph.com/"
