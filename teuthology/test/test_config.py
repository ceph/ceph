from .. import config


class TestConfig(object):
    def test_get_ceph_git_base_default(self):
        conf_obj = config.Config()
        conf_obj.teuthology_yaml = ''
        conf_obj.load_files()
        assert conf_obj.ceph_git_base_url == "https://github.com/ceph/"

    def test_set_ceph_git_base_via_private(self):
        conf_obj = config.Config()
        conf_obj._Config__conf['ceph_git_base_url'] = "git://ceph.com/"
        assert conf_obj.ceph_git_base_url == "git://ceph.com/"

    def test_set_nonstandard(self):
        conf_obj = config.Config()
        conf_obj.something = 'something else'
        assert conf_obj.something == 'something else'
