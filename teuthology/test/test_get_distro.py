from teuthology.misc import get_distro


class Mock:
    pass


class TestGetDistro(object):

    def setup(self):
        self.fake_ctx = Mock()
        self.fake_ctx.config = {}
        # os_type in ctx will always default to None
        self.fake_ctx.os_type = None

    def test_default_distro(self):
        distro = get_distro(self.fake_ctx)
        assert distro == 'ubuntu'

    def test_argument(self):
        # we don't want fake_ctx to have a config
        self.fake_ctx = Mock()
        self.fake_ctx.os_type = 'centos'
        distro = get_distro(self.fake_ctx)
        assert distro == 'centos'

    def test_teuth_config(self):
        self.fake_ctx.config = {'os_type': 'fedora'}
        distro = get_distro(self.fake_ctx)
        assert distro == 'fedora'

    def test_argument_takes_precedence(self):
        self.fake_ctx.config = {'os_type': 'fedora'}
        self.fake_ctx.os_type = "centos"
        distro = get_distro(self.fake_ctx)
        assert distro == 'centos'

    def test_no_config_or_os_type(self):
        self.fake_ctx = Mock()
        self.fake_ctx.os_type = None
        distro = get_distro(self.fake_ctx)
        assert distro == 'ubuntu'

    def test_config_os_type_is_none(self):
        self.fake_ctx.config["os_type"] = None
        distro = get_distro(self.fake_ctx)
        assert distro == 'ubuntu'
