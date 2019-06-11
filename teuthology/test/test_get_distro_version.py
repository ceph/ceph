from teuthology.misc import get_distro_version


class Mock:
    pass


class TestGetDistroVersion(object):

    def setup(self):
        self.fake_ctx = Mock()
        self.fake_ctx.config = {}
        self.fake_ctx_noarg = Mock()
        self.fake_ctx_noarg.config = {}
        self.fake_ctx_noarg.os_version = None
        self.fake_ctx.os_type = None
        self.fake_ctx_noarg.os_type = None

    def test_default_distro_version(self):
        # Default distro is ubuntu, default version of ubuntu is 16.04
        self.fake_ctx.os_version = None
        distroversion = get_distro_version(self.fake_ctx)
        assert distroversion == '18.04'

    def test_argument_version(self):
        self.fake_ctx.os_version = '13.04'
        distroversion = get_distro_version(self.fake_ctx)
        assert distroversion == '13.04'

    def test_teuth_config_version(self):
        #Argument takes precidence.
        self.fake_ctx.os_version = '13.04'
        self.fake_ctx.config = {'os_version': '13.10'}
        distroversion = get_distro_version(self.fake_ctx)
        assert distroversion == '13.04'

    def test_teuth_config_noarg_version(self):
        self.fake_ctx_noarg.config = {'os_version': '13.04'}
        distroversion = get_distro_version(self.fake_ctx_noarg)
        assert distroversion == '13.04'

    def test_no_teuth_config(self):
        self.fake_ctx = Mock()
        self.fake_ctx.os_type = None
        self.fake_ctx.os_version = '13.04'
        distroversion = get_distro_version(self.fake_ctx)
        assert distroversion == '13.04'
