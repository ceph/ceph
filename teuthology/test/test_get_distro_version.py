from .. import misc as teuthology

class Mock: pass

class TestGetDistroVersion(object):

    def setup(self):
        self.fake_ctx = Mock()
        self.fake_ctx.config = {}
        self.fake_ctx.os_version = '13.04'
        self.fake_ctx_noarg = Mock()
        self.fake_ctx_noarg.config = {}

    def test_default_distro_version(self):
        distroversion = teuthology.get_distro_version(self.fake_ctx)
        assert distroversion == '13.04'

    def test_argument_version(self):
        self.fake_ctx.os_version = '13.04'
        distroversion = teuthology.get_distro_version(self.fake_ctx)
        assert distroversion == '13.04'

    def test_teuth_config_version(self):
        self.fake_ctx.config = {'os_version': '13.04'}
        distroversion = teuthology.get_distro_version(self.fake_ctx)
        assert distroversion == '13.04'

    def test_teuth_config_downburst_version(self):
        self.fake_ctx.config = {'downburst' : {'distroversion': '13.04'}}
        distroversion = teuthology.get_distro_version(self.fake_ctx)
        assert distroversion == '13.04'

    def test_default_distro_noarg_version(self):
        distroversion = teuthology.get_distro_version(self.fake_ctx_noarg)
        #Default distro is ubuntu, default version of ubuntu is 012.04
        assert distroversion == '12.04'

    def test_teuth_config_noarg_version(self):
        self.fake_ctx_noarg.config = {'os_version': '13.04'}
        distroversion = teuthology.get_distro_version(self.fake_ctx_noarg)
        assert distroversion == '13.04'

    def test_teuth_config_downburst_noarg_version(self):
        self.fake_ctx_noarg.config = {'downburst' : {'distroversion': '13.04'}}
        distroversion = teuthology.get_distro_version(self.fake_ctx_noarg)
        assert distroversion == '13.04'
