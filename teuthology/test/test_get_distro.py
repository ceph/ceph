from .. import misc as teuthology

class Mock: pass


class TestGetDistro(object):

    def setup(self):
        self.fake_ctx = Mock()
        self.fake_ctx.config = {}
        # os_type in ctx will always default to None
        self.fake_ctx.os_type = None

    def test_default_distro(self):
        distro = teuthology.get_distro(self.fake_ctx)
        assert distro == 'ubuntu'

    def test_argument(self):
        self.fake_ctx.os_type = 'centos'
        distro = teuthology.get_distro(self.fake_ctx)
        assert distro == 'centos'

    def test_teuth_config(self):
        self.fake_ctx.config = {'os_type': 'fedora'}
        distro = teuthology.get_distro(self.fake_ctx)
        assert distro == 'fedora'

    def test_teuth_config_downburst(self):
        self.fake_ctx.config = {'downburst' : {'distro': 'sles'}}
        distro = teuthology.get_distro(self.fake_ctx)
        assert distro == 'sles'

    def test_no_config_or_os_type(self):
        self.fake_ctx = Mock()
        distro = teuthology.get_distro(self.fake_ctx)
        assert distro == 'ubuntu'
