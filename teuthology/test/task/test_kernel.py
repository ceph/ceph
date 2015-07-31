from teuthology.config import FakeNamespace
from teuthology.orchestra.cluster import Cluster
from teuthology.orchestra.remote import Remote
from teuthology.task.kernel import (
    normalize_and_apply_overrides,
    CONFIG_DEFAULT,
    TIMEOUT_DEFAULT,
)

class TestKernelNormalizeAndApplyOverrides(object):

    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('remote1'), ['mon.a', 'client.0'])
        self.ctx.cluster.add(Remote('remote2'), ['osd.0', 'osd.1', 'osd.2'])
        self.ctx.cluster.add(Remote('remote3'), ['client.1'])

    def test_default(self):
        config = {}
        overrides = {}
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'mon.a': CONFIG_DEFAULT,
            'osd.0': CONFIG_DEFAULT,
            'osd.1': CONFIG_DEFAULT,
            'osd.2': CONFIG_DEFAULT,
            'client.0': CONFIG_DEFAULT,
            'client.1': CONFIG_DEFAULT,
        }
        assert t == TIMEOUT_DEFAULT

    def test_timeout_default(self):
        config = {
            'client.0': {'branch': 'testing'},
        }
        overrides = {}
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'client.0': {'branch': 'testing'},
        }
        assert t == TIMEOUT_DEFAULT

    def test_timeout(self):
        config = {
            'client.0': {'branch': 'testing'},
            'timeout': 100,
        }
        overrides = {}
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'client.0': {'branch': 'testing'},
        }
        assert t == 100

    def test_override_timeout(self):
        config = {
            'client.0': {'branch': 'testing'},
            'timeout': 100,
        }
        overrides = {
            'timeout': 200,
        }
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'client.0': {'branch': 'testing'},
        }
        assert t == 200

    def test_override_same_version_key(self):
        config = {
            'client.0': {'branch': 'testing'},
        }
        overrides = {
            'client.0': {'branch': 'wip-foobar'},
        }
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'client.0': {'branch': 'wip-foobar'},
        }
        assert t == TIMEOUT_DEFAULT

    def test_override_different_version_key(self):
        config = {
            'client.0': {'branch': 'testing'},
        }
        overrides = {
            'client.0': {'tag': 'v4.1'},
        }
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'client.0': {'tag': 'v4.1'},
        }
        assert t == TIMEOUT_DEFAULT

    def test_override_actual(self):
        config = {
            'osd.1': {'tag': 'v4.1'},
            'client.0': {'branch': 'testing'},
        }
        overrides = {
            'osd.1': {'koji': 1234, 'kdb': True},
        }
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'osd.1': {'koji': 1234, 'kdb': True},
            'client.0': {'branch': 'testing'},
        }
        assert t == TIMEOUT_DEFAULT

    def test_override_actual_with_generic(self):
        config = {
            'osd.1': {'tag': 'v4.1', 'kdb': False},
            'client.0': {'branch': 'testing'},
        }
        overrides = {
            'osd': {'koji': 1234},
        }
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'osd.0': {'koji': 1234},
            'osd.1': {'koji': 1234, 'kdb': False},
            'osd.2': {'koji': 1234},
            'client.0': {'branch': 'testing'},
        }
        assert t == TIMEOUT_DEFAULT

    def test_override_actual_with_top_level(self):
        config = {
            'osd.1': {'tag': 'v4.1'},
            'client.0': {'branch': 'testing', 'kdb': False},
        }
        overrides = {'koji': 1234, 'kdb': True}
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'mon.a': {'koji': 1234, 'kdb': True},
            'osd.0': {'koji': 1234, 'kdb': True},
            'osd.1': {'koji': 1234, 'kdb': True},
            'osd.2': {'koji': 1234, 'kdb': True},
            'client.0': {'koji': 1234, 'kdb': True},
            'client.1': {'koji': 1234, 'kdb': True},
        }
        assert t == TIMEOUT_DEFAULT

    def test_override_generic(self):
        config = {
            'osd': {'tag': 'v4.1'},
            'client': {'branch': 'testing'},
        }
        overrides = {
            'client': {'koji': 1234, 'kdb': True},
        }
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'osd.0': {'tag': 'v4.1'},
            'osd.1': {'tag': 'v4.1'},
            'osd.2': {'tag': 'v4.1'},
            'client.0': {'koji': 1234, 'kdb': True},
            'client.1': {'koji': 1234, 'kdb': True},
        }
        assert t == TIMEOUT_DEFAULT

    def test_override_generic_with_top_level(self):
        config = {
            'osd': {'tag': 'v4.1'},
            'client': {'branch': 'testing', 'kdb': False},
        }
        overrides = {
            'client': {'koji': 1234},
        }
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'osd.0': {'tag': 'v4.1'},
            'osd.1': {'tag': 'v4.1'},
            'osd.2': {'tag': 'v4.1'},
            'client.0': {'koji': 1234, 'kdb': False},
            'client.1': {'koji': 1234, 'kdb': False},
        }
        assert t == TIMEOUT_DEFAULT

    def test_override_generic_with_actual(self):
        config = {
            'osd': {'tag': 'v4.1', 'kdb': False},
            'client': {'branch': 'testing'},
        }
        overrides = {
            'osd.2': {'koji': 1234, 'kdb': True},
        }
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'osd.0': {'tag': 'v4.1', 'kdb': False},
            'osd.1': {'tag': 'v4.1', 'kdb': False},
            'osd.2': {'koji': 1234, 'kdb': True},
            'client.0': {'branch': 'testing'},
            'client.1': {'branch': 'testing'},
        }
        assert t == TIMEOUT_DEFAULT

    def test_override_top_level(self):
        config = {'branch': 'testing'}
        overrides = {'koji': 1234, 'kdb': True}
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'mon.a': {'koji': 1234, 'kdb': True},
            'osd.0': {'koji': 1234, 'kdb': True},
            'osd.1': {'koji': 1234, 'kdb': True},
            'osd.2': {'koji': 1234, 'kdb': True},
            'client.0': {'koji': 1234, 'kdb': True},
            'client.1': {'koji': 1234, 'kdb': True},
        }
        assert t == TIMEOUT_DEFAULT

    def test_override_top_level_with_actual(self):
        config = {'branch': 'testing', 'kdb': False}
        overrides = {
            'mon.a': {'koji': 1234},
        }
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'mon.a': {'koji': 1234, 'kdb': False},
            'osd.0': {'branch': 'testing', 'kdb': False},
            'osd.1': {'branch': 'testing', 'kdb': False},
            'osd.2': {'branch': 'testing', 'kdb': False},
            'client.0': {'branch': 'testing', 'kdb': False},
            'client.1': {'branch': 'testing', 'kdb': False},
        }
        assert t == TIMEOUT_DEFAULT

    def test_override_top_level_with_generic(self):
        config = {'branch': 'testing', 'kdb': False}
        overrides = {
            'client': {'koji': 1234, 'kdb': True},
        }
        config, t = normalize_and_apply_overrides(self.ctx, config, overrides)
        assert config == {
            'mon.a': {'branch': 'testing', 'kdb': False},
            'osd.0': {'branch': 'testing', 'kdb': False},
            'osd.1': {'branch': 'testing', 'kdb': False},
            'osd.2': {'branch': 'testing', 'kdb': False},
            'client.0': {'koji': 1234, 'kdb': True},
            'client.1': {'koji': 1234, 'kdb': True},
        }
        assert t == TIMEOUT_DEFAULT
