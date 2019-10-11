import pytest


class TestLocking(object):

    def test_correct_os_type(self, ctx, config):
        os_type = ctx.config.get("os_type")
        if os_type is None:
            pytest.skip('os_type was not defined')
        for remote in ctx.cluster.remotes.keys():
            assert remote.os.name == os_type

    def test_correct_os_version(self, ctx, config):
        os_version = ctx.config.get("os_version")
        if os_version is None:
            pytest.skip('os_version was not defined')
        if ctx.config.get("os_type") == "debian":
            pytest.skip('known issue with debian versions; see: issue #10878')
        for remote in ctx.cluster.remotes.keys():
            assert remote.inventory_info['os_version'] == os_version

    def test_correct_machine_type(self, ctx, config):
        machine_type = ctx.machine_type
        for remote in ctx.cluster.remotes.keys():
            assert remote.machine_type in machine_type
