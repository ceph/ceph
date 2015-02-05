import pytest
import re


class TestLocking(object):

    def test_correct_os_type(self, ctx, config):
        os_type = ctx.config.get("os_type")
        if os_type is None:
            pytest.skip('os_type was not defined')
        for remote in ctx.cluster.remotes.iterkeys():
            assert remote.os.name == os_type

    def test_correct_os_version(self, ctx, config):
        os_version = ctx.config.get("os_version")
        if os_version is None:
            pytest.skip('os_version was not defined')
        for remote in ctx.cluster.remotes.iterkeys():
            assert remote.os.version == os_version

    def test_correct_machine_type(self, ctx, config):
        machine_type = ctx.machine_type
        for remote in ctx.cluster.remotes.iterkeys():
            # gotta be a better way to get machine_type
            # if not, should we move this to Remote?
            remote_type = re.sub("[0-9]", "", remote.shortname)
            assert remote_type in machine_type
