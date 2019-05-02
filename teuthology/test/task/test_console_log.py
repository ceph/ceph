import os

from mock import patch

from teuthology.config import FakeNamespace
from teuthology.config import config as teuth_config
from teuthology.orchestra.cluster import Cluster
from teuthology.orchestra.remote import Remote
from teuthology.task.console_log import ConsoleLog

from teuthology.test.task import TestTask


class TestConsoleLog(TestTask):
    klass = ConsoleLog
    task_name = 'console_log'

    def setup(self):
        teuth_config.ipmi_domain = 'ipmi.domain'
        teuth_config.ipmi_user = 'ipmi_user'
        teuth_config.ipmi_password = 'ipmi_pass'
        self.ctx = FakeNamespace()
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('user@remote1'), ['role1'])
        self.ctx.cluster.add(Remote('user@remote2'), ['role2'])
        self.ctx.config = dict()
        self.ctx.archive = '/fake/path'
        self.task_config = dict()
        self.start_patchers()

    def start_patchers(self):
        self.patchers = dict()
        self.patchers['makedirs'] = patch(
            'teuthology.task.console_log.os.makedirs',
        )
        self.patchers['is_vm'] = patch(
            'teuthology.lock.query.is_vm',
        )
        self.patchers['is_vm'].return_value = False
        self.patchers['get_status'] = patch(
            'teuthology.lock.query.get_status',
        )
        self.mocks = dict()
        for name, patcher in self.patchers.items():
            self.mocks[name] = patcher.start()
        self.mocks['is_vm'].return_value = False

    def teardown(self):
        for patcher in self.patchers.values():
            patcher.stop()

    def test_enabled(self):
        task = self.klass(self.ctx, self.task_config)
        assert task.enabled is True

    def test_disabled_noarchive(self):
        self.ctx.archive = None
        task = self.klass(self.ctx, self.task_config)
        assert task.enabled is False

    def test_has_ipmi_credentials(self):
        for remote in self.ctx.cluster.remotes.keys():
            remote.console.has_ipmi_credentials = False
            remote.console.has_conserver = False
        task = self.klass(self.ctx, self.task_config)
        assert len(task.cluster.remotes.keys()) == 0

    def test_remotes(self):
        with self.klass(self.ctx, self.task_config) as task:
            assert len(task.cluster.remotes) == len(self.ctx.cluster.remotes)

    @patch('teuthology.orchestra.console.PhysicalConsole')
    def test_begin(self, m_pconsole):
        with self.klass(self.ctx, self.task_config) as task:
            assert len(task.processes) == len(self.ctx.cluster.remotes)
            for remote in task.cluster.remotes.keys():
                dest_path = os.path.join(
                    self.ctx.archive, '%s.log' % remote.shortname)
                assert remote.console.spawn_sol_log.called_once_with(
                    dest_path=dest_path)

    @patch('teuthology.orchestra.console.PhysicalConsole')
    def test_end(self, m_pconsole):
        with self.klass(self.ctx, self.task_config) as task:
            pass
        for proc in task.processes.values():
            assert proc.terminate.called_once_with()
            assert proc.kill.called_once_with()
