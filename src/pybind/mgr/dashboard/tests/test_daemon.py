# -*- coding: utf-8 -*-

from ..controllers._version import APIVersion
from ..controllers.daemon import Daemon
from ..tests import ControllerTestCase, patch_orch


class DaemonTest(ControllerTestCase):

    URL_DAEMON = '/api/daemon'

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([Daemon])

    def test_daemon_action(self):
        msg = "Scheduled to stop crash.b78cd1164a1b on host 'hostname'"

        with patch_orch(True) as fake_client:
            fake_client.daemons.action.return_value = msg
            payload = {
                'action': 'restart',
                'container_image': None
            }
            self._put(f'{self.URL_DAEMON}/crash.b78cd1164a1b', payload, version=APIVersion(0, 1))
            self.assertJsonBody(msg)
            self.assertStatus(200)

    def test_daemon_invalid_action(self):
        payload = {
            'action': 'invalid',
            'container_image': None
        }
        with patch_orch(True):
            self._put(f'{self.URL_DAEMON}/crash.b78cd1164a1b', payload, version=APIVersion(0, 1))
            self.assertJsonBody({
                'detail': 'Daemon action "invalid" is either not valid or not supported.',
                'code': 'invalid_daemon_action',
                'component': None
            })
            self.assertStatus(400)

    def test_daemon_list(self):
        with patch_orch(True):
            self._get(f'{self.URL_DAEMON}')
            self.assertStatus(200)
