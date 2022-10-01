# -*- coding: utf-8 -*-

from .. import mgr
from ..controllers.erasure_code_profile import ErasureCodeProfile
from ..tests import ControllerTestCase


class ErasureCodeProfileTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        mgr.get.side_effect = lambda key: {
            'osd_map': {
                'erasure_code_profiles': {
                    'test': {
                        'k': '2',
                        'm': '1'
                    }
                }
            },
            'health': {'json': '{"status": 1}'},
            'fs_map': {'filesystems': []},

        }[key]
        cls.setup_controllers([ErasureCodeProfile])

    def test_list(self):
        self._get('/api/erasure_code_profile')
        self.assertStatus(200)
        self.assertJsonBody([{'k': 2, 'm': 1, 'name': 'test'}])
