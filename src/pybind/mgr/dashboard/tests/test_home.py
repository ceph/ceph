from __future__ import absolute_import

import logging

from . import ControllerTestCase
from ..controllers.home import HomeController


logger = logging.getLogger()


class HomeTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([HomeController])

    def test_home_default_lang(self):
        self._get('/')
        self.assertStatus(200)
        logger.info(self.body)
        self.assertIn('<html lang="en">', self.body.decode('utf-8'))

    def test_home_en_us(self):
        self._get('/', headers=[('Accept-Language', 'en-US')])
        self.assertStatus(200)
        logger.info(self.body)
        self.assertIn('<html lang="en">', self.body.decode('utf-8'))

    def test_home_non_supported_lang(self):
        self._get('/', headers=[('Accept-Language', 'NO-NO')])
        self.assertStatus(200)
        logger.info(self.body)
        self.assertIn('<html lang="en">', self.body.decode('utf-8'))
