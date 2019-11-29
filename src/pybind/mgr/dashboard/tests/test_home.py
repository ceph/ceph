from __future__ import absolute_import

import logging
import os

try:
    import mock
except ImportError:
    import unittest.mock as mock

from . import ControllerTestCase, FakeFsMixin
from .. import mgr

from ..controllers.home import HomeController, LanguageMixin

logger = logging.getLogger()


class HomeTest(ControllerTestCase, FakeFsMixin):
    @classmethod
    def setup_server(cls):
        frontend_path = mgr.get_frontend_path()
        cls.fs.reset()
        cls.fs.create_dir(frontend_path)
        cls.fs.create_file(
            os.path.join(frontend_path, '..', 'package.json'),
            contents='{"config":{"locale": "en-US"}}')
        with mock.patch(cls.builtins_open, new=cls.f_open),\
                mock.patch('os.listdir', new=cls.f_os.listdir):
            lang = LanguageMixin()
            cls.fs.create_file(
                os.path.join(lang.DEFAULT_LANGUAGE_PATH, 'index.html'),
                contents='<!doctype html><html lang="en"><body></body></html>')
        cls.setup_controllers([HomeController])

    @mock.patch(FakeFsMixin.builtins_open, new=FakeFsMixin.f_open)
    @mock.patch('os.stat', new=FakeFsMixin.f_os.stat)
    @mock.patch('os.listdir', new=FakeFsMixin.f_os.listdir)
    def test_home_default_lang(self):
        self._get('/')
        self.assertStatus(200)
        logger.info(self.body)
        self.assertIn('<html lang="en">', self.body.decode('utf-8'))

    @mock.patch(FakeFsMixin.builtins_open, new=FakeFsMixin.f_open)
    @mock.patch('os.stat', new=FakeFsMixin.f_os.stat)
    @mock.patch('os.listdir', new=FakeFsMixin.f_os.listdir)
    def test_home_en_us(self):
        self._get('/', headers=[('Accept-Language', 'en-US')])
        self.assertStatus(200)
        logger.info(self.body)
        self.assertIn('<html lang="en">', self.body.decode('utf-8'))

    @mock.patch(FakeFsMixin.builtins_open, new=FakeFsMixin.f_open)
    @mock.patch('os.stat', new=FakeFsMixin.f_os.stat)
    @mock.patch('os.listdir', new=FakeFsMixin.f_os.listdir)
    def test_home_non_supported_lang(self):
        self._get('/', headers=[('Accept-Language', 'NO-NO')])
        self.assertStatus(200)
        logger.info(self.body)
        self.assertIn('<html lang="en">', self.body.decode('utf-8'))
