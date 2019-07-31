# -*- coding: utf-8 -*-
# pylint: disable=dangerous-default-value,too-many-public-methods
from __future__ import absolute_import

import unittest

from ..services.rbd import get_image_spec, parse_image_spec


class RbdServiceTest(unittest.TestCase):

    def test_compose_image_spec(self):
        self.assertEqual(get_image_spec('mypool', 'myns', 'myimage'), 'mypool/myns/myimage')
        self.assertEqual(get_image_spec('mypool', None, 'myimage'), 'mypool/myimage')

    def test_parse_image_spec(self):
        self.assertEqual(parse_image_spec('mypool/myns/myimage'), ('mypool', 'myns', 'myimage'))
        self.assertEqual(parse_image_spec('mypool/myimage'), ('mypool', None, 'myimage'))
