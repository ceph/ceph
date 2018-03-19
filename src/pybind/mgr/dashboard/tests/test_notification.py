# -*- coding: utf-8 -*-
from __future__ import absolute_import

import random
import time
import unittest


from ..tools import NotificationQueue


class Listener(object):
    def __init__(self):
        NotificationQueue.register(self.log_type1, 'type1')
        NotificationQueue.register(self.log_type2, 'type2')
        NotificationQueue.register(self.log_type1_3, ['type1', 'type3'])
        NotificationQueue.register(self.log_all)
        self.type1 = []
        self.type2 = []
        self.type1_3 = []
        self.all = []

        # these should be ignored by the queue
        NotificationQueue.register(self.log_type1, 'type1')
        NotificationQueue.register(self.log_type1_3, ['type1', 'type3'])
        NotificationQueue.register(self.log_all)

    def log_type1(self, val):
        self.type1.append(val)

    def log_type2(self, val):
        self.type2.append(val)

    def log_type1_3(self, val):
        self.type1_3.append(val)

    def log_all(self, val):
        self.all.append(val)

    def clear(self):
        self.type1 = []
        self.type2 = []
        self.type1_3 = []
        self.all = []


class NotificationQueueTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.listener = Listener()

    def setUp(self):
        self.listener.clear()

    def test_invalid_register(self):
        with self.assertRaises(Exception) as ctx:
            NotificationQueue.register(None, 1)
        self.assertEqual(str(ctx.exception),
                         "types param is neither a string nor a list")

    def test_notifications(self):
        NotificationQueue.start_queue()
        NotificationQueue.new_notification('type1', 1)
        NotificationQueue.new_notification('type2', 2)
        NotificationQueue.new_notification('type3', 3)
        NotificationQueue.stop()
        self.assertEqual(self.listener.type1, [1])
        self.assertEqual(self.listener.type2, [2])
        self.assertEqual(self.listener.type1_3, [1, 3])
        self.assertEqual(self.listener.all, [1, 2, 3])

    def test_notifications2(self):
        NotificationQueue.start_queue()
        for i in range(0, 600):
            typ = "type{}".format(i % 3 + 1)
            if random.random() < 0.5:
                time.sleep(0.002)
            NotificationQueue.new_notification(typ, i)
        NotificationQueue.stop()
        for i in range(0, 500):
            typ = i % 3 + 1
            if typ == 1:
                self.assertIn(i, self.listener.type1)
                self.assertIn(i, self.listener.type1_3)
            elif typ == 2:
                self.assertIn(i, self.listener.type2)
            elif typ == 3:
                self.assertIn(i, self.listener.type1_3)
            self.assertIn(i, self.listener.all)

        self.assertEqual(len(self.listener.type1), 200)
        self.assertEqual(len(self.listener.type2), 200)
        self.assertEqual(len(self.listener.type1_3), 400)
        self.assertEqual(len(self.listener.all), 600)
