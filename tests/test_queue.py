# -*- coding: utf-8 -*-

import asyncio
import unittest
import aiotarantool_queue


class QueueBaseTestCase(unittest.TestCase):
    def setUp(self):
        # create new tarantool connection before each test
        self.loop = asyncio.event_loop()
        self.queue = aiotarantool_queue.Queue("127.0.0.1", 3301, user="test", password="test", loop=self.loop)


class QueueConnectionTestCase(QueueBaseTestCase):
    def test_simple_put_take(self):
        tube = self.queue.tube("test")
        # task = await tube.put({"foo": "bar", "baz": 1})
        # self.assertIsNotNone(task, "check put task")
        #
        # task = await tube.take(.5)
        # self.assertIsNotNone(task, "check put task")
        #
        # await task.ack()
        # self.assertIsNotNone(task, "check put task")

        self.assertTrue(True, "asyncio test case stub")

