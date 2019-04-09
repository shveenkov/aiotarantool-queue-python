# -*- coding: utf-8 -*-
"""
Python bindings for Tarantool Queue.

See also: https://github.com/tarantool/queue
"""

import asyncio
import aiotarantool

try:
    from tarantool.const import ENCODING_DEFAULT
except ImportError:
    from tarantool.utils import ENCODING_DEFAULT

READY = "r"
TAKEN = "t"
DONE = "-"
BURIED = "!"
DELAYED = "~"

TASK_STATE = {
    DELAYED: "delayed",
    READY: "ready",
    TAKEN: "taken",
    DONE: "done",
}


class Task(object):
    """
    Tarantool queue task wrapper.
    """
    def __init__(self, tube, task_id, state, data):
        self.tube = tube
        self.queue = tube.queue
        self.task_id = task_id
        self.state = state
        self.data = data

    def __str__(self):
        return "Task <{0}>: {1}".format(self.task_id, self.state_name)

    @property
    def state_name(self):
        """
        Returns state full name
        """
        return TASK_STATE.get(self.state, "UNKNOWN")

    @classmethod
    def create_from_tuple(cls, tube, the_tuple):
        """
        Create task from tuple.

        Returns `Task` instance.
        """
        if the_tuple is None:
            return

        if not the_tuple.rowcount:
            raise Queue.ZeroTupleException("Error creating task")

        row = the_tuple[0]

        return cls(
            tube,
            task_id=row[0],
            state=row[1],
            data=row[2]
        )

    def update_from_tuple(self, the_tuple):
        """
        Update task from tuple.
        """
        if not the_tuple.rowcount:
            raise Queue.ZeroTupleException("Error updating task")

        row = the_tuple[0]

        if self.task_id != row[0]:
            raise Queue.BadTupleException("Wrong task: id's are not match")

        self.state = row[1]
        self.data = row[2]

    async def ack(self):
        """
        Report task successful execution.

        Returns `True` is task is acked (task state is 'done' now).
        """
        the_tuple = await self.queue.ack(self.tube, self.task_id)

        self.update_from_tuple(the_tuple)

        return bool(self.state == DONE)

    async def release(self, delay=None):
        """
        Put the task back into the queue.

        May contain a possible new `delay` before the task is executed again.

        Returns `True` is task is released (task state is 'ready'
        or 'delayed' if `delay` is set now).
        """
        the_tuple = await self.queue.release(self.tube, self.task_id, delay=delay)

        self.update_from_tuple(the_tuple)

        if delay is None:
            return bool(self.state == READY)
        else:
            return bool(self.state == DELAYED)

    async def peek(self):
        """
        Look at a task without changing its state.

        Always returns `True`.
        """
        the_tuple = await self.queue.peek(self.tube, self.task_id)

        self.update_from_tuple(the_tuple)

        return True

    async def delete(self):
        """
        Delete task (in any state) permanently.

        Returns `True` is task is deleted.
        """
        the_tuple = await self.queue.delete(self.tube, self.task_id)

        self.update_from_tuple(the_tuple)

        return bool(self.state == DONE)


class Tube(object):
    """
    Tarantool queue tube wrapper.
    """
    def __init__(self, queue, name):
        self.queue = queue
        self.name = name

    def cmd(self, cmd_name):
        """
        Returns tarantool queue command name for current tube.
        """
        return "{0}.tube.{1}:{2}".format(self.queue.lua_queue_name, self.name, cmd_name)

    async def put(self, data, ttl=None, ttr=None, delay=None):
        """
        Enqueue a task.

        Returns a `Task` object.
        """
        the_tuple = await self.queue.put(self, data, ttl=ttl, ttr=ttr, delay=delay)
        if the_tuple.rowcount:
            return Task.create_from_tuple(self, the_tuple)

    async def take(self, timeout=None):
        """
        Get a task from queue for execution.

        Waits `timeout` seconds until a READY task appears in the queue.

        Returns either a `Task` object or `None`.
        """
        the_tuple = await self.queue.take(self, timeout=timeout)

        if the_tuple.rowcount:
            return Task.create_from_tuple(self, the_tuple)


class Queue(object):
    """
    Tarantool queue wrapper.

    Usage:

        >>> from aiotarantool_queue import Queue
        >>> queue = Queue("127.0.0.1", 33013, user="test", password="test")
        >>> tube = queue.tube("my_tube")
        # Put tasks into the queue
        >>> await tube.put([1, 2, 3])
        >>> await tube.put([2, 3, 4])
        # Get tasks from queue
        >>> task1 = await tube.take()
        >>> task2 = await tube.take()
        >>> print(task1.data)
            [1, 2, 3]
        >>> print(task2.data)
            [2, 3, 4]
        # Release tasks (put them back to queue)
        >>> await task2.release()
        >>> await task1.release()
        # Take task again
        >>> task = await tube.take()
        >>> print(task.data)
            [1, 2, 3]
        # Take task and mark it as complete
        >>> task = await tube.take()
        >>> await task.ack()
            True
    """
    DatabaseError = aiotarantool.DatabaseError
    NetworkError = aiotarantool.NetworkError

    class BadConfigException(Exception):
        """
        Bad config queue exception.
        """
        pass

    class ZeroTupleException(Exception):
        """
        Zero tuple queue exception.
        """
        pass

    class BadTupleException(Exception):
        """
        Bad tuple queue exception.
        """
        pass

    def __init__(self, host="localhost", port=33013, user=None, password=None, loop=None, lua_queue_name="box.queue",
                 encoding=ENCODING_DEFAULT):

        if not host or not port:
            raise self.BadConfigException(
                "host and port params must be not empty"
            )

        if not isinstance(port, int):
            raise self.BadConfigException("port must be int")

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.loop = loop or asyncio.get_event_loop()
        self.lua_queue_name = lua_queue_name
        self.tubes = dict()
        self.tnt = aiotarantool.connect(self.host, self.port,
                                        user=self.user,
                                        password=self.password,
                                        loop=self.loop,
                                        encoding=encoding)

    async def put(self, tube, data, ttl=None, ttr=None, delay=None):
        """
        Enqueue a task.

        Returns a `Task` object.
        """
        cmd = tube.cmd("put")
        args = (data,)

        params = dict()
        if ttr is not None:
            params["ttr"] = ttr
        if ttl is not None:
            params["ttl"] = ttl
        if delay is not None:
            params["delay"] = delay
        if params:
            args += (params,)

        res = await self.tnt.call(cmd, args)
        return res

    async def take(self, tube, timeout=None):
        """
        Get a task from queue for execution.

        Waits `timeout` seconds until a READY task appears in the queue.
        If `timeout` is `None` - waits forever.

        Returns tarantool tuple object.
        """
        cmd = tube.cmd("take")
        args = ()

        if timeout is not None:
            args += (timeout,)

        res = await self.tnt.call(cmd, args)
        return res

    async def ack(self, tube, task_id):
        """
        Report task successful execution.

        Ack is accepted only from the consumer, which took the task
        for execution. If a consumer disconnects, all tasks taken
        by this consumer are put back to READY state (released).

        Returns tarantool tuple object.
        """
        cmd = tube.cmd("ack")
        args = (task_id,)

        res = await self.tnt.call(cmd, args)
        return res

    async def release(self, tube, task_id, delay=None):
        """
        Put the task back into the queue.

        Used in case of a consumer for any reason can not execute a task.
        May contain a possible new `delay` before the task is executed again.

        Returns tarantool tuple object.
        """
        cmd = tube.cmd("release")
        args = (task_id,)
        params = dict()
        if delay is not None:
            params["delay"] = delay
        if params:
            args += (params,)

        res = await self.tnt.call(cmd, args)
        return res

    async def peek(self, tube, task_id):
        """
        Look at a task without changing its state.

        Returns tarantool tuple object.
        """
        cmd = tube.cmd("peek")
        args = (task_id,)

        res = await self.tnt.call(cmd, args)
        return res

    async def delete(self, tube, task_id):
        """
        Delete task (in any state) permanently.

        Returns tarantool tuple object.
        """
        cmd = tube.cmd("delete")
        args = (task_id,)

        res = await self.tnt.call(cmd, args)
        return res

    async def drop(self, tube):
        """
        Drop entire query (if there are no in-progress tasks or workers).

        Returns `True` on successful drop.
        """
        cmd = tube.cmd("drop")
        args = ()

        res = await self.tnt.call(cmd, args)

        return bool(res.return_code == 0)

    def tube(self, name):
        """
        Create tube object, if not created before.

        Returns `Tube` object.
        """
        tube = self.tubes.get(name)

        if tube is None:
            tube = Tube(self, name)
            self.tubes[name] = tube

        return tube

    async def close(self):
        await self.tnt.close()
