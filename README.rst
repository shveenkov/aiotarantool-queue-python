Tarantool Queue bindings for work with python asyncio
----------------------------------------------------------
Bindings require tarantool version 1.6 and aiotarantool connector:

    $ pip install aiotarantool_queue aiotarantool


Try it example:

.. code:: python

    import asyncio
    import aiotarantool_queue
    import random

    async def put_job(queue):
        for tube_name in ("tube1", "tube2", "tube3"):
            tube = queue.tube(tube_name)
            task = await tube.put({"task_data": random.random()})

    async def take_job(tube):
        while True:
            task = await tube.take(5)
            if not task:
                break

            print(task.data)
            await task.ack()

    loop = asyncio.get_event_loop()

    queue = aiotarantool_queue.Queue("127.0.0.1", 3301)
    put_tasks = [loop.create_task(put_job(queue))
                 for _ in range(20)]

    take_tasks = [loop.create_task(take_job(queue.tube(tube_name)))
                  for tube_name in ("tube1", "tube2", "tube3")]

    loop.run_until_complete(asyncio.wait(put_tasks + take_tasks))
    loop.run_until_complete(queue.close())
    loop.close()


This code makes it easy to develop your application to work with queue.
