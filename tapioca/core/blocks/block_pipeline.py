from collections import namedtuple
from tapioca.core import hash_block
from tapioca.core.manifest import BlockInfo
import asyncio
import inspect
import traceback
import logging
import threading


def _append_async_tuple(lst, origin, func):
    is_async = inspect.iscoroutinefunction(func(origin))
    lst.append((is_async, origin))


class FileBlockData(namedtuple("FileBlockData",
                               "file block_id hash size block",
                               defaults=(None, None, None, None, None))):

    def with_block(self, block, update_hash=False):
        updated = self._replace(block=block, size=len(block))
        if update_hash:
            updated = updated._replace(hash=hash_block(block))
        return updated

    @staticmethod
    def from_block_info(self, block_info, *args, **kwargs):
        return FileBlockData(hash=block_info.hash, size=block_info.size,
                             *args, **kwargs)

    def to_block_info(self):
        assert self.hash is not None
        assert self.size is not None
        return BlockInfo(hash=self.hash, size=self.size)


class BlockPipeline():

    def __init__(self):
        self.sources = []
        self.processors = []
        self.sinks = []

    def then(self, processor):
        """Adds a BlockProcessor to the pipeline. BlockProcessors are evaluated
        sequentially for each block, in the order they are added to the
        pipeline. If the BlockProcessor errors out or fails to return a
        bytes-like object, the pipeline will not continue for that block, but
        will not shutdown the entire pipeline.

        The provided BlockProcessors's process_block method can be implemented
        with either synchronous or asynchronous. Synchronous processors will be
        run in a background executor to avoid starving the main thread.
        """
        _append_async_tuple(self.processors, processor,
                            lambda p: p.process_block)
        return self

    def write_to(self, sink):
        """Adds a BlockSink to the pipeline. BlockSinks are run in parallel
        after all BlockProcessors have finished executing. Errors from
        BlockSinks will not stop the pipeline, and will not be retried.

        The provided BlockSink's write_block method can be implemented
        with either synchronous or asynchronous. Synchronous sinks will be run
        in a background executor to avoid starving the main thread.
        """
        _append_async_tuple(self.sinks, sink, lambda s: s.write_block)
        return self

    async def _execute(self, is_async, func):
        try:
            if is_async:
                return await func()
            else:
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(None, func)
        except Exception as e:
            # TODO(james7132): handle error
            logging.exception(e)

    async def run_block(self, block_data):
        """Runs a block through the pipeline.

        Params:
          block-data (FileBlockData):
            the hash of the block to run though the pipeline.
        """
        # Process the block
        for is_async, processor in self.processors:
            def task(): return processor.process_block(block_data)
            block_data = await self._execute(is_async, task)
            if block_data is None:
                return

        write_tasks = []
        for is_async, sink in self.sinks:
            def task(): sink.write_block(block_data)
            write_tasks.append(self._execute(is_async, task))
        await asyncio.gather(*write_tasks)

    async def run(self, source, batch_size=10):
        """|coro|
        Runs multiple block through the pipeline in parallel.

        Params:
          source (list[bytes]):
            A BlockSource to read blocks from.
        """
        def run_pipeline_impl(loop):
            tasks = []
            for block_data in source.get_blocks():
                block_task = self.run_block(block_data)
                task = asyncio.run_coroutine_threadsafe(block_task, loop)
                tasks.append(task)
            for task in tasks:
                # Wait for the task to complete.
                task.result()

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, run_pipeline_impl, loop)
