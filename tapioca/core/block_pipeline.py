from collections import namedtuple
from tapioca.core import hash_block
from tapioca.core.manifest import BlockInfo
import asyncio
import inspect
import traceback


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

    async def _execute(self, is_async, func, executor):
        try:
            if is_async:
                return await func()
            else:
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(executor, func)
        except Exception as e:
            # TODO(james7132): handle error
            print(e)
            traceback.print_exc()

    async def run_block(self, block_data, executor=None):
        """Runs a block through the pipeline.

        Params:
          block-data (FileBlockData):
            the hash of the block to run though the pipeline.
          executor (concurrent.futures.Executor):
            Optional: a background executor to run synchronous operations in.
        """
        # Process the block
        for is_async, processor in self.processors:
            def task(): return processor.process_block(block_data)
            block_data = await self._execute(is_async, task, executor)
            if block_data is None:
                return

        write_tasks = []
        for is_async, sink in self.sinks:
            def task(): sink.write_block(block_data)
            write_tasks.append(self._execute(is_async, task, executor))
        await asyncio.gather(*write_tasks)

    async def run(self, source, executor=None):
        """|coro|
        Runs multiple block through the pipeline in parallel.

        Params:
          source (list[bytes]):
            A BlockSource to read blocks from.
          executor (concurrent.futures.Executor):
            Optional: a background executor to run synchronous operations in.
        """
        async def pipeline_run_async():
            tasks = []
            for block_data in source.get_blocks():
                tasks.append(self.run_block(block_data, executor))
            await asyncio.gather(*tasks)

        def executor_coroutine():
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(pipeline_run_async())
            finally:
                loop.close()

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(executor, executor_coroutine)
