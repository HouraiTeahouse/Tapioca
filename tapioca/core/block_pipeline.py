import asyncio
import inspect


def _append_async_tuple(lst, origin, func):
    is_async = inspect.iscoroutinefunction(func(origin))
    lst.append((is_async, origin))


class BlockPipeline():

    def __init__(self):
        self.sources = []
        self.processors = []
        self.sinks = []

    def add_source(self, source):
        """Adds a BlockSource to the pipeline. BlockSources are evaluated
        sequentially for each block and in order in which they are added to the
        pipeline.  The first source to provide a valid (non-None, non-error)
        result will be used for the rest. This is meant to provide fallback
        block source support in case a source is inaccessible.

        The provided BlockSource's get_block method can be implemented with
        either synchronous or asynchronous. Synchronous sources will be run in
        a background executor to avoid starving the main thread.
        """
        _append_async_tuple(self.sources, source, lambda s: s.get_block)
        return self

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
                return await asyncio.run_in_executor(executor, func)
        except Exception:
            # TODO(james7132): handle error
            pass

    async def run_block(self, block_hash, executor=None):
        """Runs a block through the pipeline.

        Params:
          block_hash (bytes):
            the hash of the block to run though the pipeline.
          executor (concurrent.futures.Executor):
            Optional: a background executor to run synchronous operations in.
        """
        block = None

        # Fetch the block
        for is_async, source in self.sources:
            def task(): source.get_block(block_hash)
            block = await self._execute(is_async, task, executor)
            if block is not None:
                break

        if block is None:
            # TODO(james7132): handle error
            return

        # Process the block
        for is_async, processor in self.sources:
            def task(): processor.process_block(block, block_hash)
            block = await self._execute(is_async, task, executor)

        write_tasks = []
        for is_async, sink in self.sinks:
            def task(): sink.write_block(block, block_hash)
            write_tasks.append(self._execute(is_async, task, executor))
        await asyncio.gather(*write_tasks)

    def run(self, source, executor=None):
        """Runs multiple block through the pipeline in parallel.

        Params:
          block_hashes (list[bytes]):
            the hashes of the blocks to run though the pipeline.
          executor (concurrent.futures.Executor):
            Optional: a background executor to run synchronous operations in.
        """
        return asyncio.gather(*[self._run_block(block_hash, executor)
                                for block_hash in block_hashes])
