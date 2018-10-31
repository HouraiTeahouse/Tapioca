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
        _append_async_tuple(self.sources, source, lambda s: s.get_block)
        return self

    def then(self, processor):
        _append_async_tuple(self.processors, processor,
                            lambda p: p.process_block)
        return self

    def write_to(self, sink):
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

    async def _run_block(self, block_hash, executor):
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

    def run(self, block_hashes, executor=None):
        return asyncio.gather(*[self._run_block(block_hash, executor)
                                for block_hash in block_hashes])
