import os
import logging
from abc import abstractmethod


log = logging.getLogger(__name__)


class BlockSource():
    """An abstract class for reading blocks into BlockPipelines to persistent
    storage.
    """
    @abstractmethod
    def get_block(self, block_hash):
        raise NotImplementedError

    def with_cache(self, cache_dir):
        return CachedBlockSource(self, cache_dir)


class HttpBlockSource(BlockSource):
    """A BlockSource that fetches blocks from a remote location."""

    def __init__(self, prefix, session):
        self.prefix = prefix
        self.session = session

    async def get_block(self, block_hash):
        block_hex = block_hash.hex()
        url = os.path.join(self.prefix, block_hex)
        # TODO(james7132): Error handling
        # TODO(james7132): Exponential fallback
        log.info(f'Fetching block "{block_hex}" from {url}...')
        async with self.session.get(url) as response:
            block = await response.read()
            log.info(f'Fetched block "{block_hex}" from {url}.')
            return block


class CachedBlockSource(BlockSource):
    """A BlockSource that checks a local cache before downloading making a
    request to a backing store.
    """
    def __init__(self, base_source, cache_dir):
        self.base_source = base_source
        self.cache_dir = cache_dir

    def get_block(self, block_hash):
        path = os.path.join(self.cache_dir, block_hash.hex())
        if os.path.exists(path):
            log.info(f'Found block in cache: "{block_hex}"')
            with open(path, 'rb') as f:
                return f.read()
        return self.base_source.get_block(block_hash)

# TODO(james7132): Implement P2P block source (IPFS?)
