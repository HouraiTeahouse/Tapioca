from abc import abstractmethod
from tapioca.core.manifest import hash_block
import logging
import os
import zlib


log = logging.getLogger(__name__)


class BlockProcessor():
    """An abstract class for intermediate processing stages within a
    BlockPipeline. These processors may alter or change the provided blocks
    before being written out to BlockSinks.
    """

    @abstractmethod
    def process_block(self, block_hash, block):
        pass


class BlockFetcher(BlockProcessor):

    def with_cache(self, cache_dir):
        return CachedBlockFetcher(self, cache_dir)


class HttpBlockFetcher(BlockFetcher):
    """A BlockFetcher that fetches blocks from a remote HTTP(s) server."""

    def __init__(self, prefix, session):
        self.prefix = prefix
        self.session = session

    async def process_block(self, block_hash, block):
        if block is not None:
            return block
        block_hex = block_hash.hex()
        url = os.path.join(self.prefix, block_hex)
        # TODO(james7132): Error handling
        # TODO(james7132): Exponential fallback
        log.info(f'Fetching block "{block_hex}" from {url}...')
        async with self.session.get(url) as response:
            block = await response.read()
            log.info(f'Fetched block "{block_hex}" from {url}.')
            return block


# TODO(james7132): Implement P2P block fetcher (IPFS?)


class CachedBlockFetcher(BlockFetcher):
    """A BlockFetcher that checks a local cache before making a request to a i
    backing store.

    If the provided block is not None
    """

    def __init__(self, base_fetcher, cache_dir):
        self.base_fetcher = base_fetcher
        self.cache_dir = cache_dir

    async def process_block(self, block_hash, block):
        if block is not None:
            return block
        block_hex = block_hash.hex()
        path = os.path.join(self.cache_dir, block_hex)
        if os.path.exists(path):
            log.info(f'Found block in cache: "{block_hex}"')
            with open(path, 'rb') as f:
                return f.read()
        fetched_block = await self.base_fetcher.get_block(block_hash)
        if fetched_block is not None:
            # Save block to the cache
            with open(path, 'wb') as f:
                f.write(fetched_block)
        return fetched_block


class GzipBlockProcessor(BlockProcessor):
    """A BlockProcessor that gzip compresses blocks."""

    def __init__(self, level=9):
        self.level = level

    def process_block(self, block_hash, block):
        log.info(f'Compressing block (gzip -{self.level}):'
                 f'"{block_hash.hex()}..."')
        return zlib.compress(block, self.level)


class GunzipBlockProcessor(BlockProcessor):
    """A BlockProcessor that decompresses gzip compressed blocks."""

    def process_block(self, block_hash, block):
        log.info(f'Decompressing block (gzip): "{block_hash.hex()}..."')
        return zlib.decompress(block, self.level)


class ValidateBlockProcessor(BlockProcessor):
    """A BlockProcessor that validates whether the provided block matches the
    assigned block hash.
    """

    def process_block(self, block_hash, block):
        b_hash = hash_block(block)
        if b_hash != block_hash:
            log.error(f'Block hash mismatch: "{block_hash.hex()}"'
                      f'vs "{b_hash.hex()}"')
            return None
        return block
