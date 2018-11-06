from abc import abstractmethod
from tapioca.core import hash_block, hash_encode
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
    def process_block(self, block_data):
        """Processes a block in the block stream. On success, the method must
        return a FileBlockData. Returning None or raising an error will stop
        the pipeline. Returning the provided FileBlockData is safe, but
        modifications require constructing new FileBlockDatas.
        """
        raise NotImplementedError


class DedupBlockProcessor(BlockProcessor):
    """A BlockProcessor that removes deduplicates blocks as they appear in the
    block stream, based on the block's hash. Blocks without hashes will also be
    removed from the stream.
    """

    def __init__(self):
        self.seen_hashes = set()

    def process_block(self, block_data):
        if block_data.hash is None or block_data.hash in self.seen_hashes:
            return None
        self.seen_hashes.add(block_data.hash)
        return block_data


class BlockHasher(BlockProcessor):
    """A BlockProcessor that hashes provided blocks, if and only if the
    streamed blocks do not already have a hash. Blocks with a hash pass through
    unchanged.
    """

    def process_block(self, block_data):
        if block_data.hash is not None:
            return block_data
        block_hash = hash_block(block_data.block)
        return block_data._replace(hash=block_hash)


class BlockFetcher(BlockProcessor):
    """An abstract base class that fetches blocks from some storage."""

    async def process_block(self, block_data):
        if block_data.block is not None:
            return block_data
        if block_data.hash is not None:
            raise RuntimeError('Tried to fetch block without hash.')
        block = await self.fetch_block(block_data.hash)
        return block_data.with_block(block)

    @abstractmethod
    async def fetch_block(self, block_hash):
        """Attempt to fetch the block for a given block hash.

        Should not define retry logic.
        """
        raise NotImplementedError

    def with_cache(self, cache_dir):
        """Shortcut for creating a CachedBlockFetcher from the original
        BlockFetcher.
        """
        return CachedBlockFetcher(self, cache_dir)


class HttpBlockFetcher(BlockFetcher):
    """A BlockFetcher that fetches blocks from a remote HTTP(s) server."""

    def __init__(self, prefix, session):
        self.prefix = prefix
        self.session = session

    async def fetch_block(self, block_hash):
        block_suffix = hash_encode(block_hash)
        url = os.path.join(self.prefix, block_suffix)
        # TODO(james7132): Error handling
        # TODO(james7132): Exponential fallback
        log.info(f'Fetching block "{block_suffix}" from {url}...')
        async with self.session.get(url) as response:
            block = await response.read()
            log.info(f'Fetched block "{block_suffix}" from {url}.')
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

    async def fetch_block(self, block_hash):
        block_suffix = hash_encode(block_hash)
        path = os.path.join(self.cache_dir, block_suffix)
        if os.path.exists(path):
            log.info(f'Found block in cache: "{block_suffix}"')
            # TODO(james7132): Use aiofiles here
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

    def process_block(self, block_data):
        log.info(f'Compressing block (gzip -{self.level}):'
                 f'"{hash_encode(block_data.hash)}..."')
        return block_data._replace(block=zlib.compress(block_data.block,
                                                       self.level))


class GunzipBlockProcessor(BlockProcessor):
    """A BlockProcessor that decompresses gzip compressed blocks."""

    def process_block(self, block_data):
        log.info(f'Decompressing block (gzip): "{hash_encode(block_hash)}..."')
        return block_data.with_block(zlib.decompress(block, self.level))


class ValidateBlockProcessor(BlockProcessor):
    """A BlockProcessor that validates whether the provided block matches the
    assigned block hash.
    """

    def process_block(self, block_data):
        block_hash = hash_block(block_data.block)
        if block_hash != block_data.hash:
            log.error(f'Block hash mismatch: "{hash_encode(block_hash)}"'
                      f'vs "{hash_encode(b_hash)}"')
            return None
        return block
