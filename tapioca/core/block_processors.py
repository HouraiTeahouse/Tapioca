import zlib
import logging
from abc import abstractmethod
from tapioca.core.manifest import hash_block


log = logging.getLogger(__name__)


class BlockProcessor():
    """An abstract class for intermediate processing stages within a
    BlockPipeline. These processors may alter or change the provided blocks
    before being written out to BlockSinks.
    """

    @abstractmethod
    def process_block(self, block_hash, block):
        pass


class GzipBlockProcessor(BlockProcessor):
    """A BlockProcessor that gzip compresses blocks."""

    def __init__(self, level=9):
        self.level = level

    def process_block(self, block_hash, block):
        log.info(f'Compressing block (gzip -{self.level}): "{block_hash.hex()}..."')
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
            log.error(f'Block hash mismatch: "{block_hash.hex()}" vs "{b_hash.hex()}"')
            return None
        return block
