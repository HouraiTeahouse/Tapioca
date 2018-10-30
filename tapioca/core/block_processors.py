import zlib
from abc import abstractmethod


class BlockProcessor():

    @abstractmethod
    def process_block(self, block_hash, block):
        pass


class GzipBlockProcessor(BlockProcessor):

    def __init__(self, level=9):
        self.level = level

    def process_block(self, block_hash, block):
        return zlib.compress(block, self.level)


class GunzipBlockProcessor(BlockProcessor):

    def process_block(self, block_hash, block):
        return zlib.decompress(block, self.level)
