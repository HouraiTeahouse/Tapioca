import os
import requests
from abc import abstractmethod


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

    def __init__(self, prefix):
        self.prefix = prefix

    def get_block(self, block_hash):
        path = os.path.join(self.prefix, block_hash.hex())
        r = requests.get(path)
        return r.content


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
            with open(path, 'rb') as f:
                return f.read()
        return self.base_source.get_block(block_hash)

# TODO(james7132): Implement P2P block source (IPFS?)
