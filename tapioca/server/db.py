from coders import IdentityCoder, ProtobufCoder
from functools import lru_cache
from tapioca.server.data_pb2mp import ProjectConfig, Build, BlockInfo
import asyncio
import hashlib
import lmdb
import logging
import sys
import tapioca.server.config as config
import zlib

log = logging.getLogger(__name__)


class ProtoDatabase():
    __slots__ = ['key_coder', 'value_coder', 'db']

    def __init__(self, key_coder=None, value_coder=None):
        self.key_coder = key_coder or IdentityCoder()
        self.value_coder = value_coder or IdentityCoder()
        self.db = None

    def get(self, txn, key, *args, **kwargs):
        value = txn.get(self.key_coder.encode(key), db=self.db,
                        *args, **kwargs)
        return self.value_coder.decode(value)

    def put(self, txn, key, value, *args, **kwargs):
        return txn.put(self.key_coder.encode(key),
                       self.value_coder.encode(value),
                       db=self.db, *args, **kwargs)

    def delete(self, txn, key, value, *args, **kwargs):
        return txn.put(self.key_coder.encode(key),
                       self.value_coder.encode(value),
                       db=self.db, *args, **kwargs)


configs = ProtoDatabase(value_coder=ProtobufCoder(ProjectConfig))
builds = ProtoDatabase(value_coder=ProtobufCoder(Build).compressed(9))
blocks = ProtoDatabase(value_coder=ProtobufCoder(BlockInfo))
build_blocks = ProtoDatabase(value_coder=ProtobufCoder(BlockInfo))


def init():
    global lmdb_env
    lmdb_env = lmdb.open(config.DB_LOCATION)

    dbs = ['configs', 'builds', 'blocks', 'build_blocks']

    module = sys.modules[__name__]
    with lmdb_env.begin(write=True) as txn:
        for db in dbs:
            getattr(module, db).db = lmdb_env.open_db(db, txn=txn)


def get_build_key(request):
    """
    Creates a unique 64-bit key for the combination of a build descriptor.
    """
    key = ''.join(str(request.project),
                  str(request.branch),
                  str(request.build)).encode()
    return hashlib.blake2b(data=key, digest_size=8).digest()


def get_build_block_key(block_hash, build_key):
    return block_hash + build_key


@lru_cache()
def get_build(request):
    build_key = get_build_key(request)
    with lmdb_env.begin(db=builds) as txn:
        return builds.get(txn, build_key)


async def save_build(build, request):
    def save_build_impl():
        build_key = get_build_key(request)
        with lmdb_env.begin(write=True) as txn:
            builds.put(txn, build_key, build)
            for block in build.manifest.blocks:
                save_block(txn, block, build_key)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, save_build_impl)

    # Invalidate the fetch cache
    get_build.cache_clear()


async def purge_build(build):
    def purge_build_impl():
        build_key = get_build_key(build)
        with lmdb_env.begin(write=True) as txn:
            for block in build.manifest.blocks:
                delete_build_block(txn, block, build_key)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, save_build_impl)


async def is_block_dead(block):
    """|coro|
    Checks to see if a block has any existing build refrences or not.

    Returns True if no build references the block, returns False if the block
    does not
    """
    def is_block_dead_impl():
        with lmdb_env.begin() as txn:
            csr = txn.cursor(db=build_blocks.db)
            if not csr.set_range(block.hash):
                return True
            next_key = next(csr.iternext(value=False))
            return next_key.startswith(block.hash)

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, is_block_dead_impl)


def save_block(txn, block, build_key=None):
    block_info = BlockInfo()
    blocks.put(txn, block.hash, block_info)
    if build_key is not None:
        block_key = get_build_block_key(block.hash, build_key)
        build_blocks.put(txn, block_key, block_info)


def delete_build_block(txn, block, build_key):
    block_key = get_build_block_key(block.hash, build_key)
    return build_blocks.delete(txn, block_key)


