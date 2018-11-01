import asyncio
import lmdb
import sys
import logging
import zlib
import hashlib
from functools import lru_cache
import tapioca.server.config as config

log = logging.getLogger(__name__)

configs = None
manifests = None
blocks = None


def init():
    global lmdb_env
    lmdb_env = lmdb.open(config.DB_LOCATION)

    dbs = ['configs', 'manifests', 'blocks']

    module = sys.modules[__name__]
    with lmdb_env.begin(write=True) as txn:
        for db in dbs:
            setattr(module, db, lmdb_env.open_db(db, txn=txn))


def get_build_key(project, branch, build):
    """Creates a unique 64-bit key for the combination of project, branch, and
    build.
    """
    key = ''.join(str(project), str(branch), str(build)).encode()
    return hashlib.blake2b(data=key, digest_size=8).digest()


def get_build_block_key(block_hash, build_key):
    return block_hash + build_key


@lru_cache()
def get_manifest(project, branch, build):
    build_key = get_build_key(project, branch, build)
    with lmdb_env.begin(db=manifests) as txn:
        gzipped_manifest = txn.get(build_key)
        return None if gzipped_manifest is None else \
            zlib.decompress(gzipped_manifest)


async def save_manifest(manifest, project, branch, build):
    def save_manifest_impl():
        proto_bytes = manifest.to_proto().SerializeToString()
        compressed_proto = zlib.compress(proto_bytes, level=9)
        build_key = get_build_key(project, branch, build)
        with lmdb_env.begin(write=True) as txn:
            txn.put(build_key, compressed_proto, db=manifests)
            for block in manifest.blocks:
                block_key = get_build_block_key(block.hash)
                txn.put(block_key, b'', db=blocks)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, save_manifest_impl)

    # Invalidate the fetch cache
    get_manifest.cache_clear()
