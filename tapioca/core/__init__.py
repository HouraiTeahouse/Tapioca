import hashlib
import time
import logging
import base64

HASH_ALG = hashlib.sha512
BLOCK_SIZE = 1024 ** 2

log = logging.getLogger(__name__)


def hash_encode(block_hash):
    """Produces a url-safe minimal encoding of the block hash."""
    return base64.urlsafe_b64encode(block_hash).replace(b'=', b'').decode()


def hash_block(block):
    start = time.perf_counter_ns()
    result = HASH_ALG(block).digest()
    duration = time.perf_counter_ns() - start
    log.debug(f'Hashed block: {hash_encode(result)} ({duration}ns)')
    return result
