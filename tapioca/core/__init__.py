import hashlib
import time
import logging

HASH_ALG = hashlib.sha256
BLOCK_SIZE = 1024 ** 2

log = logging.getLogger(__name__)


def hash_block(block):
    start = time.perf_counter_ns()
    result = HASH_ALG(block).digest()
    duration = time.perf_counter_ns() - start
    log.debug(f'Hashed block: {result.hex()} ({duration}ns)')
    return result
