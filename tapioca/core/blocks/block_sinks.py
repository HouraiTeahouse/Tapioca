from abc import abstractmethod
from queue import PriorityQueue
from threading import Lock
from tapioca.core import hash_encode
from tapioca.core.manifest import ManifestBuilder
import logging
import os

log = logging.getLogger(__name__)

class BlockSink():
    """An abstract class for writing blocks from BlockPipelines to persistent
    storage.
    """

    @abstractmethod
    def write_block(self, block_hash, block):
        """Write a block to the backing store. Can be an async or normal
        function.

        Async functions execute in the same thread that the writer is called
        from, so keeping the amount of CPU-bound computation to a minimum is
        paramount.

        Normal functions are run in a threadpool and block the thread until
        complete.
        """
        raise NotImplementedError


class NullBlockSink(BlockSink):
    """A BlockSink that does not write the blocks anywhere, and logs the hashes
    seen.
    """
    def write_block(self, block_data):
        log.info(f'Block: {hash_encode(block_data.block)}')


class ConsoleBlockSink(BlockSink):
    """A BlockSink that outputs block information to the console."""
    def __init__(self, fmt=None):
        self.format = fmt

    def write_block(self, block_data):
        if self.format is None:
            print(f'Block: {block_data}')
        else:
            print(self.format.format(**block_data._asdict()))


class LocalStorageBlockSink(BlockSink):
    """A BlockSink that writes blocks directly to a local directory.

    Will populate a directory with files, one for each block with the block's
    hash as its name.

    This sink will not replace existing files. If a path collision occurs, it
    assumes that the block has already been written and will attempt to save
    disk IO by skipping the block.
    """
    def __init__(self, directory):
        self.directory = directory

    def write_block(self, block_data):
        path = os.path.join(self.directory, hash_encode(block_data.hash))
        if os.path.exists(path):
            # If the block is already stored, save some disk IO.
            return
        with open(path, 'wb') as f:
            f.write(block_data.block)
            f.truncate()


class ObjectStorageBlockSink(BlockSink):
    """A BlockSink that writes blocks to a remote object store.

    This sink will always write the block to the backing object store to
    attempt to minimize the number of API calls made.
    """
    def __init__(self, bucket, prefix=None):
        self.bucket = bucket
        self.prefix = prefix

    def write_block(self, block_data):
        if block_data.hash is None:
            log.error('Trying to write block to object storage without a hash')
            return
        path = hash_encode(block_data.hash)
        if self.prefix is not None:
            path = os.path.join(self.prefix, path)
        # TODO(james7132): Implement upload retry logic
        self.bucket.upload_file(path, block_data.block)


class InstallationBlockSink(BlockSink):
    """A BlockSink that writes blocks to expected installation file locations.

    Any block not in the provided Manifest during the sink's construction will
    be skipped.

    This sink will always write blocks to the installation files and will not
    validate if the stored block at that location is already valid or not.
    """
    def __init__(self, root_dir, manifest):
        self.root_dir = root_dir
        self.listeners = self._build_block_listeners(manifest)

    def write_block(self, block_data):
        listeners = self.listeners.get(block_data.hash)
        if listeners is None:
            return
        # TODO(james7132): Parallelize this.
        for path, offset in listeners:
            path = os.path.join(self.root_dir, path)
            # TODO(james7132): Use aiofiles here
            with open(path, 'wb') as f:
                f.seek(offset)
                f.write(block_data.block)

    def _build_block_listeners(self, manifest):
        max_block_size = manifest.max_block_size
        block_listeners = {}
        for file_info in manifest.files:
            for idx, block in enumerate(file_info.blocks):
                bucket = block_listeners.setdefault(block.hash, [])
                bucket.append((file_info.path, idx * max_block_size))
        return {block_hash: tuple(bucket) for block_hash, bucket in
                block_listeners.items()}


class _FileAccumulator():
    """A accumulator class for building manfiest files in a multithreaded
    enviroment. FileInfoBuilder must be built sequentially over a given file,
    for which order is not guarenteed when processing the file across a
    threadpool. _FileAccumulator serializes the process using a PriorityQueue.
    """
    def __init__(self, file_builder):
        self.builder = file_builder
        self.next_block_id = 0
        self.pending_blocks = PriorityQueue()
        self.lock = Lock()

    def add_block(self, block_data):
        with self.lock:
            self.pending_blocks.put(block_data)
            self._advance_accumulator()

    def _advance_accumulator(self):
        # File **must** be locked to run this function

        # Peek at the fist item
        while self.pending_blocks.qsize() > 0 and \
              self.pending_blocks.queue[0].block_id == self.next_block_id:
            file_block = self.pending_blocks.get()
            self.builder.append_block(file_block.to_block_info())
            self.next_block_id += 1


class ManifestBlockSink(BlockSink):
    """A BlockSink that builds a Manifest based on the block stream."""
    def __init__(self):
        self.builder = ManifestBuilder()
        self.manifest_lock = Lock()
        self.file_accumulators = {}

    def write_block(self, block_data):
        log.info('Writing block to Manifest')
        with self.manifest_lock:
            file_accumulator = self.file_accumulators.get(block_data.file)
            if file_accumulator is None:
                file_builder = self.builder.add_file(block_data.file)
                file_accumulator = _FileAccumulator(file_builder)
                self.file_accumulators[block_data.file] = file_accumulator
        file_accumulator.add_block(block_data)

    def build_manifest(self):
        """Builds the manifest based on already processed block datas streamed
        into the sink.
        """
        with self.manifest_lock:
            return self.builder.build()

# TODO(): Implement P2P block sink (IPFS?)
