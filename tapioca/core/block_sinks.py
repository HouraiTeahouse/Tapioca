from abc import abstractmethod
import os


class BlockSink():
    """An abstract class for writing blocks from BlockPipelines to persistent
    storage.
    """

    @abstractmethod
    def write_block(self, block_hash, block):
        raise NotImplementedError


class LocalStorageBlockSink(BlockSink):
    """A BlockSink that writes blocks directly to a local directory.

    Will populate a directory with files, one for each block with the block's
    hash hex digest as its name.

    This sink will not replace existing files. If a path collision occurs, it
    assumes that the block has already been written and will attempt to save
    disk IO by skipping the block.
    """
    def __init__(self, directory):
        self.directory = directory

    def write_block(self, block_hash, block):
        path = os.path.join(self.directory, block_hash.hex())
        if os.path.exists(path):
            # If the block is already stored, save some disk IO.
            return
        with open(path, 'wb') as f:
            f.write(block)
            f.truncate()


class ObjectStorageBlockSink(BlockSink):
    """A BlockSink that writes blocks to a remote object store.

    This sink will always write the block to the backing object store to
    attempt to minimize the number of API calls made.
    """
    def __init__(self, bucket, prefix=None):
        self.bucket = bucket
        self.prefix = prefix

    def write_block(self, block_hash, block):
        path = block_hash.hex()
        if self.prefix is not None:
            path = os.path.join(self.prefix, path)
        self.bucket.upload_file(path, block)


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

    def write_block(self, block_hash, block):
        listeners = self.listeners.get(block_hash)
        if listeners is None:
            return
        # TODO(james7132): Parallelize this.
        for path, offset in listeners:
            path = os.path.join(self.root_dir, path)
            with open(path, 'wb') as f:
                f.seek(offset)
                f.write(block)

    def _build_block_listeners(self, manifest):
        max_block_size = manifest.max_block_size
        block_listeners = {}
        for file_info in manifest.files:
            for idx, block in enumerate(file_info.blocks):
                bucket = block_listeners.setdefault(block.hash, [])
                bucket.append((file_info.path, idx * max_block_size))
        return {block_hash: tuple(bucket) for block_hash, bucket in
                block_listeners.items()}

# TODO(james7132): Implement P2P block sink (IPFS?)
