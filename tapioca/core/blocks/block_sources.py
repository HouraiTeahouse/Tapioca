from tapioca.core import BLOCK_SIZE
from tapioca.core.blocks import FileBlockData
from zipfile import ZipFile
import logging
import os
log = logging.getLogger(__name__)


class BlockSource():
    """An abstract class for reading blocks into BlockPipelines."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def get_blocks(self):
        """Returns: iterable[FileBlockData]"""
        raise NotImplementedError


class InMemorySource(BlockSource):
    """An in memory BlockSource that reads from a provided iterable object.

    The populated fields depend on the provided iterable.
    """
    def __init__(self, block_datas):
        self.block_datas = block_datas

    def get_blocks(self):
        return self.block_datas


class FileBlockSource(BlockSource):
    """A BlockSource that reads blocks from a single file-like object.

    Populated fields: file, block, block_id, size.
    """

    def __init__(self, path, file_descriptor, block_size=BLOCK_SIZE):
        self.path = path
        self.block_size = block_size
        self.file_descriptor = file_descriptor or open(path, 'rb')

    def __enter__(self):
        self.file_descriptor.__enter__()
        return self

    def __exit__(self, *args):
        self.file_descriptor.__exit__(*args)

    def get_blocks(self):
        self.file_descriptor.seek(0)
        blocks = iter(lambda: self.file_descriptor.read(self.block_size), b'')
        for idx, block in enumerate(blocks):
            block_data = FileBlockData(file=self.path, block_id=idx)
            yield block_data.with_block(block)


class DirectorySource(BlockSource):
    """A BlockSource that reads blocks recursively from a directory.

    Populated fields: file, block, block_id, size.
    """

    def __init__(self, root, block_size=BLOCK_SIZE):
        self.root = root
        self.block_size = block_size

    def get_blocks(self):
        for parent, _, files in os.walk(self.root):
            for file_path in files:
                yield from self._get_file_blocks(file_path)

    def _get_file_blocks(self, path):
        file_descriptor = open(path, 'rb')
        rel_path = os.relpath(path, self.root)
        with FileBlockSource(rel_path, file_descriptor,
                             self.block_size) as src:
            yield from src.get_blocks()


class ZipFileSource(BlockSource):
    """A BlockSource that reads blocks from a zip file.

    Populated fields: file, block, block_id, size.
    """

    def __init__(self, path, block_size=BLOCK_SIZE):
        self.path = path
        self.block_size = block_size
        self.zip_file = None

    def __enter__(self):
        self.zip_file = ZipFile(self.path)
        self.zip_file.__enter__()
        return self

    def __exit__(self, *args):
        if self.zip_file is not None:
            self.zip_file.__exit__(*args)

    def get_blocks(self):
        info_list = self.zip_file.infolist()
        info_list = filter(lambda info: not info.is_dir(), info_list)
        for info in info_list:
            file_descriptor = self.zip_file.open(info.filename)
            with FileBlockSource(info.filename, file_descriptor) as src:
                yield from src.get_blocks()


class ManifestBlockSource(BlockSource):
    """A BlockSource that reads block metadata from a Manifest.

    Populated fields: file, block_id, hash, size.
    """

    def __init__(self, manifest):
        self.manifest = manifest

    def get_blocks(self):
        for file_info in self.manifest.files:
            for idx, block in enumerate(file_info.blocks):
                yield FileBlockData.from_block_info(block,
                                                    file=file_info.path,
                                                    block_id=idx)


class ManifestDiffBlockSource(BlockSource):
    """A BlockSource that reads block metadata from a ManifestDiff. Only
    enumerates the changed blocks.

    Populated fields: file, block_id, hash, size.
    """

    def __init__(self, diff):
        self.diff = diff

    def get_blocks(self):
        for file_diff in self.diff.changed_files:
            # FIXME: This is broken and the API for changed_blocks needs to be
            # updated
            for _, block_hash in file_diff.changed_blocks:
                yield FileBlockData(file=file_diff.path,
                                    hash=block_hash)
