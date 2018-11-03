from collections import namedtuple
from tapioca.core import hash_block, HASH_ALG, BLOCK_SIZE
from tapioca.core.manifest_pb2 import ManifestBlockProto
from tapioca.core.manifest_pb2 import ManifestItemProto
from tapioca.core.manifest_pb2 import ManifestProto
import itertools
import os
import shutil
import logging


log = logging.getLogger(__name__)


def _generate_file_paths(root, manifest, path):
    for item in root.children:
        path.push(item.name)
        # Item is a file if it has defined block ids
        if len(item.block_ids) > 0:
            assert len(item.children) <= 0
            yield ("/".join(path), item)
        else:
            yield from _generate_file_paths(item, manifest, path)
        path.pop()


def _generate_manifest_paths(manifest):
    """Enumreates the information of all of files described by a manifest."""
    for root in manifest.items:
        yield from _generate_file_paths(root, manifest, [root.name])


class BlockRegistry():
    """A mapping of blocks.

    Used for deduplication of blocks within a project described by a manifest.
    """

    def __init__(self, parent=None):
        self.blocks = []
        self._block_map = {}
        self.parent = parent

    def _register(self, block):
        if block.hash in self._block_map:
            log.info('Collision found: {block.hash.hex()}')
            idx = self._block_map[block.hash]
            assert block.size == self.blocks[idx].size
            return idx
        else:
            block_id = len(self.blocks)
            self.blocks.append(block)
            self._block_map[block.hash] = block_id
            return block_id

    def get_id(self, block):
        return self._block_map.get(block.hash)

    def register(self, block):
        """Registers a block within the registry.

        This can be somewhat computationally intensive as the binary block will
        be hashed.

        If a parent block registry was provided at construction, the block will
        also be registered with the parent registry.

        Params:
          block (bytes):
            a bytes-like objects to register.

        Returns:
          int:
            a unique integer ID for the block within the registry.
        """
        if self.parent is not None:
            self.parent._register(block)
        return self._register(block)

    def populate_manifest(self, manifest_proto):
        """Populates a manifest with block metadata.

        Params:
          manifest (Manifest):
            A manifest proto to populate.
        """
        del manifest_proto.blocks[:]
        manifest_proto.blocks.extend(block.to_proto() for block in self.blocks)


class ItemTrie():
    """A trie of items within a manifest."""

    def __init__(self, name='', parent=None):
        if parent is None:
            self.item = ManifestItemProto()
        else:
            self.item = parent.item.children.add()
        self.item.name = name
        self.children = {}

    def add(self, path):
        """Adds a path to the trie. Returns the created Item."""
        norm = os.path.normpath(path)
        path = norm.split(os.sep)
        path.reverse()

        # TODO(james7132): Handle errors

        current = self
        while len(path) > 0:
            prefix = path.pop()
            if prefix not in self.children:
                child = ItemTrie(prefix, parent=current)
                current.children[prefix] = child
            current = current.children[prefix]
        return current.item

    def populate_manifest(self, manifest):
        """Populates a manifest with item metadata.

        Params:
          manifest (Manifest):
            A manifest proto to populate.
        """
        del manifest.items[:]
        manifest.items.extend(self.item.children)


class BlockInfo(namedtuple("BlockInfo", "hash size")):

    @staticmethod
    def from_proto(proto, manifest=None):
        size = proto.size
        if manifest is not None and not proto.HasField('size'):
            size = manifest.max_block_size
        return BlockInfo(hash=proto.hash, size=size)

    def to_proto(self):
        proto = ManifestBlockProto()
        proto.hash = self.hash
        proto.size = self.size
        return proto


class FileInfo(namedtuple('FileInfo', 'path blocks hash size')):

    @staticmethod
    def from_proto(proto, manifest, path=None):
        return FileInfo(
          path=proto.name if path is None else path,
          blocks=tuple(BlockInfo.from_proto(manifest.blocks[idx])
                       for idx in proto.block_ids),
          hash=proto.hash,
          size=proto.size
        )

    def to_proto(self, block_registry, item_trie):
        item = item_trie.add(self.path)
        block_ids = (block_registry.get_id(block) for block in self.blocks)

        item.hash = self.hash
        item.size = self.size
        del item.block_ids[:]
        item.block_ids.extend(block_ids)
        return item


class Manifest():

    def __init__(self, files, max_block_size):
        self.files = files
        self.max_block_size = max_block_size

    @staticmethod
    def from_proto(proto):
        manifest = Manifest()
        for path, item in _generate_file_paths(manifest):
            file_info = FileInfo.from_proto(item, manifest, path)
            manifest.add_file(file_info)
        return manifest

    def to_proto(self):
        block_registry = BlockRegistry()
        item_trie = ItemTrie()

        for file_info in self.files:
            for block in file_info.blocks:
                block_registry.register(block)
            file_info.to_proto(block_registry, item_trie)

        manifest_proto = ManifestProto()
        block_registry.populate_manifest(manifest_proto)
        item_trie.populate_manifest(manifest_proto)

        # Clean up redundant information in block sizes
        manifest_proto.max_block_size = self.max_block_size
        for block in manifest_proto.blocks:
            if block.size == self.max_block_size:
                block.ClearField('size')
        return manifest_proto

    @property
    def blocks(self):
        seen_blocks = set()
        for file_info in self.files:
            for block in file_info.blocks:
                if block.hash in seen_blocks:
                    continue
                yield block
                seen_blocks.add(block.hash)

    @property
    def total_space(self):
        """Gets the total space used by the files described by the manifest in
        bytes.
        """
        return sum(file_info.size for file_info in self.files)

    def preallocate_space(self, root_dir):
        """Preallocates space for the files described by a manifest."""
        # Make sure there is enough disk space to allocate the files.
        #
        # TODO(james7132): Make this take into consideration already written
        # files in the root directory
        disk_usage = shutil.disk_usage(root_dir)
        if disk_usage.free < self.total_space:
            raise RuntimeError("Cannot allocate more space to drive.")

        for file_info in self.files:
            full_path = os.path.join(root_dir, file_info.path)

            # Make sure the containing directory has been created.
            os.makedirs(os.path.dirname(full_path))

            #  Create a file of the approriate size
            with open(full_path, 'wb') as f:
                f.seek(file_info.size)
                f.write(b'\0')
                f.truncate()

    def verify_installation(self, root_dir):
        """Verify if the installation of a build matches a reference
        manifest.
        """
        raise NotImplementedError
        # current_manifest = ManifestBuilder() \
        # .add_source(DirectoryManifestSource(root_dir)) \
        # .build()
        # return not ManifestDiff(self, current_manifest).has_changed()


class FileDiff():

    def __init__(self, remote_file=None, current_file=None):
        self.deleted = remote_file is None
        self.new_size = remote_file.size if remote_file is not None else None
        self.changed_blocks = self._generate_changed_blocks(remote_file,
                                                            current_file)

    def _generate_changed_blocks(remote, current):
        changed_blocks = {}

        r_blocks = remote.blocks if remote is not None else ()
        c_blocks = current.blocks if current is not None else ()

        blocks = itertools.zip_longest(r_blocks, c_blocks)
        for idx, (r_block, c_block) in enumerate(blocks):
            r_hash = r_block.hash if r_block is not None else None
            c_hash = c_block.hash if c_block is not None else None
            if c_hash != r_hash:
                changed_blocks[idx] = (c_hash, r_hash)
        return changed_blocks

    @property
    def has_changed(self):
        return self.deleted or len(self.changed_blocks) > 0

    def apply(self):
        raise NotImplementedError


class ManifestDiff():

    def __init__(self, remote_manifest, current_manifest):
        self.changed_files = self._generate_changed_files(remote_manifest,
                                                          current_manifest)

    def _generate_changed_files(remote, current):
        r_files = {file_info.path: file_info for file_info in remote.files}
        c_files = {file_info.path: file_info for file_info in current.files}

        paths = set(r_files.keys()) + set(c_files.keys)

        diffs = {path: FileDiff(r_files.get(path), c_files.get(path))
                 for path in paths}
        return {path: diff for path, diff in diffs.items() if diff.has_changed}

    @property
    def has_changed(self):
        return len(self.changed_files) > 0

    def apply(self):
        for file_diff in self.changed_files:
            file_diff.apply()


class ManifestBuilder():
    """A builder object for Manifests."""

    def __init__(self, max_block_size=BLOCK_SIZE):
        self.files = {}
        self.max_block_size = max_block_size

    def add_file(self, path):
        """Adds a file to the manifest

        Parameters:
          path (str): the relative path to the root of the build the file is
                      located at. Does not support '.', '..' relative path
                      elements.

        Returns: a FileInfoBuilder object for the file.
        """
        builder = self.files.get(path)
        if builder is None:
            builder = FileInfoBuilder(path, self.max_block_size)
            self.files[path] = builder
        return builder

    def build(self):
        """Builds the Manifest object."""
        files = (file_builder.build() for file_builder in self.files.values())
        return Manifest(tuple(files), self.max_block_size)


class FileInfoBuilder():
    """A builder object for FileInfos."""

    def __init__(self, path, max_block_size):
        self.path = path
        self._hash_alg = HASH_ALG()
        self.blocks = []
        self.size = 0
        self.max_block_size = max_block_size

    def append_block(self, block_info):
        """Adds a block info to the end of the file.

        Raises a RuntimeError if the block is bigger than the max_block_size
        for the corresponding manifest.

        Parameters:
            block_info (BlockInfo): the block metadata for the next block in
                                    the file.
        """
        if block_info.size > self.max_block_size:
            raise RuntimeError(
                    "Attempted to add a block bigger than the manifest's"
                    " max_block_size")

        self.blocks.append(block_info)
        self.size += block_info.size

    def update_hash(self, block):
        """Updates the file hash with the next file block.

        Parameters:
            block (bytes-like object): the next block in the object.
        """
        self._hash_alg.update(block)

    def process_block(self, block):
        """A shortcut for proccessing the next block in the file.
        Constructs a BlockInfo representing the block, hashing the block,
        appends it to the file blocks, and updates the file hash state.

        Parameters:
            block (bytes-like object): the next block in the object.

        Returns: the created BlockInfo.
        """
        block_info = BlockInfo(hash=hash_block(block), size=len(block))
        self.append_block(block_info)
        self.update_hash(block)
        return block_info

    def build(self):
        """Builds a FileInfo."""
        return FileInfo(path=self.path, blocks=tuple(self.blocks),
                        hash=self._hash_alg.digest(), size=self.size)
