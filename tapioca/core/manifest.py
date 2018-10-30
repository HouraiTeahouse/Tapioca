import os
import shutil
import itertools
import hashlib
from collections import namedtuple
from tapioca.core.manifest_pb2 import ManifestProto
from tapioca.core.manifest_pb2 import ManifestItemProto
from tapioca.core.manifest_pb2 import ManifestBlockProto
from tapioca.core.sources import DirectorySource


HASH_ALG = hashlib.sha512


def hash_block(block):
    return HASH_ALG(block).digest()


def _generate_file_paths(root, manifest, path):
    for item in root.children:
        path.push(item.name)
        if item.WhichOneOf("descriptor") == "file":
            yield ("/".join(path), item)
        else:
            yield from _generate_file_paths(item.directory, manifest, path)
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
            print('Collision found: {block_hash.hex()}')
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
        manifest_proto.blocks.clear()
        manifest_proto.blocks.extend(block.to_proto() for block in self.blocks)


class ItemTrie():
    """A trie of items within a manifest."""

    def __init__(self, name, parent=None):
        if parent is None:
            self.item = ManifestItemProto()
        else:
            self.item = parent.directory.children.add()
        self.item.name = name
        self.children = {}

    def add(self, item):
        """Adds a path to the trie. Returns the created Item."""
        norm = os.path.normpath(item.path)
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
        manifest.items.clear()
        manifest.items.extend(self.item.children)


class BlockInfo(namedtuple("BlockInfo", "hash size")):

    @staticmethod
    def from_proto(proto):
        return BlockInfo(hash=proto.hash, size=proto.size)

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

        file_proto = item.file
        file_proto.hash = self.hash
        file_proto.size = self.size
        file_proto.block_ids.clear()
        file_proto.blocks.extend(block_ids)
        return item


class Manifest():

    def __init__(self):
        self.files = []

    def add_file(self, file_info):
        self.files.append(file_info)

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
        return manifest_proto

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
        current_manifest = ManifestFactory().build(DirectorySource(root_dir))
        return not ManifestDiff(self, current_manifest).has_changed()


class FileBuilder():

    def __init__(self, path):
        self.path = path
        self._hash_alg = HASH_ALG()
        self.blocks = []
        self.size = 0

    def process_block(self, block):
        info = BlockInfo(hash=hash_block(block), size=len(block))
        self.size += info.size
        self._hash_alg.update(block)
        return info

    def build(self):
        return FileInfo(path=self.path, blocks=tuple(self.blocks),
                        hash=self._hash_alg.digest(), size=self.size)


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


class ManifestFactory():
    """A factory class that generates manifests for builds."""

    def build(self, source, template=None):
        """Builds a manifest from a provided TapiocaSource.

        Params:
          source (TapiocaSource):
            A tapioca source to read files from.
          template (Manifest):
            A template manifest to copy base parameters from.

        Returns
          Manifest:
            The built manifest from the provided source.
        """
        manifest = Manifest()
        with source as src:
            # TODO(james7132): Parallelize this process
            for file_path in src.get_files():
                builder = FileBuilder(file_path)
                for block in self.source.get_blocks(file_path):
                    builder.process_block(block)
                manifest.add_file(builder.build())
        return manifest
