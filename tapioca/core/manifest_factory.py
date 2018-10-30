import hashlib
import os
from tapioca.core.manifest_pb2 import Manifest, Block, Item

HASH_ALG = hashlib.sha512


def hash_block(block):
    return HASH_ALG(block).digest()


class BlockRegistry():
    """A mapping of blocks.

    Used for deduplication of blocks within a project described by a manifest.
    """

    def __init__(self, parent=None):
        self.blocks = []
        self._block_map = {}
        self.parent = parent

    def _register(self, block, block_hash):
        if block_hash in self._block_map:
            print('Collision found: {block_hash.hex()}')
            return self._block_map[block_hash]
        else:
            block = Block()
            block.size = len(block)
            block.hash = block_hash
            block_id = len(self.blocks)
            self._block_map = block_id
            self.blocks.append(block)
        return block_id

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
        block_hash = hash_block(block)
        if self.parent is not None:
            self.parent._register(block, block_hash)
        return self._register(block, block_hash), block_hash

    def populate_manifest(self, manifest):
        """Populates a manifest with block metadata.

        Params:
          manifest (Manifest):
            A manifest proto to populate.
        """
        manifest.blocks.clear()
        manifest.blocks.extend(self.blocks)


class ItemTrie():
    """A trie of items within a manifest."""

    def __init__(self, item=None, parent=None):
        if parent is None:
            self.item = Item()
        else:
            self.item = parent.item.children.add()
        if item is not None:
            self.item.CopyFrom(item)
        self.children = {}

    def add(self, item):
        """Adds a file to the trie."""
        norm = os.path.normpath(item.path)
        path = norm.split(os.sep)
        path.reverse()

        current = self
        while len(path) > 0:
            prefix = path.pop()
            if prefix not in self.children:
                child = ItemTrie(parent=current)
                child.item.name = prefix
                current.children[prefix] = child
            current = current.children[prefix]
        item.name = prefix
        current.item = item

    def populate_manifest(self, manifest):
        """Populates a manifest with item metadata.

        Params:
          manifest (Manifest):
            A manifest proto to populate.
        """
        manifest.items.clear()
        manifest.items.extend(self.item.children)


class ManifestFactory():
    """A factory class that generates manifests for builds."""

    def __init__(self, block_registry=None, block_processors=None):
        self.block_registry = block_registry
        self.block_processors = block_processors or []

    def _create_file(self, path, source, block_registry):
        item = Item()
        item.name = path

        hash_alg = HASH_ALG()
        for block in self.source.get_blocks(path):
            hash_alg.update(block)
            block_id, block_hash = block_registry.register(block)
            item.blocks.add(block_id)

            for processor in self.block_processors:
                processor(block, block_hash)

        item.hash = hash_alg.digest()
        return item

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
        items = ItemTrie()
        blocks = BlockRegistry(parent=self.block_registry)

        if template is not None:
            manifest.CopyFrom(template)

        with source as src:
            # TODO(james7132): Parallelize this process
            for file_path in src.get_files():
                items.add(self.create_file(file_path, source, blocks))

        blocks.populate_manifest(manifest)
        items.populate_manifest(manifest)
        return manifest
