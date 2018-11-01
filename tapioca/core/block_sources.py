import logging
from tapioca.core.manifest import ManifestBuilder

log = logging.getLogger(__name__)


class BlockSource():
    """An abstract class for reading blocks into BlockPipelines to persistent
    storage.
    """
    def get_blocks(self):
        """
        Returns: iterable[(bytes, byte or None)]: (block hash, block data)
        """
        raise NotImplementedError


class BlockHashSource(BlockSource):

    def __init__(self, hashes):
        self.hashes = hashes

    def get_blocks(self):
        for block_hash in self.hashes:
            yield (block_hash, None)


class ManifestBlockSource(BlockSource):

    def __init__(self, manifest):
        self.manifest = manifest

    def get_blocks(self):
        seen_hashes = set()
        for file_info in self.manifest.files:
            for block in file_info.blocks:
                if block.hash in seen_hashes:
                    continue
                yield (block.hash, None)
                seen_hashes.add(block.hash)


class ManifestDiffBlockSource(BlockSource):

    def __init__(self, diff):
        self.diff = diff

    def get_blocks(self):
        seen_hashes = set()
        for file_diff in self.diff.changed_files:
            for _, block_hash in file_diff.changed_blocks:
                if block_hash in seen_hashes:
                    continue
                yield (block_hash, None)
                seen_hashes.add(block_hash)


class ManifestBuilderBlockSource(BlockSource):

    def __init__(self, source):
        self.source = source
        self.manifest_builder = ManifestBuilder()

    def get_blocks(self):
        return self.manifest_builder.add_source_iter(self.source)

    def build_manifest(self):
        return self.manifest_builder.build()
