from tapioca.core.manifest import ManifestBuilder
import logging

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

    def deduplicated(self):
        return


class BlockHashSource(BlockSource):

    def __init__(self, hashes):
        self.hashes = hashes

    def get_blocks(self):
        for block_hash in self.hashes:
            yield (block_hash, None)


class DeduplicatedBlockSource(BlockSource):

    def __init__(self, base_source):
        self.base_source = base_source

    def get_blocks(self):
        seen_hashes = set()
        for block_hash, block in self.base_source.get_blocks():
            if block.hash in seen_hashes:
                continue
            yield block_hash, block
            seen_hashes.add(block.hash)


class FileBlockSource(BlockSource):

    def __init__(self, file_descriptor):
        self.file_descriptor = file_descriptor

    def get_blocks(self):
        for file_info in self.manifest.files:
            for block in file_info.blocks:
                if block.hash in seen_hashes:
                    continue
                yield (block.hash, None)
                seen_hashes.add(block.hash)


class ManifestBlockSource(BlockSource):

    def __init__(self, manifest):
        self.manifest = manifest

    def get_blocks(self):
        for file_info in self.manifest.files:
            for block in file_info.blocks:
                yield (block.hash, None)


class ManifestDiffBlockSource(BlockSource):

    def __init__(self, diff):
        self.diff = diff

    def get_blocks(self):
        for file_diff in self.diff.changed_files:
            for _, block_hash in file_diff.changed_blocks:
                yield (block_hash, None)


class ManifestBuilderBlockSource(BlockSource):

    def __init__(self, source):
        self.source = source
        self.manifest_builder = ManifestBuilder()

    def get_blocks(self):
        builder = self.manifest_builder
        for block_hash, block in builder.add_source_iter(self.source):
            yield (block_hash, block)

    def build_manifest(self):
        return self.manifest_builder.build()
