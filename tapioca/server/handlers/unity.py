from tapioca.core.blocks import BlockPipeline
from tapioca.core.blocks import GzipBlockProcessor
from tapioca.core.blocks import ObjectStorageBlockSink
from tapioca.core.blocks import ManifestBuilderBlockSource
from tapioca.core.manifest_sources import ZipFileSource
import tapioca.server.db as db
import tapioca.server.util as util
import zipfile


class UnityCloudBuildHandler():

    async def run(self, request):
        download_url = self.get_download_url(request)
        async with util.download_temporary_file(download_url) as fd:
            zip_file = zipfile.ZipFile(fd)
            b2_bucket = None

            pipeline = BlockPipeline() \
                .with_processor(GzipBlockProcessor(level=9)) \
                .write_to(ObjectStorageBlockSink(b2_bucket))

            source = ManifestBuilderBlockSource(ZipFileSource(zip_file))
            await pipeline.run(source)
            await db.save_manifest(source.build_manifest(), request)

    def get_download_url(self, request):
        raise NotImplementedError
