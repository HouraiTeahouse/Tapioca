import asyncio
import zipfile
from tapioca.core.manifest import
from tapioca.core.manifest_sources import ZipFileSource
from tapioca.core.block_sources import ManifestBuilderBlockSource
from tapioca.core.block_processors import GzipBlockProcessor
from tapioca.core.block_sinks import ObjectStorageBlockSink
import tapioca.deploy.util as util


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
            manifest = source.build_manifest()

            # Upload


    def get_download_url(self, request):
        raise NotImplementedError
