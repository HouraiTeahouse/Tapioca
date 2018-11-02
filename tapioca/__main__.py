from tapioca.core.block_pipeline import BlockPipeline
from tapioca.core.block_sinks import NullBlockSink
from tapioca.core.block_sources import ManifestBuilderBlockSource
from tapioca.core.manifest_sources import DirectorySource, ZipFileSource
from google.protobuf import text_format
import asyncio
import click
import logging
import os


logging.basicConfig(level=logging.DEBUG,
                    # format='%(asctime)s %(levelname)s %(message)s',
                    filename='tapioca.log',
                    filemode='w')


@click.group()
def cli():
    pass


@cli.group()
def run():
    pass


@run.command()
# @click.option('path', help='The socket path for the server to bind to.')
# @click.option('port', help='The network port for the server to bind to.')
def deploy_server(path, port):
    import tapioca.deploy as deploy
    deploy.run_server(path=path, port=port)


def get_manifest_source(src):
    src = os.path.abspath(src)
    if os.path.isdir(src):
        return DirectorySource(src)
    _, ext = os.path.splitext(src)
    if ext == '.zip':
        return ZipFileSource(src)
    return None


@cli.command()
@click.argument('src', nargs=1)
@click.argument('dst', nargs=1)
def manifest(src, dst):
    pipeline = BlockPipeline().write_to(NullBlockSink())
    block_source = ManifestBuilderBlockSource(get_manifest_source(src))
    asyncio.run(pipeline.run(block_source))
    proto = block_source.build_manifest().to_proto()
    with open(dst, 'w+b') as f:
        f.write(proto.SerializeToString())

cli()
