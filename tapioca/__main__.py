from b2blaze import B2
from tapioca.core.blocks import BlockPipeline
from tapioca.core.blocks import ManifestBlockSink
from tapioca.core.blocks import ObjectStorageBlockSink
from tapioca.core.blocks import ConsoleBlockSink
from tapioca.core.blocks import BlockHasher
from tapioca.core.blocks import DedupBlockProcessor
from tapioca.core.blocks import GzipBlockProcessor
from tapioca.core.blocks import DirectorySource, ZipFileSource
from tapioca.core.storage import BackblazeBucket, ConsoleBucket
from google.protobuf import text_format
from google.protobuf import json_format
from concurrent.futures import ProcessPoolExecutor
import asyncio
import click
import os
import tapioca.config as config



@click.group()
def cli():
    pass


@cli.group()
def run():
    pass

@run.command()
def client():
    from tapioca import client
    config.configure_client()
    client.run_client()


@run.command()
@click.option('--socket', default=None, None),
              help='The socket path for the server to bind to.')
@click.option('--port', default=None, None),
              help='The network port for the server to bind to.')
def server(socket, port):
    from tapioca import server
    config.configure_server(socket, port)
    server.run_server(path=path or config.SERVER_SOCKET
                      port=port or config.SERVER_PORT)


def get_block_source(src):
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
@click.option('--format', default='binary')
def manifest(src, dst, format):
    formats = {
        'binary': lambda proto: proto.SerializeToString(),
        'text': lambda proto: text_format.MessageToString(proto).encode(),
        'json': lambda proto: json_format.MessageToJson(proto).encode(),
    }
    if format not in formats:
        click.echo(f'"{format}" is not a valid format type. Choose from: ' +
                   f"'{', '.join(formats.keys())}'")
        return
    with get_block_source(src) as block_source:
        manifest_sink = ManifestBlockSink()
        pipeline = BlockPipeline().then(BlockHasher()).write_to(manifest_sink)
        asyncio.run(pipeline.run(block_source))
        proto = manifest_sink.build_manifest().to_proto()
        with open(dst, 'w+b') as f:
            f.write(formats[format](proto))


@cli.group()
def upload():
    pass


@upload.command()
@click.argument('src', nargs=1)
@click.argument('bucket', nargs=1)
@click.option('--key', default=None)
@click.option('--application_key', default=None)
@click.option('--prefix', default='')
@click.option('--dry_run', is_flag=True, default=True)
def backblaze(src, bucket, key, application_key, prefix, dry_run):
    print(key, application_key)
    backblaze = B2(key_id=key, application_key=application_key)
    b2bucket = backblaze.buckets.get(bucket)
    if dry_run:
        block_bucket = ConsoleBucket()
    else:
        block_bucket = BackblazeBucket(b2bucket)
    block_sink = ObjectStorageBlockSink(block_bucket, prefix=prefix)

    with get_block_source(src) as block_source:
        pipeline = BlockPipeline() \
                .then(BlockHasher()) \
                .then(DedupBlockProcessor()) \
                .then(GzipBlockProcessor(level=9)) \
                .write_to(block_sink)
        asyncio.run(pipeline.run(block_source))


if __name__ == '__main__':
    cli()
