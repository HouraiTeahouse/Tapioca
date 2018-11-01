import os
import contextlib
import tempfile
import aiohttp


async def download_large_file(session, file_descriptor, chunk_size=1024**2):
    async with session.get(url) as request:
        while True:
            chunk = await request.content.read(chunk_size)
            if not chunk:
                break
            file_descriptor.write(chunk)


@contextlib.asynccontextmanager
async def download_temporary_file(session, url, *args, **kwargs):
    with tempfile.TemporaryFile(filename, *args, **kwargs) as fd:
        await download_large_file(session, fd)
        yield fd
