from collections import namedtuple
import contextlib
import logging
import tempfile


log = logging.getLogger(__name__)


class BuildDeployment(namedtuple("BuildDeployment",
                                 "project branch build http_request")):

    @staticmethod
    def from_http_request(request):
        parameters = request.match_info
        return BuildDeployment(project=parameters['project'],
                               branch=parameters['handler'],
                               build=parameters.get('build'),
                               http_request=request)


async def download_large_file(session, url, file_descriptor,
                              chunk_size=1024**2):
    async with session.get(url) as request:
        while True:
            chunk = await request.content.read(chunk_size)
            if not chunk:
                break
            file_descriptor.write(chunk)


@contextlib.asynccontextmanager
async def download_temporary_file(session, url, *args, **kwargs):
    with tempfile.SpooledTemporaryFile(*args, **kwargs) as file_descriptor:
        log.info(f'Downloading {url} to temp file...')
        await download_large_file(session, file_descriptor)
        yield file_descriptor
