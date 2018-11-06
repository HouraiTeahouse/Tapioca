import logging

log =  logging.getLogger(__name__)

class BlockBucket():
    """An abstract base class for interacting with object stores."""

    def upload_file(self, path, data):
        raise NotImplementedError


class ConsoleBucket():
    """A bucket that logs and prints the file location sent"""

    def upload_file(self, path, data):
        msg = f'Saving block to: "{path}"'
        log.info(msg)
        print(msg)


from tapioca.core.storage.backblaze import BackblazeBucket
