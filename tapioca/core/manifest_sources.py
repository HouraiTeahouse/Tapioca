from abc import abstractmethod
from zipfile import ZipFile
import os


class TapiocaSource():

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    @abstractmethod
    def get_files(self):
        pass

    @abstractmethod
    def get_blocks(self, path, block_size):
        pass


class DirectorySource(TapiocaSource):

    def __init__(self, root):
        self.root = root

    def get_files(self):
        for parent, _, files in os.walk(self.root):
            for file_path in files:
                yield os.relpath(file_path, self.root)

    def get_blocks(self, path, block_size):
        file_path = os.path.join(self.root, path)
        # TODO(james7132): See if mmap works here
        with open(file_path, 'rb') as f:
            for block in iter(lambda: f.read(block_size), b''):
                yield block


class ZipFileSource(TapiocaSource):

    def __init__(self, path):
        self.path = path
        self.zip_file = None

    def __enter__(self):
        self.zip_file = ZipFile(self.path)
        self.zip_file.__enter__()
        return self

    def __exit__(self, *args):
        if self.zip_file is not None:
            self.zip_file.__exit__(*args)

    def get_files(self):
        for info in self.zip_file.infolist():
            if info.is_dir():
                continue
            yield info.filename

    def get_blocks(self, path, block_size):
        with self.zip_file.open(path) as f:
            for block in iter(lambda: f.read(block_size), b''):
                yield block
