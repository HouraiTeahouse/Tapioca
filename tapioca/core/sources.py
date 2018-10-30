import collections
import os
import zipfile
import mmap
from abc import abstractmethod

CHUNK_SIZE = 1024**3

class TapiocaSource():

  def __init__(self):
    pass

  def __enter__(self):
    return self

  def exit(self, *args):
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
    parent_path = os.path.join(self.root, path)
    for child in os.scandir(parent_path):
      yield child

  def get_blocks(self, path, block_size):
    file_path = os.path.join(self.root, path)
    # TODO(james7132): See if mmap works here
    with open(file_path, 'rb') as f:
      for block in iter(lambda: f.read(block_size), b''):
        yield block


class ZipSource(TapiocaSource):

  def __init__(self, zip_file):
    self.zip_file = zip_file

  def get_files(self):
    for info in self.zip_file.infolist():
      if info.is_dir():
        continue
      yield info.filename

  def get_blocks(self, path, block_size):
    with self.zip_file.open(path) as f:
      for block in iter(lambda: f.read(block_size), b''):
        yield block

  def __enter__(self):
    self.zip_file.__enter__()
    return self

  def __exit__(self, exc_type, value, traceback):
    self.zip_file.__exit__(exc_type, value, traceback)
