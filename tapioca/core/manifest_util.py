import os


def _get_directory_paths(root, path):
  for item in root.children:
    path.push(item.name)
    if item.WhichOneOf("descriptor") == "block_id":
      yield "/".join(path)
    else:
      yield from _get_directory_paths(item, path)
    path.pop()


def get_file_paths(manifest):
  """Enumreates all paths of files described by a manifest."""
  for root in manifest.items:
    yield from _get_directory_paths(root, [root.name])


def get_directory_paths(manifest):
  """Enumreates all directories described by a manifest."""
  return {os.dirname(path) for path in get_file_paths(manifest)}


def make_dirs(manifest, basedir=""):
  """Creates all directories needed to build a manifest."""
  for dir_path in get_directory_paths(manifest):
    full_path = os.path.join(basedir, dir_path)
    if os.path.exists(full_path):
      if os.isdir(full_path):
        continue
      else:
        # TODO(james7132): Handle error
        pass
    os.makedirs(full_path)
