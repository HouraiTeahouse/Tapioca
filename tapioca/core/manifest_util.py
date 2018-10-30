import os
import shutil
from tapioca.core.sources import DirectorySource
from tapioca.core.manifest_factory import ManifestFactory


class FileInfo():

    def __init__(self, manifest, item, path):
        self.path = path
        self.filename = item.name
        self.size = item.file.size
        self.file_hash = item.file.hash
        self.blocks = [manifest.blocks[idx] for idx in item.block_ids]


class FileDiff():

    def __init__(self, remote_file=None, current_file=None):
        self.deleted = remote_file is None
        self.new_size = remote_file.size if remote_file is not None else None
        self.changed_blocks = self._generate_changed_blocks(remote_file,
                                                            current_file)

    def _generate_changed_blocks(remote, current):
        if current is None:
            return {idx: (None, block.hash) for idx, block in
                    enumerate(remote.blocks)}

        changed_blocks = {}
        for idx in range(len(remote.blocks)):
            r_hash = remote.blocks[idx].hash
            c_hash = None
            if idx < len(current.blocks):
                c_hash = current.blocks[idx].hash
            if c_hash != r_hash:
                changed_blocks[idx] = (c_hash, r_hash)
        return changed_blocks

    def has_changed(self):
        return self.deleted or len(self.changed_blocks) > 0

    def apply(self):
        raise NotImplementedError


class ManifestDiff():

    def __init__(self, remote_manifest, current_manifest):
        self.changed_files = self._generate_changed_files(remote_manifest,
                                                          current_manifest)

    def _create_file_map(self, manifest):
        file_infos = get_file_infos(manifest)
        return {file_info.path: file_info for file_info in file_infos}

    def _generate_changed_files(self, remote, current):
        r_files = self._create_file_map(remote)
        c_files = self._create_file_map(current)

        paths = set(r_files.keys()) + set(c_files.keys)

        changed_files = {}
        for path in paths:
            diff = FileDiff(r_files.get(path), c_files.get(path))
            if diff.has_changed():
                changed_files[path] = diff
        return changed_files

    def has_changed(self):
        return len(self.changed_files) > 0

    def apply(self):
        for file_diff in self.changed_files:
            file_diff.apply()


def _get_files(root, manifest, path):
    for item in root.children:
        path.push(item.name)
        if item.WhichOneOf("descriptor") == "file":
            file_path = "/".join(path)
            yield FileInfo(manifest, item, file_path)
        else:
            yield from _get_files(item.directory, manifest, path)
        path.pop()


def get_file_infos(manifest):
    """Enumreates the information of all of files described by a manifest."""
    for root in manifest.items:
        yield from _get_files(root, manifest, [root.name])


def get_total_space(manifest):
    """Gets the total space used by the files described by the manifest in
    bytes.
    """
    return sum(file_info.size for file_info in get_file_infos(manifest))


def preallocate_space(manifest, root):
    """Preallocates space for the files described by a manifest."""
    # Make sure there is enough disk space to allocate the files.
    #
    # TODO(james7132): Make this take into consideration already written files
    # in the root directory
    disk_usage = shutil.disk_usage(root)
    if disk_usage.free < get_total_space(manifest):
        raise RuntimeError("Cannot allocate more space to drive.")

    for file_info in get_file_infos(manifest):
        full_path = os.path.join(root, file_info.path)

        # Make sure the containing directory has been created.
        os.makedirs(os.path.dirname(full_path))

        #  Create a file of the approriate size
        with open(full_path, 'wb') as f:
            f.seek(file_info.size)
            f.write(b'\0')
            f.truncate()


def verify_installation(root_dir, reference_manifest):
    """Verify if the installation of a build matches a reference manifest."""
    current_manifest = ManifestFactory().build(DirectorySource(root_dir))
    return not ManifestDiff(reference_manifest, current_manifest).has_changed()
