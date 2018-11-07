package tapioca

type Manifest struct {
  Files   map[string]ManifestFile
}

type ManifestFile struct {
  Path    string
  Blocks  []ManifestBlock
}

type ManifestBlock struct {
  Hash    BlockHash
  Size    uint64
}
