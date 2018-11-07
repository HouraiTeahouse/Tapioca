package manifests

import (
	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
)

type Manifest struct {
	Files map[string]ManifestFile
}

type ManifestFile struct {
	Path  string
	Block []ManifestBlock
}

type ManifestBlock struct {
	Hash blocks.BlockHash
	Size uint64
}
