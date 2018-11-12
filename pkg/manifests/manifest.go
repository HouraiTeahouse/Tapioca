package manifests

import (
	"fmt"
	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
	"github.com/HouraiTeahouse/Tapioca/pkg/proto"
	"strings"
)

const ManifestPathDelimiter = "/"

type Manifest struct {
	Files map[string]ManifestFile
}

type ManifestFile struct {
	Path   string
	Blocks []ManifestBlock
}

type ManifestBlock struct {
	Hash blocks.BlockHash
	Size uint64
}

func (m *Manifest) ToProto() (*proto.ManifestProto, error) {
	manifestProto := new(proto.ManifestProto)
	blockIds := make(map[string]uint64)
	for path, file := range m.Files {
		item, err := insertFile(manifestProto, path)
		if err != nil {
			return nil, err
		}
		var blockRange *proto.ManifestBlockRange
		for _, block := range file.Blocks {
			hash := block.Hash.String()
			blockProto := block.ToProto()
			id, ok := blockIds[hash]
			if !ok {
				manifestProto.Blocks = append(manifestProto.Blocks, blockProto)
				id = uint64(len(manifestProto.Blocks))
				blockIds[hash] = id
			}
			if blockRange == nil {
				blockRange = &proto.ManifestBlockRange{
					StartId: id,
					Size:    1,
				}
			} else if id == blockRange.StartId+blockRange.Size {
				// Block is the next in line, extend the block range
				blockRange.Size++
			} else {
				// Block is discontinuous
				item.Blocks = append(item.Blocks, blockRange)
				blockRange = &proto.ManifestBlockRange{
					StartId: id,
					Size:    1,
				}
			}
			if blockRange != nil {
				item.Blocks = append(item.Blocks, blockRange)
			}
		}
	}
	return manifestProto, nil
}

func (b *ManifestBlock) ToProto() *proto.ManifestBlockProto {
	return &proto.ManifestBlockProto{
		Hash: b.Hash.AsSlice(),
		Size: b.Size,
	}
}

func insertFile(p *proto.ManifestProto, path string) (*proto.ManifestItemProto, error) {
	parts := strings.Split(path, ManifestPathDelimiter)

	if p.Items == nil {
		p.Items = make(map[string]*proto.ManifestItemProto)
	}
	directory := p.Items
	for {
		if len(parts) <= 0 {
			return nil, fmt.Errorf("Cannot create file with an empty path")
		}
		name := parts[0]
		if len(parts) == 1 {
			if name == "" {
				return nil, fmt.Errorf("Cannot create file with an empty name")
			}
			item := &proto.ManifestItemProto{}
			directory[name] = item
			return item, nil
		} else {
			item, ok := directory[name]
			if !ok {
				item = &proto.ManifestItemProto{}
				directory[name] = item
			}
			if item.Children == nil {
				item.Children = make(map[string]*proto.ManifestItemProto)
			}
			directory = item.Children
			parts = parts[1:]
		}
	}
}
