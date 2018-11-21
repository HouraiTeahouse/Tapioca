package manifests

import (
	"fmt"
	"sync"

	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
	"github.com/HouraiTeahouse/Tapioca/pkg/proto"
)

type ManifestBuilder struct {
	Files map[string]*FileBuilder
	mutex sync.Mutex
}

func (b *ManifestBuilder) AddFile(file string) *FileBuilder {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.Files == nil {
		b.Files = make(map[string]*FileBuilder)
	}

	if builder, ok := b.Files[file]; ok {
		return builder
	}
	builder := new(FileBuilder)
	builder.Path = file
	b.Files[file] = builder
	return builder
}

func (b *ManifestBuilder) Build() (*Manifest, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	manifest := new(Manifest)
	manifest.Files = make(map[string]ManifestFile)
	for path, file := range b.Files {
		file, err := file.Build()
		if err != nil {
			return nil, err
		}
		manifest.Files[path] = *file
	}
	return manifest, nil
}

// Adds the information from a protobuffer into the builder.
func (b *ManifestBuilder) FromProto(p *proto.ManifestProto) error {
	blocks, err := convertBlocks(p)
	if err != nil {
		return err
	}
	for name, item := range p.GetItems() {
		err := b.buildFiles(item, name, blocks)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *ManifestBuilder) buildFiles(p *proto.ManifestItemProto,
	path string,
	blocks []ManifestBlock) error {

	blocksRanges := p.GetBlocks()
	if len(blocksRanges) <= 0 {
		// File: Create ManifestFile
		file := b.AddFile(path)
		var blockIndex uint64 = 0
		for _, blockRange := range blocksRanges {
			start := blockRange.StartId
			end := start + blockRange.Size
			if start < 0 || end > uint64(len(blocks)) {
				return fmt.Errorf("Invalid block range: outside of valid block IDs.")
			}
			for i := start; i < end; i++ {
				err := file.AddBlock(blockIndex, &blocks[i])
				if err != nil {
					return err
				}
				blockIndex++
			}
		}
		return nil
	} else {
		// Directory: Recurse further
		for name, child := range p.GetChildren() {
			err := b.buildFiles(child, path+ManifestPathDelimiter+name, blocks)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func convertBlocks(p *proto.ManifestProto) ([]ManifestBlock, error) {
	protoBlocks := p.GetBlocks()
	blocks := make([]ManifestBlock, len(protoBlocks))
	for idx, proto := range protoBlocks {
		block, err := blockFromProto(proto)
		if err != nil {
			return nil, err
		}
		blocks[idx] = *block
	}
	return blocks, nil
}

func blockFromProto(p *proto.ManifestBlockProto) (*ManifestBlock, error) {
	hash, err := blocks.HashFromSlice(p.GetHash())
	if err != nil {
		return nil, err
	}
	return &ManifestBlock{
		Hash: *hash,
		Size: p.GetSize(),
	}, nil
}

type FileBuilder struct {
	Path   string
	Blocks map[uint64]ManifestBlock
	mutex  sync.Mutex
}

func (b *FileBuilder) AddBlock(blockId uint64, block *ManifestBlock) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.Blocks == nil {
		b.Blocks = make(map[uint64]ManifestBlock)
	}

	if _, ok := b.Blocks[blockId]; ok {
		return fmt.Errorf("File already has the block for ID %d written.", blockId)
	}
	b.Blocks[blockId] = *block
	return nil
}

func (b *FileBuilder) Build() (*ManifestFile, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	file := ManifestFile{Path: b.Path}

	var id uint64 = 0
	for {
		if block, ok := b.Blocks[id]; ok {
			file.Blocks = append(file.Blocks, block)
		} else {
			break
		}
		id++
	}
	if len(file.Blocks) != len(b.Blocks) {
		return nil, fmt.Errorf("Invalid FileBilder (%s): missing blocks: %d vs %d",
			file.Path, len(file.Blocks), len(b.Blocks))
	}
	return &file, nil
}

func (b *ManifestBuilder) BuildProto() (*proto.ManifestProto, error) {
	manifest, err := b.Build()
	if err != nil {
		return nil, err
	}
	return manifest.ToProto()
}
