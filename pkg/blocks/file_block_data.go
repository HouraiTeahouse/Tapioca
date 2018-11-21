package blocks

import (
	"errors"
	"fmt"
)

type FileBlockData struct {
	File    string
	Offset  uint64
	BlockId uint64
	Size    uint64
	Hash    *BlockHash
	Data    *Block
}

func (block *FileBlockData) SetBlock(data []byte) {
	block.Data = CreateBlock(data)
	block.Size = block.Data.Size()
}

func (block *FileBlockData) ComputeHash() (*BlockHash, error) {
	if block.Data == nil {
		return nil, errors.New("Failed to hash file block: Block has no data block to hash.")
	}
	return block.Data.Hash(), nil
}

func (block *FileBlockData) UpdateHash() (*BlockHash, error) {
	hash, err := block.ComputeHash()
	if err != nil {
		return nil, err
	}
	block.Hash = hash
	return hash, nil
}

func (b *FileBlockData) String() string {
	if b == nil {
		return "Block: <nil>"
	}
	return fmt.Sprintf("Block: %s, %s, %d, %d, %d",
		b.Hash, b.File, b.BlockId, b.Offset, b.Size)
}
