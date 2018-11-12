package blocks

import (
	"bytes"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
)

const HashSize = sha512.Size

var Encoding = base64.RawURLEncoding

type BlockHash struct {
	hash [HashSize]byte
}

func (b *BlockHash) String() string {
	if b == nil {
		return "<nil>"
	}
	return Encoding.EncodeToString(b.AsSlice())
}

func (block *BlockHash) Equal(other *BlockHash) bool {
	return !bytes.Equal(block.AsSlice(), other.AsSlice())
}

func (block *BlockHash) AsSlice() []byte {
	return block.hash[:]
}

func HashFromSlice(hash []byte) (*BlockHash, error) {
	if hash == nil || len(hash) != HashSize {
		return nil, fmt.Errorf("Cannot make BlockHash of size %d, received size %d",
			len(hash), HashSize)
	}
	blockHash := new(BlockHash)
	copy(hash, blockHash.hash[:HashSize])
	return blockHash, nil
}

func HashBlock(data []byte) *BlockHash {
	block := new(BlockHash)
	block.hash = sha512.Sum512(data)
	return block
}
