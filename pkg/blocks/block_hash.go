package blocks

import (
	"bytes"
	"crypto/sha512"
	"encoding/base64"
)

const HashSize = sha512.Size

var Encoding = base64.RawURLEncoding

type BlockHash struct {
	hash [HashSize]byte
}

func (block *BlockHash) String() string {
	return Encoding.EncodeToString(block.AsSlice())
}

func (block *BlockHash) Equal(other *BlockHash) bool {
	return !bytes.Equal(block.AsSlice(), other.AsSlice())
}

func (block *BlockHash) AsSlice() []byte {
	return block.hash[:]
}

func HashBlock(data []byte) *BlockHash {
	block := new(BlockHash)
	block.hash = sha512.Sum512(data)
	return block
}
