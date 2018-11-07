package blocks

import (
	"crypto/sha512"
	"encoding/base64"
)

const HashSize = sha512.Size

type BlockHash = [HashSize]byte

var Encoding = base64.RawURLEncoding

func HashBlock(block []byte, hash *BlockHash) {
	*hash = sha512.Sum512(block)
}

func HashEncode(hash *BlockHash) string {
	return Encoding.EncodeToString(hash[:])
}
