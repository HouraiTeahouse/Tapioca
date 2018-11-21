package blocks

const DefaultBlockSize = 1024 * 1024

type Block struct {
	data []byte
}

func CreateBlock(data []byte) *Block {
	block := new(Block)
	block.Update(data)
	return block
}

func (block *Block) Size() uint64 {
	return uint64(len(block.data))
}

func (block *Block) Update(data []byte) {
	temp := make([]byte, len(data))
	copy(temp, data)
	block.data = temp
}

func (block *Block) AsSlice() []byte {
	return block.data
}

func (block *Block) Hash() *BlockHash {
	return HashBlock(block.data)
}
