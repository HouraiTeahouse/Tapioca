package blocks

type Block struct {
	data []byte
}

func CreateBlock(data []byte) *Block {
	block := new(Block)
	block.data = data
	return block
}

func (block *Block) Size() uint64 {
	return uint64(len(block.data))
}

func (block *Block) Update(data []byte) {
	block.data = data
}

func (block *Block) AsSlice() []byte {
	return block.data
}

func (block *Block) Hash() *BlockHash {
	return HashBlock(block.data)
}
