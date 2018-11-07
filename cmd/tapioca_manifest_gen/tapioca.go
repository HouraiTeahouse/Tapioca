package main

import (
	"fmt"

	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
)

func main() {
	block := []byte("Hello world!")
	var hash blocks.BlockHash
	blocks.HashBlock(block, &hash)
	fmt.Println(blocks.HashEncode(&hash))
}
