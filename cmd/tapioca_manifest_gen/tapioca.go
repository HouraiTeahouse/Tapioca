package main

import (
	"fmt"

	"github.com/HouraiTeahouse/Tapioca/pkg/core"
)

func main() {
	block := []byte("Hello world!")
	var hash tapioca.BlockHash
	tapioca.HashBlock(block, &hash)
	fmt.Println(tapioca.HashEncode(&hash))
}
