package main

import (
	"fmt"

	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
)

func main() {
	block := blocks.CreateBlock([]byte("Hello world!"))
	fmt.Println(block.Hash())
}
