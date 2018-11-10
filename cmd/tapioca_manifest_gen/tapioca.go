package main

import (
	"archive/zip"
	"log"

	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
)

func main() {
	pipeline := blocks.NewPipeline("HashBlocks", blocks.HashBlockProcessor())

	pipeline.ParDo("PrintBlock", blocks.PrintBlockProcessor())

	rc, err := zip.OpenReader("test.zip")
	if err != nil {
		panic(err)
	}
	defer rc.Close()

	source := blocks.ZipFileBlockSource(rc.Reader, blocks.DefaultBlockSize)

	execution := pipeline.RunAllFromSource(source)

	execution.Wait()

	log.Println("Finished!")

	//block := blocks.CreateBlock([]byte("Hello world!"))
	//fmt.Println(block.Hash())
}
