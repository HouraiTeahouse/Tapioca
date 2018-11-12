package main

import (
	"archive/zip"
	"bufio"
	"flag"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
	"github.com/HouraiTeahouse/Tapioca/pkg/manifests"
	proto "github.com/golang/protobuf/proto"
)

var (
	input = flag.String("input", "", "Input zip file")
)

func main() {
	flag.Parse()
	rc, err := zip.OpenReader(*input)
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()

	//processors := []block.BlockProcessor {

	//}

	var wg sync.WaitGroup
	builder := manifests.ManifestBuilder{}
	for _, file := range rc.File {
		if strings.HasSuffix(file.Name, "/") {
			continue
		}
		rc, err := file.Open()
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		go func(name string, f io.ReadCloser) {
			defer f.Close()
			defer wg.Done()
			r := bufio.NewReader(f)
			var blockId uint64 = 0
			buf := make([]byte, 1024*1024)
			manifestFile := builder.AddFile(name)
			for {
				n, err := io.ReadFull(r, buf)
				block := blocks.CreateBlock(buf[:n])

				wg.Add(1)
				go func(id uint64) {
					defer wg.Done()

					blockErr := manifestFile.AddBlock(id, &manifests.ManifestBlock{
						Hash: *block.Hash(),
						Size: block.Size(),
					})
					if blockErr != nil {
						log.Fatal(blockErr)
					}
				}(blockId)

				if err != nil {
					if err != io.EOF && err != io.ErrUnexpectedEOF {
						log.Fatal(err)
					}
					return
				}
				blockId++
			}
		}(file.Name, rc)
	}
	wg.Wait()
	manifestProto, err := builder.BuildProto()
	if err != nil {
		log.Fatalf("Manifest Proto error: %s", err)
	}
	log.Println(proto.MarshalTextString(manifestProto))
}

//func RunProcessors(b *FileBlockData, p []blocks.BlockProcessor) error {
//for _, processor := range p {
//b, err := processor(b)
//if err != nil {
//return err
//}
//return err
//}
//}
