package main

import (
	"archive/zip"
	"flag"
	"log"
  "sync"
  "io"
  "bufio"
  "strings"

  proto "github.com/golang/protobuf/proto"
	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
  "github.com/HouraiTeahouse/Tapioca/pkg/manifests"
)

var (
	input      = flag.String("input", "", "Input zip file")
	//output = flag.String("output", "", "Input zip file")
)

func main() {
  flag.Parse()
  rc, err := zip.OpenReader(*input)
  if err != nil {
    log.Fatal(err)
  }
  defer rc.Close()
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
      r := bufio.NewReader(rc)
			buf := make([]byte, 1024 * 1024)
      var blockId uint64 = 0
      manifestFile := builder.AddFile(name)
      for {
        n, err := io.ReadFull(r, buf)
        block := blocks.CreateBlock(buf[:n])
        //log.Printf("Block: %s %d", name, blockId)
        blockErr := manifestFile.AddBlock(blockId, &manifests.ManifestBlock{
          Hash: *block.Hash(),
          Size: block.Size(),
        })
        if blockErr != nil {
          log.Fatal(blockErr)
        }

        if err != nil {
          if err != io.EOF && err != io.ErrUnexpectedEOF {
            log.Fatal(err)
          }
          return
        }
        blockId++
      }
    } (file.Name, rc)
  }
  wg.Wait()
  manifest, err := builder.Build()
  if err != nil {
    log.Fatalf("Manifest error: %s", err)
  }
  manifestProto, err := manifest.ToProto()
  if err != nil {
    log.Fatalf("Manifest error: %s", err)
  }
  log.Println(proto.MarshalTextString(manifestProto))
}
