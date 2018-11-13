package main

import (
	"archive/zip"
	"bufio"
	"context"
	"flag"
	"io"
	"log"
  "os"
	"strings"

	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
	"github.com/HouraiTeahouse/Tapioca/pkg/manifests"
	proto "github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"
)

var (
	input = flag.String("input", "", "Input zip file")
	output = flag.String("output", "", "Input zip file")
)

func main() {
	flag.Parse()
	rc, err := zip.OpenReader(*input)
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()

	queue := make(chan *blocks.FileBlockData, 100)

	ctx, _ := context.WithCancel(context.Background())
	ZipBlockSource(ctx, &rc.Reader, queue)
	manifest, err := ProcessBlocks(ctx, queue)
	if err != nil {
		log.Fatal(err)
	}

	manifestProto, err := manifest.ToProto()
	if err != nil {
		log.Fatalf("Manifest Proto error: %s", err)
	}
  if output == nil {
    return
  }

  w, err := os.OpenFile(*output, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.ModePerm)
  if err != nil {
    log.Fatal(err)
  }
  defer w.Close()
  if err := proto.MarshalText(w, manifestProto); err != nil {
    log.Fatal(err)
  }
}

func ProcessBlocks(ctx context.Context, in <-chan *blocks.FileBlockData) (*manifests.Manifest, error) {
	var g *errgroup.Group
	builder := manifests.ManifestBuilder{}
	g, ctx = errgroup.WithContext(ctx)

  loop:for {
		select {
		case file, ok := <-in:
			if !ok {
				break loop
			}
			f := file
			g.Go(func() error {
        return builder.AddFile(f.File).
					AddBlock(f.BlockId, &manifests.ManifestBlock{
						Hash: *f.Data.Hash(),
						Size: f.Size,
					})
			})
		case <-ctx.Done():
			break
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	manifest, err := builder.Build()
	if err != nil {
		return nil, err
	}
	return manifest, nil
}

func ZipBlockSource(ctx context.Context, r *zip.Reader, out chan<- *blocks.FileBlockData) {
	var g *errgroup.Group
	g, ctx = errgroup.WithContext(ctx)
	for _, file := range r.File {
		if strings.HasSuffix(file.Name, "/") {
			continue
		}
		rc, err := file.Open()
		if err != nil {
			log.Fatal(err)
		}
		name := file.Name
		f := rc
		g.Go(func() error {
			defer f.Close()
			return ProcessReader(ctx, name, f, out)
		})
	}
	go func() {
		defer close(out)
		g.Wait()
	}()
}

func ProcessReader(parent context.Context, name string, f io.Reader,
	out chan<- *blocks.FileBlockData) error {
	r := bufio.NewReader(f)
	var blockId uint64 = 0
	var offset uint64 = 0
	g, ctx := errgroup.WithContext(parent)
	buf := make([]byte, 1024*1024)
	for {
		n, err := io.ReadFull(r, buf)
		block := blocks.CreateBlock(buf[:n])

		fileBlock := &blocks.FileBlockData{
			File:    name,
			Offset:  offset,
			BlockId: blockId,
			Size:    block.Size(),
			Data:    block,
		}
		g.Go(func() error {
			select {
			case out <- fileBlock:
			case <-ctx.Done():
			}
			return nil
		})

		if err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				return err
			}
			return nil
		}
		blockId++
		offset += block.Size()
	}
	return g.Wait()
}
