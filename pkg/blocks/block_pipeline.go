package blocks

import (
	"archive/zip"
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
)

type BlockPipeline struct {
	context.Context
	sync.WaitGroup
	ChunkSize uint64
	Blocks    chan *FileBlockData
}

type BlockProcessor func(b *FileBlockData) (*FileBlockData, error)

func NewBlockPipeline(ctx context.Context, chunkSize uint64, bufferSize int) *BlockPipeline {
	return &BlockPipeline{
		Context:   ctx,
		ChunkSize: chunkSize,
		Blocks:    make(chan *FileBlockData, bufferSize),
	}
}

func (b *BlockPipeline) FromSlice(blocks []FileBlockData) *BlockPipeline {
	for idx, _ := range blocks {
		b.Blocks <- &blocks[idx]
	}
	return b
}

func (b *BlockPipeline) FromZip(r *zip.Reader) *BlockPipeline {
	b.Add(1)
	g, ctx := errgroup.WithContext(b.Context)
	for _, file := range r.File {
		if strings.HasSuffix(file.Name, "/") {
			continue
		}
		f := file
		g.Go(func() error {
			rc, err := f.Open()
			if err != nil {
				return err
			}
			defer rc.Close()
			return b.processReader(ctx, f.Name, rc)
		})
	}
	go func() {
		defer b.WaitGroup.Done()
		defer close(b.Blocks)
		g.Wait()
	}()
	return b
}

func (b *BlockPipeline) FromDirectory(root string) *BlockPipeline {
	b.Add(1)
	g, ctx := errgroup.WithContext(b.Context)
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		g.Go(func() error {
			rc, err := os.Open(path)
			if err != nil {
				return err
			}
			defer rc.Close()
			return b.processReader(ctx, relPath, rc)
		})
		return nil
	})
	go func() {
		defer b.WaitGroup.Done()
		defer close(b.Blocks)
		g.Wait()
	}()
	return b
}

func (b *BlockPipeline) processReader(parent context.Context, name string, r io.Reader) error {
	r = bufio.NewReader(r)
	var blockId uint64 = 0
	var offset uint64 = 0
	g, ctx := errgroup.WithContext(parent)
	buf := make([]byte, b.ChunkSize)
	for {
		n, err := io.ReadFull(r, buf)
		block := CreateBlock(buf[:n])

		fileBlock := &FileBlockData{
			File:    name,
			Offset:  offset,
			BlockId: blockId,
			Size:    block.Size(),
			Data:    block,
		}
		g.Go(func() error {
			select {
			case b.Blocks <- fileBlock:
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

func (b *BlockPipeline) RunBatch(p []BlockProcessor) error {
	return b.Run(func(b *FileBlockData) (*FileBlockData, error) {
		for _, processor := range p {
			var err error
			b, err = processor(b)
			if err != nil {
				return nil, err
			}
		}
		return b, nil
	})
}

func (b *BlockPipeline) Run(p BlockProcessor) error {
	g, ctx := errgroup.WithContext(b.Context)

loop:
	for {
		select {
		case file, ok := <-b.Blocks:
			if !ok {
				break loop
			}
			f := file
			g.Go(func() error {
				var err error
				_, err = p(f)
				if err != nil {
					return err
				}
				return nil
			})
		case <-ctx.Done():
			break
		}
	}
	b.Wait()
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}
