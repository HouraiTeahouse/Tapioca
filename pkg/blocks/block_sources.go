package blocks

import (
	"archive/zip"
	"bufio"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type FileReader struct {
	io.Reader
	io.Closer
	BaseBlock FileBlockData
	ChunkSize uint64
	errors    chan<- error
}

func ChannelBlockSource(blocks <-chan *FileBlockData) BlockSource {
	return func(errors chan<- error) chan (<-chan *FileBlockData) {
		result := make(chan (<-chan *FileBlockData), 1)
		result <- blocks
		close(result)
		return result
	}
}

func InMemoryBlockSource(blocks []FileBlockData) BlockSource {
	channel := make(chan *FileBlockData, len(blocks))
	for idx, _ := range blocks {
		channel <- &blocks[idx]
	}
	return func(errors chan<- error) chan (<-chan *FileBlockData) {
		return ChannelBlockSource(channel)(errors)
	}
}

func ReaderBlockSource(reader FileReader) BlockSource {
	return func(errors chan<- error) chan (<-chan *FileBlockData) {
		fileReader := reader
		fileReader.errors = errors
		return ChannelBlockSource(fileReader.ReadBlocks(nil))(errors)
	}
}

func DirectoryBlockSource(root string, chunkSize uint64) BlockSource {
	return func(errors chan<- error) chan (<-chan *FileBlockData) {
		streams := make(chan (<-chan *FileBlockData))
		go func() {
			defer close(streams)
			var wg sync.WaitGroup
			filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
				if info.IsDir() {
					return nil
				}
				relpath, err := filepath.Rel(root, path)
				if err != nil {
					errors <- err
					return err
				}
				file, err := os.Open(path)
				if err != nil {
					errors <- err
					return err
				}
				wg.Add(1)
				streams <- (&FileReader{
					Reader: file,
					Closer: file,
					BaseBlock: FileBlockData{
						File: relpath,
						Size: chunkSize,
					},
					ChunkSize: chunkSize,
					errors:    errors,
				}).ReadBlocks(&wg)
				return nil
			})
			wg.Wait()
		}()
		return streams
	}
}

func ZipFileBlockSource(reader zip.Reader, chunkSize uint64) BlockSource {
	return func(errors chan<- error) chan (<-chan *FileBlockData) {
		streams := make(chan (<-chan *FileBlockData))
		go func() {
			defer close(streams)
			var wg sync.WaitGroup
			for _, file := range reader.File {
				if strings.HasSuffix(file.Name, "/") {
					continue
				}
				rc, err := file.Open()
				if err != nil {
					errors <- err
					return
				}
				wg.Add(1)
				streams <- (&FileReader{
					Reader: rc,
					Closer: rc,
					BaseBlock: FileBlockData{
						File: filepath.Clean(file.Name),
						Size: chunkSize,
					},
					ChunkSize: chunkSize,
					errors:    errors,
				}).ReadBlocks(&wg)
			}
			wg.Wait()
		}()
		return streams
	}
}

func (f *FileReader) ReadBlocks(wg *sync.WaitGroup) <-chan *FileBlockData {
	out := make(chan *FileBlockData)
	r := bufio.NewReader(f.Reader)
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		defer close(out)
		defer f.Close()

		base := f.BaseBlock
		base.BlockId = 0
		base.Offset = 0

		for {
			// Create new buffer for next block
			buf := make([]byte, f.ChunkSize)
			n, err := io.ReadFull(r, buf)

			base.Size = uint64(n)

			block := base
			block.Data = CreateBlock(buf[:n])
			log.Println(block)
			out <- &block

			base.BlockId++
			base.Offset += base.Size

			if err != nil {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					log.Fatal(err)
					//f.errors <- err
				}
				return
			}
		}
	}()
	return out
}
