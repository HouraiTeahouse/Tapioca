package blocks

import (
	"io"
	"os"
)

type BlockSourceResult struct {
	Block *FileBlockData
	Error error
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

func FileBlockSource(path string, chunkSize uint64) BlockSource {
	return func(errors chan<- error) chan (<-chan *FileBlockData) {
		file, err := os.Open(path)
		if err != nil {
			errors <- err
			return nil
		}
		baseBlock := FileBlockData{File: path}
		channel := ReadBlocks(file, baseBlock, chunkSize, errors)
		return ChannelBlockSource(channel)(errors)
	}
}

func ReadBlocks(reader io.Reader,
	baseBlock FileBlockData,
	chunkSize uint64,
	errors chan<- error) <-chan *FileBlockData {
	out := make(chan *FileBlockData)
	go func() {
		defer close(out)
		var blockId uint64
		var offset uint64
		for {
			buffer := make([]byte, chunkSize)
			bytesread, err := reader.Read(buffer)
			if err != nil {
				if err != io.EOF {
					errors <- err
				}
				break
			}

			block := baseBlock
			block.BlockId = blockId
			block.Offset = offset
			block.SetBlock(buffer[:bytesread])

			out <- &block

			blockId += 1
			offset += uint64(bytesread)
		}
	}()
	return out
}
