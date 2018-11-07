package tapioca

import (
	"io"
	"os"
)

func InMemoryBlockSource(blocks []FileBlockData) BlockSource {
	return func() (chan BlockSourceResult, error) {
		channel := make(chan BlockSourceResult)
		go func() {
			for idx, _ := range blocks {
				channel <- BlockSourceResult{Block: &blocks[idx]}
			}
		}()
		return channel, nil
	}
}

func FileBlockSource(path string, chunkSize int) BlockSource {
	return func() (chan BlockSourceResult, error) {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		channel := make(chan BlockSourceResult)
		go func() {
			defer file.Close()
			var blockId uint64 = 0
			for {
				buffer := make([]byte, chunkSize)
				bytesread, err := file.Read(buffer)
				if err != nil {
					if err != io.EOF {
						channel <- BlockSourceResult{Error: err}
					}
					break
				}
				blockData := buffer[:bytesread]
				channel <- BlockSourceResult{
					Block: &FileBlockData{
						File:    path,
						BlockId: blockId,
						Data:    &blockData,
					},
				}
				blockId++
			}
		}()
		return channel, nil
	}
}
