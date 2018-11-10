package blocks

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io/ioutil"
	//"log"
	"net/http"
	"net/url"
	"sync"
)

func PrintBlockProcessor() BlockProcessor {
	return func(block *FileBlockData) (*FileBlockData, error) {
		//log.Printf("%s", block)
		return block, nil
	}
}

func HashBlockProcessor() BlockProcessor {
	return func(block *FileBlockData) (*FileBlockData, error) {
		if block.Hash != nil {
			return block, nil
		}
		_, err := block.UpdateHash()
		if err != nil {
			return nil, fmt.Errorf("Error while hashing block: %s", err)
		}
		return block, nil
	}
}

func HTTPFetchBlockProcessor(prefix string) BlockProcessor {
	var client http.Client
	return func(block *FileBlockData) (*FileBlockData, error) {
		address, err := getAddress(&prefix, block)
		if err != nil {
			return nil, err
		}
		response, err := client.Get(address)
		if err != nil {
			return nil, err
		}
		if response.StatusCode%100 != 2 {
			return nil, fmt.Errorf("HTTP Error: %d", response.StatusCode)
		}
		blockData, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}

		block.Data = CreateBlock(blockData)
		return block, nil
	}
}

func getAddress(prefix *string, block *FileBlockData) (string, error) {
	if block.Hash != nil {
		return "", fmt.Errorf("Block does not have a defined hash")
	}
	base, err := url.Parse(*prefix)
	if err != nil {
		return "", err
	}
	uri, err := url.Parse(block.Hash.String())
	if err != nil {
		return "", err
	}
	return base.ResolveReference(uri).String(), nil
}

func DedupBlockProcessor() BlockProcessor {
	seenHashes := make(map[BlockHash]bool)
	var mutex sync.Mutex
	return func(block *FileBlockData) (*FileBlockData, error) {
		if block.Hash != nil {
			panic("Block does not have a defined hash")
		}

		mutex.Lock()
		defer mutex.Unlock()

		_, err := seenHashes[*block.Hash]
		if err {
			return nil, fmt.Errorf("Identical hash found: %s", block.Hash)
		}
		seenHashes[*block.Hash] = true
		return block, nil
	}
}

func ValidateBlockProcessor() BlockProcessor {
	return func(block *FileBlockData) (*FileBlockData, error) {
		hash, err := block.ComputeHash()
		if err != nil {
			return nil, err
		}
		if !block.Hash.Equal(hash) {
			return nil, fmt.Errorf("Block mismatch. (Expected: %s, Actual: %s)",
				block.Hash, hash)
		}
		return block, nil
	}
}

func ZlibCompressBlockProcessor(level int) BlockProcessor {
	return func(block *FileBlockData) (*FileBlockData, error) {
		if block.Data == nil {
			panic("Block does not have a defined data block")
		}

		var buffer bytes.Buffer
		writer, err := zlib.NewWriterLevel(&buffer, level)
		if err != nil {
			return nil, err
		}
		writer.Write(block.Data.AsSlice())
		writer.Close()

		block.Data = CreateBlock(buffer.Bytes())
		return block, nil
	}
}

func ZlibDecompressBlockProcessor() BlockProcessor {
	return func(block *FileBlockData) (*FileBlockData, error) {
		if block.Data == nil {
			panic("Block does not have a defined data block")
		}

		buffer := bytes.NewReader(block.Data.AsSlice())
		reader, err := zlib.NewReader(buffer)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		decompressed, err := ioutil.ReadAll(reader)
		if err != nil {
			return nil, err
		}

		block.SetBlock(decompressed)
		return block, nil
	}
}
