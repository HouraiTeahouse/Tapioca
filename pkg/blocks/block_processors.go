package blocks

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
)

type BlockProcessor func(block *FileBlockData) error

func HashBlockProcessor() BlockProcessor {
	return func(block *FileBlockData) error {
		if block.Hash != nil {
			return nil
		}
		_, err := block.UpdateHash()
		if err != nil {
			return fmt.Errorf("Error while hashing block: %s", err)
		}
		return nil
	}
}

func HTTPFetchBlockProcessor(prefix string) (BlockProcessor, error) {
	var client http.Client
	return func(block *FileBlockData) error {
		address, err := getAddress(prefix, block)
		if err != nil {
			return err
		}
		response, err := client.Get(address)
		if err != nil {
			return err
		}
		if response.StatusCode%100 != 2 {
			return fmt.Errorf("HTTP Error: %d", response.StatusCode)
		}
		blockData, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}

		block.Data = CreateBlock(blockData)
		return nil
	}, nil
}

func getAddress(prefix string, block *FileBlockData) (string, error) {
	if block.Hash != nil {
		return "", fmt.Errorf("Block does not have a defined hash")
	}
	base, err := url.Parse(prefix)
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
	return func(block *FileBlockData) error {
		if block.Hash != nil {
			panic("Block does not have a defined hash")
		}

		mutex.Lock()
		defer mutex.Unlock()

		_, err := seenHashes[*block.Hash]
		if err {
			return fmt.Errorf("Identical hash found: %s", block.Hash)
		}
		seenHashes[*block.Hash] = true
		return nil
	}
}

func ValidateBlockProcessor() BlockProcessor {
	return func(block *FileBlockData) error {
		hash, err := block.ComputeHash()
		if err != nil {
			return err
		}
		if !block.Hash.Equal(hash) {
			return fmt.Errorf("Block mismatch. (Expected: %s, Actual: %s)",
				block.Hash, hash)
		}
		return nil
	}
}

func ZlibCompressBlockProcessor(level int) BlockProcessor {
	return func(block *FileBlockData) error {
		if block.Data == nil {
			panic("Block does not have a defined data block")
		}

		var buffer bytes.Buffer
		writer, err := zlib.NewWriterLevel(&buffer, level)
		if err != nil {
			return err
		}
		writer.Write(block.Data.AsSlice())
		writer.Close()

		block.Data = CreateBlock(buffer.Bytes())
		return nil
	}
}

func ZlibDecompressBlockProcessor() BlockProcessor {
	return func(block *FileBlockData) error {
		if block.Data == nil {
			panic("Block does not have a defined data block")
		}

		buffer := bytes.NewReader(block.Data.AsSlice())
		reader, err := zlib.NewReader(buffer)
		if err != nil {
			return err
		}
		defer reader.Close()
		decompressed, err := ioutil.ReadAll(reader)
		if err != nil {
			return err
		}

		block.SetBlock(decompressed)
		return nil
	}
}
