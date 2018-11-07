package blocks

import (
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
)

func HashBlockProcessor() BlockProcessor {
	return func(block *FileBlockData) error {
		if block.Hash != nil {
			return nil
		}
		if block.Data == nil {
			panic("Block does not have a defined data block")
		}
		var hash BlockHash
		block.Hash = &hash
		HashBlock(*block.Data, block.Hash)
		return nil
	}
}

func HTTPFetchBlockProcessor(prefix string) (BlockProcessor, error) {
	base, err := url.Parse(prefix)
	if err != nil {
		return nil, err
	}
	var client http.Client
	return func(block *FileBlockData) error {
		if block.Hash != nil {
			panic("Block does not have a defined hash")
		}

		uri, err := url.Parse(HashEncode(block.Hash))
		if err != nil {
			return err
		}
		endpoint := base.ResolveReference(uri)
		response, err := client.Get(endpoint.String())
		if err != nil {
			return err
		}
		if response.StatusCode%100 != 2 {
			// TODO(james7132): Handle HTTP errors
			return errors.New(fmt.Sprintf("HTTP Error: %d", response.StatusCode))
		}
		blockData, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}

		block.Data = &blockData
		return nil
	}, nil
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
			return errors.New(
				fmt.Sprintf("Identical hash found: %s", HashEncode(block.Hash)))
		}
		seenHashes[*block.Hash] = true
		return nil
	}
}

func ValidateBlockProcessor() BlockProcessor {
	return func(block *FileBlockData) error {
		if block.Hash != nil {
			return nil
		}
		if block.Data == nil {
			panic("Block does not have a defined data block")
		}

		var hash BlockHash
		HashBlock(*block.Data, &hash)
		if !bytes.Equal(hash[:], block.Hash[:]) {
			return errors.New(
				fmt.Sprintf("Block mismatch. (Expected: %s, Actual: %s)",
					HashEncode(block.Hash),
					HashEncode(&hash)))
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
		writer.Write(*block.Data)
		writer.Close()

		*block.Data = buffer.Bytes()
		return nil
	}
}

func ZlibDecompressBlockProcessor() BlockProcessor {
	return func(block *FileBlockData) error {
		if block.Data == nil {
			panic("Block does not have a defined data block")
		}

		buffer := bytes.NewReader(*block.Data)
		reader, err := zlib.NewReader(buffer)
		if err != nil {
			return err
		}
		defer reader.Close()
		decompressed, err := ioutil.ReadAll(reader)
		if err != nil {
			return err
		}

		*block.Data = decompressed
		return nil
	}
}
