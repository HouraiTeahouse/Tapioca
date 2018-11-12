package manifests

import (
	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
	//"log"
)

func ManifestBuilderSink() (blocks.BlockProcessor, *ManifestBuilder) {
	builder := new(ManifestBuilder)
	return func(block *blocks.FileBlockData) (*blocks.FileBlockData, error) {
		//if block.BlockId != 0  {
		//log.Println(block)
		//}
		file := builder.AddFile(block.File)
		err := file.AddBlock(block.BlockId, &ManifestBlock{
			Hash: *block.Hash,
			Size: block.Size,
		})
		if err != nil {
			return nil, err
		}
		return block, nil
	}, builder
}
