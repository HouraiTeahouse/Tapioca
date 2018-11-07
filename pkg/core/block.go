package tapioca

import (
  "sync"
)

type FileBlockData struct {
  File        string
  BlockId     uint64
  Hash        *BlockHash
  Data        *[]byte
}

type BlockPipeline struct {
  // Intermeiate processsors for the pipeline
  Processors  []BlockProcessor

  // Sinks to write the blocks to after they are processed
  Sinks       []BlockProcessor
}

type BlockSourceResult struct {
  Block       *FileBlockData
  Error       error
}

type BlockSource    func() (chan BlockSourceResult, error)
type BlockProcessor func(block *FileBlockData) error

func (pipeline *BlockPipeline) Run(blocks BlockSource) error {
  stream, err := blocks()
  if err != nil {
    return err
  }

  var wg sync.WaitGroup
  runBlock := func(block *FileBlockData) error {
    defer wg.Done()
    return pipeline.RunBlock(block)
  }
  for result := range stream {
    if result.Error != nil {
      return result.Error
    }
    wg.Add(1)
    go runBlock(result.Block)
  }
  wg.Wait()
  return nil
}

func (pipeline *BlockPipeline) RunBlock(block *FileBlockData) error {
  // Run the processors in sequence
  for _, processor := range pipeline.Processors{
    err := processor(block)
    if err != nil {
      // TODO(james7132): Handle error
      return err
    }
  }

  // Write the results out in parallel
  var wg sync.WaitGroup
  wg.Add(len(pipeline.Sinks))
  sinkBlock := func(sink BlockProcessor) {
    defer wg.Done()
    sink(block)
  }
  for _, sink := range pipeline.Sinks {
    go sinkBlock(sink)
  }
  wg.Wait()
  return nil
}
