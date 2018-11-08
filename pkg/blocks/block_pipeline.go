package blocks

import (
	"sync"
)

type BlockPipeline struct {
	// Intermeiate processsors for the pipeline
	Processors []BlockProcessor

	// Sinks to write the blocks to after they are processed
	Sinks []BlockProcessor
}

type pipelineWorker struct {
  pipeline      *BlockPipeline
  source        <-chan BlockSourceResult
  errors        chan error
  quit          chan struct{}
}

func (pipeline *BlockPipeline) Run(blocks BlockSource) error {
  streams := make(chan (<-chan BlockSourceResult))
  errors  := make(chan error)

	var wg sync.WaitGroup
  workers := make([]*pipelineWorker, 0, 1024)

  for stream := range streams {
    worker := new(pipelineWorker)
    worker.source = stream
    worker.errors = errors
    worker.quit = make(chan struct{})
    worker.Start(&wg)
    workers = append(workers, worker)
  }

  kill := func() {
    for _, worker := range workers {
      worker.quit <- 0
    }
  }

  for {

  }

	wg.Wait()
	return nil
}

func (pipeline *BlockPipeline) RunBlock(block *FileBlockData) error {
  err := pipeline.ProcessBlock(block)
  if err != nil {
    return err
  }
	return pipeline.WriteBlock(block)
}

func (pipeline *BlockPipeline) ProcessBlock(block *FileBlockData) error {
	// Run the processors in sequence
	for _, processor := range pipeline.Processors {
		err := processor(block)
		if err != nil {
			return err
		}
	}
  return nil
}

func (pipeline *BlockPipeline) WriteBlock( block *FileBlockData) error {
	var wg sync.WaitGroup
	wg.Add(len(pipeline.Sinks))
	for _, sink := range pipeline.Sinks {
    // TODO(james7132): Backpropogate errors
		go func() {
      defer wg.Done()
      sink(block)
    } ()
	}
	wg.Wait()
  return nil
}

func (worker pipelineWorker) Start(wg *sync.WaitGroup) {
  wg.Add(1)
  go func() {
    defer wg.Done()
    var result BlockSourceResult
    for {
      select {
      case result := <-worker.source:
        if result.Error != nil {
          // An error has occuredA
          worker.errors <- result.Error
          return
        }
        go worker.RunBlock(result.Block)
      }
    }
  } ()
}

func (worker pipelineWorker) RunBlock(block *FileBlockData) {
  err := worker.pipeline.RunBlock(block)
  if err != nil {
    worker.errors <- err
  }
}
