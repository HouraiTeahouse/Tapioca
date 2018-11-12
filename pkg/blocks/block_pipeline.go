package blocks

import (
	"log"
	"sync"
)

type BlockCollection struct {
	stageName string
	processor BlockProcessor
	parent    *BlockCollection
	children  []*BlockCollection
}

type BlockPipelineExecution struct {
	sync.WaitGroup
	errors chan error
}

func (e *BlockPipelineExecution) LogErrors() {
	go func() {
		for err := range e.errors {
			log.Printf("Pipeline error: %s", err)
		}
	}()
}

type BlockSource func(errors chan<- error) chan (<-chan *FileBlockData)
type BlockProcessor func(block *FileBlockData) (*FileBlockData, error)

func NewPipeline(stageName string, p BlockProcessor) *BlockCollection {
	return &BlockCollection{
		stageName: stageName,
		processor: p,
	}
}

func (b *BlockCollection) ParDo(stageName string, p BlockProcessor) *BlockCollection {
	collection := NewPipeline(stageName, p)
	collection.parent = b
	b.children = append(b.children, collection)
	return collection
}

func (b *BlockCollection) Run(source <-chan *FileBlockData) *BlockPipelineExecution {
	execution := &BlockPipelineExecution{errors: make(chan error)}
	runner := b.newRunner(source, *execution)
	runner.propogate()
	go func() {
		defer close(execution.errors)
		execution.Wait()
	}()
	return execution
}

func (b *BlockCollection) RunAllFromSource(source BlockSource) *BlockPipelineExecution {
	errors := make(chan error)
	sourceErrors := make(chan error)
	execution := &BlockPipelineExecution{errors: errors}
	streams := source(sourceErrors)
	execution.Add(1)
	go func() {
		defer close(sourceErrors)
		defer execution.Done()
		for {
			select {
			case channel, ok := <-streams:
				if !ok {
					return
				}
				runner := b.newRunner(channel, *execution)
				runner.propogate()
			case <-sourceErrors:
				// TODO(james7312): Handle error and cancel
				panic("Error in fetching blocks!")
			}
		}
	}()
	go func() {
		defer close(errors)
		execution.Wait()
	}()
	return execution
}

type blockRunner struct {
	BlockCollection
	BlockPipelineExecution
	input   <-chan *FileBlockData
	outputs []chan *FileBlockData
}

func (b *BlockCollection) newRunner(source <-chan *FileBlockData,
	execution BlockPipelineExecution) *blockRunner {
	runner := &blockRunner{
		BlockCollection:        *b,
		BlockPipelineExecution: execution,
		input:                  source,
		outputs:                make([]chan *FileBlockData, len(b.children)),
	}

	// Make immutable copy of children in the case it is altered during execution
	runner.children = make([]*BlockCollection, len(runner.children))
	copy(runner.children, b.children)

	// Create children channels
	for idx, _ := range runner.children {
		runner.outputs[idx] = make(chan *FileBlockData)
	}
	return runner
}

func (b *blockRunner) start() {
	// Close utput channels on exit
	defer b.Close()
	defer b.Wait()

	for {
		select {
		case block, ok := <-b.input:
			if !ok {
				break
			}
			b.Add(1)
			go func() {
				defer b.Done()
				block, err := b.processor(block)
				if err != nil {
					b.errors <- err
					return
				}
				b.publish(block)
			}()
		}
	}
}

func (b *blockRunner) publish(block *FileBlockData) {
	// Push results to next stages
	for _, output := range b.outputs {
		b.Add(1)
		go func(o chan *FileBlockData) {
			defer b.Done()
			o <- block
		}(output)
	}
}

func (b *blockRunner) propogate() {
	for idx, child := range b.children {
		runner := child.newRunner(b.outputs[idx], b.BlockPipelineExecution)
		runner.propogate()
	}
	go b.start()
}

// impl: io.Closer
func (b *blockRunner) Close() error {
	for _, output := range b.outputs {
		close(output)
	}
	return nil
}
