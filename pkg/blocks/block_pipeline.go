package blocks

import (
	"sync"
)

type BlockCollection struct {
	stageName string
	processor BlockProcessor
	parent    *BlockCollection
	children  []*BlockCollection
}

type BlockSource func(errors chan<- error) chan (<-chan *FileBlockData)
type BlockProcessor func(block *FileBlockData) (*FileBlockData, error)

func NewPipeline(stageName string, p BlockProcessor) *BlockCollection {
	return &BlockCollection{
		processor: p,
	}
}

func (b *BlockCollection) ParDo(stageName string, p BlockProcessor) *BlockCollection {
	collection := NewPipeline(stageName, p)
	collection.parent = b
	b.children = append(b.children, collection)
	return collection
}

func (b *BlockCollection) Run(source <-chan *FileBlockData) <-chan error {
	errors := make(chan error)
	runner := b.newRunner(source, errors)
	runner.propogate()
	return errors
}

type blockRunner struct {
	BlockCollection
	sync.WaitGroup
	input   <-chan *FileBlockData
	outputs []chan *FileBlockData
	errors  chan<- error
}

func (b *BlockCollection) newRunner(source <-chan *FileBlockData,
	errors chan<- error) *blockRunner {
	runner := &blockRunner{
		BlockCollection: *b,
		input:           source,
		outputs:         make([]chan *FileBlockData, len(b.children)),
		errors:          errors,
	}

	// Make immutable copy of children in the case it is altered during execution
	runner.children = make([]*BlockCollection, len(runner.children))
	copy(b.children, runner.children)

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
			block, err := b.processor(block)
			if err != nil {
				b.errors <- err
				continue
			}
			b.publish(block)
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
		runner := child.newRunner(b.outputs[idx], b.errors)
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
