package main

import (
	"archive/zip"
	"context"
	"flag"
	"log"
	"os"

	"github.com/HouraiTeahouse/Tapioca/pkg/blocks"
	"github.com/HouraiTeahouse/Tapioca/pkg/manifests"
	proto "github.com/golang/protobuf/proto"
)

var (
	input  = flag.String("input", "", "Input zip file")
	output = flag.String("output", "", "Input zip file")
)

func main() {
	flag.Parse()
	rc, err := zip.OpenReader(*input)
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()

	ctx, _ := context.WithCancel(context.Background())
	manifestProcessor, builder := manifests.ManifestBuilderSink()
	err = blocks.NewBlockPipeline(ctx, 1024*1024, 100).
		FromZip(&rc.Reader).
		RunBatch([]blocks.BlockProcessor{
			blocks.HashBlockProcessor(),
			manifestProcessor,
		})
	if err != nil {
		log.Fatal(err)
	}

	manifestProto, err := builder.BuildProto()
	if err != nil {
		log.Fatalf("Manifest Proto error: %s", err)
	}
	if output == nil {
		return
	}

	w, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()
	if err := proto.MarshalText(w, manifestProto); err != nil {
		log.Fatal(err)
	}
}
