package main

import (
	"flag"
	"fmt"
	tapiocaGrpc "github.com/HouraiTeahouse/Tapioca/api/grpc"
	tapiocaHttp "github.com/HouraiTeahouse/Tapioca/api/http"
	"github.com/soheilhy/cmux"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
	"time"
)

var (
	port = flag.String("port", "8080", "The port to expose the server on")
)

func main() {
	flag.Parse()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatal(err)
	}

	m := cmux.New(listener)
	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	g := new(errgroup.Group)
	g.Go(func() error {
		log.Println("Starting gRPC server...")
		return serveGrpc(grpcL)
	})
	g.Go(func() error {
		log.Println("Starting HTTP server...")
		return serveHttp(httpL)
	})
	g.Go(func() error {
		log.Printf("Serving listener on port: %s", *port)
		return m.Serve()
	})
	log.Println("Run server: ", g.Wait())
}

func serveHttp(l net.Listener) error {
	s := &http.Server{
		Addr:         fmt.Sprintf(":%s", *port),
		Handler:      tapiocaHttp.Handler(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return s.Serve(l)
}

func serveGrpc(l net.Listener) error {
	s := grpc.NewServer()
	tapiocaGrpc.InitServices(s)
	return s.Serve(l)
}
