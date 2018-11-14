package main

import (
	"flag"
	"fmt"
	tapiocaHttp "github.com/HouraiTeahouse/Tapioca/api/http"
	"github.com/soheilhy/cmux"
	"golang.org/x/sync/errgroup"
	"log"
	"net/http"
	"time"
)

var (
	port = flags.StringVar("port", "8080", "The port to expose the server on")
)

func serveHttp(l net.Listener) error {
	s := &http.Server{
		Addr:         fmt.Sprintf(":%s", *port),
		Handler:      tapiocaHttp.Handler(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return s.Serve(l)
}

func main() {
	flag.Parse()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%", *port))
	if err != nil {
		log.Fatal(err)
	}

	// TODO(james7132): Add GRPC endpoints to this
	m := cmux.New(listener)
	httpListener := m.Match(cmux.HTTP1Fast())

	g := new(errgroup.Group)
	g.Go(func() error { return serveHttp(httpListener) })
	g.Go(func() error { return m.Serve() })
	log.Println("Run server: ", g.Wait())
}
