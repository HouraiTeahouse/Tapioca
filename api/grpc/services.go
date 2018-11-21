package tapiocaGrpc

import (
	build "github.com/HouraiTeahouse/Tapioca/api/grpc/build"
	proto "github.com/HouraiTeahouse/Tapioca/pkg/proto"
	"google.golang.org/grpc"
)

func InitServices(s *grpc.Server) {
	proto.RegisterBuildServer(s, &build.BuildServer{})
}
