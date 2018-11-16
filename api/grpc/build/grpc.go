package build

import (
	"context"
	proto "github.com/HouraiTeahouse/Tapioca/pkg/proto"
)

type BuildServer struct{}

func (b *BuildServer) GetLatestBuild(context.Context, *proto.BuildRequest) (*proto.BuildResponse, error) {
	return &proto.BuildResponse{}, nil
}
