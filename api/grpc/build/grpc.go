package build

import (
	"context"
	"github.com/HouraiTeahouse/Tapioca/pkg/database"
	"github.com/HouraiTeahouse/Tapioca/pkg/proto"
)

type BuildServer struct {
	Db database.Database
}

func (b *BuildServer) GetLatestBuild(context.Context, *proto.BuildRequest) (*proto.BuildResponse, error) {
	return &proto.BuildResponse{}, nil
}
