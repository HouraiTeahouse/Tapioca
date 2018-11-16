package deploy

import (
	"net/http"
)

type UnityCloudBuildHandler struct{}

func (h UnityCloudBuildHandler) Deploy(d *Deployment, r *http.Request) error {
	return nil
}
