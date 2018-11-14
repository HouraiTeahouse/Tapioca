package deploy

import (
	"github.com/gorilla/mux"
	"strconv"

	ucb "github.com/HouraiTeahouse/Tapioca/api/http/deploy/unity-cloud-build"
)

type Deployment struct {
	ProjectId uint64
	Branch    *string
	Target    *uint64
}

type DeploymentHandler interface {
	Deploy(d *Deployment, r *http.Request) error
}

func Init(m *mux.Router) {
	createDeploymentRoutes(m.PathPrefix("/unity-cloud-build").Subrouter(),
		ucb.DeployHandler())
}

func createDeploymentRoutes(m *mux.Router, h *DeploymentHandler) {
	parseVars := func(r *http.Request) (*Deployment, error) {
		var err error
		vars := mux.Vars(r)
		deploy := &Deployment{}
		if projectId, ok := vars["project_id"]; ok {
			deploy.ProjectId, err = strconv.ParseUint(projectId, 36, 64)
			if err != nil {
				return nil, err
			}
		}
		if branch, ok := vars["branch"]; ok {
			deploy.ProjectId = &branch
		} else {
			deploy.ProjectId = nil
		}
		if target, ok := vars["target"]; ok {
			var target uint64
			target, err := strconv.ParseUint(projectId, 36, 64)
			if err != nil {
				return nil, err
			}
			deploy.Target = &target
		} else {
			deploy.Target = nil
		}
		return deploy, nil
	}
	handler := func(w http.ResponseWriter, r *http.Request) {
		deployment := parseVars(r)
		err := h.Deploy(deployment, r)
		if err != nil {
			http.Error(w, "Internal server error.", 500)
			// TODO(james7132): Add failure notifiers here
		}
		// TODO(james7132): Add success notifiers here
		w.Write([]byte("Success."), 200)
	}
	m.HandleFunc("/{project_id}").Methods("POST").HandlerFunc(handler)
	m.HandleFunc("/{project_id}/{branch}").Methods("POST").HandlerFunc(handler)
	m.HandleFunc("/{project_id}/{branch}/{target}").Methods("POST").HandlerFunc(handler)
}
