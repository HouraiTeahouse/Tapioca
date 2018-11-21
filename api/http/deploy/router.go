package deploy

import (
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
)

type Deployment struct {
	ProjectId uint64
	Branch    *string
	Target    *uint64
}

type DeploymentHandler interface {
	Deploy(d *Deployment, r *http.Request) error
}

var (
	router   *mux.Router = nil
	handlers             = map[string]DeploymentHandler{
		"unity-cloud-build": UnityCloudBuildHandler{},
	}
)

func Init(r *mux.Router) {
	router = r
	routes := []string{
		"/{project_id}",
		"/{project_id}/{branch}",
		"/{project_id}/{branch}/{target}",
	}
	for _, route := range routes {
		router.HandleFunc(route, handleDeploy).Methods("POST")
	}
}

func parseVars(r *http.Request) (d *Deployment, err error) {
	var ok bool
	d = &Deployment{
		Branch: new(string),
		Target: new(uint64),
	}
	vars := mux.Vars(r)
	d.ProjectId, err = strconv.ParseUint(vars["project_id"], 36, 64)
	if err != nil {
		return
	}
	if *d.Branch, ok = vars["branch"]; !ok {
		d.Branch = nil
	}
	*d.Target, err = strconv.ParseUint(vars["target"], 36, 64)
	if err != nil {
		d.Target = nil
	}
	return
}

func parseQuery(r *http.Request) (h DeploymentHandler, err error) {
	params := r.URL.Query()["h"]
	if len(params) <= 0 {
		err = fmt.Errorf("No query param")
		return
	}
	handlerName := params[0]
	h, ok := handlers[handlerName]
	if !ok {
		err = fmt.Errorf("No matching handler for %s", h)
		return
	}
	return
}

func handleDeploy(w http.ResponseWriter, r *http.Request) {
	deployHandler, err := parseQuery(r)
	if err != nil {
		http.Error(w, "Invalid Handler", 400)
		return
	}
	deployment, err := parseVars(r)
	if deployHandler == nil {
		http.Error(w, "Error parsing request.", 400)
		return
	}
	err = deployHandler.Deploy(deployment, r)
	if err != nil {
		http.Error(w, "Internal server error.", 500)
		// TODO(james7132): Add failure notifiers here
	}
	// TODO(james7132): Add success notifiers here
	w.WriteHeader(200)
	w.Write([]byte("Success."))
}
