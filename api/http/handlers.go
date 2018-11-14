package tapiocaHttp

import (
	"github.com/gorilla/mux"
	"net/http"

	"github.com/HouraiTeahouse/Tapioca/api/http/deploy"
)

func Handler() http.Handler {
	r := mux.NewRouter()
	deploy.Init(r.PathPrefix("/deploy").Subrouter())
	return r
}
