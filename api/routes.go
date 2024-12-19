package api

import (
	"github.com/Bl4ck-h00d/stashdb/types"
	"github.com/gorilla/mux"
)

type Handlers struct {
	store types.Store
}

func NewHandler(store types.Store) *Handlers {
	return &Handlers{store: store}
}

func (h *Handlers) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/create-bucket",nil).Methods("POST")
	router.HandleFunc("/set",nil).Methods("POST")
	router.HandleFunc("/get",nil).Methods("GET")
	router.HandleFunc("/delete",nil).Methods("DELETE")
}
