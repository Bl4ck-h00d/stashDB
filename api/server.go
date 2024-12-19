package api

import "github.com/gorilla/mux"

type Server struct {
	address string
	db      string
}

func NewAPIServer(address, db string) (*Server, error) {
	return &Server{address: address, db: db}, nil
}

func (s *Server) Start() error {
	_ = mux.NewRouter()
	// subrouter := router.PathPrefix("/api/v1").Subrouter()

	// Routes



	// Handlers
	

	return nil
}
