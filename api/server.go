package api

import (
	"log"
	"net/http"

	"github.com/Bl4ck-h00d/stashdb/core/store"
	"github.com/gorilla/mux"
)

type Server struct {
	address string
	dbPath  string
}

func NewAPIServer(address, dbPath string) *Server {
	return &Server{address: address, dbPath: dbPath}
}

func (s *Server) Start() error {
	router := mux.NewRouter()
	subrouter := router.PathPrefix("/api/v1").Subrouter()

	dataStore, err := store.NewStore("bolt", s.dbPath)
	if err != nil {
		log.Fatalf("error starting the store: %v", err)
	}

	// Handlers
	handlers := NewHandler(dataStore)

	// Register routes
	handlers.RegisterRoutes(subrouter)

	// Start server
	log.Println("Listening on ", s.address)
	return http.ListenAndServe(s.address, router)
}
