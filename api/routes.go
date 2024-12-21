package api

import (
	"errors"
	"log"
	"net/http"

	"github.com/Bl4ck-h00d/stashdb/core/utils"
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
	router.HandleFunc("/create-bucket", h.handleCreateBucket).Methods("POST")
	router.HandleFunc("/set", h.handleSet).Methods("POST")
	router.HandleFunc("/get", h.handleGet).Methods("GET")
	router.HandleFunc("/delete", h.handleDelete).Methods("DELETE")
}

// POST /create-bucket
func (h *Handlers) handleCreateBucket(w http.ResponseWriter, r *http.Request) {
	var payload types.CreateBucketRequestPayload

	if err := utils.ParseJSON(r, &payload); err != nil {
		log.Printf("[%d] bucket creation failed: %v", http.StatusBadRequest, err)
		utils.WriteError(w, http.StatusBadRequest, err)
		return
	}
	err := h.store.CreateBucket(payload.Name)
	if err != nil {
		log.Printf("[%d] bucket creation failed: %v", http.StatusInternalServerError, err)
		utils.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	log.Printf("created bucket: %+v", payload)
	utils.WriteJSON(w, http.StatusCreated, nil)
}

// POST /set
func (h *Handlers) handleSet(w http.ResponseWriter, r *http.Request) {
	var payload types.SetRequestPayload

	if err := utils.ParseJSON(r, &payload); err != nil {
		log.Printf("[%d] set operation failed: %v", http.StatusBadRequest, err)
		utils.WriteError(w, http.StatusBadRequest, err)
		return
	}
	err := h.store.Set(payload.Bucket, payload.Key, []byte(payload.Value))
	if err != nil {
		log.Printf("[%d] set operation failed: %v", http.StatusInternalServerError, err)
		utils.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	log.Printf("set: %+v", payload)
	utils.WriteJSON(w, http.StatusCreated, nil)
}

// GET /get?key=_key&bucket=_bucket
func (h *Handlers) handleGet(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	key := query.Get("key")
	bucket := query.Get("bucket")

	if key == "" || bucket == "" {
		log.Printf("[%d] get operation failed: %v,%v", http.StatusBadRequest, key, bucket)
		utils.WriteError(w, http.StatusBadRequest, errors.New("key and bucket are required"))
		return
	}

	value := h.store.Get(bucket, key)
	if value == nil {
		log.Printf("[%d] get operation failed: %v,%v", http.StatusNotFound, key, bucket)
		utils.WriteError(w, http.StatusNotFound, errors.New("key not found"))
		return
	}
	log.Printf("get: %v:%v", key, string(value[:]))
	utils.WriteJSON(w, http.StatusOK, string(value[:]))
}

// DELETE /delete?key=_key&bucket=_bucket
func (h *Handlers) handleDelete(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	key := query.Get("key")
	bucket := query.Get("bucket")

	if key == "" || bucket == "" {
		log.Printf("[%d] delete operation failed: %v,%v", http.StatusBadRequest, key, bucket)
		utils.WriteError(w, http.StatusBadRequest, errors.New("key and bucket are required"))
		return
	}

	if err := h.store.Delete(bucket, key); err != nil {
		log.Printf("[%d] delete operation failed: %v", http.StatusInternalServerError, err)
		utils.WriteError(w, http.StatusInternalServerError, err)
		return
	}

	utils.WriteJSON(w, http.StatusOK, nil)
}
