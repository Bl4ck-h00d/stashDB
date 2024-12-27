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
		utils.Log("bucket creation failed", http.StatusBadRequest, err)
		utils.WriteError(w, http.StatusBadRequest, err)
		return
	}
	err := h.store.CreateBucket(payload.Name)
	if err != nil {
		utils.Log("bucket creation failed", http.StatusInternalServerError, err)
		utils.WriteError(w, http.StatusInternalServerError, err)
		return
	}

	utils.Log("bucket created", http.StatusCreated, nil)
	utils.WriteJSON(w, http.StatusCreated, nil)
}

// POST /set
func (h *Handlers) handleSet(w http.ResponseWriter, r *http.Request) {
	var payload types.SetRequestPayload

	if err := utils.ParseJSON(r, &payload); err != nil {
		utils.Log("SET operation failed", http.StatusBadRequest, err)
		utils.WriteError(w, http.StatusBadRequest, err)
		return
	}
	err := h.store.Set(payload.Bucket, payload.Key, []byte(payload.Value))
	if err != nil {
		utils.Log("SET operation failed", http.StatusInternalServerError, err)
		utils.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	log.Printf("set: %+v", payload)
	utils.Log("SET succeeded", http.StatusOK, nil)
	utils.WriteJSON(w, http.StatusCreated, nil)
}

// GET /get?key=_key&bucket=_bucket
func (h *Handlers) handleGet(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	key := query.Get("key")
	bucket := query.Get("bucket")

	if key == "" || bucket == "" {
		utils.Log("GET operation failed", http.StatusBadRequest, errors.New("empty key or bucket"))
		utils.WriteError(w, http.StatusBadRequest, errors.New("key and bucket are required"))
		return
	}

	value := h.store.Get(bucket, key)
	if value == nil {
		utils.Log("GET operation failed", http.StatusNotFound, errors.New("key not found"))
		utils.WriteError(w, http.StatusNotFound, errors.New("key not found"))
		return
	}

	utils.Log("GET succeeded", http.StatusOK, nil)
	utils.WriteJSON(w, http.StatusOK, string(value[:]))
}

// DELETE /delete?key=_key&bucket=_bucket
func (h *Handlers) handleDelete(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	key := query.Get("key")
	bucket := query.Get("bucket")

	if key == "" || bucket == "" {
		utils.Log("DELETE operation failed", http.StatusBadRequest, errors.New("empty key or bucket"))
		utils.WriteError(w, http.StatusBadRequest, errors.New("key and bucket are required"))
		return
	}

	if err := h.store.Delete(bucket, key); err != nil {
		utils.Log("DELETE operation failed", http.StatusInternalServerError, err)
		utils.WriteError(w, http.StatusInternalServerError, err)
		return
	}

	utils.WriteJSON(w, http.StatusOK, nil)
}
