package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func ParseJSON(r *http.Request, payload any) error {
	if r.Body == nil {
		return fmt.Errorf("missing request body")
	}

	return json.NewDecoder(r.Body).Decode(payload)
}

func WriteJSON(w http.ResponseWriter, status int, payload any) error {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(payload)
}

func WriteError(w http.ResponseWriter, status int, err error) error {
	return WriteJSON(w, status, map[string]string{"err": err.Error()})
}

func Log(action string, status int, err error) {
	if err != nil {
		log.Printf("[%d] %s: %v", status, action, err)
	} else {
		log.Printf("[%d] %s", status, action)
	}
}
