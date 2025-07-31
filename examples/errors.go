// main.go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/oarkflow/mq/apperror"
)

// hook every error to console log
func OnError(e *apperror.AppError) {
	log.Printf("ERROR %s: %s (HTTP %d) metadata=%v\n",
		e.Code, e.Message, e.StatusCode, e.Metadata)
}

func main() {
	// pick your environment
	os.Setenv("APP_ENV", apperror.EnvDevelopment)
	apperror.OnError(OnError)
	mux := http.NewServeMux()
	mux.Handle("/user", apperror.HTTPMiddleware(http.HandlerFunc(userHandler)))
	mux.Handle("/panic", apperror.HTTPMiddleware(http.HandlerFunc(panicHandler)))
	mux.Handle("/errors", apperror.HTTPMiddleware(http.HandlerFunc(listErrors)))

	fmt.Println("Listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}

func userHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		if e, ok := apperror.Get("ErrInvalidInput"); ok {
			apperror.WriteJSONError(w, r, e)
			return
		}
	}

	if id == "0" {
		root := errors.New("db: no rows")
		appErr := apperror.Wrap(root, http.StatusNotFound, 1, 2, 5, "User not found")
		// code → "404010205"
		apperror.WriteJSONError(w, r, appErr)
		return
	}

	if id == "exists" {
		if e, ok := apperror.Get("ErrUserExists"); ok {
			apperror.WriteJSONError(w, r, e)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"id":"%s","name":"Alice"}`, id)
}

func panicHandler(w http.ResponseWriter, r *http.Request) {
	panic("unexpected crash")
}

func listErrors(w http.ResponseWriter, r *http.Request) {
	all := apperror.List()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(all)
}
