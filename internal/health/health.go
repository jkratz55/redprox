package health

import (
	"encoding/json"
	"net/http"
	_ "net/http/pprof"
)

type Handler struct {
	mux *http.ServeMux
}

func NewHandler() *Handler {
	mux := http.NewServeMux()

	// Endpoint for K8s liveness/health check
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "ok",
		})
	})

	// Add pprof endpoints
	mux.Handle("/debug/pprof/", http.DefaultServeMux)

	return &Handler{
		mux: mux,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}
