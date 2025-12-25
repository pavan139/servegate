package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

func jsonResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// Handler exposes inference and process-local metrics. X-Tenant-ID is a caller
// label in this lab, not authentication or proof of tenant identity.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, http.StatusOK, map[string]any{"draining": g.draining.Load(), "backend": "operator-configured"})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		jsonResponse(w, http.StatusOK, g.Stats())
	})
	mux.HandleFunc("/v1/infer", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, int64(g.config.MaxInputBytes+128))
		defer r.Body.Close()
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var body struct {
			Input string `json:"input"`
		}
		if err := decoder.Decode(&body); err != nil {
			jsonResponse(w, http.StatusBadRequest, map[string]string{"error": "invalid_json"})
			return
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			jsonResponse(w, http.StatusBadRequest, map[string]string{"error": "invalid_json"})
			return
		}
		ctx := r.Context()
		if raw := r.Header.Get("X-Timeout-Ms"); raw != "" {
			ms, err := strconv.Atoi(raw)
			if err != nil || ms < 1 || int64(ms) > g.config.MaxRequestTimeout.Milliseconds() {
				jsonResponse(w, http.StatusBadRequest, map[string]string{"error": "invalid_timeout"})
				return
			}
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
			defer cancel()
		}
		result, err := g.Infer(ctx, r.Header.Get("X-Tenant-ID"), body.Input)
		if err == nil {
			jsonResponse(w, http.StatusOK, result)
			return
		}
		status, code := http.StatusBadGateway, "backend_error"
		switch {
		case errors.Is(err, ErrInvalid):
			status, code = http.StatusBadRequest, "invalid_request"
		case errors.Is(err, ErrOverloaded), errors.Is(err, ErrTenantFull), errors.Is(err, ErrTenantLimit):
			status, code = http.StatusTooManyRequests, err.Error()
			w.Header().Set("Retry-After", "1")
		case errors.Is(err, ErrDraining), errors.Is(err, ErrCircuitOpen):
			status, code = http.StatusServiceUnavailable, err.Error()
		case errors.Is(err, context.DeadlineExceeded):
			status, code = http.StatusGatewayTimeout, "deadline_exceeded"
		case errors.Is(err, context.Canceled):
			status, code = 499, "request_cancelled"
		}
		jsonResponse(w, status, map[string]string{"error": code})
	})
	return mux
}
