package main

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// startHealthServer brings up a tiny HTTP server for k8s probes and basic
// observability. Returns nil if addr is empty or bind fails (we don't want a
// missing /healthz to take down the metrics agent — log and continue).
func startHealthServer(addr string, stats *selfStats, reg *registry, log *slog.Logger) *http.Server {
	if addr == "" {
		return nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Warn("health server bind failed", "addr", addr, "err", err)
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		snap := stats.snapshot()
		if reg != nil {
			snap.ByProject = reg.snapshot()
		}
		_ = json.NewEncoder(w).Encode(snap)
	})
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Warn("health server exited", "err", err)
		}
	}()
	log.Info("health server started", "addr", ln.Addr().String())
	return srv
}
