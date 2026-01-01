package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"servegate/experiment"
	"servegate/gateway"
)

func writeJSON(value any, path string) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if path == "" {
		_, err = os.Stdout.Write(data)
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: servegate demo|evaluate|serve [--output PATH]")
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	output := flags.String("output", "", "JSON report path")
	defaultRequests, defaultClients, defaultRounds := 128, 16, 256
	if args[0] == "demo" {
		defaultRequests, defaultClients, defaultRounds = 32, 8, 64
	}
	requests := flags.Int("requests", defaultRequests, "bounded loopback HTTP request count")
	clients := flags.Int("clients", defaultClients, "bounded HTTP clients")
	rounds := flags.Int("rounds", defaultRounds, "SHA-256 rounds per input (CPU fixture)")
	listen := flags.String("listen", "127.0.0.1:8080", "serve address")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	switch args[0] {
	case "demo":
		report, err := experiment.HTTPWorkload(*requests, *clients, *rounds, 8)
		if err != nil {
			return err
		}
		if err = writeJSON(map[string]any{"backend": "deterministic_sha256_cpu_fixture", "model_calls": 0, "workload": report, "passed": report.Passed}, *output); err != nil {
			return err
		}
		if !report.Passed {
			return errors.New("demo invariant failed")
		}
		return nil
	case "evaluate":
		report, err := experiment.Evaluate(*requests, *clients, *rounds)
		if err != nil {
			return err
		}
		if err = writeJSON(report, *output); err != nil {
			return err
		}
		if report["passed"] != report["total"] {
			return errors.New("evaluation invariant failed")
		}
		return nil
	case "serve":
		if *rounds < 1 || *rounds > 10000 {
			return errors.New("rounds must be1..10000")
		}
		g, err := gateway.New(gateway.DefaultConfig(), gateway.CPUBackend{Rounds: *rounds})
		if err != nil {
			return err
		}
		server := &http.Server{Addr: *listen, Handler: g.Handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		failed := make(chan error, 1)
		go func() { failed <- server.ListenAndServe() }()
		select {
		case err := <-failed:
			if !errors.Is(err, http.ErrServerClosed) {
				closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = g.Close(closeCtx)
				return err
			}
		case <-ctx.Done():
		}
		drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = g.Close(drain)
		return server.Shutdown(drain)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
func main() {
	if err := run(os.Args[1:]); err != nil {
		_ = json.NewEncoder(os.Stderr).Encode(map[string]string{"error": "servegate_failed", "detail": err.Error()})
		os.Exit(1)
	}
}
