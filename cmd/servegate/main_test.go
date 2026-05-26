package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDemoAndEvaluateWriteActualJSON(t *testing.T) {
	for _, command := range []string{"demo", "evaluate"} {
		path := filepath.Join(t.TempDir(), "nested", "report.json")
		if err := run([]string{command, "--output", path, "--requests", "16", "--clients", "8", "--rounds", "8"}); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var report map[string]any
		if err = json.Unmarshal(data, &report); err != nil || report["backend"] != "deterministic_sha256_cpu_fixture" {
			t.Fatalf("err=%v report=%v", err, report)
		}
	}
}

func TestCLIRejectsUnknownArgumentsAndOutputFailures(t *testing.T) {
	for _, args := range [][]string{{}, {"unknown"}, {"demo", "extra"}, {"evaluate", "--requests", "0"}, {"serve", "--rounds", "0"}, {"demo", "--output", t.TempDir()}} {
		if err := run(args); err == nil {
			t.Fatalf("invalid CLI succeeded: %v", args)
		}
	}
}
