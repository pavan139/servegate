package experiment

import "testing"

func TestActualLoopbackAndControlledExperiments(t *testing.T) {
	report, err := Evaluate(32, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	if report["passed"] != 4 || report["total"] != 4 {
		t.Fatalf("failed workload: %+v", report)
	}
	baseline := report["unbatched"].(Workload)
	batched := report["microbatched"].(Workload)
	if baseline.Stats.BackendCalls != 32 || batched.Stats.BackendCalls >= 32 || batched.ClientLatency.Count != 32 {
		t.Fatal("actual batching/latency measurements missing")
	}
	if report["cancellation"].(Cancellation).CancelledClients != 4 {
		t.Fatal("HTTP cancellations were not observed")
	}
}

func TestWorkloadInputBounds(t *testing.T) {
	for _, parameters := range [][4]int{{0, 8, 1, 8}, {32, 0, 1, 8}, {32, 8, 0, 8}, {32, 8, 1, 0}, {4097, 8, 1, 8}} {
		if _, err := HTTPWorkload(parameters[0], parameters[1], parameters[2], parameters[3]); err == nil {
			t.Fatal("invalid workload admitted")
		}
	}
}
