GO ?= go
export GOFLAGS ?= -p=2
export GOMAXPROCS ?= 2

.PHONY: test vet fmt demo evaluate build
test:
	$(GO) test -race -count=1 ./...
vet:
	$(GO) vet ./...
fmt:
	$(GO) fmt ./...
demo:
	$(GO) run ./cmd/servegate demo --output artifacts/demo.json
evaluate:
	$(GO) run ./cmd/servegate evaluate --output artifacts/evaluation.json
build:
	mkdir -p build
	$(GO) build -trimpath -o build/servegate ./cmd/servegate
