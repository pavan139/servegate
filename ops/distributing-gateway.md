# Distributing ServeGate

First decide whether you are sharing source or a binary. The source needs Go 1.23 or later and has no external Go modules. A binary is built for a particular OS/architecture; publish that target alongside its build settings rather than presenting it as portable source.

```sh
GOFLAGS=-p=2 GOMAXPROCS=2 go test -race -count=1 ./...
go build -trimpath -o build/servegate ./cmd/servegate
./build/servegate demo --output artifacts/binary-demo.json
python -m pip install -r requirements-dev.txt
python hack/check_gateway.py --require-lint --wheel --go /path/to/real/go
```

The verifier's `--wheel` flag checks the Go executable from a separate temporary directory. It does not turn this module into a Python package. Run the HTTP example against loopback before changing listen addresses, and document whatever authenticates tenant identity in a service that uses this gateway.

Keep the small `loadtests/request.json` and measured `benchmarks/` with the source. Compiled files under `build/` and local load outputs under `artifacts/` are generated. If a timing table changes, update `saturation-runs.md` with the same request count, client count and SHA-256 work configuration used by the JSON.

The workspace directory has its own Git repository. A ZIP extraction needs Git initialization and an initial commit after reviewing its files. Add the empty repository you created on your host:

```sh
git remote add origin https://github.com/YOUR_USERNAME/servegate.git
git push -u origin HEAD
```

CI provisions Go 1.25.1, runs the race tests and verification, and retains the report. A local successful build is separate from the hosted workflow's result after pushing.
