.PHONY: build run test test-race clean

build:
	go build -o bin/enclave ./cmd/enclave

run:
	ENV=development go run ./cmd/enclave

test:
	go test ./... -v

# The race detector is ThreadSanitizer, a C runtime, so it needs cgo and a C
# toolchain — which a Windows dev box does not have. Running it in the Go image
# gets the same detector every other machine uses without installing a compiler
# here. GOFLAGS=-buildvcs=false because the mounted .git is owned by another uid
# inside the container.
test-race:
	docker run --rm \
		-v "$(CURDIR):/src" \
		-v zk-gomod:/go/pkg/mod \
		-w /src \
		-e CGO_ENABLED=1 \
		-e GOFLAGS=-buildvcs=false \
		golang:1.26 \
		go test -race ./internal/... ./pkg/... -count=1

clean:
	rm -rf bin/

lint:
	golangci-lint run ./...

docker-build:
	docker build -t enclave:latest .
