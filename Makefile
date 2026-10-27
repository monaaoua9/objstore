GO ?= go

.PHONY: build test race bench vet fmt cluster docker clean

build:
	$(GO) build -trimpath -o bin/ ./cmd/...

test:
	$(GO) test -short ./...

race:
	$(GO) test -race -count=1 ./...

bench:
	$(GO) test -run xxx -bench . -benchmem ./internal/erasure ./internal/hashring

vet:
	$(GO) vet ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

cluster: build
	./scripts/dev-cluster.sh

docker:
	docker build -t objstore:dev .

clean:
	rm -rf bin .dev-data
