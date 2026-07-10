.PHONY: build test vet fmt-check demo benchmark

BINARY := bin/topoqueue

build:
	@mkdir -p $(dir $(BINARY))
	go build -o $(BINARY) ./cmd/topoqueue

test:
	go test -race ./...

vet:
	go vet ./...

fmt-check:
	@set -e; \
	unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		printf 'The following files require gofmt:\n%s\n' "$$unformatted"; \
		exit 1; \
	fi

demo:
	go run ./cmd/topoqueue compare \
		--cluster examples/cluster.yaml \
		--jobs examples/jobs.yaml \
		--output text

benchmark:
	go test -run '^$$' -bench . ./internal/scheduler
