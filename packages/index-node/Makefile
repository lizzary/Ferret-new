.PHONY: build test race coverage proto lint check

build:
	go build -buildvcs=false ./cmd/indexnode

test:
	go test -buildvcs=false -count=1 ./...

race:
	go test -buildvcs=false -race -count=1 ./...

coverage:
	go test -buildvcs=false -count=1 -cover ./internal/store ./internal/debounce ./internal/errclass ./internal/scheduler

proto:
	buf generate

lint:
	go vet ./...

check: build lint test race coverage
