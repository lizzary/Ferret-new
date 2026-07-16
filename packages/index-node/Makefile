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
	protoc -I api/proto \
		--go_out=gen --go_opt=paths=source_relative \
		--go-grpc_out=gen --go-grpc_opt=paths=source_relative \
		api/proto/compute/v1/compute.proto

lint:
	go vet -buildvcs=false ./...

check: build lint test race coverage
