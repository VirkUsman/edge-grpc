# Tooling, once:
#   go install github.com/bufbuild/buf/cmd/buf@v1.47.2
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.2
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
#   go install honnef.co/go/tools/cmd/staticcheck@latest

.PHONY: proto lint vet test race check run

proto:
	buf lint
	buf generate

vet:
	go vet ./...

lint:
	staticcheck ./...

test:
	go test ./...

race:
	go test -race -count=1 ./...

check: vet lint race

run:
	go run ./cmd/edged
