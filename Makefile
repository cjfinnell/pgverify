.PHONY: build
build:
	go build -o pgverify ./cmd/pgverify

.PHONY: clean
clean:
	@rm -f pgverify ||:
	@go clean -testcache ||:

.PHONY: lint
lint:
	@go run vendor/github.com/golangci/golangci-lint/cmd/golangci-lint/main.go -v run

.PHONY: test
test:
	go test -v -cover -coverprofile coverage.txt -covermode=atomic ./...
