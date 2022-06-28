.PHONY: build
build:
	go build -o pgverify ./cmd/pgverify

.PHONY: clean
clean:
	@rm -f pgverify coverage.txt ||:
	@rm -rf .bin ||:
	@go clean -testcache ||:

.PHONY: lint
lint: .bin/golangci-lint
	@.bin/golangci-lint run

.PHONY: test
test:
	go test -v -cover -coverprofile coverage.txt -covermode=atomic ./...


################################################################################
# Tools
################################################################################

.bin/golangci-lint: $(wildcard vendor/github.com/golangci/*/*.go)
	@echo "building linter..."
	@cd vendor/github.com/golangci/golangci-lint/cmd/golangci-lint && go build -o $(shell git rev-parse --show-toplevel)/.bin/golangci-lint .
