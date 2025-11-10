.PHONY: build
build:
	go build -o pgverify ./cmd/pgverify

.PHONY: clean
clean:
	-@rm -f pgverify coverage.txt int-test-junit.xml
	-@go clean -testcache

.PHONY: lint
lint:
	@go tool golangci-lint run

.PHONY: lint-fix
lint-fix:
	@go tool golangci-lint run --fix

.PHONY: unit-test
unit-test:
	@go tool gotestsum --format=testname -- \
	    -v -short ./...

.PHONY: test
test:
	@go tool gotestsum --format=testname --junitfile int-test-junit.xml -- \
	    -v -cover -coverprofile coverage.txt -covermode=atomic -race ./...
