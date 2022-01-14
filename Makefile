.PHONY: build
build:
	go build -o dbverify ./cmd/dbverify

.PHONY: clean
clean:
	@rm -f dbverify ||:
	@go clean -testcache ||:

.PHONY: lint
lint:
	@go run vendor/github.com/golangci/golangci-lint/cmd/golangci-lint/main.go -v run

.PHONY: test
test:
	go test ./...
