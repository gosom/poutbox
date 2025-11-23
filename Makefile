.PHONY: gen lint help

help:
	@echo "Available commands:"
	@echo "  make gen		- Generate sqlc code"
	@echo "  make lint		- Run golangci-lint"

gen:
	cd postgres && sqlc generate

lint:
	golangci-lint run ./...
