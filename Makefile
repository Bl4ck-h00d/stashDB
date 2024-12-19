build:
	@go build -o bin/server main.go

test:
	@go test -v ./...

run: build
	@./bin/server