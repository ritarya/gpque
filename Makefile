.PHONY: build test lint tidy docker-build up down

build:
	go build ./cmd/...

test:
	go test ./...

lint:
	go vet ./...

tidy:
	go mod tidy

docker-build:
	docker build -f docker/Dockerfile.mq        -t gpqueue-mq:latest        .
	docker build -f docker/Dockerfile.streamer   -t gpqueue-streamer:latest  .
	docker build -f docker/Dockerfile.collector  -t gpqueue-collector:latest .
	docker build -f docker/Dockerfile.gateway    -t gpqueue-gateway:latest   .

up:
	docker compose up --build

down:
	docker compose down -v
