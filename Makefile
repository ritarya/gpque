.PHONY: build test lint tidy cover cover-html \
        minikube-build minikube-deploy minikube-undeploy \
        minikube-logs-mq minikube-logs-streamer minikube-logs-collector minikube-logs-gateway \
        minikube-port-forward minikube-port-forward-postgres minikube-open-gateway

build:
	go build ./cmd/...

test:
	go test ./...

# Run tests with coverage and print a per-package summary to the terminal.
cover:
	go test -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out

# Open an HTML coverage report in the default browser.
cover-html: cover
	go tool cover -html=coverage.out

lint:
	go vet ./...

tidy:
	go mod tidy

# ── Minikube targets ─────────────────────────────────────────────────────────
# Build images directly inside minikube's Docker daemon so no registry push
# is needed.  imagePullPolicy: Never in the Helm chart uses them as-is.
minikube-build:
	@bash -c 'eval $$(minikube docker-env) && \
		docker build -f docker/Dockerfile.mq        -t gpqueue-mq:latest        . && \
		docker build -f docker/Dockerfile.streamer  -t gpqueue-streamer:latest  . && \
		docker build -f docker/Dockerfile.collector -t gpqueue-collector:latest . && \
		docker build -f docker/Dockerfile.gateway   -t gpqueue-gateway:latest   . && \
		docker pull timescale/timescaledb:latest-pg16'

# Deploy (or upgrade) the Helm release.  Runs minikube-build first.
minikube-deploy: minikube-build
	helm upgrade --install telemetry ./helm/telemetry

# Tear down the release (leaves PVC intact; add --purge to delete data too).
minikube-undeploy:
	helm uninstall telemetry

# Tail logs for each component.
minikube-logs-mq:
	kubectl logs -l app=telemetry-mq -f --tail=50

minikube-logs-streamer:
	kubectl logs -l app=telemetry-streamer -f --tail=50

minikube-logs-collector:
	kubectl logs -l app=telemetry-collector -f --tail=50

minikube-logs-gateway:
	kubectl logs -l app=telemetry-gateway -f --tail=50

# Expose the MQ HTTP API locally at http://localhost:8080
minikube-port-forward:
	kubectl port-forward svc/telemetry-mq 8080:8080

# Expose PostgreSQL locally at localhost:5432
minikube-port-forward-postgres:
	kubectl port-forward svc/telemetry-postgres 5432:5432

# Open the gateway in the default browser (NodePort via minikube)
minikube-open-gateway:
	minikube service telemetry-gateway
