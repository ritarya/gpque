.PHONY: build test lint tidy \
        minikube-build minikube-deploy minikube-undeploy \
        minikube-logs-mq minikube-logs-streamer minikube-port-forward

build:
	go build ./cmd/...

test:
	go test ./...

lint:
	go vet ./...

tidy:
	go mod tidy

# ── Minikube targets ─────────────────────────────────────────────────────────
# Build images directly inside minikube's Docker daemon so no registry push
# is needed.  imagePullPolicy: Never in the Helm chart uses them as-is.
minikube-build:
	@bash -c 'eval $$(minikube docker-env) && \
		docker build -f docker/Dockerfile.mq      -t gpqueue-mq:latest      . && \
		docker build -f docker/Dockerfile.streamer -t gpqueue-streamer:latest .'

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

# Expose the MQ HTTP API locally at http://localhost:8080
minikube-port-forward:
	kubectl port-forward svc/telemetry-mq 8080:8080
