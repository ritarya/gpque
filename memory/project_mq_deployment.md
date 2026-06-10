---
name: project-mq-deployment
description: MQ service deployment target — minikube with Helm, no docker-compose
metadata:
  type: project
---

Deployment target is **minikube + Helm**, not docker-compose.

**Why:** User explicitly rejected docker-compose in favour of a local Kubernetes cluster for pilot deployment and testing.

**How to apply:** Always target minikube for local deployment. Use `make minikube-build && make minikube-deploy`. Images are built inside minikube's Docker daemon (`eval $(minikube docker-env)`) with `imagePullPolicy: Never`. Helm chart lives at `helm/telemetry/`.
