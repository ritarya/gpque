# gpque
A GPU telemetry pipeline built around a **custom message queue service** as its core infrastructure. Telemetry data is ingested by one or more Streamers, routed through the queue to one or more Collectors that parse and persist it, and then exposed to consumers via a REST API Gateway.
