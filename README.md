## Prerequisites:
- Run a NATS server with JetStream enabled. You can use Docker:
  ```bash
  sudo docker run -p 4222:4222 --name nats -d nats --jetstream
  ```
- Ensure you have the NATS Go client and OpenTelemetry dependencies.


