# Strimzi Kafka Bridge

Strimzi Kafka Bridge provides an HTTP interface to Apache Kafka® clusters.
It allows HTTP clients to send and receive messages to/from Kafka topics without using the native Kafka protocol.

See [README.md](README.md) for project overview, quick starts, and community information.

## Architecture

Single-module Maven project.

### Requirements

- **Java**: Java 21
- **Build tools**: Maven 3.5+, make, bash
- **Container runtime**: Docker or Podman (use `DOCKER_CMD=podman` for Podman)
- **Testing**: Docker or Podman (for test-containers with Strimzi Kafka container)

### Key Dependencies

- Vert.x and Kafka client versions: See `pom.xml`

### Packages

| Package | Purpose                                                            |
|--------|--------------------------------------------------------------------|
| `io.strimzi.kafka.bridge` | Main bridge implementation (core components)                              |
| `io.strimzi.kafka.bridge.http` | HTTP-specific bridge implementation (REST API)           |
| `io.strimzi.kafka.bridge.config` | Configuration management                                 |
| `io.strimzi.kafka.bridge.converter` | Message format converters (JSON, binary, etc.)           |
| `io.strimzi.kafka.bridge.metrics` | Metrics and monitoring                                   |
| `io.strimzi.kafka.bridge.tracing` | OpenTelemetry tracing integration                        |

## Contributing

- **Commit sign-off (DCO)** (REQUIRED on all commits): always use `git commit -s`
  - If forgotten: `git commit --amend -s` to fix last commit
  - CI will fail without sign-off

- **Code style**: Enforced by checkstyle (runs in CI on all PRs)
  - Config: `.checkstyle/checkstyle.xml`
  - Run locally: `mvn checkstyle:check`
  - CI will fail if checkstyle errors are found

## Developer Notes

### Common Development Tasks

**Building:**
- Build project: `make all` (compiles Java, runs tests, builds Docker images)
- Clean build artifacts: `make clean`
- Customize Maven behavior: Use `MVN_ARGS` environment variable
  - Skip all tests: `MVN_ARGS=-DskipTests make all`
  - Skip integration tests only: `MVN_ARGS=-DskipITs make all`

**Testing:**
- Unit tests only: `mvn test`
- Unit + integration tests: `mvn verify`
- Run specific test: `mvn test -Dtest=TestClassName#testMethodName`
- See [TESTING.md](development-docs/TESTING.md) for detailed testing information

**OpenAPI Changes** (If you changed the REST API):
1. Update `src/main/resources/openapi.json` with your changes
2. Regenerate API documentation: `make docu_api`
   - This generates `.adoc` files in `documentation/book/api/` from the OpenAPI spec
   - Uses the template in `documentation/book/api/template/`
3. Do NOT edit generated `.adoc` files in `documentation/book/api/` directly, they will be overwritten

**Container Images:**
- Built via Makefiles and Dockerfile (in the project root folder)
- Use `DOCKER_CMD=podman` to use Podman instead of Docker
- Environment variables for custom registry:
  - `DOCKER_ORG`: your registry organization/username (e.g., Docker Hub or Quay.io username, default: `$USER`)
  - `DOCKER_REGISTRY`: registry to use (e.g., `docker.io`, `quay.io`, default: `docker.io`)
  - `DOCKER_TAG`: image tag (default: `latest`)
  - `DOCKER_ARCHITECTURE`: target architecture for container images (e.g., `amd64`, `arm64`)

## Documentation

User-facing documentation is in `documentation/` folder (AsciiDoc format):
- **HTTP Bridge overview**: Introduction and concepts
- **HTTP Bridge quickstart**: Getting started guide with practical examples
- **HTTP Bridge configuration**: Configuration guide for Kafka properties, metrics, and tracing
- **API reference**: Generated from OpenAPI specification

**Important**: Do NOT edit the API reference `.adoc` files in `documentation/book/api` directly - they are generated from `src/main/resources/openapi.json`.