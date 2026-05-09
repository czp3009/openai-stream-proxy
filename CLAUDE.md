# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working in this repository.

## Build Commands

```bash
./gradlew build
./gradlew :sniffer:run
./gradlew :mock-client:run
```

No test suites exist yet.

## Running for Testing

Since `sniffer` and the future proxy/CLI are long-running reverse proxy processes, prefer running them via IDEA run
configurations rather than in the terminal. This keeps the process easy to stop and inspect.

## Project Architecture

Kotlin Multiplatform project using Gradle version catalogs. Kotlin `2.3.21`, Ktor `3.4.3`.

## Current Status

The repository is still in an early skeleton state:

- `proxy` currently only exposes a placeholder `hello()` function.
- `cli` currently only has a placeholder `main()` and does not start an HTTP server.
- `sniffer` and `mock-client` are the only runnable helper modules today.

Descriptions below about `proxy` and `cli` are target architecture, not already-implemented behavior.

### Subprojects

#### Core

- **proxy** - Core library of this project (not yet implemented). It only handles one protocol conversion path:
  downstream non-streaming `POST /v1/responses` requests are rewritten to upstream `stream: true`, the upstream SSE is
  consumed, the final `Response` state is aggregated in memory, and then a normal non-streaming JSON response is
  returned downstream. Requests that already have `stream=true`, requests with `background=true`, and all other paths
  are passed through unchanged. Depends on `ktor-client-core`, `ktor-http`, `ktor-io`, and
  `kotlinx-serialization-json`. No platform-specific engines; consumers provide the engine.
- **cli** - CLI wrapper for `proxy` (not yet implemented). Reads config, starts the HTTP server, and delegates all
  request handling to `proxy`.

#### Auxiliary

- **sniffer** - JVM-only development tool for analyzing OpenAI API traffic. A reverse proxy that logs request and
  response headers/bodies to stdout via `TrafficLogger`. Uses Ktor CIO server. Configured via `UPSTREAM_BASE_URL` and
  `LISTEN_PORT`.
- **mock-client** - JVM-only development tool using the official `openai-java` SDK. Works with `sniffer` or the future
  proxy for data collection and testing. Requires `OPENAI_API_KEY`. Also configurable via `OPENAI_BASE_URL`,
  `OPENAI_MODEL`, and `OPENAI_PROMPT`.

### Key Patterns

- `proxy` is engine-agnostic and depends only on common Ktor client/I/O APIs.
- `proxy` only converts synchronous non-streaming `POST /v1/responses` requests.
- Requests with `background=true` are not converted and must be passed through unchanged.
- Requests with `stream=true` are not converted and must be passed through unchanged.
- For the supported conversion path, `proxy` should aggregate the final `Response` state in memory and only then write
  the downstream non-streaming JSON response. It should not stream partial JSON fragments downstream.
- `cli` wires platform-specific HTTP client engines in platform source sets.
- `proxy` and `cli` target JVM, mingwX64, linuxX64, linuxArm64, and macosArm64.
- `sniffer` and `mock-client` are JVM executables with code in `commonMain` and `mainClass.set(...)` in the JVM block.
- Configuration is read from environment variables in the current auxiliary tools. The future `proxy`/`cli` may use
  `expect/actual` for cross-platform access if needed.
