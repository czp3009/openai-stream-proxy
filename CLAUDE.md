# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working in this repository.

## Build Commands

```bash
./gradlew build
./gradlew :sniffer:run
./gradlew :mock-client:run
```

## Running for Testing

Modules require environment variables that are configured in IDEA run configurations. When running any module,
**always use IDEA MCP to execute the IDEA run configuration** (`mcp__idea__execute_run_configuration`). Never run
`./gradlew :module:run` from the command line, because the environment variables won't be set.

The IDEA MCP cannot read the specific environment variable values inside a run configuration. To verify that a
configuration is correct, run it and infer from the output (e.g. log lines showing resolved config values, or missing
variable errors). If a required run configuration does not exist, or if the output suggests the configuration is wrong,
ask the user to create or fix it in IDEA (Run → Edit Configurations).

Current run configurations and their expected environment variables:

- **sniffer [jvm]** — `UPSTREAM_BASE_URL` (required), `LISTEN_PORT` (optional)
- **mock-client [jvm]** — `OPENAI_API_KEY` (required), `OPENAI_BASE_URL`, `OPENAI_MODEL`, `OPENAI_PROMPT` (optional)

## Project Architecture

Kotlin Multiplatform project using Gradle version catalogs. Kotlin `2.3.21`, Ktor `3.4.3`.

### Subprojects

#### Core

- **proxy** - Core library. Handles one protocol conversion path: downstream non-streaming `POST /v1/responses`
  requests are rewritten to upstream `stream: true`, the upstream SSE is consumed, the final `Response` state is
  aggregated in memory, and then a normal non-streaming JSON response is returned downstream. Requests
  that do not match the conversion criteria (non-POST, non-`/responses` path, non-JSON content type,
  missing `model` field, or `stream=true`) are passed through unchanged. Depends on
  `ktor-client-core`, `ktor-http`, `ktor-io`, and `kotlinx-serialization-json`. No platform-specific engines;
  consumers provide the engine.
- **cli** - CLI wrapper for `proxy`. Reads a JSON config file (via `--config-file`, default `config.json`), starts one
  Ktor CIO server per config rule (each listening on a different port). Routes non-POST, non-`/responses`,
  or non-JSON requests to `passthrough()` and all others to `proxy()`. Both return `OutgoingContent?`; null → 502
  `upstream_error`,
  exceptions → 500 `internal_error` via StatusPages.
  Uses `kotlinx-cli` for argument parsing. Targets JVM, mingwX64, linuxX64, macosArm64 (not linuxArm64 — `kotlinx-cli`
  doesn't support it).

#### Auxiliary

- **sniffer** - JVM-only development tool for analyzing OpenAI API traffic. A reverse proxy that logs request and
  response headers/bodies to stdout via `TrafficLogger`. Uses Ktor CIO server. Configured via `UPSTREAM_BASE_URL` and
  `LISTEN_PORT`.
- **mock-client** - JVM-only development tool using the official `openai-java` SDK. Works with `sniffer`, `cli`, or
  any OpenAI-compatible proxy for data collection and testing. Supports both streaming and non-streaming modes.
  Requires `OPENAI_API_KEY`. Also configurable via `OPENAI_BASE_URL`, `OPENAI_MODEL`, `OPENAI_PROMPT`,
  and `OPENAI_STREAM` (optional).

### Key Patterns

- `proxy` is engine-agnostic and depends only on common Ktor client/I/O APIs.
- `proxy` only converts synchronous non-streaming `POST /v1/responses` requests.
- Requests with `stream=true` are not converted and must be passed through unchanged.
- For the supported conversion path, `proxy` aggregates the final `Response` state in memory and only then writes
  the downstream non-streaming JSON response. It does not stream partial JSON fragments downstream.
- `cli` uses CIO engine everywhere (client and server), no HTTPS support.
- `proxy` targets JVM, mingwX64, linuxX64, linuxArm64, and macosArm64.
- `cli` targets JVM, mingwX64, linuxX64, and macosArm64 (no linuxArm64).
- `sniffer` and `mock-client` are JVM executables with code in `commonMain` and `mainClass.set(...)` in the JVM block.
