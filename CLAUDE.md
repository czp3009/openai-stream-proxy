# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
./gradlew build              # Build all subprojects
./gradlew :sniffer:run       # Run sniffer on JVM
./gradlew :mock-client:run   # Run mock-client on JVM
./gradlew :sniffer:mingwX64Run   # Run sniffer as native binary (Windows)
./gradlew :sniffer:linuxX64Run   # Run sniffer as native binary (Linux)
```

No test suites exist yet.

## Running for Testing

Since sniffer and proxy are long-running reverse proxy processes, prefer running them via **IDEA run configurations**
rather than terminal commands. This ensures the process is trackable and can be easily stopped through IDEA's Run tool
window.

## Project Architecture

Kotlin Multiplatform (KMP) project using Gradle with version catalog (`gradle/libs.versions.toml`). Targets: JVM,
mingwX64, linuxX64, linuxArm64, macosArm64. Kotlin 2.3.21, Ktor 3.4.3.

### Subprojects

#### Core

- **proxy** — Core library of this project (not yet implemented). Provides a reverse proxy that converts OpenAI
  non-streaming requests to streaming requests sent upstream, or streaming requests to non-streaming requests sent
  upstream. Depends on ktor-client-core, ktor-http, ktor-io, kotlinx-serialization-json. No platform-specific engines;
  consumers provide the engine.
- **cli** — CLI wrapper for proxy (not yet implemented). A command-line program that reads a YAML config file to call
  proxy and listens on the configured port, performing stream/non-stream conversion. Depends on `:proxy`.

#### Auxiliary (not part of the core project)

- **sniffer** — Development tool for analyzing OpenAI API traffic. A reverse proxy that intercepts and logs request/
  response headers and bodies to stdout via `TrafficLogger`. The collected data informs proxy development. Uses Ktor CIO
  server. Consumes `:proxy`. Has `expect/actual` for `environment()` (JVM: `System.getenv`, native: `posix.getenv`).
  Configured via env vars `UPSTREAM_BASE_URL` and `LISTEN_PORT`.
- **mock-client** — Development tool for programmatic data collection and testing. A client using the official
  `openai-java` SDK (JVM-only). Works with sniffer to collect traffic data programmatically, or tests proxy
  programmatically. Requires API key (env var `OPENAI_API_KEY`). Run via IDEA run configuration (env vars already set
  there). Also configurable via `OPENAI_BASE_URL`, `OPENAI_MODEL`, `OPENAI_PROMPT`.

### Key Patterns

- Platform-specific HTTP engines are wired in `sourceSets`: JVM uses `ktor-client-cio`, native uses `ktor-client-curl`.
- Native entry points use `entryPoint = "com.hiczp.openai.responses.stream.proxy.sniffer.main"` on all
  `KotlinNativeTarget` targets. JVM uses `mainClass.set(...)` via the `binaries.executable` block.
- Configuration is read from environment variables through an `expect/actual` function to support both JVM and native
  platforms.
