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

## Project Architecture

Kotlin Multiplatform (KMP) project using Gradle with version catalog (`gradle/libs.versions.toml`). Targets: JVM,
mingwX64, linuxX64, linuxArm64, macosArm64. Kotlin 2.3.21, Ktor 3.4.3.

### Subprojects

- **proxy** — Shared library module with streaming HTTP reverse proxy logic. Depends on ktor-client-core, ktor-http,
  ktor-io, kotlinx-serialization-json. No platform-specific engines; consumers provide the engine.
- **sniffer** — Standalone reverse proxy that intercepts and logs OpenAI API traffic (request/response headers and
  bodies) to stdout via `TrafficLogger`. Uses Ktor CIO server. Consumes `:proxy`. Has `expect/actual` for
  `environment()` (JVM: `System.getenv`, native: `posix.getenv`). Configured via env vars `UPSTREAM_BASE_URL` and
  `LISTEN_PORT`.
- **cli** — Placeholder module depending on `:proxy`. Currently just prints a hello message.
- **mock-client** — Test utility that sends a non-streaming OpenAI Responses API request. Uses the official
  `openai-java` SDK (JVM-only). Configured via env vars `OPENAI_API_KEY`, `OPENAI_BASE_URL`, `OPENAI_MODEL`,
  `OPENAI_PROMPT`.

### Key Patterns

- Platform-specific HTTP engines are wired in `sourceSets`: JVM uses `ktor-client-cio`, native uses `ktor-client-curl`.
- Native entry points use `entryPoint = "com.hiczp.openai.responses.stream.proxy.sniffer.main"` on all
  `KotlinNativeTarget` targets. JVM uses `mainClass.set(...)` via the `binaries.executable` block.
- Configuration is read from environment variables through an `expect/actual` function to support both JVM and native
  platforms.
