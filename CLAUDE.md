# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working in this repository.

## Build & Test

```bash
./gradlew :proxy:compileKotlinJvm            # compile proxy (JVM only, fast)
./gradlew :cli:compileKotlinJvm              # compile cli (JVM only, fast)
./gradlew :proxy:jvmTest                     # proxy tests (JVM only)
./gradlew :cli:jvmTest                       # cli tests (JVM only)
./gradlew build                              # build all modules (all platforms, slow)
./gradlew test                               # all tests (all platforms, slow)
```

JVM toolchain: 21. Kotlin `2.3.21`, Ktor `3.5.0`.

Several modules are Kotlin Multiplatform projects targeting multiple platforms (JVM, mingwX64, linuxX64,
macOSArm64, etc.). Building all platforms is slow. To check whether code compiles or to run tests, **JVM
only is sufficient** — prefer the JVM-only commands above over `build` and `test`.

## Running Modules

Modules require environment variables that are configured in IDEA run configurations. When running any module,
**always use IDEA MCP to execute the IDEA run configuration** (`mcp__idea__execute_run_configuration`). Never run
`./gradlew :module:run` from the command line, because the environment variables won't be set.

The IDEA MCP cannot read the specific environment variable values inside a run configuration. To verify that a
configuration is correct, run it and infer from the output (e.g. log lines showing resolved config values, or missing
variable errors). If a required run configuration does not exist, or if the output suggests the configuration is wrong,
ask the user to create or fix it in IDEA (Run → Edit Configurations).

Current run configurations and their expected environment variables:

- **cli [jvm]** — no environment variables; reads a JSON config file (default `config.json`, override with
  `--config-file`)
- **sniffer [jvm]** — `UPSTREAM_BASE_URL` (optional, default `https://api.openai.com`), `LISTEN_PORT` (optional, default
  `8080`)
- **mock-client-responses [jvm] stream** / **mock-client-responses [jvm] non-stream** — `OPENAI_API_KEY` (required),
  `OPENAI_BASE_URL`,
  `OPENAI_MODEL`, `OPENAI_PROMPT` (optional)
- **mock-client-chat-completions [jvm] stream** / **mock-client-chat-completions [jvm] non-stream** — same environment
  variables as mock-client-responses
- **openai-stream-proxy-0.0.1[mingwX64/linuxX64/macosArm64]** — native CLI binaries; same config-file argument
  as JVM

## Project Architecture

Kotlin Multiplatform project using Gradle version catalogs.

### Subprojects

#### Core

- **proxy** - Core library. `AbstractApiProxy` is the shared base class providing the upstream HTTP
  client (with SSE and timeout plugins), `passthrough()` for forwarding requests unchanged, and
  `stripHopByHopHeaders()`. The public entry point `proxy()` is a template method that validates
  the request and delegates to `convert()` (another template method). Subclasses implement
  `needConvert()` (path/method matching), `rewriteBody()` (body rewriting), `createAccumulator()`
  (SSE event accumulator factory), and `buildResult()` (final response assembly) to define
  protocol-specific conversion logic. `ResponsesApiProxy` extends `AbstractApiProxy` and handles one protocol conversion
  path:
  downstream non-streaming `POST /v1/responses` requests are rewritten to upstream `stream: true`,
  the upstream SSE is consumed by `ResponseAccumulator` (which collects `response.output_item.done`
  events and waits for a terminal event: `response.completed`, `response.failed`, or
  `response.incomplete`), the final `Response` state is assembled in memory, and a non-streaming
  JSON response is returned downstream.
  Requests that do not match the conversion criteria (non-POST, non-`/responses` path, non-JSON
  content type, missing `model` field, or `stream=true`) are passed through unchanged via
  `passthrough()`.
  When the upstream returns a non-SSE error response during the SSE connect phase, it is relayed
  as-is. Returns `OutgoingContent?` — null signals an upstream failure (incomplete stream, network
  error); exceptions signal proxy bugs (caller should map to 500). Depends on `ktor-client-core`,
  `ktor-http`, `ktor-io`, `kotlinx-serialization-json`, and `kotlin-logging`. No platform-specific
  engines; consumers provide the engine.
  Also contains `OpenAiErrors` (utility for building OpenAI-compatible error responses),
  `SseAccumulator` (interface for SSE event accumulators),
  `ChatCompletionsAccumulator` (implements `SseAccumulator`, merges streamed Chat Completions SSE
  deltas into a single non-streaming JSON response, used by `ChatCompletionsApiProxy`),
  and `ResponseAccumulator` (implements `SseAccumulator`, used by `ResponsesApiProxy`).
  `ChatCompletionsApiProxy` extends `AbstractApiProxy` and handles Chat Completions conversion:
  downstream non-streaming `POST /v1/chat/completions` requests are rewritten to upstream
  `stream: true` with `stream_options.include_usage=true`, the upstream SSE is consumed by
  `ChatCompletionsAccumulator` (which merges chunk deltas and waits for `data: [DONE]`),
  the final `chat.completion` JSON is assembled in memory, and a non-streaming JSON response
  is returned downstream with HTTP 200. Requests that do not match the conversion criteria are
  passed through unchanged via `passthrough()`.
- **cli** - CLI wrapper for `proxy`. Reads a JSON config file (via `--config-file`, default `config.json`)
  with the structure `{"timeoutSeconds": 600, "rules": [{"listenPort": 8080, "upstreamUrl": "..."}]}`,
  starts one Ktor CIO server per config rule (each listening on a different port). All requests are
  handled by a catch-all `route("/{...}")` that selects the proxy implementation based on the request
  path: paths ending with `/responses` use `ResponsesApiProxy`, all other paths use
  `ChatCompletionsApiProxy` (which falls through to passthrough for non-`/chat/completions` paths).
  Proxy instances are lazily created and cached by `(listen port, proxy class)` with
  `kotlinx-atomicfu` `synchronized` for thread safety. A single `HttpClientEngine` is shared across
  all proxy instances and closed on server shutdown via the `ApplicationStopping` monitor event.
  Both `proxy()` and `passthrough()` return `OutgoingContent?`; null → 502 `upstream_error`
  (via `OpenAiErrors.errorResponse()`), exceptions → 500 `internal_error` via StatusPages
  (`ErrorHandler.kt` uses `CancellationException`-aware handler). Platform-specific shutdown: JVM uses
  `Runtime.addShutdownHook`, native registers `SIGTERM`/`SIGINT` handlers; servers stopped
  with 2s grace / 5s timeout. Uses `kotlinx-cli` for argument parsing, `kotlinx-atomicfu` for
  cross-platform synchronization, and the `shadow` Gradle plugin for JVM fat JAR packaging. Targets
  JVM, mingwX64, linuxX64, macosArm64 (not linuxArm64 — `kotlinx-cli` doesn't support it).

#### Auxiliary

- **sniffer** - JVM-only development tool for analyzing OpenAI API traffic. A reverse proxy
  (`ReverseProxy`) that buffers entire request/response bodies and logs headers and bodies both to
  stdout (via `TrafficLogger` + `kotlin-logging`) and to per-path files: requests ending with
  `/responses` go to `temp/responses.txt`, `/chat/completions` to `temp/chat_completions.txt`,
  others to `temp/others.txt`. Uses Ktor CIO client and server. Configured via environment variables:
  `UPSTREAM_BASE_URL` (default `https://api.openai.com`) and `LISTEN_PORT` (default `8080`).
- **mock-client-responses** - JVM-only development tool using the official `openai-java` SDK (`OpenAIOkHttpClient`).
  Tests the Responses API. Works with `sniffer`, `cli`, or any OpenAI-compatible proxy.
  Supports streaming (`createStreaming`) and non-streaming (`create`) modes. Requires `OPENAI_API_KEY`.
  Also configurable via `OPENAI_BASE_URL` (default `http://localhost:8080/v1`), `OPENAI_MODEL`
  (default `gpt-5.3-codex`), `OPENAI_PROMPT` (default `"Hello"`), and `OPENAI_STREAM` (default `false`).
- **mock-client-chat-completions** - Same as `mock-client-responses` but uses the Chat Completions API
  (`client.chat().completions()`). Same environment variables and defaults.

### Key Patterns

- `proxy` is engine-agnostic and depends only on common Ktor client/I/O APIs.
- `proxy` converts synchronous non-streaming `POST /v1/responses` requests (via `ResponsesApiProxy`)
  and `POST /v1/chat/completions` requests (via `ChatCompletionsApiProxy`).
- Requests with `stream=true` are not converted and must be passed through unchanged.
- For the supported conversion paths, `proxy` aggregates the final state in memory and only then writes
  the downstream non-streaming JSON response. It does not stream partial JSON fragments downstream.
- `ResponsesApiProxy` and `ChatCompletionsApiProxy` extend `AbstractApiProxy`; subclasses implement
  `needConvert()`, `rewriteBody()`, `createAccumulator()`, and `buildResult()` for protocol-specific conversion.
- `ResponseAccumulator` and `ChatCompletionsAccumulator` both implement the `SseAccumulator` interface.
  They are not thread-safe — `accumulate()` must be called from a single coroutine.
- `cli` uses CIO engine everywhere (client and server), no HTTPS support.
- `cli` uses `expect/actual` for `configureLogging()` (JVM sets logback to INFO; native is no-op) and
  `registerShutdownHook()` (JVM uses `Runtime.addShutdownHook`; native uses POSIX `signal`).
- `proxy` targets JVM, mingwX64, linuxX64, linuxArm64, and macosArm64.
- `cli` targets JVM, mingwX64, linuxX64, and macosArm64 (no linuxArm64).
- `sniffer`, `mock-client-responses`, and `mock-client-chat-completions` are JVM executables with code in `commonMain`
  and `mainClass.set(...)` in the JVM block.
- Test SSE fixtures are in `commonTest/resources` under each module's package path.
- Tests use real Ktor CIO servers on ephemeral ports (`ServerSocket(0)`) with Ktor CIO clients
  for HTTP-level end-to-end verification — no mocks or SDK clients.
