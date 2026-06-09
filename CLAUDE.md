# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working in this repository.

## Build & Test

```bash
./gradlew :proxy:compileKotlinJvm            # compile proxy (JVM only, fast)
./gradlew :cli:compileKotlinJvm              # compile cli (JVM only, fast)
./gradlew :proxy:jvmTest                     # proxy tests (JVM only)
./gradlew :cli:jvmTest                       # cli tests (JVM only)
./gradlew build                              # build all modules (all platforms, slow)
./gradlew allTests                           # all tests (all platforms, slow)
```

JVM toolchain: 21. Kotlin `2.3.21`, Ktor `3.5.0`.

Several modules are Kotlin Multiplatform projects targeting multiple platforms (JVM, mingwX64, linuxX64,
macosArm64, etc.). Building all platforms is slow. To check whether code compiles or to run tests, **JVM
only is sufficient** — prefer the JVM-only commands above over `build` and `allTests`.

If the `version` in the root `build.gradle.kts` is updated, also update the version numbers in the README.md
examples to match.

Unit tests must be deterministic and reproducible. For coroutine-related behavior, use coroutine primitives
(e.g. `CompletableDeferred`, `Channel`, `Mutex`/`Semaphore`) and test schedulers where applicable to make key
execution points happen in a fixed order. Do not rely on probabilistic timing behavior, wall-clock delays, sleeps,
elapsed-time polling, short timeouts, or timeout-based synchronization.

A timeout such as `withTimeout` may be used only as a fail-fast guard against deadlocks, not to establish ordering
or to make the tested behavior happen. When testing timeout behavior itself, prefer virtual time, a fake clock, or a
controllable fake dependency. For socket/server tests, coordinate client and server progress with explicit handshake
signals so every assertion observes a known protocol state.

## Running Modules

For executable modules, prefer using the developer's local IDEA run configuration so required arguments and
environment variables come from that developer's environment. Do not encode specific run configuration names here:
local IDEA configuration state can differ per machine, and the agent should choose the appropriate configuration from
the current workspace. If a run configuration is missing or appears misconfigured, ask the developer to create or fix
it.

## Project Architecture

Kotlin Multiplatform project using Gradle version catalogs.

### Subprojects

#### Core

- **proxy** - Core library. `AbstractApiProxy` is the shared base class providing the upstream HTTP
  client (with SSE and timeout plugins), `passthrough()` for forwarding requests unchanged, and
  `stripHopByHopHeaders()`. The public entry point `proxy()` is a template method that first checks
  `needConvert()` (path/method matching); non-matching requests are forwarded unchanged, while matching
  conversion requests are validated and delegated to `convert()` (another template method). Conversion
  subclasses implement `rewriteBody()` (body rewriting), `createAccumulator()` (SSE event accumulator
  factory), and `buildResult()` (final response assembly) to define protocol-specific conversion logic.
  `ResponsesApiProxy` extends `AbstractApiProxy` and handles one protocol conversion
  path:
  downstream non-streaming `POST /v1/responses` requests are rewritten to upstream `stream: true`,
  the upstream SSE is consumed by `ResponseAccumulator` (which records `response.output_item.done`
  items as an output fallback and waits for a terminal event: `response.completed`,
  `response.failed`, or `response.incomplete`), and the terminal state is assembled in memory.
  Completed and incomplete terminal responses are returned as non-streaming Response JSON; failed
  terminal responses with an upstream error object are returned as OpenAI-compatible error JSON with
  an HTTP status mapped from `error.code`.
  Requests that do not match the conversion criteria (non-POST, non-`/responses` path, non-JSON
  content type, missing `model` field, or `stream=true`) are passed through unchanged via
  `passthrough()`.
  When the upstream returns a non-SSE error response during the SSE connect phase, it is relayed
  as-is. `proxy()` receives a `respond` callback and writes downstream responses internally;
  conversion-flow upstream failures (incomplete stream, network error) are converted to 502
  `upstream_error`. Passthrough upstream `IOException`s before response headers are converted to
  504 `upstream_timeout`; once passthrough response headers have been received, the started
  response is preserved and later upstream I/O failures are only logged.
  Exceptions signal proxy bugs (caller should map to 500). Depends on `ktor-client-core`,
  `ktor-http`, `ktor-io`, `kotlinx-serialization-json`, and `kotlin-logging`. No platform-specific
  engines; consumers provide the engine.
  Also contains `OpenAiErrors` (utility for building OpenAI-compatible error responses),
  `SseAccumulator` (interface for SSE event accumulators),
  `ChatCompletionsAccumulator` (implements `SseAccumulator`, merges streamed Chat Completions SSE
  deltas into a single non-streaming JSON response, used by `ChatCompletionsApiProxy`),
  `ResponseAccumulator` (implements `SseAccumulator`, used by `ResponsesApiProxy`), and
  `PassthroughApiProxy` (extends `AbstractApiProxy` with `needConvert()` always false so every
  request is forwarded unchanged).
  `ChatCompletionsApiProxy` extends `AbstractApiProxy` and handles Chat Completions conversion:
  downstream non-streaming `POST /v1/chat/completions` requests are rewritten to upstream
  `stream: true` with `stream_options.include_usage=true`, the upstream SSE is consumed by
  `ChatCompletionsAccumulator` (which merges chunk deltas and waits for `data: [DONE]`),
  the final `chat.completion` JSON is assembled in memory, and a non-streaming JSON response
  is sent downstream with HTTP 200. Requests that do not match the conversion criteria are
  passed through unchanged via `passthrough()`.
- **cli** - CLI wrapper for `proxy`. Reads a JSON config file (via `--config-file`, default `config.json`)
  with the structure `{"timeoutSeconds": 600, "rules": [{"listenPort": 8080, "upstreamUrl": "..."}]}`,
  starts one Ktor CIO server with one connector per config rule (each listening on a different port). All requests are
  handled by a catch-all `route("/{...}")` that selects the proxy implementation based on the request
  path via `selectProxyClass()`: paths ending with `/responses` use `ResponsesApiProxy`, paths ending
  with `/chat/completions` use `ChatCompletionsApiProxy`, and all other paths use `PassthroughApiProxy`.
  Proxy instances are lazily created and cached by `(listen port, proxy class)` with
  `kotlinx.coroutines.sync.Mutex` for coroutine-friendly synchronization. A single `HttpClientEngine` is
  shared across all proxy instances and closed on server shutdown via the `ApplicationStopping` monitor event.
  `proxy()` injects Ktor's response operation through its `respond` parameter; upstream failures
  are handled by `AbstractApiProxy` (including passthrough pre-response I/O failures → 504
  `upstream_timeout`), exceptions → 500 `internal_error` via StatusPages
  (`ErrorHandler.kt` uses `CancellationException`-aware handler). Platform-specific shutdown: JVM uses
  `Runtime.addShutdownHook`, native registers `SIGTERM`/`SIGINT` handlers; servers stopped
  with 2s grace / 5s timeout. Uses `kotlinx-cli` for argument parsing, `kotlinx.coroutines`
  for shutdown coordination and cache synchronization, and the `shadow` Gradle plugin for JVM fat JAR
  packaging. Targets JVM, mingwX64, linuxX64, macosArm64 (not linuxArm64).

#### Auxiliary

- **sniffer** - JVM-only development tool for analyzing OpenAI API traffic. A reverse proxy
  (`ReverseProxy`) that buffers entire request/response bodies and logs headers and bodies both to
  console/log output (via `TrafficLogger` + `kotlin-logging`) and to per-path files: requests ending with
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

- `proxy` is engine-agnostic: it uses common Ktor client/I/O APIs and does not include a concrete client engine.
- `proxy` converts synchronous non-streaming `POST /v1/responses` requests (via `ResponsesApiProxy`)
  and `POST /v1/chat/completions` requests (via `ChatCompletionsApiProxy`).
- `PassthroughApiProxy` is the explicit unconditional forwarding proxy; it never enters the conversion
  flow because `needConvert()` always returns false.
- Requests with `stream=true` are not converted and must be passed through unchanged.
- For the supported conversion paths, `proxy` aggregates the final state in memory and only then writes
  the downstream non-streaming JSON response. It does not stream partial JSON fragments downstream.
- `ResponsesApiProxy`, `ChatCompletionsApiProxy`, and `PassthroughApiProxy` extend `AbstractApiProxy`;
  conversion proxies implement `needConvert()`, `rewriteBody()`, `createAccumulator()`, and `buildResult()`
  for protocol-specific conversion.
- `ResponseAccumulator` and `ChatCompletionsAccumulator` both implement the `SseAccumulator` interface.
  They are not thread-safe — `accumulate()` must be called from a single coroutine.
- `cli` serves plain HTTP listeners with Ktor CIO server; its upstream client engine is CIO on JVM
  and Curl on native.
- `cli` uses `expect/actual` for `configureLogging()` (JVM configures logback, native configures
  kotlin-logging direct mode; both read `LOG_LEVEL` and default to DEBUG) and
  `registerShutdownHook()` (JVM uses `Runtime.addShutdownHook`; native uses POSIX `signal`).
- `proxy` targets JVM, mingwX64, linuxX64, linuxArm64, and macosArm64.
- `cli` targets JVM, mingwX64, linuxX64, and macosArm64 (no linuxArm64).
- `sniffer`, `mock-client-responses`, and `mock-client-chat-completions` are JVM executables with code in `commonMain`
  and `mainClass.set(...)` in the JVM block.
- Test SSE fixtures are in `commonTest/resources` under each module's package path.
- HTTP-level proxy and CLI tests use real Ktor CIO servers on ephemeral ports (`ServerSocket(0)`)
  with Ktor CIO clients or raw sockets; accumulator tests feed parsed SSE fixture events directly.
