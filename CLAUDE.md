# CLAUDE.md

Guidance for Claude Code when working in this repository.

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

This is a Kotlin Multiplatform repo. Several modules target JVM plus native platforms, so full `build` and
`allTests` are slow. Prefer the JVM-only commands above for ordinary compile/test validation unless the change is
specifically native-related.

If the root `build.gradle.kts` `version` changes, update the version numbers in README examples too.

Tests must be deterministic. For coroutine, socket, and server behavior, coordinate progress with primitives such as
`CompletableDeferred`, `Channel`, `Mutex`/`Semaphore`, test schedulers, or explicit client/server handshake signals.
Do not use sleeps, elapsed-time polling, probabilistic timing, or short timeout-based synchronization. `withTimeout`
is acceptable as a fail-fast deadlock guard, not as the mechanism that makes behavior happen.

## Running Modules

For executable modules, prefer the developer's local IDEA run configuration so local arguments and environment
variables are used. Do not hard-code run configuration names here; ask the developer if the needed local config is
missing or misconfigured.

## Architecture

Kotlin Multiplatform project using Gradle version catalogs.

### Core Library: `proxy`

`proxy` is engine-agnostic. It uses common Ktor client/I/O APIs and expects consumers to provide the
`HttpClientEngine`.

`AbstractApiProxy` owns the shared proxy template:

- non-matching requests are passed through unchanged;
- matching conversion requests are rewritten, streamed from upstream SSE, accumulated, and returned as one
  non-streaming JSON response;
- hop-by-hop HTTP headers are stripped where appropriate;
- upstream failures are mapped to OpenAI-compatible errors where the public proxy contract requires it.

Conversion implementations:

- `ResponsesApiProxy`: non-streaming `POST /v1/responses` -> upstream `stream: true`, accumulated by
  `ResponseAccumulator`.
- `ChatCompletionsApiProxy`: non-streaming `POST /v1/chat/completions` -> upstream `stream: true` with
  `stream_options.include_usage=true`, accumulated by `ChatCompletionsAccumulator`.
- `PassthroughApiProxy`: unconditional forwarding; it never enters conversion flow.

Requests with `stream=true` must not be converted.

`ResponseAccumulator` and `ChatCompletionsAccumulator` implement `SseAccumulator` and are not thread-safe;
call `accumulate()` from a single coroutine.

### CLI: `cli`

`cli` reads a JSON config file such as:

```json
{"timeoutSeconds": 600, "rules": [{"listenPort": 8080, "upstreamUrl": "https://api.openai.com"}]}
```

It starts one Ktor CIO server with one connector per rule. A single `HttpClientEngine` is shared by HTTP proxy
instances and the WebSocket upstream client, and is closed on application shutdown.

WebSocket requests are handled before the HTTP catch-all route. The CLI uses Ktor server `webSocket` and client
`webSocket`, maps `http(s)` upstream URLs to `ws(s)`, forwards only headers valid for a new upstream WebSocket
handshake, and pipes both sessions without payload rewriting or whole-session buffering. Keep WebSocket passthrough
as direct read-one-frame/write-one-frame logic with zero-capacity Ktor frame channels for backpressure; do not
aggregate WebSocket messages or sessions in memory.
`WebSocketProxy.kt` owns WebSocket header forwarding and bidirectional session piping.

Non-WebSocket HTTP requests go through the catch-all `route("/{...}")`, which selects:

- paths ending with `/responses` -> `ResponsesApiProxy`;
- paths ending with `/chat/completions` -> `ChatCompletionsApiProxy`;
- all other paths -> `PassthroughApiProxy`.

Proxy instances are cached by `(listen port, proxy class)` behind a coroutine-friendly `Mutex`.

Platform-specific CLI behavior:

- upstream client engine is CIO on JVM and Curl on native;
- logging and shutdown hooks use `expect/actual`;
- CLI targets JVM, mingwX64, linuxX64, and macosArm64 (not linuxArm64).

### Auxiliary Modules

- `sniffer`: JVM-only development reverse proxy that intentionally buffers and logs full HTTP bodies for traffic
  inspection. Do not copy its buffering style into production proxy paths.
- `mock-client-responses`: JVM-only OpenAI SDK test client for the Responses API.
- `mock-client-chat-completions`: JVM-only OpenAI SDK test client for Chat Completions.

## Testing Patterns

HTTP-level proxy and CLI tests use real Ktor CIO servers on ephemeral ports (`ServerSocket(0)`) with Ktor CIO clients
or raw sockets. WebSocket behavior should be tested through the full CLI server path, not only by unit-testing the
session piping helper.

SSE fixture files live under each module's `commonTest/resources` package path. Accumulator tests may feed parsed SSE
events directly.

When changing shared proxy behavior, prefer focused tests around the affected public behavior instead of broad
snapshot-style assertions.
