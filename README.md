# openai-stream-proxy

A transparent proxy for OpenAI-compatible HTTP and WebSocket traffic. For supported HTTP JSON endpoints, it converts
non-streaming OpenAI API requests into upstream SSE streaming requests, aggregates the stream in memory, and sends a
non-streaming response to the downstream client.

Supports the **Responses API** (`/v1/responses`) and **Chat Completions API** (`/v1/chat/completions`). Requests already
using `stream=true` are passed through unchanged.

WebSocket requests are proxied to the corresponding upstream `ws://` or `wss://` URL. Data is forwarded in both
directions as it arrives, with backpressure and without payload rewriting or whole-session buffering. For established
upstream WebSocket sessions, normal close reasons are forwarded and abnormal disconnects are reported to the downstream
client as WebSocket internal errors.

This project only converts between non-streaming and streaming modes. It does not support protocol conversion, including
but not limited to converting the Responses protocol to the Chat Completions protocol, or converting the Responses
protocol to the Anthropic protocol.

## Usage

Download executables from the [releases page](https://github.com/czp3009/openai-stream-proxy/releases)
or [build from source](#build).

Create `config.json` in the working directory:

```json
{
  "timeoutSeconds": 600,
  "rules": [
    {
      "listenPort": 8080,
      "upstreamUrl": "https://api.openai.com"
    },
    {
      "listenPort": 8081,
      "upstreamUrl": "https://some-other-provider.example.com"
    }
  ]
}
```

| Field            | Description                                         | Default |
|------------------|-----------------------------------------------------|---------|
| `timeoutSeconds` | Upstream request timeout in seconds                 | `600`   |
| `rules`          | List of proxy rules (port to upstream URL mappings) | —       |

Each rule maps a listen port to an upstream base URL. All paths under that port are proxied to the corresponding
upstream. With the config above:

- `POST http://localhost:8080/v1/responses` → `POST https://api.openai.com/v1/responses`
- `POST http://localhost:8080/v1/chat/completions` → `POST https://api.openai.com/v1/chat/completions`
- `POST http://localhost:8080/v1/other/path` → `POST https://api.openai.com/v1/other/path` (passthrough, not converted)
- `WS ws://localhost:8080/v1/realtime?model=...` → `WSS wss://api.openai.com/v1/realtime?model=...`
- `POST http://localhost:8081/v1/responses` → `POST https://some-other-provider.example.com/v1/responses`

The proxy listens on the configured ports and converts non-streaming requests whose path ends with `/responses` or
`/chat/completions` into upstream SSE streams. Streaming requests (`stream: true`) and requests to any other path are
forwarded unchanged.

For example, a non-streaming Chat Completions request from the downstream client:

```
POST /v1/chat/completions
Content-Type: application/json

{"model": "gpt-5.5", "messages": [{"role": "user", "content": "Hello"}]}
```

is rewritten to the following before being sent upstream:

```
POST /v1/chat/completions
Content-Type: application/json

{"model": "gpt-5.5", "messages": [{"role": "user", "content": "Hello"}], "stream": true, "stream_options": {"include_usage": true}}
```

The upstream SSE stream is then aggregated in memory and returned as a single non-streaming JSON response to the
downstream client.

For WebSocket requests, the proxy keeps the same path and query string, maps `http://` upstream URLs to `ws://` and
`https://` upstream URLs to `wss://`, and forwards request headers such as `Authorization`. WebSocket traffic is
forwarded bidirectionally in streaming mode with backpressure so the proxy can handle long-lived connections with low
memory usage. When the upstream closes the WebSocket normally, the close reason is forwarded downstream. If the upstream
WebSocket session ends without a normal close, the downstream WebSocket is closed with an internal error.

The request path is appended to `upstreamUrl`, so `upstreamUrl` typically should not include `/v1`.

Then run the executable with the config file in place. For JVM:

```bash
java -jar openai-stream-proxy-0.0.5-fat.jar
```

For native (Linux):

```bash
./openai-stream-proxy-0.0.5-linuxX64.kexe
```

The proxy reads `config.json` from the working directory by default. To use a different path:

```bash
java -jar openai-stream-proxy-0.0.5-fat.jar --config-file /path/to/config.json
# or
java -jar openai-stream-proxy-0.0.5-fat.jar -c /path/to/config.json
# same with native executable
./openai-stream-proxy-0.0.5-linuxX64.kexe --config-file /path/to/config.json
```

## Build

```bash
# JVM
./gradlew :cli:shadowJar           # fat JAR

# Native executables (build on the corresponding platform)
./gradlew :cli:linkReleaseExecutableLinuxX64      # Linux x86_64
./gradlew :cli:linkReleaseExecutableMingwX64      # Windows x86_64
./gradlew :cli:linkReleaseExecutableMacosArm64    # macOS Apple Silicon
```

The fat JAR is output to `cli/build/libs/`. Native executables are output to
`cli/build/bin/<target>/releaseExecutable/`.

## Proxy Library

The `proxy` module is a Kotlin Multiplatform library you can embed in your own application. It is engine-agnostic — you
provide the Ktor `HttpClientEngine`, and the library handles request rewriting, SSE aggregation, and response assembly.
WebSocket passthrough is implemented by the CLI module, not by the `proxy` library classes.

### Gradle Dependency

```kotlin
dependencies {
   implementation("com.hiczp:openai-stream-proxy:0.0.5")
}
```

The library is published for JVM, mingwX64, linuxX64, linuxArm64, and macosArm64.

### Quick Start

```kotlin
import com.hiczp.openai.stream.proxy.ChatCompletionsApiProxy
import com.hiczp.openai.stream.proxy.PassthroughApiProxy
import com.hiczp.openai.stream.proxy.ResponsesApiProxy
import io.ktor.client.engine.cio.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.cio.CIO as ServerCIO
import io.ktor.server.engine.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

fun main() {
    val engine = CIO.create()
    val upstreamUrl = "https://api.openai.com"
    val timeoutMillis = 600_000L

    val responsesProxy = ResponsesApiProxy(engine, upstreamUrl, timeoutMillis)
    val chatProxy = ChatCompletionsApiProxy(engine, upstreamUrl, timeoutMillis)
   val passthroughProxy = PassthroughApiProxy(engine, upstreamUrl, timeoutMillis)

    embeddedServer(ServerCIO, port = 8080) {
        routing {
            post("/v1/responses") {
               responsesProxy.proxy(
                    call.request.httpMethod,
                    call.request.uri,
                    call.request.headers,
                    call.receiveChannel(),
               ) { response ->
                  call.respond(response)
                }
            }
            post("/v1/chat/completions") {
               chatProxy.proxy(
                    call.request.httpMethod,
                    call.request.uri,
                    call.request.headers,
                    call.receiveChannel(),
               ) { response ->
                  call.respond(response)
                }
            }
           route("/{...}") {
              handle {
                 passthroughProxy.proxy(
                    call.request.httpMethod,
                    call.request.uri,
                    call.request.headers,
                    call.receiveChannel(),
                 ) { response ->
                    call.respond(response)
                 }
              }
            }
        }
    }.start(wait = true)
}
```

### How It Works

Conversion proxy classes follow the same flow:

1. **Validate** — Check HTTP method, path, content type, and request body (must be JSON with a `model` field and
   `stream` absent or `false`).
2. **Rewrite** — Add `stream: true` (and `stream_options` for Chat Completions) to the request body.
3. **Stream** — Send the rewritten request upstream as an SSE request.
4. **Aggregate** — Collect SSE events into a single accumulated response in memory.
5. **Respond** — Send a non-streaming JSON response to the downstream client through the injected responder.

Requests that don't match the conversion criteria are forwarded unchanged (passthrough). This also applies when a
protocol-specific proxy receives traffic outside its path match: for example, `ResponsesApiProxy` will passthrough
non-`*/responses` requests, and `ChatCompletionsApiProxy` will passthrough non-`*/chat/completions` requests.
`PassthroughApiProxy` skips conversion entirely and forwards every request unchanged.

### Proxy Classes

| Class                     | API              | Path match           |
|---------------------------|------------------|----------------------|
| `ResponsesApiProxy`       | Responses API    | `*/responses`        |
| `ChatCompletionsApiProxy` | Chat Completions | `*/chat/completions` |
| `PassthroughApiProxy`     | Any              | all requests         |

All proxy classes extend `AbstractApiProxy`, which provides streaming `passthrough()` for forwarding requests unchanged,
upstream error responses for failed or invalid upstream streams, and `stripHopByHopHeaders()` for header cleanup.

### Resource Lifecycle

The `HttpClientEngine` is **not** owned by the proxy — the caller is responsible for closing it when no longer needed (
e.g., on application shutdown). Do not call `close()` on the internal `HttpClient` created by the proxy, as this would
shut down the shared engine.

```kotlin
val engine = CIO.create()
try {
    val proxy = ResponsesApiProxy(engine, upstreamUrl)
    // ... use proxy
} finally {
    engine.close()
}
```

### Accumulators

SSE event aggregation is handled by `SseAccumulator` implementations:

| Accumulator                  | Used by                   | Terminal condition                                                |
|------------------------------|---------------------------|-------------------------------------------------------------------|
| `ResponseAccumulator`        | `ResponsesApiProxy`       | `response.completed`, `response.failed`, or `response.incomplete` |
| `ChatCompletionsAccumulator` | `ChatCompletionsApiProxy` | `data: [DONE]`                                                    |

Accumulators are not thread-safe — `accumulate()` must be called from a single coroutine.

## Build Requirements

JVM toolchain: 21

## License

[MIT](LICENSE.txt)
