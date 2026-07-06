# openai-stream-proxy CLI

`:cli` is the Kotlin Multiplatform executable subproject for `openai-stream-proxy`. It starts a local Ktor server that
proxies OpenAI-compatible HTTP and WebSocket traffic to one or more upstream providers.

The main use case is running clients that make non-streaming OpenAI API requests against an upstream provider through
streaming SSE. For supported endpoints, the CLI rewrites a downstream non-streaming request into an upstream streaming
request, consumes the upstream SSE stream, accumulates the result, and returns a single non-streaming JSON response.

The CLI is built on top of the shared `:proxy` multiplatform library and adds configuration loading, server startup,
WebSocket passthrough, platform-specific HTTP client engines, logging, and shutdown handling.

## Behavior

HTTP requests are routed by path:

- paths ending with `/responses` use the Responses API conversion flow;
- paths ending with `/chat/completions` use the Chat Completions API conversion flow;
- all other HTTP paths are forwarded without conversion.

Requests that already contain `stream: true` are not converted. They are forwarded upstream unchanged.

WebSocket requests are proxied to the matching upstream `ws://` or `wss://` URL. Frames are forwarded directly in both
directions without payload rewriting or whole-session buffering.

This project does not convert between API protocols. For example, it does not convert Responses API payloads to Chat
Completions payloads or to another provider-specific protocol.

## Configuration

The CLI reads `config.json` from the current working directory by default:

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

`timeoutSeconds` is the upstream request timeout in seconds. If omitted, it defaults to `600`.

Each `rules` entry maps one local listen port to one upstream base URL. The request path and query string are appended
to `upstreamUrl`, so `upstreamUrl` usually should not include `/v1`.

With the config above:

- `POST http://localhost:8080/v1/responses` is proxied to `POST https://api.openai.com/v1/responses`;
- `POST http://localhost:8080/v1/chat/completions` is proxied to `POST https://api.openai.com/v1/chat/completions`;
- `WS ws://localhost:8080/v1/realtime?model=...` is proxied to `WSS wss://api.openai.com/v1/realtime?model=...`;
- `POST http://localhost:8081/v1/responses` is proxied to `POST https://some-other-provider.example.com/v1/responses`.

Use `--config-file` or `-c` to load a config file from another path.

## Run From Gradle KMP

When running from the GitHub repository, use the repository root as the working directory so the default `config.json`
is resolved from the root project.

Run the JVM target:

```bash
./gradlew :cli:jvmRun
```

Run with an explicit config file:

```bash
./gradlew :cli:jvmRun --args="--config-file /path/to/config.json"
```

Build and run the JVM fat JAR:

```bash
./gradlew :cli:shadowJar
java -jar cli/build/libs/openai-stream-proxy-<version>-fat.jar --config-file config.json
```

Build native executables on their corresponding platforms:

```bash
./gradlew :cli:linkReleaseExecutableLinuxX64
./gradlew :cli:linkReleaseExecutableMingwX64
./gradlew :cli:linkReleaseExecutableMacosArm64
```

The CLI native targets are Linux x64, Windows x64, and macOS Apple Silicon. The JVM target is also available for local
development and JVM distribution.

## Run With npx

After the npm packages are published, run the native launcher with:

```bash
npx @czp3009/openai-stream-proxy
```

Run with an explicit config file:

```bash
npx @czp3009/openai-stream-proxy --config-file /path/to/config.json
npx @czp3009/openai-stream-proxy -c /path/to/config.json
```

The npm package is a launcher package with platform-specific optional native packages. At runtime, it selects the native
binary for the current OS and CPU.
