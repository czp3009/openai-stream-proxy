# CLI 实现计划（修订版）

校对日期：2026-05-18

## 概述

实现 `cli` 模块，使其成为可运行的代理服务器宿主程序。`cli` 读取 JSON 配置文件，根据配置规则同时启动多个 Ktor
服务器实例，每个实例监听不同端口并将流量转发到指定的上游地址。协议转换逻辑委托给 `proxy` 模块的 `ResponsesApiProxy`
。所有平台统一使用 CIO 引擎（客户端 + 服务端），不支持 HTTPS。

## 新增依赖

在 `gradle/libs.versions.toml` 中添加：

```toml
[versions]
kotlinx-cli = "0.3.6"

[libraries]
kotlinx-cli = { module = "org.jetbrains.kotlinx:kotlinx-cli", version.ref = "kotlinx-cli" }
```

在 `cli/build.gradle.kts` 中调整 `commonMain.dependencies`：

```kotlin
commonMain.dependencies {
    implementation(project(":proxy"))
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.cio)
    implementation(libs.ktor.client.cio)     // 统一使用 CIO 客户端引擎
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.cli)
    implementation(libs.kotlin.logging)
}
jvmMain.dependencies {
    implementation(libs.logback.classic)
}
// nativeMain 无额外依赖，CIO 已在 commonMain
```

移除 `nativeMain` 中的 `ktor-client-curl` 依赖。

注意：`kotlinx-serialization-json` 已在 `commonMain` 中。文件读取使用 `kotlinx.io.files.SystemFileSystem`，由 `ktor-io`（
`proxy` 模块通过 `api(libs.ktor.io)` 已暴露）传递引入，无需额外依赖。

## 配置文件格式

JSON 文件，根节点为数组。每条规则包含两个属性：

```json
[
  {
    "listenPort": 8080,
    "upstreamUrl": "http://api.openai.com"
  },
  {
    "listenPort": 8081,
    "upstreamUrl": "http://custom-provider.example.com"
  }
]
```

数据类：

```kotlin
@Serializable
data class ProxyRule(
    val listenPort: Int,
    val upstreamUrl: String,
)
```

通过 `kotlinx-serialization-json` 解析：`Json.decodeFromString<List<ProxyRule>>(text)`

文件读取使用 `kotlinx.io.files.SystemFileSystem`（全平台通用，无需 expect/actual）。

## 文件结构

```
cli/src/
  commonMain/kotlin/com/hiczp/openai/responses/stream/proxy/cli/
    Main.kt           — 入口：CLI 参数解析、配置加载、多服务器启动与生命周期管理
    Config.kt         — ProxyRule 数据类 + JSON 解析 + 文件读取
    ErrorHandler.kt   — StatusPages 拦截器：捕获 Throwable 返回 500
    ShutdownHook.kt   — expect fun registerShutdownHook(block: () -> Unit)
  jvmMain/kotlin/com/hiczp/openai/responses/stream/proxy/cli/
    ShutdownHook.kt   — Runtime.addShutdownHook
  nativeMain/kotlin/com/hiczp/openai/responses/stream/proxy/cli/
    ShutdownHook.kt   — POSIX signal(SIGTERM, SIGINT)
  jvmTest/kotlin/com/hiczp/openai/responses/stream/proxy/cli/
    ConfigTest.kt     — JSON 解析测试
    ServerTest.kt     — 集成测试：使用 Ktor mock 上游服务器验证 502/500/正常响应
```

修改的已有文件：

- `gradle/libs.versions.toml` — 添加 `kotlinx-cli` 版本和库声明
- `cli/build.gradle.kts` — 添加依赖、移除 `nativeMain` 的 curl 依赖、添加 `jvmTest` 依赖
- 删除 `cli/src/commonMain/kotlin/Main.kt`（旧占位文件，包路径也不规范）

## 实现步骤

### 步骤 1：添加依赖

1. 在 `gradle/libs.versions.toml` 中添加 `kotlinx-cli` 的版本号和库声明
2. 在 `cli/build.gradle.kts` 的 `commonMain.dependencies` 中引入 `kotlinx-cli` 和 `kotlin-logging`
3. 将 `ktor-client-cio` 移入 `commonMain`（从 `jvmMain` 移出）
4. 移除 `nativeMain` 的 `ktor-client-curl` 依赖
5. 在 `jvmTest.dependencies` 中添加 `ktor-server-cio`、`ktor-client-cio`、`logback-classic`、`kotlin-test`
6. 验证构建通过

### 步骤 2：实现 Config.kt

1. 创建 `cli/src/commonMain/kotlin/com/hiczp/openai/responses/stream/proxy/cli/Config.kt`
2. 定义 `@Serializable data class ProxyRule(val listenPort: Int, val upstreamUrl: String)`
3. 实现 `fun loadConfig(path: String): List<ProxyRule>`：
    - 使用 `kotlinx.io.files.SystemFileSystem.source(Path(path))` 读取文件内容
    - 使用 `Json.decodeFromString<List<ProxyRule>>(text)` 解析
4. 编写 JVM 测试验证 JSON 解析：正常多规则、空数组、格式错误抛异常

### 步骤 3：实现 ErrorHandler.kt

1. 创建 `ErrorHandler.kt`
2. 定义 `fun Application.installErrorHandler()` 函数，内部安装 `StatusPages` 插件
3. 拦截 `Throwable`（兜底全部错误），使用 `ResponsesApiProxy.errorResponse` 构造 500 响应

```kotlin
fun Application.installErrorHandler() {
    install(StatusPages) {
        exception<Throwable> { call, cause ->
            call.respond(
                ResponsesApiProxy.errorResponse(
                    message = cause.message ?: "Unknown error",
                    type = "internal_error",
                    status = HttpStatusCode.InternalServerError,
                )
            )
        }
    }
}
```

### 步骤 4：实现 ShutdownHook.kt（expect/actual）

在 `commonMain` 声明：

```kotlin
expect fun registerShutdownHook(block: () -> Unit)
```

JVM 实现：

```kotlin
actual fun registerShutdownHook(block: () -> Unit) {
    Runtime.getRuntime().addShutdownHook(Thread(block))
}
```

Native 实现（POSIX signal）：

```kotlin
private var shutdownCallback: (() -> Unit)? = null

actual fun registerShutdownHook(block: () -> Unit) {
    shutdownCallback = block
    signal(SIGTERM, staticCFunction<Int, Unit> { shutdownCallback?.invoke() })
    signal(SIGINT, staticCFunction<Int, Unit> { shutdownCallback?.invoke() })
}
```

注意：`staticCFunction` 无法捕获闭包，需要通过全局变量中转。`platform.posix.signal` 在
linuxX64、linuxArm64、macosArm64、mingwX64 上均可用。

### 步骤 5：实现 Main.kt

1. 删除旧的 `cli/src/commonMain/kotlin/Main.kt`
2. 在正确包路径下创建新的 `Main.kt`
3. 使用 `kotlinx-cli` 解析命令行参数
4. 读取并解析配置文件
5. 创建共享的 CIO `HttpClientEngine`
6. 为每条规则创建 `ResponsesApiProxy` 实例和对应的 Ktor 服务器（路由 handler 内联）
7. 异步启动所有服务器，引用存入 list
8. 注册关闭钩子
9. 阻塞主线程

```kotlin
private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {
    val parser = ArgParser("openai-responses-stream-proxy")
    val configFile by parser.option(
        ArgType.String,
        fullName = "config-file",
        description = "Path to config JSON file",
    )
    parser.parseOrExit(args)

    val configFilePath = configFile ?: "config.json"
    val rules = loadConfig(configFilePath)
    logger.info { "Loaded ${rules.size} rule(s) from $configFilePath" }

    val engine = CIO.create()

    val servers = rules.map { rule ->
        val proxy = ResponsesApiProxy(engine, rule.upstreamUrl)
        logger.info { "Rule: port=${rule.listenPort} -> ${rule.upstreamUrl}" }

        embeddedServer(CIO, port = rule.listenPort) {
            installErrorHandler()
            routing {
                route("/{...}") {
                    handle {
                        val result = proxy.proxy(
                            requestMethod = call.request.httpMethod,
                            requestPath = call.request.uri,
                            requestHeaders = call.request.headers,
                            requestBody = call.receiveChannel(),
                        )
                        if (result != null) {
                            call.respond(result)
                        } else {
                            call.respond(
                                ResponsesApiProxy.errorResponse(
                                    message = "Upstream returned incomplete or invalid response",
                                    type = "upstream_error",
                                )
                            )
                        }
                    }
                }
            }
        }.start(wait = false)
    }

    registerShutdownHook {
        logger.info { "Shutting down ${servers.size} server(s)..." }
        servers.forEach { it.stop(gracePeriodMillis = 1000, timeoutMillis = 2000) }
    }

    // 阻塞主线程，防止进程退出
    runBlocking { awaitCancellation() }
}
```

#### 为什么 `proxy()` 返回 null

根据 `ResponsesApiProxy` 源码分析，`proxy()` 返回 `null` 涵盖以下场景：

1. **上游网络错误**（`IOException`）：上游不可达、连接被重置、连接超时等，发生在透传路径或 SSE 请求中
2. **SSE 流解析失败**：`SSEClientException` 且 cause 也是 `SSEClientException`，表示网络错误、SSE 数据格式不合法、或重连耗尽
3. **SSE 流异常终止**：上游关闭了 SSE 连接，但未发送终结事件（`response.completed` / `response.failed` /
   `response.incomplete`）
4. **下游请求体读取失败**：下游客户端在请求体传输过程中断开连接（`IOException`），此时 502 响应无法被下游接收

路由 handler 中构造的 `upstream_error` 响应应覆盖上述所有情况。error message 可以概括为 "Upstream returned incomplete or
invalid response"。

#### 多服务器架构

一个 Ktor `embeddedServer` 实例只能监听一个端口。虽然可以通过 `connector {}`
块配置多个连接器，但所有连接器共享同一套路由规则。由于不同端口需要转发到不同的上游地址，因此必须为每条规则创建独立的
`embeddedServer` 实例。

所有服务器通过 `start(wait = false)` 异步启动，引用存入 `List<EmbeddedServer>`。主线程通过
`runBlocking { awaitCancellation() }` 阻塞，保持进程存活。

#### 优雅关闭

注册关闭钩子（JVM `ShutdownHook` / Native `signal`），在钩子中逐个调用
`server.stop(gracePeriodMillis = 1000, timeoutMillis = 2000)` 停止所有服务器。

### 步骤 6：编写测试

在 `jvmTest` 中添加。参照 `proxy` 模块的 `ProxyTest.kt`，使用另一个 Ktor 服务器模拟上游。

1. **ConfigTest** — 验证 JSON 解析：
    - 正常多规则解析
    - 空数组
    - 缺少必要字段时抛异常
    - 格式错误的 JSON 抛异常

2. **ServerTest** — 使用真实 Ktor 服务器验证集成行为：
    - 启动 mock 上游服务器（返回 SSE 事件流）
    - 启动代理服务器
    - 用 `HttpClient` 发送请求验证响应
    - 测试场景：
        - 非 null 返回 -> 正常代理响应（参照 `ProxyTest` 的 SSE 测试）
        - null 返回（上游 SSE 流未终结）-> 502 + `upstream_error`（参照 `ProxyTest` 的 incomplete SSE 测试）
        - 异常上抛 -> 500 + `internal_error`（参照 `ProxyTest` 的 unreachable port 测试）

## 关键设计决策

### 1. 统一使用 CIO 引擎

客户端和服务端均使用 CIO 引擎，放在 `commonMain`。CIO 在 Ktor 3.x 中支持 JVM、Linux、macOS、Windows 目标。不再需要 `jvmMain`/
`nativeMain` 的平台特定引擎配置。cli 不支持 HTTPS。

### 2. JSON 配置替代 YAML

原计划使用 `kaml` 解析 YAML，但 `kaml` 已停止维护。改用 `kotlinx-serialization-json` 解析 JSON
配置文件，该库已在项目依赖中，无需引入额外依赖。文件读取使用 `kotlinx.io.files.SystemFileSystem`，全平台通用，无需
expect/actual。

### 3. 异常处理分层

| 层级                             | 处理方式                   | 状态码 |
|--------------------------------|------------------------|-----|
| 路由 handler 中 `proxy()` 返回 null | 构造 `upstream_error` 响应 | 502 |
| 路由 handler 中 `proxy()` 抛异常     | 上抛到 StatusPages 拦截器    | 500 |
| Ktor 服务器框架异常                   | StatusPages 拦截器        | 500 |

StatusPages 拦截 `Throwable`（而非 `Exception`），确保 `Error` 等类型也被兜底。路由 handler 本身不含 try-catch，异常自然传播到拦截器。

### 4. 路由 handler 内联

路由 handler 逻辑简单（调用 proxy + 判断 null），直接内联在 `embeddedServer` 配置块中，不单独提取函数。使用
`call.receiveChannel()` 传递请求体，避免缓冲整个请求。

### 5. 多服务器生命周期管理

```
main thread: parse args -> load config -> create servers -> start all (async) -> register shutdown hook -> runBlocking { awaitCancellation() }
shutdown: signal/hook -> stop all servers -> main thread unblocked -> process exits
```

所有服务器异步启动，引用存入 list。`awaitCancellation()` 阻塞主线程直到协程被取消。关闭钩子调用 `server.stop()`
停止所有服务器后，JVM/Native runtime 负责终止进程。

### 6. 共享 HttpClientEngine

所有 `ResponsesApiProxy` 实例共享同一个 `HttpClientEngine`（`CIO.create()`）。Ktor 的 `HttpClientEngine` 内部管理连接池，天然支持并发。不同
proxy 实例各自持有独立的 `HttpClient`，内部共享引擎的连接池。

### 7. 日志

使用 `kotlin-logging`，与 `proxy` 模块保持一致：`private val logger = KotlinLogging.logger {}`。JVM 平台通过
`logback-classic` 输出日志。不做额外日志配置。
