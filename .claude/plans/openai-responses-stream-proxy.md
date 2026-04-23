# Implementation Plan: OpenAI Responses Stream Proxy

校对日期：2026-04-23

## 目标

这是一个独立项目：

- `proxy` 是 Kotlin Multiplatform 核心库
- `cli` 是使用 Kotlin + Ktor 的宿主程序

整个项目是一个 OpenAI API 反向代理，只对 `POST /v1/responses`（create a Response）做协议级流转换，其他所有接口原封不动以最高效的字节流手段透传到上游。

它不处理：

- 认证配置注入
- 自定义 base path 路由
- 路由重写

它只对以下接口做协议级流转换：

- `POST /v1/responses`（create a Response）

其他所有接口（包括但不限于）：

- `GET /v1/responses/{response_id}`
- `POST /v1/chat/completions`
- `POST /v1/embeddings`
- 以及任何其他路径

均以原始字节流透传，不做任何解析或转换。

程序分两个子项目：

1. `proxy`
   - 核心库
   - 负责 `POST /v1/responses` 的协议级流转换
   - 负责所有其他接口的原始字节流透传
2. `cli`
   - 可执行程序
   - 读取配置文件
   - 启动 Ktor 服务
   - 一次监听多个端口
   - 根据本地监听端口选择上游 origin 和默认上游模式
   - 所有请求统一交给 `proxy` 处理

目录名：

- `proxy/`
- `cli/`

---

## 文档核对后的结论

### OpenAI Responses 协议结论

根据官方文档：

1. `POST /v1/responses` 支持 `stream: true` 建立 SSE 事件流。
2. Responses 的”流”是协议事件流，不是简单的字节分块。
3. 断点续流和去重必须以 `sequence_number` 为准。
4. 终态不只有 `response.completed`，还包括 `response.incomplete` 和 `response.failed`。`error` 是错误路径。
5. 代理内部续流使用 `GET /v1/responses/{response_id}?stream=true&starting_after={seq}`，但必须尊重 `store` 语义（`store: false` 的 response 无法续流）。
6. Retrieve 端点（`GET /v1/responses/{response_id}`）虽然也支持 `stream=true`，但本代理不对它做流转换，直接透传。

### Ktor I/O 结论

核心库要异步写数据回调用者，公共写出抽象应使用：

- `io.ktor.utils.io.ByteWriteChannel`

对应的上游读取抽象应使用：

- `io.ktor.utils.io.ByteReadChannel`

原因：

- 这两个类型是 Ktor common I/O 抽象
- 适合做协议转换时的持续读写
- 不需要把 `ApplicationCall` 暴露给核心库

### `kotlinx.serialization` 调研结论

影响”尽可能早发数据”的设计：

- `kotlinx.serialization` 的 `json-io` 模块不支持 Jackson 风格的 token writer / token parser
- `ByteWriteChannel.asSink()` / `ByteReadChannel.asSource()` 不是通用 common API 方案
- 结论：v1 不依赖 `kotlinx-serialization-json-io`
- `流 -> 非流` 路径采用手写的增量 JSON writer，直接向 `ByteWriteChannel` 输出
- `非流 -> 流` 路径在 v1 中只做到”拿到完整 JSON 后立即开始增量写 SSE”，不能承诺边收边转

---

## 总体架构

```text
Client
  -> cli (Ktor server)
    -> proxy (core library)
      -> upstream origin
```

职责边界：

- `proxy` 不接触 `ApplicationCall`
- `cli` 不实现协议转换

`cli` 只负责：

- 接入 HTTP
- 读取配置
- 端口监听
- 请求转成 `proxy` 的输入模型
- 调用 `proxy`
- 把 `proxy` 返回的 body writer 接到 Ktor 响应上

---

## 目录与 Gradle 结构

```text
openai-responses-stream-proxy/
├── settings.gradle.kts
├── build.gradle.kts
├── proxy/
│   ├── build.gradle.kts
│   └── src/
└── cli/
    ├── build.gradle.kts
    └── src/
```

`settings.gradle.kts` 中：

```kotlin
include(":proxy")
include(":cli")
```

项目名称在发布到 Maven 时通过发布配置重命名，不在 Gradle 项目名中处理。

---

## 平台与依赖收敛

### `proxy` 模块

`proxy` 是 Kotlin Multiplatform 模块。

目标：

- 只用 common 可用类型做公共 API
- 不使用平台特定类型

建议依赖：

- `io.ktor:ktor-client-core`
- `io.ktor:ktor-http`
- `io.ktor:ktor-io`
- `org.jetbrains.kotlinx:kotlinx-serialization-json`
- `org.jetbrains.kotlinx:kotlinx-coroutines-core`

不放进 `proxy` 的依赖：

- 任何 server engine
- 任何 client engine
- `ktor-server-*`
- `kotlinx-serialization-json-io`
- 额外配置文件解析库

### `cli` 模块

`cli` 是宿主程序模块，依赖 `proxy`。

`cli` 也应按 KMP 模块组织，但初版以 Kotlin/Native 可执行产物为主要交付目标。

原因：

- 整个项目以 KMP 为前提
- 最终使用时大多数情况下会以 Kotlin/Native 方式编译
- Ktor server 在 Native 侧使用 `embeddedServer`，且 Native server 仅支持 `CIO`
- 关键要求仍然是：平台特定类型不能泄漏进 `proxy` 的公共 API

建议依赖：

- `project(":proxy")`
- `io.ktor:ktor-server-core`
- `io.ktor:ktor-server-cio`
- `org.jetbrains.kotlinx:kotlinx-serialization-json`
- `ch.qos.logback:logback-classic`（仅 JVM，日志）

这里要明确：

- `ktor-server-cio` 属于 `cli` 的宿主目标依赖，不属于 `proxy` 的 common 依赖
- Ktor server 在 Native 侧仅支持 `CIO` engine
- `ApplicationCall`、`embeddedServer` 一类类型和 API 只存在于 `cli` 侧

客户端 engine 按平台 source set 引入：

| Source set | Engine artifact | 说明 |
|---|---|---|
| `jvmMain` | `io.ktor:ktor-client-cio` | JVM |
| `nativeMain` | `io.ktor:ktor-client-curl` | Linux / macOS |
| `mingwX64Main` | `io.ktor:ktor-client-winhttp` | Windows |

也就是说：

- `proxy/commonMain` 不出现任何 engine artifact
- `cli` 在各平台 source set 中引入对应的 client engine
- `proxy` 的 `HttpClient()` 使用默认 engine，由宿主实际引入的 engine 决定

可选依赖：

- `io.ktor:ktor-server-call-logging`
- `io.ktor:ktor-server-status-pages`

初版不引入：

- YAML/TOML/HOCON 解析库

原因：

- 配置文件初版直接用 JSON，避免再引入一套 parser

---

## 命名风格

命名遵循 Kotlin / Ktor 风格：

- 类、对象、接口：`UpperCamelCase`
- 函数、属性：`lowerCamelCase`
- 包名：全小写
- 配置项序列化名：`snake_case`

建议命名：

- `ResponsesProxy`
- `ResponsesProxyRequest`
- `PreparedResponse`
- `UpstreamMode`
- `ResponsesSseParser`
- `ResponsesJsonWriter`

避免：

- Rust 风格命名
- 过长的前缀重复
- `TO_STREAM` 这类全大写公开枚举名

公开枚举建议为：

```kotlin
enum class UpstreamMode {
    ToStream,
    ToNonStream,
}
```

配置文件里可序列化成：

- `"to_stream"`
- `"to_non_stream"`

---

## 认证与头部透传原则

网关不处理认证。

也就是说：

- 不从配置文件读取 API key
- 不注入 `Authorization`
- 不改写认证头
- 不维护任何账号状态

所有身份验证信息都由下游客户端直接提供，并被网关透传到上游。

对代理内部发起的 retrieve/resume 请求也一样：

- 复用同一份下游请求头
- 只剔除 hop-by-hop 头

---

## 路由与上游地址策略

代理接收所有路径的请求。

路由规则：

- `POST /v1/responses` → 协议级流转换（根据模式矩阵）
- 其他所有路径和方法 → 原始字节流透传

不支持：

- base path 前缀拼接
- 路由重写规则

配置里不设计 base URL。

配置里只保留上游 origin：

- 例如 `https://api.openai.com`

上游请求 URL 规则固定为：

```text
{upstreamOrigin}{originalPathAndQuery}
```

透传路径的行为：

- 不解析请求体
- 不解析响应体
- 直接使用 `ByteReadChannel.copyTo(ByteWriteChannel)` 做最高效的字节流转发
- 请求头和响应头遵循 Header 策略中的通用规则

---

## 核心模式矩阵

模式矩阵**仅适用于 `POST /v1/responses`（create a Response）**。

其他所有请求直接走原始字节流透传，不经过此矩阵。

对 `POST /v1/responses` 的每个请求先判断：

```kotlin
val clientWantsStream: Boolean  // 从请求 body 中的 stream 字段读取
val upstreamMode: UpstreamMode  // 从 listener 配置读取
val needsConversion = clientWantsStream != (upstreamMode == UpstreamMode.ToStream)
```

四种情况：

| 客户端 `stream` | 上游 mode | 是否转换 | 协议路径 | 代理是否负责续传 |
|---|---|---:|---|---|
| `true` | `ToStream` | 否 | Responses 流 -> Responses 流 | 否 |
| `false` | `ToNonStream` | 否 | Responses 非流 -> Responses 非流 | 否 |
| `false` | `ToStream` | 是 | Responses 流 -> Responses 非流 | 是 |
| `true` | `ToNonStream` | 是 | Responses 非流 -> Responses 流 | 有限支持 |

这里要强调：

- 这里说的“流/非流”，是 Responses 协议层的流/非流
- 不是说底层 I/O 是否分块

底层字节流始终都应该尽量异步、尽量早写、受下游背压控制。

---

## Core 公共 API

第一版要尽可能保持简单，因此 `proxy` 的公共接口面应尽量小。

v1 公开给外部使用的核心类型只保留：

1. `UpstreamMode`
2. `ResponsesProxyRequest`
3. `PreparedResponse`
4. `ResponsesProxy`

其他类型全部视为内部实现细节，不承诺稳定。

### 核心类

```kotlin
class ResponsesProxy(
    httpClient: HttpClient? = null,
    json: Json = defaultJson,
)
```

行为约束：

- 如果外部传入 `HttpClient`，就直接使用
- 如果没有传入，则内部构建默认 `HttpClient()`
- 若内部自己创建了 client，则由 `ResponsesProxy` 自己关闭
- 若外部注入了 client，则 `ResponsesProxy` 不负责关闭它

补充说明：

- `HttpClient()` 使用默认 engine
- engine 由宿主实际引入的 Ktor client engine 决定
- 因此 `proxy` 自己不需要依赖任何 engine artifact
- 若宿主目标中没有可用的 client engine 依赖，则默认 client 构造会失败
- 因此 CLI 必须显式依赖一个 client engine，这里初版选择 `ktor-client-cio`

### 请求与响应抽象

```kotlin
typealias BodyWriter = suspend ByteWriteChannel.() -> Unit

data class ResponsesProxyRequest(
    val method: HttpMethod,
    val path: String,
    val query: Parameters,
    val headers: Headers,
    val bodyBytes: ByteArray?,
    val upstreamOrigin: String,
    val upstreamMode: UpstreamMode,  // 仅对 POST /v1/responses 有效，其他请求忽略
)

data class PreparedResponse(
    val status: HttpStatusCode,
    val headers: Headers,
    val contentType: ContentType?,
    val bodyWriter: BodyWriter,
)
```

主入口建议为：

```kotlin
suspend fun prepareResponse(
    request: ResponsesProxyRequest,
): PreparedResponse
```

理由：

- `cli` 先拿到状态码、头和 body writer
- 然后在 Ktor 里使用 `respondBytesWriter`
- `bodyWriter` 的接收者就是下游 `ByteWriteChannel`

这是目前最适合保持 common API 干净的方式。

### 请求归一化

`proxy` 在 `prepareResponse(...)` 内部先做路由判断：

1. 检查 `method == POST && path == "/v1/responses"`
2. 若是 → 进入模式矩阵，需要解析 body 中的 `stream` 字段
3. 若不是 → 直接走原始字节流透传

对 `POST /v1/responses` 的归一化是内部私有实现，不作为公开类型暴露。

它至少要提取出：

- 客户端是否要求流式（从 body 的 `stream` 字段）
- 完整的请求体（可能需要改写 `stream` 字段）

---

## Core 内部结构建议

```text
proxy/src/commonMain/kotlin/
├── api/
│   ├── ResponsesProxy.kt
│   ├── ResponsesProxyRequest.kt
│   ├── PreparedResponse.kt
│   └── UpstreamMode.kt
└── internal/
    ├── ProxyExecutor.kt          # 主流程分发：判断 create vs 透传
    ├── RawPassThrough.kt         # 原始字节流透传（非 create 路径）
    ├── RequestRewriter.kt        # 改写 create body 中的 stream
    ├── UpstreamHttp.kt           # 上游请求 helper
    ├── ResponsesSseParser.kt
    ├── ResponsesSseWriter.kt
    ├── ResponsesJsonWriter.kt
    ├── ResumeState.kt
    └── HeaderFilter.kt
```

这里也要刻意收敛：

- 第一版不需要过度拆分很多小类
- 不需要 `Planner`、`Manager`、`Factory`、`TerminalState` 这类抽象满天飞
- 一个 `ProxyExecutor` 负责主流程分发（create 走模式矩阵，其他走 `RawPassThrough`）即可
- 其余只保留几个明确的协议辅助类

---

## 上游客户端设计

内部上游客户端不暴露给 CLI，也不单独抽成公开接口。

第一版只由 `ResponsesProxy` 内部持有一个 `HttpClient`，通过一个收敛后的内部 helper 负责发请求。

内部 helper 覆盖两种场景：

### 1. 原始字节流透传（非 `POST /v1/responses`）

- 直接转发原始请求
- 响应使用 `ByteReadChannel` 直接 copyTo 下游 `ByteWriteChannel`
- 不解析请求体和响应体
- 不缓存

### 2. Responses create（`POST /v1/responses`）

覆盖两种动作：

- create non-stream
- create stream

约束：

- 流式响应使用 `ByteReadChannel`
- 非流响应读取为完整 `ByteArray`
- 不在流路径上调用会一次性读完 body 的快捷 API

流式上游 body 处理要点：

- 逐块读取 `ByteReadChannel`
- 增量喂给 SSE parser
- 事件级转换后尽快写到下游 `ByteWriteChannel`

上游请求改写规则（仅 `POST /v1/responses`）：

- Create:
  - 改写 body 中的 `stream`
- 内部续流:
  - 使用 `GET /v1/responses/{response_id}?stream=true&starting_after={last_sequence_number}`
  - 这是 proxy 内部行为，不是客户端触发的路由

除此之外不改写业务参数。透传路径不做任何改写。

---

## 协议流转换与字节异步写出

### 设计原则

“Responses 流式转换”处理的是协议事件语义：

- `response.created`
- `response.output_item.added`
- `response.output_text.delta`
- `response.completed`
- 等等

而字节层原则是：

- 只要协议上已经能确定一段合法输出，就立即写给下游
- 不额外把整份下游结果缓存到内存
- 始终遵守单 writer 协程和背压

### `流 -> 非流`

这条路径可以做到真正的增量写出。

- 上游是协议事件流，代理边收事件边构造最终 JSON
- 安全立即落盘粒度：顶层稳定字段 → 已完成的 output item → 终态字段
- 内存上界：顶层状态 + 所有未完成 output item 的暂存状态
- 不把整份最终 JSON 常驻内存

### `非流 -> 流`

- v1 先收完上游 JSON，再逐事件写出合成 SSE
- 不把”完整 JSON + 完整 SSE”同时存下来
- 不能边收上游 JSON 边做协议级转换（缺少合适的 common token 级 JSON parser）

### 背压与取消

无论是哪条路径，都遵循同一条 I/O 规则：

- 对下游 `ByteWriteChannel` 的写入天然受背压约束
- 若下游变慢，转换协程应自然挂起，而不是把大量中间结果堆进内存
- 若下游取消，必须向上游传播取消

这条规则比“尽快 flush”更高优先级。

---

## 透传路径

### 通用透传（所有非 `POST /v1/responses` 的请求）

条件：

- 请求方法不是 `POST`，或路径不是 `/v1/responses`

行为：

- 不解析请求体
- 不解析响应体
- 上游请求保持原始 method / path / query / headers
- 使用 `ByteReadChannel.copyTo(ByteWriteChannel)` 直接转发响应字节
- 这是最高效的字节流处理手段

适用场景（包括但不限于）：

- `GET /v1/responses/{response_id}` — retrieve（无论 stream=true 还是 stream=false）
- `POST /v1/chat/completions` — Chat Completions（包括流式）
- `POST /v1/embeddings`
- `DELETE /v1/responses/{response_id}`
- 任何其他 OpenAI API 路径

这些路径不做任何协议转换、不做请求体改写、不做响应解析。

### Responses create 直通（无转换时的 `POST /v1/responses`）

当 `clientWantsStream == (upstreamMode == ToStream)` 时：

- `stream=true` + `ToStream` → 请求体不改 `stream`，逐块透传上游 SSE 字节
- `stream=false` + `ToNonStream` → 请求体不改 `stream`，直接转发 JSON 响应

这两条路径同样不做协议转换。

---

## 转换路径 A：Responses 流 -> Responses 非流

仅适用于 `POST /v1/responses`（create a Response）。

条件：

- 客户端 `stream=false`
- 上游 mode = `ToStream`

### 处理流程

1. 改写 body 中的 `stream=true`
2. 连接上游 SSE
3. 增量解析 Responses 事件
4. 增量写出最终 JSON
5. 在终态时收尾

### 终态规则

最终结果优先以终态事件中的完整 `response` 为准。

接受的终态：

- `response.completed`
- `response.incomplete`
- `response.failed`

`error` 视为错误路径，不是成功终态。

### 续流

当满足以下条件时，允许代理级续流：

1. 已从 SSE 事件中拿到 `response_id`
2. 已拿到 `last_sequence_number`
3. 断开发生在终态前
4. 原始请求 body 中没有显式 `store:false`

续流方式（proxy 内部行为）：

```http
GET /v1/responses/{response_id}?stream=true&starting_after={last_sequence_number}
```

要求：

- 用 `sequence_number` 去重
- 续流次数有限
- 续流时复用原始下游请求头
- 续流使用的 `response_id` 从 SSE 事件中获取

注意：续流是对 retrieve 端点的内部调用，不是客户端触发的。客户端无法通过此代理对 retrieve 端点做流转换。

续流的 `store` 语义：如果原始 create body 中 `store` 为 `false`，上游不会保存该 response，续流调用会失败。proxy 在 create body 中检测 `store` 字段，若为 `false` 则不触发续流。若上游续流请求返回错误（如 404），视为传输错误处理。

### 客户端提前断开

若下游在最终 JSON 写完前断开：

- 立即取消上游 SSE
- 丢弃续流状态
- 不在后台继续消费

---

## 转换路径 B：Responses 非流 -> Responses 流

仅适用于 `POST /v1/responses`（create a Response）。

条件：

- 客户端 `stream=true`
- 上游 mode = `ToNonStream`

### 处理流程

1. 改写 body 中的 `stream=false`，向上游发普通非流 create
2. 收完整个 JSON 响应
3. 解析为内部模型
4. 逐事件写出合成 SSE

### 重要限制

这条路径不能承诺复刻真实上游流。

v1 只保证生成兼容的 Responses 事件流，不保证：

- 真实 token 节奏
- 真实中间状态节奏
- 真实 reasoning 演化过程

### 合成事件最小集合

合成事件中的 `response_id` 直接从上游非流 JSON 响应的 `id` 字段提取，不使用代理本地生成的值。其他字段（`object`、`status` 等）同理从上游 JSON 中提取。

首版支持：

- `response.created`
- `response.in_progress`
- `response.output_item.added`
- `response.content_part.added`
- `response.output_text.delta`
- `response.output_text.done`
- `response.content_part.done`
- `response.function_call_arguments.delta`
- `response.function_call_arguments.done`
- `response.output_item.done`
- `response.completed`
- `response.incomplete`
- `response.failed`

### 本地 `sequence_number`

合成流中的 `sequence_number` 由代理本地生成。

只保证：

- 单调递增
- 对本次合成流有效

不保证：

- 可映射到上游真实 `starting_after`

因此：

- 下游若在这条合成流上断开
- 代理不能拿本地序号去上游续流

### 上游非流中途断开

若上游 JSON body 未收完就断开：

- 不自动重放 create
- 若当时尚未知 `response_id`，直接报错
- 即使已知 `response_id`，v1 也不把这条路径设计成通用自动恢复

结论：

- `非流 -> 流` 的恢复能力是有限的

### 客户端提前断开

- 若上游 JSON 还没收完，立即取消上游请求
- 若 JSON 已收完但正在写合成 SSE，立即停止写出

---

## 事件模型策略

v1 只显式实现构成主路径所需的 Responses 事件。

最少要处理：

- `response.created`
- `response.in_progress`
- `response.output_item.added`
- `response.output_item.done`
- `response.content_part.added`
- `response.content_part.done`
- `response.output_text.delta`
- `response.output_text.done`
- `response.function_call_arguments.delta`
- `response.function_call_arguments.done`
- `response.completed`
- `response.incomplete`
- `response.failed`
- `error`

策略：

- 未知非终态事件不应直接导致崩溃
- `response.failed` / `response.incomplete` 是协议内终态
- `error`、连接中断、超时是错误路径

### `response.output_item.done` 的泛化处理

`response.output_item.done` 是一个通用事件，不论 output item 类型都会触发。

对于 `流 -> 非流` 转换路径的增量 JSON writer：

- 已知类型（text、function_call）的中间 delta 事件由专门逻辑处理
- 未知类型（file_search、code_interpreter 等）的中间事件可以安全跳过
- 但 `response.output_item.done` 携带完整的 output item 数据，**必须**用于将已完成 item 写入 JSON 的 `output` 数组
- 这样即使 proxy 不理解 file_search / code_interpreter 的中间事件，最终的 `output` 数组仍然完整

### v1 不处理的事件

以下事件在 v1 中不专门处理，但不会导致崩溃：

- `response.refusal.delta` / `response.refusal.done`
- `response.output_text.annotation.added`
- `response.file_search_call.in_progress` / `searching` / `completed`
- `response.code_interpreter.*`（五个事件）

`response.output_item.done` 的泛化处理确保这些类型的 output item 仍出现在最终的 JSON 中。

### `include_obfuscation` 说明

OpenAI retrieve 端点有 `include_obfuscation` 参数（默认 `true`）。v1 不处理流混淆，proxy 的内部续流请求不设置此参数，使用上游默认行为。如果后续发现混淆影响续流，再增加处理。

---

## SSE 与 JSON writer 设计

### SSE parser

`ResponsesSseParser` 从 `ByteReadChannel` 增量读取：

- 识别 `event:`
- 识别 `data:`
- 以空行分隔事件
- 支持跨 chunk 半包

### SSE writer

`ResponsesSseWriter` 直接写到 `ByteWriteChannel`：

```kotlin
suspend fun writeEvent(
    sink: ByteWriteChannel,
    eventName: String,
    dataJson: String,
)
```

### JSON writer

`ResponsesJsonWriter` 是 `流 -> 非流` 路径的关键。

它不是通用 JSON 库替代品，而是一个专门针对 Responses 最终响应结构的增量 writer。

职责：

- 写对象前缀
- 写字段分隔符
- 写数组项
- 写字符串转义
- 写终态尾部字段
- 保证在任意时刻都只维护最小状态

建议固定输出字段顺序，优先满足“可尽早写出”，而不是对齐参考文档中的展示顺序。

例如：

1. `id`
2. `object`
3. `created_at`
4. `model`
5. `output`
6. `status`
7. `error`
8. `incomplete_details`
9. `usage`
10. 其余稳定字段

说明：

- JSON 对象字段顺序不影响语义
- 但固定顺序有助于 writer 状态机更简单
- 也能把只在终态才知道的字段集中到尾部

写法要求：

- 单 writer 协程
- 适时 `flush()`
- 不多协程并发写同一 `ByteWriteChannel`

---

## Header / Status 策略

### 通用请求头策略

向上游转发时：

- 尽量透传原始头
- 删除 hop-by-hop 头
- 不改认证头

### 转换路径响应头

`流 -> 非流`：

- `Content-Type: application/json`

`非流 -> 流`：

- `Content-Type: text/event-stream`
- `Cache-Control: no-cache`

不直接透传：

- `Content-Length`
- `Transfer-Encoding`
- 与上游实体编码绑定的头

---

## CLI 设计

### 配置文件

初版配置文件直接使用 JSON。

示例：

```json
{
  "listeners": [
    {
      "host": "0.0.0.0",
      "port": 8081,
      "upstream_origin": "https://api.openai.com",
      "upstream_mode": "to_stream"
    },
    {
      "host": "0.0.0.0",
      "port": 8082,
      "upstream_origin": "https://api.openai.com",
      "upstream_mode": "to_non_stream"
    }
  ]
}
```

没有：

- auth 配置
- base path 配置
- 自定义路由配置

初版也不加入：

- 每端口独立的自定义 header 注入
- 路由重写
- 请求体改写规则

### 多端口监听

CLI 使用一个 Ktor server + 多个 connector。

请求到来后，根据本地监听端口选择 listener 配置。

决策依据是：

- 本地实际监听端口

而不是：

- `Host` 头

### CLI 路由范围

注册一个 catch-all 路由，接收所有请求。

CLI 不做路由判断——所有请求统一构造 `ResponsesProxyRequest` 交给 `proxy`。`proxy` 内部判断是 `POST /v1/responses` 还是透传。

### Ktor 集成方式

所有请求统一处理：

1. 从 `ApplicationCall` 读取方法、路径、query、headers、body bytes
2. 根据本地端口找到 listener 配置
3. 构造 `ResponsesProxyRequest`
4. 调用 `ResponsesProxy.prepareResponse(...)`
5. 用 Ktor 的 `respondBytesWriter` 执行 `bodyWriter`

`proxy` 内部判断 `POST /v1/responses` 走流转换，其他走透传。CLI 不需要区分。

---

## 错误处理与关闭语义

### `proxy` 生命周期

`ResponsesProxy` 建议直接提供平台无关的关闭 API：

```kotlin
suspend fun close()
```

规则：

- 内部创建的 `HttpClient` 由 `ResponsesProxy.close()` 关闭
- 外部注入的 `HttpClient` 不由 `ResponsesProxy` 关闭

### 错误映射

建议把错误分成三类：

1. 上游 HTTP 错误
   - 优先透传状态码和 body
2. 协议错误
   - SSE 格式损坏
   - 缺失必需终态
   - 非法事件顺序
3. 传输错误
   - 上游断开
   - 下游断开
   - 超时

其中：

- 上游明确返回的 HTTP 错误，不要包装成 200 + `error` 事件
- 协议转换路径里若在下游已经开始写 body 后再出错，只能中止连接，不能再补发一个新的完整错误对象

---

## 测试计划

### `proxy` 单元测试

1. 路由判断：`POST /v1/responses` 走模式矩阵，其他走透传
2. 模式矩阵判定
3. create body 里的 `stream` 改写
4. SSE 半包/拆包
5. `流 -> 非流` 增量 JSON 写出
6. output item 交错时的最小缓冲策略
7. `response.output_item.done` 对未知类型的泛化写入
8. `流 -> 非流` 续流与去重
9. `非流 -> 流` 合成 SSE
10. `store:false` 禁止自动续流
11. 未知非终态事件前向兼容
12. 原始字节流透传（非 create 路径）

### `cli` 集成测试

1. 单端口监听
2. 多端口监听
3. 按本地端口选择上游 origin
4. 认证头透传
5. `respondBytesWriter` 下 SSE 正常写出
6. 下游提前断开时正确取消上游
7. 非 create 路径的原始字节流透传
8. `POST /v1/chat/completions` 流式透传正常

### 内存与时序验证

重点验证：

1. `流 -> 非流`
   - 不需要先收完整个 SSE 再输出
   - 下游能尽早收到 JSON 字节
2. `非流 -> 流`
   - v1 虽然不能边收边转
   - 但不会把完整 SSE 结果再整体缓存一份

---

## 分阶段实施

### Phase 1：基础框架

- 建立 `proxy` / `cli` 两个子项目
- 定义 core API（`ResponsesProxy`、`ResponsesProxyRequest`、`PreparedResponse`、`UpstreamMode`）
- 实现原始字节流透传（所有请求默认路径）
- 打通单端口端到端

### Phase 2：`POST /v1/responses` 无转换路径

- 实现路由判断（`POST /v1/responses` vs 其他）
- 实现 create 请求的 stream 字段提取
- 实现 `stream=true` + `ToStream` 直通
- 实现 `stream=false` + `ToNonStream` 直通

### Phase 3：流转换

- 实现 `流 -> 非流` 增量 JSON writer
- 实现 `非流 -> 流` 合成 SSE
- SSE parser / writer
- `response.output_item.done` 泛化处理

### Phase 4：续流与健壮性

- 实现 `流 -> 非流` 的 retrieve + stream 续流
- 补齐断开、重试、去重逻辑
- 收敛 header / status 处理
- 错误处理完善

### Phase 5：CLI 完整化

- 多端口监听
- 配置文件（JSON）
- 集成测试
- 内存/时序验证

---

## 计划结论

本计划收敛后的核心点是：

- 这是一个 OpenAI API 反向代理，只对 `POST /v1/responses` 做协议级流转换
- 所有其他接口以原始字节流透传，不做任何解析或转换
- 项目目录为 `proxy/` 和 `cli/`，Maven 发布时再配置 artifact 名称
- `proxy` 是 multiplatform 核心库，公开 API 只使用 common 类型
- `proxy` 第一版公开接口面故意压到最小，只暴露一个主类和少量数据类型
- `cli` 负责 Ktor 接入与多端口监听，所有请求统一交给 `proxy`
- 核心协议转换处理的是 Responses 事件语义，不是单纯字节拷贝
- 字节层始终尽量异步和尽量早写
- `流 -> 非流` 在 v1 中可以做到真正的增量写 JSON
- `response.output_item.done` 的泛化处理确保未知类型 output item 仍出现在最终 JSON 中
- `非流 -> 流` 在 v1 中只做到”完整解析后立即增量写 SSE”
- 续流是 proxy 内部行为，使用 retrieve 端点，不受客户端直接触发
- 网关不处理认证，所有认证信息都由下游透传
- `proxy` 保持 multiplatform 公共 API，`cli` 作为 Native 优先的宿主模块承接 Ktor server engine
