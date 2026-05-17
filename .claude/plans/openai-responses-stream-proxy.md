# Implementation Plan: OpenAI Responses Non-Stream to Stream Proxy

校对日期：2026-05-18

## 目标

本项目是一个 OpenAI API 反向代理，但协议转换范围严格收窄为单向：

- 只处理 `POST /v1/responses`
- 只处理下游非流、非后台 create 请求
- 代理把它改写为上游 `stream: true`
- 代理消费上游 SSE，等待终态后，再向下游返回普通 JSON

本项目明确不实现本范围之外的其他转换方向。

## 当前代码状态

截至本文校对日期，仓库中的实现状态仍然很早期：

- `proxy` 已实现核心协议转换逻辑（`ResponsesApiProxy` + `ResponseAccumulator`），通过单元测试验证
- `cli` 目前只有占位 `main()`，没有启动 HTTP 服务，也没有调用真实代理逻辑
- 真正可运行的开发工具目前只有 `sniffer` 和 `mock-client`

因此，本文后续内容描述的是目标实现，而不是当前已经存在的能力。

## 非目标

本项目不处理：

- 认证配置注入
- 自定义 base path 路由
- 路由重写
- 结构化输出解析
- 工具调用执行
- SDK convenience 字段补全

## 接口边界

### 进入协议转换的请求

- `POST /v1/responses`
- 请求体 JSON 中 `stream` 缺失或为 `false`
- 请求体 JSON 中 `background` 缺失或为 `false`

### 直接透传的请求

- `POST /v1/responses` 且 `stream=true`
- `POST /v1/responses` 且 `background=true`
- `GET /v1/responses/{response_id}`
- `POST /v1/chat/completions`
- `POST /v1/embeddings`
- 以及任何其他路径

## 为什么 `background=true` 必须透传

根据 OpenAI 官方文档：

- `background=true` 表示后台异步任务
- 后续调用方会自己 `retrieve` / `cancel`
- 若同时使用 `background=true` 和 `stream=true`，还会涉及官方定义的 resume 语义
- 后台模式要求 `store=true`
- 后台模式会短时存储响应，因此不兼容 ZDR

所以它不是普通同步 create 的“返回格式参数”，而是不同的生命周期模型。

本项目不应把 `background=true` 请求纳入“非流转流”的同步等待路径。

## 协议结论

### 已确认的 OpenAI 事实

1. `POST /v1/responses` 支持 `stream: true`。
2. Responses 流是语义化 SSE 事件流。
3. 关键事件包括 `response.output_item.done`（完整输出项快照）、`response.completed`（终态，含完整元数据）、`response.failed`、
   `response.incomplete`。
4. `response.completed` 的 `response` 字段包含完整元数据（`id`、`model`、`usage` 等），但 `output` 数组
   可能为空（见聚合规则设计依据）。
5. 同步 response 的取消方式是终止连接。
6. 非流语义应尽量表现为”完整结果就绪后再返回普通 JSON”。

### 对本项目的含义

1. 同步非流 create 可以改写为上游流 create。
2. 代理按 SSE 聚合规则处理事件，无需理解输出项内部语义。
3. 上游 / 下游断连、非 SSE 响应等异常场景的处理详见 SSE 聚合规则。

## SSE 聚合规则

代理按顺序逐个读取上游 SSE 事件，每处理完一个事件后即丢弃，不在内存中保留已处理的原始事件数据。
仅维护当前聚合所需的 `output` 数组和终态 `response` 对象。

1. **收集 output 项**：每收到一个 `response.output_item.done` 事件，按其 `output_index` 作为数组下标，将 `item` 字段存入
   output 数组。
2. **成功终态**：收到 `response.completed` 时，检查其 `response.output`：
    - 若为非空数组 → 直接将该 `response` 作为最终结果。
    - 若为 `null` / `undefined` / 空数组 → 将第 1 步积累的 output 数组填入 `response.output`。
    - HTTP 状态码使用 `200`。
3. **失败终态**：收到 `response.failed` 时，取其中的 `response` 对象作为最终结果。HTTP 状态码根据
   `response.error.code` 映射：

   | `error.code`           | HTTP 状态码 |
      |------------------------|----------|
   | `insufficient_quota`   | `429`    |
   | `rate_limit_exceeded`  | `429`    |
   | `usage_not_included`   | `429`    |
   | `server_is_overloaded` | `503`    |
   | `slow_down`            | `503`    |
   | `server_error`         | `500`    |
   | 其他所有值                  | `400`    |

4. **未完成终态**：收到 `response.incomplete` 时，取其中的 `response` 对象作为最终结果，HTTP 状态码设为 `502`。
5. **终结事件后的上游事件**：`response.completed`、`response.failed`、`response.incomplete` 为终结事件。
   收到终结事件并取得最终结果后，继续消费上游 SSE 但丢弃所有后续事件，不主动关闭与上游的连接，
   直到上游自己断开连接，然后返回。
6. **上游断开连接**：若上游在发送终结事件之前断开连接（无 `response.completed` / `response.failed` /
   `response.incomplete`），则关闭与下游的连接。
7. **下游断开连接**：若下游在收到终结事件之前断开连接，则中断与上游的连接。
8. **非 SSE 响应透传**：若上游返回的 `Content-Type` 不是 `text/event-stream`，则直接将上游的 HTTP 状态码、响应头和响应体透传给下游，不做任何处理。

### 聚合规则的设计依据

- `response.completed` 的 `response` 字段与非流式 `POST /v1/responses` 返回值结构相同，包含完整的元数据（`id`、`model`、
  `status`、`usage` 等）。
- 实践中 `response.completed` 的 `response.output` 可能为空数组（已通过 sniffer 截获确认，sub2api 项目也处理了此情况）。
- `response.output_item.done` 的 `item` 字段包含完整的输出项 JSON（文本、annotations、function call 参数等），已通过官方文档和两个官方
  SDK 源码确认。
- 所有服务端实现（vLLM、ollama、LocalAI 等）均确保 `output_item.done` 包含完整内容。
- 不需要处理 delta 级别的第三层回退——没有任何项目在 `output_item.done` 内容为空时退化为 delta 重建。
- 工具调用不会导致代理阻塞——OpenAI Responses API 的工具调用是多轮请求模式，流在发送 `function_call` 后即正常结束（
  `response.completed`），客户端在新 HTTP 请求中提交工具结果。

## 下游响应构造

代理向下游返回的 HTTP 响应构造规则：

- **响应头**：以上游 SSE 响应的响应头为基础，覆盖以下字段以满足非流式 JSON 响应的语义：
    - `Content-Type` → `application/json`
    - `Transfer-Encoding` → 移除（非分块传输）
    - 其他上游响应头原样保留（如 `x-request-id`、`openai-organization` 等）。
- **响应体**：终态 `response` 对象序列化为 JSON。
- **HTTP 状态码**：按 SSE 聚合规则 2–4 中的映射确定。

`ResponsesApiProxy.proxy()` 通过返回 `OutgoingContent?`（来自 `io.ktor.http.content`，属于 `ktor-http`）
向下游传递完整响应，`cli` 侧只需 `call.respond(result)`。

- **协议转换路径**：返回 `OutgoingContent.ByteArrayContent`，响应已完全聚合在内存中，无活连接。
  `ByteArrayContent` 是 Ktor 最高效的 `OutgoingContent` 类型。
- **透传路径**：返回 `OutgoingContent.ReadChannelContent`，上游响应体以 `ByteReadChannel` 形式直接
  交给 Ktor。Ktor 在 `respondOutgoingContent` 中对 `ReadChannelContent` 有 try-finally 保护，
  保证 `readFrom()` 返回的 channel 一定会被 `cancel()`，上游连接不会泄漏。

## 模块职责

### `proxy`

- 核心库
- 负责识别是否进入转换路径
- 负责最小 JSON 改写
- 负责上游 SSE 解析
- 负责按聚合规则维护 output 数组和终态 response
- 负责构造 `OutgoingContent` 返回给下游
- 负责其他路径的原始字节透传

### `cli`

- 宿主程序
- 读取配置
- 启动 Ktor 服务
- 绑定监听端口
- 创建 `HttpClient`
- 调用 `ResponsesApiProxy.proxy()` 并将返回的 `OutgoingContent` 通过 `call.respond()` 写给下游
- `proxy()` 返回 `null` 时关闭下游连接

## 核心类设计

### `ResponseAccumulator`

每次流式转换创建一个新实例，负责消费上游 SSE 事件并拼装出最终的 `Response` 对象。

- **生命周期**：每次进入协议转换路径时新建实例，转换结束后丢弃。
- **`accumulate(event)`**：接收一个已解析的 SSE 事件，按 SSE 聚合规则 1 处理
  `response.output_item.done` 事件（按 `output_index` 存入 output 数组），遇到终结事件
  （规则 2–4）时完成聚合。终结事件之后的事件静默丢弃（规则 5）。
- **`response`**：仅当收到过终结事件后可调用，返回拼装完成的最终 `Response` 对象。
  调用前应通过 `isCompleted` 等状态字段确认已终结。

内部状态仅包含：

- `output` 数组（来自 `response.output_item.done` 事件）
- 终态 `response` 对象（来自终结事件）
- `isCompleted`：终结标记
- 终结类型（`completed` / `failed` / `incomplete`）

### `ResponsesApiProxy`

在整个反向代理的生命周期中持续存在的服务对象，负责将下游非流请求改写为上游流请求并返回结果。

- **生命周期**：随代理服务启动创建，随代理服务停止销毁。
- **构造参数**：
    - 上游 `HttpClient`
    - 上游 base URL
- **`proxy(requestMethod, requestPath, requestHeaders, requestBody): OutgoingContent?`**：接收下游请求参数，返回
  `OutgoingContent` 或 `null`。
    - **协议转换路径**：返回 `ByteArrayContent`，其中封装了状态码、响应头和完整 JSON 响应体。
    - **透传路径**：返回 `ReadChannelContent`，封装上游状态码、响应头，`readFrom()` 返回上游 body channel，由 Ktor
      直接流式写入下游。Ktor 对 `ReadChannelContent` 有 try-finally 保护，保证 channel 一定被 cancel。
    - **失败（SSE 已开始后上游断连）**：返回 `null`，`cli` 侧关闭下游连接。
    - **内部流程**：
        1. 将请求体中 `stream` 改写为 `true`
        2. 向上游发送改写后的请求
        3. 若上游返回非 SSE 响应（规则 8），直接透传
        4. 创建 `ResponseAccumulator` 实例，逐个读取上游 SSE 事件并调用 `accumulate`
        5. 收到终结事件后，从 `ResponseAccumulator` 取出最终 `response`
        6. 按 SSE 聚合规则 2–4 确定 HTTP 状态码
        7. 按下游响应构造规则构造 `OutgoingContent` 返回
        8. 继续消费上游剩余事件直到上游关闭连接（规则 5）

## 推荐实现步骤

1. 实现原始反向代理透传骨架
2. 只在 `POST /v1/responses` 上增加请求体识别
3. 对同步非流 create 执行最小 JSON 改写：
   - 仅把 `stream` 改成 `true`
4. 实现通用 SSE 解析器
5. 实现 SSE 聚合规则（完整覆盖规则 1–8）及下游响应构造
6. 对 `background=true` 和 `stream=true` 请求直接透传

## Ktor / I/O 结论

`proxy` 通过 `OutgoingContent`（`io.ktor.http.content`，属于 `ktor-http`）向 `cli` 传递完整响应，
不依赖 `ktor-server-core`，不需要将 `ApplicationCall` 暴露给 `proxy`。

- 协议转换路径返回 `ByteArrayContent`（最简，无活连接）
- 透传路径返回 `ReadChannelContent`（Ktor 自带 try-finally 资源保护）

内部 I/O 使用 Ktor common 抽象：

- `io.ktor.utils.io.ByteReadChannel`

## v1 交付范围

v1 只交付：

- `POST /v1/responses` 的单向非流转流
- 基于内存聚合的最终 `Response` 回写
- `background=true` 透传
- `stream=true` 透传
- 其他所有接口透传

v1 不交付：

- 续流恢复
- 后台任务聚合
- 其他方向的协议转换

## 参考文档

- https://developers.openai.com/api/reference/resources/responses/methods/create
- https://developers.openai.com/api/docs/guides/streaming-responses?api-mode=responses
- https://developers.openai.com/api/docs/guides/background
