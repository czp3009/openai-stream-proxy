# Implementation Plan: OpenAI Responses Non-Stream to Stream Proxy

校对日期：2026-05-17

## 目标

本项目是一个 OpenAI API 反向代理，但协议转换范围严格收窄为单向：

- 只处理 `POST /v1/responses`
- 只处理下游非流、非后台 create 请求
- 代理把它改写为上游 `stream: true`
- 代理消费上游 SSE，等待终态后，再向下游返回普通 JSON

本项目明确不实现本范围之外的其他转换方向。

## 当前代码状态

截至本文校对日期，仓库中的实现状态仍然很早期：

- `proxy` 目前只有占位 `hello()` 函数，没有任何协议转换逻辑
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
4. `response.completed` 的 `response` 字段包含完整元数据（`id`、`model`、`usage` 等），但 `output` 数组可能为空（需要从
   `response.output_item.done` 回退填充）。
5. 同步 response 的取消方式是终止连接。
6. 非流语义应尽量表现为”完整结果就绪后再返回普通 JSON”。

### 对本项目的含义

1. 同步非流 create 可以改写为上游流 create。
2. 代理按 SSE 聚合规则处理事件，无需理解输出项内部语义。
3. 上游 / 下游断连、非 SSE 响应等异常场景的处理详见 SSE 聚合规则。

## SSE 聚合规则

代理按顺序读取上游 SSE 事件，执行以下逻辑：

1. **收集 output 项**：每收到一个 `response.output_item.done` 事件，按其 `output_index` 作为数组下标，将 `item` 字段存入
   output 数组。
2. **成功终态**：收到 `response.completed` 时，检查其 `response.output`：
   - 若为非空数组 → 直接将该 `response` 作为最终结果返回给下游。
   - 若为 `null` / `undefined` / 空数组 → 将第 1 步积累的 output 数组填入 `response.output`，然后返回。
3. **失败 / 未完成终态**：收到 `response.failed` 或 `response.incomplete` 时，直接返回其中的 `response` 对象。
4. **上游断开连接**：若上游在发送终态事件之前断开连接（无 `response.completed` / `response.failed` / `response.incomplete`
   ），则中断与下游的连接。
5. **下游断开连接**：若下游在收到终态事件之前断开连接，则中断与上游的连接。
6. **非 SSE 响应透传**：若上游返回的 `Content-Type` 不是 `text/event-stream`，则直接将上游的 HTTP 状态码、响应头和响应体透传给下游，不做任何处理。

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

## 模块职责

### `proxy`

- 核心库
- 负责识别是否进入转换路径
- 负责最小 JSON 改写
- 负责上游 SSE 解析
- 负责按聚合规则维护 output 数组和终态 response
- 负责把终态 `response` 还原为下游普通 JSON
- 负责其他路径的原始字节透传

### `cli`

- 宿主程序
- 读取配置
- 启动 Ktor 服务
- 绑定监听端口
- 创建 `HttpClient`
- 把 HTTP 请求/响应桥接到 `proxy`

## 推荐实现步骤

1. 实现原始反向代理透传骨架
2. 只在 `POST /v1/responses` 上增加请求体识别
3. 对同步非流 create 执行最小 JSON 改写：
   - 仅把 `stream` 改成 `true`
4. 实现通用 SSE 解析器
5. 实现 SSE 聚合规则（完整覆盖规则 1–6）
6. 对 `background=true` 和 `stream=true` 请求直接透传

## Ktor / I/O 结论

核心库对外的公共 I/O 抽象应继续使用：

- `io.ktor.utils.io.ByteReadChannel`
- `io.ktor.utils.io.ByteWriteChannel`

原因：

- 它们是 Ktor common I/O 抽象
- 足以覆盖请求透传、SSE 解析、内存聚合和终态 JSON 写回
- 不需要把 `ApplicationCall` 暴露给 `proxy`

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

- `protocol.md`
- https://developers.openai.com/api/reference/resources/responses/methods/create
- https://developers.openai.com/api/docs/guides/streaming-responses?api-mode=responses
- https://developers.openai.com/api/docs/guides/background
