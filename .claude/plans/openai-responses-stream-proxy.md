# Implementation Plan: OpenAI Responses Non-Stream to Stream Proxy

校对日期：2026-05-10

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
3. 常见事件包括 `response.created`、`response.in_progress`、`response.completed`、`response.failed`、
   `response.output_text.delta` 等。
4. create 的流式示例里，`response.completed` 带完整 `response`。
5. 同步 response 的取消方式是终止连接。
6. 非流语义应尽量表现为“完整结果就绪后再返回普通 JSON”。

### 对本项目的含义

1. 同步非流 create 可以改写为上游流 create。
2. 代理不需要自己发明最终协议，只需要保留终态 `response`。
3. 下游若中途断开，必须立刻中止上游同步流。
4. v1 最稳的返回策略是：先在内存中聚合最终 `Response` 状态，再一次性返回下游。

## 模块职责

### `proxy`

- 核心库
- 负责识别是否进入转换路径
- 负责最小 JSON 改写
- 负责上游 SSE 解析
- 负责维护最终 `Response` 的内存聚合状态
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
5. 在内存中维护最终 `Response` 所需的聚合状态，而不是缓存全部原始 SSE 文本
6. 在 `response.completed` 时抽取完整 `response`
7. 定义 `response.failed` / `error` / 异常 EOF 的失败映射
8. 监听下游断连并及时取消上游同步流
9. 对 `background=true` 和 `stream=true` 请求直接透传

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
