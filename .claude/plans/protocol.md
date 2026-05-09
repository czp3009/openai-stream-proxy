# Responses API 协议报告（下游非流，上游流）

校对日期：2026-05-10

## 范围

本文只讨论本项目计划支持的唯一转换方向：

- 下游请求：`POST /v1/responses`
- 下游请求体：`stream` 缺失或为 `false`
- 上游请求体：代理改写为 `stream: true`
- 下游响应：仍然返回普通非流 JSON

当前仓库中的 `proxy`/`cli` 代码仍是占位骨架；本文描述的是协议目标，而不是当前已经完成的功能状态。

本文不讨论、也不设计本项目范围之外的其他转换方向。

本文也明确规定，下列请求不在“非流转流”转换范围内，应直接透传：

- 下游已是 `stream: true`
- 下游是 `background: true`
- `POST /v1/responses` 之外的所有接口

## 核心结论

- `Responses API` 的流控开关是请求体里的 `stream: true`。
- `Responses API` 的流是基于 SSE 的语义化事件流，不是 Chat Completions 风格的 data-only chunk。
- 对同步 create 请求，`下游非流 -> 上游流 -> 下游仍返回非流 JSON` 这条路径在协议上是可行的，因为上游流式终态会给出最终
  `response`。
- 这条路径本质上是“丢弃中间事件，保留终态对象”，不是“发明新协议”。
- `background=true` 必须单独处理。它不是普通同步请求的一个小修饰，而是另一种资源生命周期。

## 官方文档明确给出的事实

- `POST /v1/responses` 支持 `stream: true`，会返回 SSE 事件流。
- Streaming guide 明确把 `Responses API` 描述为 semantic events，而不是单纯按字节块消费。
- 文档展示的常见流式事件至少包括：
    - `response.created`
    - `response.in_progress`
    - `response.failed`
    - `response.completed`
    - `response.output_item.added`
    - `response.output_item.done`
    - `response.content_part.added`
    - `response.content_part.done`
    - `response.output_text.delta`
    - `response.output_text.annotation.added`
    - `response.output_text.done`
    - `response.refusal.delta`
    - `response.refusal.done`
    - `response.function_call_arguments.delta`
    - `response.function_call_arguments.done`
    - `response.file_search_call.in_progress`
    - `response.file_search_call.searching`
    - `response.file_search_call.completed`
    - `response.code_interpreter_call.in_progress`
    - `response.code_interpreter_call.code.delta`
    - `response.code_interpreter_call.code.done`
    - `response.code_interpreter_call.interpreting`
    - `response.code_interpreter_call.completed`
    - `error`
- Create 接口的流式示例明确展示了典型顺序：
    - `response.created`
    - `response.in_progress`
    - `response.output_item.added`
    - `response.content_part.added`
    - 多个 `response.output_text.delta`
    - `response.output_text.done`
    - `response.content_part.done`
    - `response.output_item.done`
    - `response.completed`
- 该示例里的 `response.completed` 事件直接带完整 `response` 对象。
- Retrieve 文档给出的 `Response.status` 取值至少包括：
    - `completed`
    - `failed`
    - `in_progress`
    - `cancelled`
    - `queued`
    - `incomplete`
- 顶层聚合字段 `response.output_text` 是 SDK-only convenience property。
- 但 `output[].content[].type == "output_text"` 是原始 Response 协议中的真实内容项。

## `background` 的协议含义

`background: true` 是 `Responses create` 的官方模式，不依赖 `stream`。

文档明确说明：

- `background=true` 时，请求在后台异步执行。
- 调用方通过 `GET /v1/responses/{response_id}` 轮询后台任务状态。
- 后台任务可以通过 `POST /v1/responses/{response_id}/cancel` 取消。
- 只有在创建时同时使用 `background=true` 和 `stream=true` 时，才有官方文档化的断点续流能力。
- 续流接口是 `GET /v1/responses/{response_id}?stream=true&starting_after=...`。
- 后台续流依赖 `sequence_number`。
- 后台模式要求 `store=true`。
- 后台模式会存储响应数据大约 10 分钟，因此不兼容 ZDR。

这意味着：

- `background=true` 不是普通同步请求的“返回格式选项”。
- 它会改变请求的生命周期、取消方式、恢复方式和数据保留语义。

## 为什么 `background=true` 不能进入本项目的转换路径

本项目只实现：

- 下游同步非流 create
- 上游同步流 create
- 最终再还原为下游同步非流 JSON

但 `background=true` 的下游非流 create 语义不是“等最终结果再返回”，而是：

- 先创建后台任务
- 立刻返回一个后台 response 资源
- 后续由调用方自己 retrieve / cancel / resume

因此，如果代理把 `background=true` 的请求也改成上游 `stream:true`，再一直等到 `response.completed` 才回下游，就会把一个后台异步
create 错误地改成同步等待。

此外，若代理擅自把普通同步请求改成 `background=true`，也会改变：

- 取消方式
- 是否可恢复流
- 是否要求 `store=true`
- 是否短时存储响应
- 是否兼容 ZDR

所以本项目的规则应当是：

- 若入站请求 `background=true`，直接透传，不做非流转流

## 本项目的推荐边界

### 进入转换路径的请求

必须同时满足：

- 方法是 `POST`
- 路径是 `/v1/responses`
- 请求体是可解析 JSON
- `stream` 缺失或为 `false`
- `background` 缺失或为 `false`

### 不进入转换路径的请求

以下任一条件成立，都直接透传：

- 不是 `POST /v1/responses`
- 入站已是 `stream: true`
- 入站是 `background: true`
- 代理无法安全解析请求体

## 转换路径的协议步骤

1. 解析下游请求 JSON。
2. 仅把 `stream` 改成 `true`。
3. 其余业务字段原样保留，包括未来新增字段。
4. 不要偷偷改这些语义字段：
    - `background`
    - `store`
    - `include`
    - `metadata`
    - `service_tier`
    - `stream_options.include_obfuscation`
5. 转发到上游时，不要保留原始 `Content-Length`。
6. 上游若直接返回 HTTP 4xx/5xx，则原样透传给下游。
7. 上游若进入流式响应，则按 SSE 协议解析事件。
8. 记录必要状态：
    - 最新 `response` 快照
    - `response_id`
    - 终态事件
9. 收到 `response.completed` 时，直接取其中完整 `response`，作为下游普通 JSON 返回。
10. 收到 `response.failed` 或 `error` 时，进入代理定义的失败映射逻辑。

## 下游返回策略

本项目应尽量模仿 OpenAI 官方非流端点的行为：

- 先得到完整结果
- 再返回一个普通 HTTP JSON 响应

因此，对转换路径最稳的实现原则是：

- 不要把上游 SSE 中途收到的部分内容立刻按普通 JSON chunk 提前写给下游
- 应在代理内部先聚合出最终 `Response` 状态
- 只有在拿到明确终态后，才一次性向下游返回普通 JSON

这里的“在内存里聚合”指的是：

- 维护最终 `Response` 所需的结构化状态
- 而不是缓存全部原始 SSE 文本

推荐保留的内存状态包括：

- 最新 `response` 快照
- `response_id`
- output items 的最终结构
- function call arguments 的聚合结果
- final status
- usage
- 其他最终 JSON 所需字段

不推荐的做法是：

- 一边收到上游事件，一边向下游写半截 JSON

原因：

- 下游拿到的会是未完成 JSON
- 上游若中途 `response.failed`、`error` 或异常 EOF，代理无法回滚已写出的响应体
- 工具调用、并行 output item、注解等复杂事件会显著提高“边写边拼 JSON”的脆弱性

所以，本项目 v1 应采用：

- 先在内存中聚合最终 `Response` 状态
- 再一次性回下游

## `response.failed` / `error` 的定位

官方文档已经明确：

- `response.failed` 是流式生命周期事件的一部分
- `error` 也是 streaming 里的错误路径

但文档没有直接规定：

- 一个“上游流、下游非流”的反向代理在这些情况下必须返回哪个 HTTP 状态码

因此，这一部分应当被视为代理策略，而不是 OpenAI 官方已经定义好的 wire 行为。

稳妥要求只有两个：

- 不要把 `response.failed` 和上游 HTTP 4xx/5xx 混为一谈
- 要能把失败状态向下游表达出来

## SSE 解析层必须遵守的规则

- 按 UTF-8 增量解码，不要按裸字节块直接切字符串。
- 每条 SSE 消息由空行分隔。
- `event:` 是事件名。
- `data:` 是事件负载。
- 同一消息里的多行 `data:` 要按换行拼接。
- 以 `:` 开头的行是注释，必须忽略。
- 不能假设一条底层 read chunk 就对应一个完整 SSE 消息。

## 同步取消

官方文档对同步 response 的取消方式说得很直接：

- 要取消同步 response，应终止连接。

这对本项目的代理实现意味着：

- 本项目内部虽然把下游非流请求转成了上游流请求
- 但它语义上仍然是在代理一个同步 create
- 所以下游若提前断开连接，代理必须立刻中止上游这条同步流请求

否则会出现两个问题：

- 取消语义和官方同步请求不一致
- 下游已经不要结果，但上游还在继续生成

## 这条转换路径仍然剩下的实现问题

在排除“模型是否支持 streaming”之后，本项目的剩余问题主要是实现问题，而不是结构性协议冲突：

- 正确解析和改写请求体
- 正确实现 SSE 解析
- 正确维护最终 `Response` 的内存聚合状态
- 明确 `response.failed` / `error` / 异常 EOF 的失败映射
- 在下游断连时及时中止上游同步流
- 允许未知事件类型安全跳过，避免未来事件扩展导致代理失效

## 不应做的事

- 不要实现本项目范围之外的其他转换方向
- 不要为恢复能力擅自把普通请求改成 `background=true`
- 不要伪造顶层聚合 `response.output_text`
- 不要发明 `[DONE]` 或 Chat Completions 风格 delta
- 不要因为看到 `output_text.delta` 就自己拼一个“自创最终协议”

## 实现建议

- 本项目只对 `POST /v1/responses` 做协议转换。
- 转换只发生在：
    - 下游非流
    - 下游非后台
    - 同步 create
- 对 `background=true`，直接透传。
- 对 `stream=true`，直接透传。
- 对转换路径，先在内存中聚合最终 `Response` 状态，读到明确终态后再一次性向下游回普通 JSON。
- 其余 OpenAI 接口一律做字节透传。

## 参考文档

- OpenAI Responses create:
    - https://developers.openai.com/api/reference/resources/responses/methods/create
- OpenAI Responses retrieve:
    - https://developers.openai.com/api/reference/resources/responses/methods/retrieve
- OpenAI Responses cancel:
    - https://developers.openai.com/api/reference/resources/responses/methods/cancel
- OpenAI Streaming guide for Responses:
    - https://developers.openai.com/api/docs/guides/streaming-responses?api-mode=responses
- OpenAI Background mode guide:
    - https://developers.openai.com/api/docs/guides/background
- SSE event stream format:
    - https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
