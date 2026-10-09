import cds from "@sap/cds"
import { OrchestrationClient } from "@sap-ai-sdk/orchestration"
import { createAssistantMessageEventStream, parseStreamingJson } from "@earendil-works/pi-ai"

import { buildContentFilter, toSdkFilterFormat } from "./aicore.js"

const DEFAULT_CONTEXT_WINDOW = 200_000
const DEFAULT_MAX_TOKENS = 4096

export default class PiAICoreService {
  constructor(name, options = {}) {
    const config = { ...options.credentials, ...options }
    const modelName = config.model || config.modelName
    const maxTokens =
      config.params?.max_tokens || cds.env.agents?.params?.max_tokens || DEFAULT_MAX_TOKENS

    this.name = name
    this.options = config
    this.model = {
      id: modelName,
      name: modelName,
      api: "sap-aicore",
      provider: "sap-aicore",
      baseUrl: "",
      reasoning: false,
      input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: config.contextWindow || config.contextLength || DEFAULT_CONTEXT_WINDOW,
      maxTokens,
    }
    this.streamFn = (model, context, streamOptions) =>
      streamAICore(model, context, streamOptions, config)
    this.completeFn = async (model, context, streamOptions) =>
      this.streamFn(model, context, streamOptions).result()
  }
}

export function streamAICore(model, context, streamOptions = {}, config = {}) {
  const eventStream = createAssistantMessageEventStream()
  const output = {
    role: "assistant",
    content: [],
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage: emptyUsage(),
    stopReason: "stop",
    timestamp: Date.now(),
  }

  void (async () => {
    try {
      const client = createClient(model, context, streamOptions, config)
      const response = await client.stream(
        { messages: toAICoreMessages(context) },
        streamOptions.signal,
        { promptTemplating: { include_usage: true } },
      )
      eventStream.push({ type: "start", partial: output })

      const toolCalls = new Map()
      let textBlock
      let thinkingBlock

      for await (const chunk of response.stream) {
        output.responseId ||= chunk._data?.request_id

        const usage = chunk.getTokenUsage?.()
        if (usage) output.usage = toUsage(usage)

        const text = textDelta(chunk)
        if (text) {
          textBlock ||= startBlock(eventStream, output, { type: "text", text: "" }, "text_start")
          textBlock.text += text
          eventStream.push({
            type: "text_delta",
            contentIndex: output.content.indexOf(textBlock),
            delta: text,
            partial: output,
          })
        }

        const thinking = chunk.getDeltaReasoningContent?.()?.join("") || ""
        if (thinking) {
          thinkingBlock ||= startBlock(
            eventStream,
            output,
            { type: "thinking", thinking: "" },
            "thinking_start",
          )
          thinkingBlock.thinking += thinking
          eventStream.push({
            type: "thinking_delta",
            contentIndex: output.content.indexOf(thinkingBlock),
            delta: thinking,
            partial: output,
          })
        }

        for (const delta of chunk.getDeltaToolCalls?.() || []) {
          let block = toolCalls.get(delta.index)
          if (!block) {
            block = {
              type: "toolCall",
              id: delta.id || "",
              name: delta.function?.name || "",
              arguments: {},
              partialArgs: "",
            }
            toolCalls.set(delta.index, block)
            startBlock(eventStream, output, block, "toolcall_start")
          }
          block.id ||= delta.id || ""
          block.name ||= delta.function?.name || ""
          const argumentDelta = delta.function?.arguments || ""
          block.partialArgs += argumentDelta
          block.arguments = parseStreamingJson(block.partialArgs)
          eventStream.push({
            type: "toolcall_delta",
            contentIndex: output.content.indexOf(block),
            delta: argumentDelta,
            partial: output,
          })
        }

        const finishReason = chunk.getFinishReason?.()
        if (finishReason) output.stopReason = toStopReason(finishReason)
      }

      if (textBlock) {
        eventStream.push({
          type: "text_end",
          contentIndex: output.content.indexOf(textBlock),
          content: textBlock.text,
          partial: output,
        })
      }
      if (thinkingBlock) {
        eventStream.push({
          type: "thinking_end",
          contentIndex: output.content.indexOf(thinkingBlock),
          content: thinkingBlock.thinking,
          partial: output,
        })
      }
      for (const block of toolCalls.values()) {
        delete block.partialArgs
        eventStream.push({
          type: "toolcall_end",
          contentIndex: output.content.indexOf(block),
          toolCall: block,
          partial: output,
        })
      }

      output.responseId ||= response.getRequestId?.()
      output.usage = toUsage(response.getTokenUsage?.() || output.usage)
      output.stopReason = toStopReason(response.getFinishReason?.() || output.stopReason)
      eventStream.push({ type: "done", reason: output.stopReason, message: output })
      eventStream.end()
    } catch (error) {
      output.stopReason = streamOptions.signal?.aborted ? "aborted" : "error"
      output.errorMessage =
        error?.response?.data?.error?.message ||
        error?.response?.data?.message ||
        error?.cause?.response?.data?.error?.message ||
        error?.cause?.response?.data?.message ||
        error?.cause?.message ||
        (error instanceof Error ? error.message : String(error))
      eventStream.push({ type: "error", reason: output.stopReason, error: output })
      eventStream.end()
    }
  })()

  return eventStream
}

function createClient(model, context, streamOptions, config) {
  const gpt5 = /^gpt-5(?:[.-]|$)/.test(model.id)
  const params = {
    ...(cds.env.agents?.params || {}),
    ...(config.params || {}),
    ...(streamOptions.maxTokens && { max_tokens: streamOptions.maxTokens }),
    ...(!gpt5 &&
      streamOptions.temperature !== undefined && { temperature: streamOptions.temperature }),
  }
  if (gpt5) delete params.temperature
  const prompt = context.tools?.length
    ? {
        tools: context.tools.map((tool) => ({
          type: "function",
          function: {
            name: tool.name,
            description: tool.description,
            parameters: tool.parameters,
          },
        })),
      }
    : undefined
  const contentFilter = config.contentFilter === false ? undefined : buildContentFilter()
  const deployment = config.destinationName
    ? { resourceGroup: config.resourceGroup || "default" }
    : config.resourceGroup
      ? { resourceGroup: config.resourceGroup }
      : undefined
  const destination = config.destinationName
    ? { destinationName: config.destinationName }
    : undefined

  return new OrchestrationClient(
    {
      promptTemplating: { model: { name: model.id, params }, ...(prompt && { prompt }) },
      ...(contentFilter && { filtering: toSdkFilterFormat({ output: contentFilter.output }) }),
    },
    deployment,
    destination,
  )
}

function toAICoreMessages(context) {
  const messages = context.systemPrompt ? [{ role: "system", content: context.systemPrompt }] : []

  for (const message of context.messages) {
    if (message.role === "user") {
      messages.push({ role: "user", content: textContent(message.content) })
    } else if (message.role === "assistant") {
      const toolCalls = message.content
        .filter((block) => block.type === "toolCall")
        .map((block) => ({
          id: block.id,
          type: "function",
          function: { name: block.name, arguments: JSON.stringify(block.arguments) },
        }))
      messages.push({
        role: "assistant",
        content: message.content
          .filter((block) => block.type === "text")
          .map((block) => block.text)
          .join(""),
        ...(toolCalls.length && { tool_calls: toolCalls }),
      })
    } else if (message.role === "toolResult") {
      messages.push({
        role: "tool",
        tool_call_id: message.toolCallId,
        content: textContent(message.content),
      })
    }
  }

  return messages
}

function textContent(content) {
  if (typeof content === "string") return content
  return content
    .filter((block) => block.type === "text")
    .map((block) => block.text)
    .join("")
}

function textDelta(chunk) {
  const content = chunk.getDeltaContent?.()
  if (typeof content === "string") return content
  return Array.isArray(content)
    ? content
        .filter((block) => block?.type === "text")
        .map((block) => block.text || "")
        .join("")
    : ""
}

function startBlock(stream, output, block, type) {
  output.content.push(block)
  stream.push({ type, contentIndex: output.content.length - 1, partial: output })
  return block
}

function emptyUsage() {
  return {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  }
}

function toUsage(usage) {
  if ("input" in usage) return usage
  const input = usage.prompt_tokens || 0
  const output = usage.completion_tokens || 0
  return {
    input,
    output,
    cacheRead: usage.prompt_tokens_details?.cached_tokens || 0,
    cacheWrite: usage.prompt_tokens_details?.cache_creation_tokens || 0,
    reasoning: usage.completion_tokens_details?.reasoning_tokens,
    totalTokens: usage.total_tokens || input + output,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  }
}

function toStopReason(reason) {
  if (reason === "tool_calls" || reason === "tool_use" || reason === "toolUse") return "toolUse"
  if (reason === "length" || reason === "max_tokens") return "length"
  return reason === "error" || reason === "aborted" ? reason : "stop"
}
