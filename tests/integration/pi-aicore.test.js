import { beforeEach, describe, expect, it, vi } from "vitest"

const sdk = vi.hoisted(() => ({ instances: [], response: undefined }))

vi.mock("@sap-ai-sdk/orchestration", () => ({
  OrchestrationClient: class {
    constructor(...args) {
      this.args = args
      sdk.instances.push(this)
    }

    async stream(request, signal, options) {
      this.request = request
      this.signal = signal
      this.streamOptions = options
      return sdk.response
    }
  },
}))

import PiAICoreService from "../../lib/models/pi-aicore.js"

beforeEach(() => {
  sdk.instances.length = 0
  sdk.response = undefined
})

describe("Pi AI Core model", () => {
  it("streams a Pi tool call through the SAP AI SDK", async () => {
    const chunks = [
      {
        _data: { request_id: "request-1" },
        getDeltaContent: () => "",
        getDeltaReasoningContent: () => undefined,
        getDeltaToolCalls: () => [
          { index: 0, id: "call-1", function: { name: "query_books", arguments: '{"top":' } },
        ],
        getFinishReason: () => undefined,
        getTokenUsage: () => undefined,
      },
      {
        _data: {},
        getDeltaContent: () => "",
        getDeltaReasoningContent: () => undefined,
        getDeltaToolCalls: () => [{ index: 0, function: { arguments: "3}" } }],
        getFinishReason: () => "tool_calls",
        getTokenUsage: () => undefined,
      },
    ]
    sdk.response = {
      stream: chunks,
      getRequestId: () => "request-1",
      getFinishReason: () => "tool_calls",
      getTokenUsage: () => ({ prompt_tokens: 20, completion_tokens: 5, total_tokens: 25 }),
    }

    const llm = new PiAICoreService("llm", {
      model: "anthropic--claude-4.6-sonnet",
      contentFilter: false,
    })
    const context = {
      systemPrompt: "Use tools",
      tools: [
        {
          name: "query_books",
          description: "Query books",
          parameters: { type: "object", properties: { top: { type: "number" } } },
        },
      ],
      messages: [{ role: "user", content: [{ type: "text", text: "List books" }], timestamp: 1 }],
    }

    const result = await llm.completeFn(llm.model, context, {})

    expect(result).toMatchObject({
      stopReason: "toolUse",
      responseId: "request-1",
      usage: { input: 20, output: 5, totalTokens: 25 },
      content: [{ type: "toolCall", id: "call-1", name: "query_books", arguments: { top: 3 } }],
    })
    expect(sdk.instances[0].request).toEqual({
      messages: [
        { role: "system", content: "Use tools" },
        { role: "user", content: "List books" },
      ],
    })
    expect(sdk.instances[0].args[0].promptTemplating.prompt.tools[0]).toMatchObject({
      type: "function",
      function: { name: "query_books" },
    })
  })

  it("replays assistant tool calls and tool results", async () => {
    sdk.response = {
      stream: [
        {
          _data: {},
          getDeltaContent: () => "done",
          getDeltaReasoningContent: () => undefined,
          getDeltaToolCalls: () => undefined,
          getFinishReason: () => "stop",
          getTokenUsage: () => undefined,
        },
      ],
      getRequestId: () => undefined,
      getFinishReason: () => "stop",
      getTokenUsage: () => undefined,
    }
    const llm = new PiAICoreService("llm", {
      model: "anthropic--claude-4.6-sonnet",
      contentFilter: false,
    })

    await llm.completeFn(
      llm.model,
      {
        messages: [
          {
            role: "assistant",
            content: [
              { type: "text", text: "Checking" },
              { type: "toolCall", id: "call-1", name: "query_books", arguments: { top: 3 } },
            ],
          },
          {
            role: "toolResult",
            toolCallId: "call-1",
            toolName: "query_books",
            content: [{ type: "text", text: "Three books" }],
          },
        ],
      },
      {},
    )

    expect(sdk.instances[0].request.messages).toEqual([
      {
        role: "assistant",
        content: "Checking",
        tool_calls: [
          {
            id: "call-1",
            type: "function",
            function: { name: "query_books", arguments: '{"top":3}' },
          },
        ],
      },
      { role: "tool", tool_call_id: "call-1", content: "Three books" },
    ])
  })

  it("omits temperature for GPT-5 models", async () => {
    sdk.response = {
      stream: [],
      getRequestId: () => undefined,
      getFinishReason: () => "stop",
      getTokenUsage: () => undefined,
    }
    const llm = new PiAICoreService("llm", { model: "gpt-5.6", contentFilter: false })

    await llm.completeFn(llm.model, { messages: [] }, { temperature: 0.7 })

    expect(sdk.instances[0].args[0].promptTemplating.model.params).not.toHaveProperty("temperature")
  })
})
