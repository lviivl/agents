import { describe, expect, it, vi } from "vitest"

const { harnessConfigs } = vi.hoisted(() => ({ harnessConfigs: [] }))

vi.mock("@sap/webagent", () => ({
  AgentHarness: class {
    constructor(config) {
      harnessConfigs.push(config)
      this.agent = { state: {} }
      this.state = {}
    }

    async prompt() {
      return "WebAgent mock response"
    }
  },
}))

import WebAgentExecutor from "../../srv/webagent-executor-srv.js"

describe("WebAgent executor", () => {
  it("passes the model runtime transport to the harness", async () => {
    const result = { stopReason: "end_turn", content: [] }
    const stream = { result: vi.fn().mockResolvedValue(result) }
    const streamFn = vi.fn(() => stream)
    const completeFn = vi.fn().mockResolvedValue(result)
    const system = { role: "system", content: "Be helpful", timestamp: 0 }
    const user = { role: "user", content: [{ type: "text", text: "hello" }], timestamp: 1 }
    const srv = {
      name: "WebAgentTestService",
      send: vi.fn(async (event) =>
        event === "buildModel"
          ? { model: { id: "claude-test" }, streamFn, completeFn }
          : event === "buildTools"
            ? []
            : "Be helpful",
      ),
    }
    const events = []
    const eventBus = { publish: (event) => events.push(event), finished: vi.fn() }

    await new WebAgentExecutor().execute(
      srv,
      {},
      {
        taskId: "webagent-task",
        contextId: "webagent-context",
        userMessage: { parts: [{ kind: "text", text: "hello" }] },
      },
      eventBus,
    )

    const [{ transport }] = harnessConfigs
    expect(transport.stream).toBeTypeOf("function")
    expect(harnessConfigs[0].compactionSettings).toBeUndefined()
    await expect(
      transport.complete({ id: "claude-test" }, { messages: [system, user] }, {}),
    ).resolves.toBe(result)
    expect(completeFn).toHaveBeenCalledWith(
      { id: "claude-test" },
      { systemPrompt: "Be helpful", tools: [], messages: [user] },
      {},
    )
    expect(streamFn).not.toHaveBeenCalled()
    expect(events.at(-1)).toMatchObject({ status: { state: "completed" }, final: true })
    expect(eventBus.finished).toHaveBeenCalledOnce()
  })
})
