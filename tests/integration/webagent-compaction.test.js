import { describe, expect, it, vi } from "vitest"
import { AgentHarness } from "@sap/webagent"

describe("WebAgent compaction transport", () => {
  it("uses the injected completion transport when compaction is forced", async () => {
    const complete = vi.fn().mockResolvedValue({
      role: "assistant",
      content: [{ type: "text", text: "summary" }],
      stopReason: "stop",
    })
    const harness = new AgentHarness({
      model: {
        id: "test",
        name: "test",
        api: "sap-aicore",
        provider: "sap-aicore",
        baseUrl: "",
        reasoning: false,
        input: ["text"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: 100,
        maxTokens: 1024,
      },
      transport: { stream: vi.fn(), complete },
      compactionSettings: { enabled: true, reserveTokens: 50, keepRecentTokens: 10 },
    })

    await harness.compactionTransformer.transform([
      { role: "user", content: "a".repeat(800), timestamp: 1 },
      { role: "user", content: "u-1", timestamp: 2 },
      { role: "user", content: "u-2", timestamp: 3 },
    ])

    expect(complete).toHaveBeenCalledOnce()
  })
})
