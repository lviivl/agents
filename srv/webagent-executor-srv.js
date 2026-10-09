import cds from "@sap/cds"

import { AgentHarness } from "@sap/webagent"
import { partsToText } from "../lib/utils/message-handling.js"
import { publishedStatus, toPiTools } from "./pi-executor-srv.js"

const LOG = cds.log("agents")

function toRuntimeContext(context) {
  let systemPrompt = context.systemPrompt || ""
  const tools = new Map((context.tools || []).map((tool) => [tool.name, tool]))
  const messages = []

  for (const message of context.messages) {
    if (message.role !== "system") {
      messages.push(message)
      continue
    }
    if (typeof message.content === "string" && message.content) {
      systemPrompt += systemPrompt ? `\n\n${message.content}` : message.content
    }
    for (const tool of message.toolsAdded || []) tools.set(tool.name, tool)
    for (const tool of message.toolsRemoved || []) tools.delete(tool.name)
  }

  return { systemPrompt, tools: [...tools.values()], messages }
}

/**
 * A2A executor backed by @sap/webagent AgentHarness.
 * Reuses the same CDS tool and model contracts as PiExecutor but delegates
 * the agent loop (including retry and context-compaction) to AgentHarness.
 */
export default class WebAgentExecutor {
  static _instance

  _sessions = new Map()
  _running = new Map()

  static for(srv, options = {}) {
    this._instance ??= new WebAgentExecutor()
    return this._instance.for(srv, options)
  }

  for(srv, options = {}) {
    return {
      execute: (requestContext, eventBus) => this.execute(srv, options, requestContext, eventBus),
      cancelTask: (taskId, eventBus) => this.cancelTask(taskId, eventBus),
      abort: (taskId) => this.abort(taskId),
    }
  }

  _sessionKey(srv, contextId) {
    return `${cds.context?.tenant || "anonymous"}:${srv.name}:${contextId}`
  }

  async _createHarness(srv, streamed, taskId, contextId, eventBus) {
    const [runtimeModel, tools, systemPrompt] = await Promise.all([
      srv.send("buildModel"),
      srv.send("buildTools"),
      srv.send("buildSystemPrompt"),
    ])

    if (!runtimeModel?.model || typeof runtimeModel.streamFn !== "function") {
      throw new Error(
        "WebAgent executor requires a Pi-compatible model; configure a model kind such as pi-anthropic",
      )
    }

    const authToken = runtimeModel.getApiKey
      ? await runtimeModel.getApiKey()
      : runtimeModel.authToken
    const runtimeStream = (model, context, options) =>
      runtimeModel.streamFn(model, toRuntimeContext(context), options)
    const runtimeComplete = (model, context, options) =>
      runtimeModel.completeFn
        ? runtimeModel.completeFn(model, toRuntimeContext(context), options)
        : runtimeStream(model, context, options).result()
    const harness = new AgentHarness({
      model: runtimeModel.model,
      authToken,
      systemPrompt,
      transport: { stream: runtimeStream, complete: runtimeComplete },
      tools: toPiTools(tools),
      onEvent(event) {
        const update = event?.assistantMessageEvent
        if (event?.type !== "message_update" || update?.type !== "text_delta" || !update.delta)
          return
        eventBus.publish({
          kind: "artifact-update",
          taskId,
          contextId,
          append: streamed.value,
          lastChunk: false,
          artifact: { artifactId: "response", parts: [{ kind: "text", text: update.delta }] },
        })
        streamed.value = true
      },
    })

    return harness
  }

  async execute(srv, options, requestContext, eventBus) {
    const { taskId, contextId } = requestContext
    const controller = new AbortController()
    const sessionKey = this._sessionKey(srv, contextId)
    // Mutable ref so the onEvent closure and execute body share the same streamed flag.
    const streamed = { value: false }

    if (cds.context) {
      cds.context["agent.task.id"] = taskId
      cds.context["agent.context.id"] = contextId
      cds.context["agent.service"] = srv.name
      cds.context["agent.eventBus"] = eventBus
    }

    this._running.set(taskId, { controller })
    if (!requestContext.task) {
      eventBus.publish({
        kind: "task",
        id: taskId,
        contextId,
        status: { state: "submitted", timestamp: new Date().toISOString() },
      })
    }
    eventBus.publish(publishedStatus(taskId, contextId, "working", undefined, false))

    try {
      let harness = this._sessions.get(sessionKey)
      if (!harness) {
        harness = await this._createHarness(srv, streamed, taskId, contextId, eventBus)
        this._sessions.set(sessionKey, harness)
      }
      this._running.set(taskId, { controller, harness })

      const prompt = partsToText(requestContext.userMessage?.parts)
      const output = await harness.prompt(prompt)

      if (controller.signal.aborted)
        throw new globalThis.DOMException("Task canceled", "AbortError")
      if (harness.state?.errorMessage) throw new Error(harness.state.errorMessage)

      eventBus.publish({
        kind: "artifact-update",
        taskId,
        contextId,
        append: false,
        lastChunk: true,
        artifact: { artifactId: "response", parts: [{ kind: "text", text: output }] },
      })
      eventBus.publish(publishedStatus(taskId, contextId, "completed", output, true))
    } catch (error) {
      const canceled = controller.signal.aborted || error?.name === "AbortError"
      const state = canceled ? "canceled" : "failed"
      const production = process.env.NODE_ENV === "production" || process.env.CDS_ENV === "prod"
      const text = canceled
        ? "Task canceled."
        : production && error?.$sanitize !== false
          ? cds.i18n.messages.at(500) || "Internal Server Error"
          : `Agent error: ${error.message}`
      if (!canceled)
        LOG.error("WebAgent executor failed", { service: srv.name, error: error.message })
      eventBus.publish(publishedStatus(taskId, contextId, state, text, true))
    } finally {
      this._running.delete(taskId)
      eventBus.finished()
    }
  }

  abort(taskId) {
    const running = this._running.get(taskId)
    if (!running) return
    running.controller.abort()
    running.harness?.agent?.abort?.()
  }

  async cancelTask(taskId, eventBus) {
    if (this._running.has(taskId)) return this.abort(taskId)
    eventBus.publish(publishedStatus(taskId, undefined, "canceled", "Task canceled.", true))
    eventBus.finished()
  }
}

export { WebAgentExecutor }
