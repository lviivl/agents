import { createModels } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"

const DEFAULT_MODEL = "gpt-5"

export default class PiOpenAIService {
  constructor(name, options = {}) {
    const credentials = options.credentials || {}
    const config = { ...credentials, ...options }
    const modelName = config.model || config.modelName || process.env.OPENAI_MODEL || DEFAULT_MODEL
    const models = createModels()
    models.setProvider(openaiProvider())

    const catalogModel = models.getModel("openai", modelName)
    if (!catalogModel) throw new Error(`Pi does not know model "openai/${modelName}"`)

    const baseUrl =
      config.openaiApiUrl || config.baseUrl || config.apiUrl || config.url || haiBaseUrl()
    const model = { ...catalogModel, api: "openai-completions" }
    if (baseUrl) model.baseUrl = baseUrl
    if (config.headers) model.headers = { ...model.headers, ...config.headers }

    const apiKey =
      config.apiKey ||
      config.openaiApiKey ||
      process.env.OPENAI_API_KEY ||
      process.env.HAI_PROXY_API_KEY ||
      process.env.ANTHROPIC_AUTH_TOKEN
    this.name = name
    this.options = config
    this.model = model
    this.streamFn = (m, ctx, opts) => models.streamSimple(m, ctx, { apiKey, ...opts })
    this.getApiKey = async () => apiKey
  }
}

function haiBaseUrl() {
  if (!process.env.HAI_PROXY_URL && !process.env.HAI_PROXY_API_KEY) return undefined
  return `${process.env.HAI_PROXY_URL || "http://localhost:6655"}/openai/v1`
}
