# Models through AI Gateway

Use the exact `provider/model` ID returned by
[`GET /v1/models`](https://ai-gateway.vercel.sh/v1/models). Catalog presence
describes availability and capabilities; a working agent also needs to call a
tool and consume its result through its harness's protocol.

The shared catalog reader preserves context limits, output limits, input
modalities and prices for interactive text-language models. Embedding,
media-generation and batch IDs are not added as interactive chat models.
Gateway's prices are per token; model registries use prices per million tokens.

For harness-specific configuration, see
[Codex](../packages/harness-codex/README.md) and
[OpenCode](../packages/harness-opencode/README.md).

Provider settings also matter. Some reasoning models reject requests that
disable thinking; choose a setting supported by the routed provider. A
provider-policy 403 is an access restriction, and a tool-result request failing
with a provider's message-template error needs investigation at that
protocol/provider boundary. Neither error is fixed by adding an ID to a catalog.
