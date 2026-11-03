# Pi models through AI Gateway

`durable-harness/pi` first uses pi-ai's bundled Gateway registry. For an ID
that registry does not know, it refreshes from Gateway before resolving the
model. Existing bundled entries retain pi-ai's compatibility settings. A
failed discovery request is reported as the cause of a missing-model error;
known bundled models do not require discovery.

`durable-harness/pi-durable` keeps the synchronous `gatewayModels()` API.
Refresh its returned collection before opening a Harness when using newer
IDs, and check the refresh result:

```ts
import { GATEWAY_PROVIDER, gatewayModels } from 'durable-harness/pi-durable';

const models = gatewayModels();
const refreshed = await models.refresh({ providers: [GATEWAY_PROVIDER] });
const error = refreshed.errors.get(GATEWAY_PROVIDER);
if (error) throw error;
```

The refresh adds interactive language models, including their context limits,
output limits, input modalities and prices. It preserves bundled compatibility
settings and keeps the previous catalog if discovery fails. Offline refresh
(`allowNetwork: false`) makes no request. Image-generation, embedding and batch
IDs are not added as interactive chat models.

Some reasoning models reject requests that disable thinking. For the kernel
Pi harness, choose a supported `thinkingLevel` instead of its default `'off'`
when required by the routed provider. Catalog discovery does not resolve a
provider's reasoning or message-template constraints.
