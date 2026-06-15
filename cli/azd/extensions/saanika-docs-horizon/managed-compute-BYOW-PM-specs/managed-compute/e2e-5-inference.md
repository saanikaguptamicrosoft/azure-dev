# Inference

All deployments — serverless and managed compute — share the unified Foundry project endpoint. Unlike legacy Hub-based MaaP where every deployment had its own endpoint (creating complexity in auth, keys, and managed identity setup), accelerator deployments inherit the same auth and networking configuration as the Foundry project endpoint.

### Get endpoint and keys

```python
account_info = cog.accounts.get(resource_group_name=rg, account_name=account)
endpoint = account_info.properties.endpoint

keys = cog.accounts.list_keys(resource_group_name=rg, account_name=account)
api_key = keys.key1
```

```bash
ENDPOINT=$(az cognitiveservices account show --name $ACCOUNT -g $RG \
  --query "properties.endpoint" -o tsv)

API_KEY=$(az cognitiveservices account keys list --name $ACCOUNT -g $RG \
  --query "key1" -o tsv)
```

### Routing: two paths for managed compute

OSS models are a breadth play — they come with a variety of container runtimes and API schemas. This makes it impractical for the APIM layer to be "schema-aware" and parse incoming payloads (like it does for OpenAI/ADM models). For managed compute deployments, APIM handles auth and private networking only — the payload is forwarded to the backend compute endpoint without processing.

This means managed compute models use a **dedicated route** that doesn't overlap with OpenAI or ADM models:

```
<endpoint>/managed-deployments/<deployment-name>/<model-specific-path>
```

The deployment name is in the route (not a query parameter) because all clients take a base path as input and append the relative path — this gives the best UX across SDKs and tools.

**Additionally**, models offering OpenAI-compatible runtimes (e.g., vLLM serving `/chat/completions`) also work on the standard OpenAI route:

```
<endpoint>/openai/deployments/<deployment-name>/chat/completions
```

When APIM detects that the deployment name corresponds to a managed compute deployment, it forks and routes traffic accordingly (similar to how Nexus handles non-OpenAI ADM models today). If a user invokes the OpenAI route with a deployment whose runtime does *not* support it, the runtime returns a 404.

| Route | Works for | Schema awareness |
|---|---|---|
| `/managed-deployments/<name>/<path>` | **All** managed compute models | None — passthrough |
| `/openai/deployments/<name>/chat/completions` | Only models with OpenAI-compatible runtimes | Full — APIM parses payload |

### Case 1: OpenAI-compatible models (`gpt-oss-120b`)

Models like `gpt-oss-120b` use vLLM with `/chat/completions` — they work on **both routes**. The OpenAI SDK, AI Projects SDK, and curl all work identically for serverless and managed compute deployments.

**SDK (OpenAI — `openai`):**
```python
from openai import AzureOpenAI

client = AzureOpenAI(
    azure_endpoint=endpoint,
    api_key=api_key,
    api_version="2025-09-01",
)

prompt = [{"role": "user", "content": "Explain quantum computing in one paragraph."}]

# Serverless — pay per token, shared infrastructure
response_sl = client.chat.completions.create(
    model="gpt-oss-120b-serverless",
    messages=prompt,
    max_tokens=200,
)

# Managed Compute — dedicated GPUs, same API
response_mc = client.chat.completions.create(
    model="gpt-oss-120b-gpu",
    messages=prompt,
    max_tokens=200,
)

# Both return the same response format — same model, same answers
print("Serverless:", response_sl.choices[0].message.content)
print("GPU:       ", response_mc.choices[0].message.content)
```

**SDK (Azure AI Projects — `azure.ai.projects`):**

The Foundry portal surfaces the project endpoint and API key directly (see screenshot). The AI Projects SDK wraps `AzureOpenAI` and handles endpoint/key resolution automatically — no need to fetch them via ARM.

```python
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential

project_client = AIProjectClient(
    endpoint="https://aashishb-7191-resource.services.ai.azure.com",
    credential=DefaultAzureCredential(),
)

with project_client.get_openai_client() as openai_client:
    # Serverless
    response = openai_client.responses.create(
        model="gpt-oss-120b-serverless",
        input="Explain quantum computing in one paragraph.",
    )
    print(f"Serverless: {response.output_text}")

    # Managed Compute — same client, different deployment name
    response = openai_client.responses.create(
        model="gpt-oss-120b-gpu",
        input="Explain quantum computing in one paragraph.",
    )
    print(f"GPU:        {response.output_text}")
```

> The `AIProjectClient.get_openai_client()` returns an `AzureOpenAI` instance pre-configured with the project's endpoint and credential. It uses the Responses API (`responses.create`) rather than Chat Completions — both work against the same deployments.

**CLI (using curl) — OpenAI route:**
```bash
# Serverless (OpenAI route — same as always)
curl -s "${ENDPOINT}/openai/deployments/gpt-oss-120b-serverless/chat/completions?api-version=2025-09-01" \
  -H "api-key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Hello"}],"max_tokens":100}'

# Managed Compute — same OpenAI route, different deployment name
curl -s "${ENDPOINT}/openai/deployments/gpt-oss-120b-gpu/chat/completions?api-version=2025-09-01" \
  -H "api-key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Hello"}],"max_tokens":100}'
```

**CLI (using curl) — managed-deployments route:**
```bash
# Same managed compute deployment, but using the passthrough route
# (works because vLLM exposes /v1/chat/completions)
curl -s "${ENDPOINT}/managed-deployments/gpt-oss-120b-gpu/v1/chat/completions" \
  -H "api-key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Hello"}],"max_tokens":100}'
```

Same model, same prompt, same response schema. The difference is invisible to the inference caller — they don't know or care whether their request was served by shared serverless infrastructure or a dedicated H100 node.

### Case 2: Non-OpenAI-compatible models (e.g., Cohere Rerank)

Models with bespoke API schemas — like Cohere Rerank — do not support `/chat/completions`. These models **only work on the managed-deployments route**. The payload is model-specific; APIM passes it through without parsing.

**The route suffix problem:** The full inference URL is `<endpoint>/managed-deployments/<deployment-name>/<route-suffix>`. For OpenAI-compatible models the suffix is well-known (`/v1/chat/completions`), but for bespoke models the user must know what path the container runtime exposes — e.g., `/v1/rerank` for Cohere Rerank, `/v1/embeddings` for an embedding model, etc. Today there is no programmatic way to discover this.

**Proposal:** Publish the route suffix as model metadata — either as a tag (e.g., `inference_route_suffix: /v1/rerank`) or a property on the deployment template. This way the Catalog API, ML Registry model, or deployment template surfaces the correct suffix at discovery time, and SDKs/CLI can construct the full URL automatically. Without this, the user is left reading model documentation to find the right path.

Suppose `cohere-rerank-v3` is deployed as a managed compute accelerator deployment named `cohere-rerank-gpu`. The deployment template would include:

```json
{
  "scoringPath": "/v1/rerank",
  "scoringPort": 8000
}
```

> `scoringPath` already exists in the deployment template spec (see Section 2.3 of [Get Model Details](e2e-2-get-model.md)). For OpenAI-compatible models it's `/v1/chat/completions`; for Cohere Rerank it's `/v1/rerank`. The user (or SDK) reads this from the template to construct the inference URL: `<endpoint>/managed-deployments/<deployment-name><scoringPath>`.

**CLI (using curl):**
```bash
# Rerank — model-specific schema, passthrough route only
# Route suffix /v1/rerank comes from the deployment template's scoringPath
curl -s "${ENDPOINT}/managed-deployments/cohere-rerank-gpu/v1/rerank" \
  -H "api-key: $API_KEY" -H "Content-Type: application/json" \
  -d '{
    "model": "rerank-english-v3.0",
    "query": "What is the capital of France?",
    "documents": [
      "Paris is the capital of France.",
      "Berlin is the capital of Germany.",
      "Madrid is the capital of Spain."
    ],
    "top_n": 2
  }'
```

**Response:**
```json
{
  "results": [
    { "index": 0, "relevance_score": 0.998, "document": { "text": "Paris is the capital of France." } },
    { "index": 2, "relevance_score": 0.112, "document": { "text": "Madrid is the capital of Spain." } }
  ]
}
```

**SDK (model-specific — `requests` or model's own SDK):**
```python
import requests

response = requests.post(
    f"{endpoint}/managed-deployments/cohere-rerank-gpu/v1/rerank",
    headers={"api-key": api_key, "Content-Type": "application/json"},
    json={
        "model": "rerank-english-v3.0",
        "query": "What is the capital of France?",
        "documents": [
            "Paris is the capital of France.",
            "Berlin is the capital of Germany.",
            "Madrid is the capital of Spain.",
        ],
        "top_n": 2,
    },
)
for result in response.json()["results"]:
    print(f"  [{result['index']}] score={result['relevance_score']:.3f}: "
          f"{result['document']['text']}")
```

> The OpenAI SDK cannot be used here — Cohere Rerank has its own schema. The managed-deployments route is the only path. The key gap: the user must know the route suffix (`/v1/rerank`) and the request/response schema. The deployment template's `scoringPath` field (proposed in Section 2.3 of [Get Model Details](e2e-2-get-model.md)) closes the route discovery gap — the SDK or CLI can read it to construct the full URL. The payload schema remains the user's responsibility to know (from model docs or the publisher's SDK). This is the tradeoff of breadth: any model can be deployed, but not every model gets a unified SDK experience.
