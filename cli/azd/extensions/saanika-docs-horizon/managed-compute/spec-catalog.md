# Catalog and Discovery API Spec

This document covers model discovery and metadata retrieval in Azure AI Foundry. It compares the two primary API surfaces (**Catalog API** and **Cognitive Services Models API**), documents their gaps, and proposes design options for SDK/CLI coverage, model identity rationalization, and unified vs. separate surfaces for serverless and managed compute.

---

## Current State: Catalog API vs. Cognitive Services Models API

### Overview

| Dimension | Catalog API | Cognitive Services Models API |
|---|---|---|
| **Auth** | Anonymous (no login required) | ARM auth (subscription/account scoped) |
| **Scope** | Global — not scoped to tenant, subscription, or region | Subscription/account/location-scoped; response shape varies by query type |
| **SDK / CLI** | None — REST-only, undocumented | SDK: `azure-mgmt-cognitiveservices`. CLI: `az cognitiveservices` |
| **Model coverage** | All publisher models across all registries (10,000+) | Models available to the queried subscription/account (~345 today) |
| **Search & filter** | Rich server-side: free-text, publisher, task, offer, capabilities, sort | None — returns all models; client must filter |
| **Response consistency** | Stable schema across all models | Two response shapes: camelCase (location-scoped) vs. snake_case (account-scoped) |

### Discovery and Metadata Capabilities

| Capability | Catalog API | Models API | Alignment |
|---|---|---|---|
| Publisher | `summaries[].publisher` | Often `null` | **Broken** in Models API |
| Task taxonomy | `inferenceTasks[]` (semantic arrays) | `capabilities.*` (boolean-as-string flags) | **Misaligned** — different concepts, no shared vocabulary |
| Offer / SKU type | `azureOffers[]` (coarse: `standard-paygo`, `PTU`, `VM`) | `skus[].name` (ARM: `GlobalStandard`, `ProvisionedManaged`, etc.) | **Misaligned** — no mapping between vocabularies |
| Model limits | Structured `modelLimits` object with typed ints | String values in flat `capabilities` dict; absent for reasoning models | **Misaligned** — different schemas |
| SKU details (capacity, rate limits, meters) | None | Yes — per-SKU capacity contracts, rate limits, usage meters | Catalog has no SKU-level detail |
| Deprecation | `inferenceRetirementDate` only (89% null) | Full: dates, `lifecycleStatus`, `replacementConfig` with auto-upgrade | Models API is richer |
| Model card (description, license, evaluation) | Yes (Get API only) | None | Catalog is the only source |
| Region availability | None | None (requires separate Model Capacities API) | Neither |
| Pricing | None | None (Commerce/billing APIs only) | Neither |

### Model Capacities API Overlap

The Models API and Model Capacities API return overlapping info about the same models, but neither is a superset:

- **Models API**: SKU capacity contracts, rate limits, usage meters, deprecation. **No region availability.**
- **Model Capacities API**: Region × SKU × live available capacity across all locations. **No rate limits, meters, or deprecation.**

To answer "can I deploy gpt-4.1 as GlobalStandard in australiaeast, and what are the rate limits?" requires calling **both** APIs and joining on `(model, version, SKU)`. A unified API should return all of this in one response.

---

## Why the Models API Cannot Scale to Managed Compute

| Challenge | Detail |
|---|---|
| **Scale** | 345 model entries with 572 SKU rows today. At catalog scale: ~20,000 model×SKU rows (35×). Model Capacities: 247 records for one model today → 4.1M at full scale. |
| **Publishing** | Populated by manual config-file edits. Won't scale to dozens of independent publishers. ML Registries offer self-service publishing with automated indexing. |
| **Search** | No server-side search, filter, or sort. Returns everything; client must filter. |
| **Anonymous browsing** | Requires ARM auth. Catalog supports anonymous access needed for public browsing UX. |

---

## Catalog API Data Quality Issues

These must be fixed before the Catalog can serve as the authoritative discovery source.

### Offer/SKU mapping is incorrect

`azureOffers[]` uses coarse values that don't map to ARM SKUs:

| Model | Catalog `azureOffers` | Actual ARM SKUs |
|---|---|---|
| `claude-opus-4-6` | `["standard-paygo"]` | `GlobalStandard` only |
| `gpt-4.1-nano` | `["standard-paygo", "VM"]` | 9 SKUs: `GlobalStandard`, `DataZoneStandard`, `GlobalBatch`, `GlobalProvisionedManaged`, etc. |
| `deepseek-r1` | `["standard-paygo"]` | `GlobalProvisionedManaged`, `GlobalStandard`, `Provisioned` |

97 of 114 overlapping models show `standard-paygo` but only support `GlobalStandard` (not `Standard`). Only 6 models carry `PTU` in the catalog while 171 model/versions support `Provisioned` or related SKUs in ARM.

### Deprecation is incomplete

- **89%** of catalog models have `inferenceRetirementDate = null`. Models API has dates on all 345 models.
- No replacement/auto-upgrade policy. Models API has `replacementConfig` with target model, auto-upgrade date, and lead time.
- No `lifecycleStatus` (`GenerallyAvailable`, `Deprecated`, `Preview`).

### Capabilities are inconsistent

- **71%** of models (213/298) have `modelCapabilities: []`, including `Phi-4`, `Llama-3.3-70B`.
- Fragmented vocabulary: `chat-completion` (207) vs. `chat-completions` (6) vs. `messages` (6) for the same concept. Three terms each for agents and fine-tuning chat.

### Model card metadata is sparse

- **34%** have `summary: null` (including `gpt-4o`, `gpt-4`, `dall-e-3`, `whisper`).
- **9%** have `publisher: null` (including `gpt-4`, `gpt-35-turbo`).
- Rich card fields (description, evaluation, license) only available via Get API, not list — requires a second call per model.

### Missing metadata

No region/location data, no pricing, no `lifecycleStatus`, no `isDefaultVersion`.

### No SDK/CLI

REST-only, undocumented. No SDK wrapper, no CLI command, no published API reference.

---

## Design Principles

### Keep the Models API simple for serverless

The ~200 serverless models (primarily Azure OpenAI) work today. Don't break existing SDK/CLI/REST consumers. Keep the Models API focused on by-name lookup for serverless models. Rationalize the overlap between Models API and Model Capacities API.

### Make the Catalog API the authoritative discovery surface

The Catalog should become a consistent superset for discovery, serving three use cases:
1. **Large-scale discovery** — 10,000+ models with server-side search, filter, sort.
2. **Powering the UX** — anonymous, rich metadata, paginated.
3. **Programmatic integration** — documented, SDK-wrapped, for ISVs and platform builders.

Fix data quality: correct offer/SKU mapping, complete deprecation, normalize task vocabulary, fill sparse model cards.

### Align on a single taxonomy

- **One offer/SKU vocabulary** mapping catalog filters directly to ARM SKU names.
- **One task/capability vocabulary** shared by both APIs.
- **One deprecation/lifecycle schema** surfaced consistently.

### Self-service publishing for Managed Compute

Serverless models continue via managed publishing (small, curated set). Managed Compute and long-tail models must use **self-service publishing through ML Registries** with automated indexing pipelines.

---

## Two-Phase Discovery Pattern

Region availability and deployment compatibility are subscription/account-scoped. The Catalog API is global and anonymous. This calls for a two-phase pattern:

**Phase 1 — Global discovery (Catalog, anonymous):** Find models by publisher, task, offer, capabilities. Returns model identity: `(format, name, version)` or ML Registry asset ID.

**Phase 2 — Deployment readiness (Account-scoped, authenticated):** Pass model identity to check regions, SKUs, capacity, and quota.

```
# Phase 1: Catalog — discover (anonymous)
POST https://api.catalog.azureml.ms/asset-gallery/v1.0/models
→ returns (name, version, registryName, assetId)

# Phase 2: Model Capacities — check readiness (authenticated)
GET /subscriptions/{sub}/providers/Microsoft.CognitiveServices/modelCapacities
    ?modelFormat=OpenAI&modelName=gpt-4.1&modelVersion=2025-04-14
→ returns region × SKU × available capacity
```

The join key has a vocabulary mismatch: Catalog returns `registryName` (e.g., `azure-openai`); Capacities API requires `modelFormat` (e.g., `OpenAI`). See [Model Identity Rationalization](#rationalize-model-identity-across-apis) for resolution options.

**SDK pattern options:**

| Pattern | Description | Tradeoff |
|---|---|---|
| **Explicit two-call** | Separate `catalog.list_models()` and `capacities.list()` calls | Simple, transparent. Requires two SDK packages. |
| **SDK orchestration** | `get_deployment_options(model)` internally calls both | Convenient. Hides the two-API split. Requires both credentials. |
| **Catalog with capacity hints** | Precomputed hints in catalog response | Fewer round-trips. But stale data and breaks anonymous access. |

**Recommendation**: Start with **explicit two-call** — matches current API structure, honest about scope boundaries. SDK should accept catalog model objects directly in capacity API calls.

---

## SDK and CLI Coverage for Discovery

The Catalog API has no SDK, no CLI, and no documentation. Three options for where to put the surface:

### Option A: New `azure-ai-catalog` + `az ai catalog`

Standalone package for anonymous catalog discovery.

**SDK:**
```python
from azure.ai.catalog import CatalogClient

client = CatalogClient()  # anonymous — no credential needed

models = client.list_models(
    filters=[{"field": "InferenceTasks", "operator": "eq", "values": ["chat-completion"]}],
    order_by="displayName", page_size=50,
)

model = client.get_model(
    registry_name="azureml-meta",
    model_name="Meta-Llama-3.1-405B-Instruct",
    version="2",
)
```

**CLI:**
```bash
az ai catalog model list --task chat-completion --page-size 50 -o table
az ai catalog model show --registry azureml-meta --name Meta-Llama-3.1-405B-Instruct --version 2
```

| Pros | Cons |
|---|---|
| Clean separation (anonymous vs. authenticated) | Yet another package to install |
| Independent versioning | Users must discover it exists |
| Natural home for search/filter/sort | No two-phase integration unless paired with another SDK |

### Option B: Add to `azure-ai-projects` + `az ai`

Catalog operations on the existing Foundry Project SDK. Catalog calls are anonymous internally.

**SDK:**
```python
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential

project_client = AIProjectClient(
    endpoint="https://myproject.services.ai.azure.com/api",
    credential=DefaultAzureCredential(),
)

# Catalog calls are anonymous — credential is not sent
models = project_client.catalog.list_models(
    filters=[{"field": "Publisher", "operator": "eq", "values": ["Microsoft"]}],
    page_size=100,
)

model = project_client.catalog.get_model(
    registry_name="azureml-openai",
    model_name="gpt-4.1",
    version="2025-04-14",
)
```

**CLI:**
```bash
az ai model list --publisher Microsoft --page-size 100 -o table
az ai model show --registry azureml-openai --name gpt-4.1 --version 2025-04-14
```

| Pros | Cons |
|---|---|
| Single SDK for discover → deploy → infer | Mixes anonymous and authenticated on one client |
| Lowest friction for existing users | `az ai` CLI doesn't exist yet |
| Enables SDK orchestration (two-phase handoff built-in) | Catalog versioning tied to `azure-ai-projects` releases |

### Option C: Add to `azure-mgmt-cognitiveservices` + `az cognitiveservices`

Catalog operations on the existing Cognitive Services management SDK.

**SDK:**
```python
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.identity import DefaultAzureCredential

client = CognitiveServicesManagementClient(
    credential=DefaultAzureCredential(),
    subscription_id="00000000-0000-0000-0000-000000000000",
)

# Catalog calls are anonymous — credential is not sent
models = client.catalog.list_models(
    filters=[{"field": "AzureOffers", "operator": "eq", "values": ["standard-paygo"]}],
    page_size=50,
)

model = client.catalog.get_model(
    registry_name="azureml-anthropic",
    model_name="claude-opus-4-6",
    version="1",
)
```

**CLI:**
```bash
az cognitiveservices catalog model list --offer standard-paygo --page-size 50 -o table
az cognitiveservices catalog model show --registry azureml-anthropic --name claude-opus-4-6 --version 1
```

| Pros | Cons |
|---|---|
| All model discovery in one package | Management SDK requires `subscription_id` — awkward for anonymous calls |
| Two-phase handoff trivial (same client) | `azure-mgmt-*` is auto-generated from ARM; Catalog is not ARM |
| CLI already has `az cognitiveservices` | Couples anonymous discovery to subscription-scoped package |

### SDK/CLI Comparison

| Dimension | Option A: `azure-ai-catalog` | Option B: `azure-ai-projects` | Option C: `azure-mgmt-cognitiveservices` |
|---|---|---|---|
| Package scope | Anonymous discovery only | Project data-plane + discovery | ARM management + discovery |
| Auth model fit | Clean (anonymous default) | Mixed (credential required, unused by catalog) | Mixed (subscription required, unused by catalog) |
| Two-phase handoff | Requires pairing with another SDK | Built-in | Built-in |
| CLI home | New `az ai catalog` | New `az ai` | Existing `az cognitiveservices` |
| New package overhead | Yes | No | No |
| Existing user base | None | Growing (Foundry-first users) | Large (existing ARM automation) |

---

## Rationalize Model Identity Across APIs

The Catalog and Cognitive Services APIs use incompatible identity schemes for the same models:

| Component | Catalog API | Cognitive Services APIs |
|---|---|---|
| Name | `name` (e.g., `gpt-4.1`) | `name` (e.g., `gpt-4.1`) |
| Version | `version` (e.g., `2025-04-14`) | `version` (e.g., `2025-04-14`) |
| Third key | `registryName` (e.g., `azure-openai`) | `format` / `modelFormat` (e.g., `OpenAI`) |
| URI | `assetId` | `model_catalog_asset_id` (same URI) |

`name` and `version` match. The third key does not — `registryName` vs. `format` is a 1:1 mapping that is undocumented:

| Cog Services `format` | Catalog `registryName` |
|---|---|
| `OpenAI` | `azure-openai` |
| `Anthropic` | `azureml-anthropic` |
| `Meta` | `azureml-meta` |
| `Microsoft` | `azureml` |
| `DeepSeek` | `azureml-deepseek` |
| `Mistral AI` | `azureml-mistral` |
| `Cohere` | `azureml-cohere` |
| `AI21 Labs` | `azureml-ai21` |
| `Stability AI` | `azureml-stabilityai` |
| `xAI` | `azureml-xai` |
| `MoonshotAI` | `azureml-moonshotai` |
| `Black Forest Labs` | `azureml-blackforestlabs` |
| `MAI` | `azure-mai` |
| `Core42` | `azureml-core42` |
| `Alibaba` | `azureml-alibaba` |
| `OpenAI-OSS` | `azureml-openai-oss` |

A user who discovers a model via the Catalog (Phase 1) must translate `registryName` → `modelFormat` before calling the Capacities API (Phase 2). No API performs this translation today.

### Option 1: Add `modelFormat` to Catalog response

Simplest fix — one additive field unblocks the two-phase handoff.

```python
model = catalog_client.get_model("azureml-openai", "gpt-4.1", "2025-04-14")
capacities = cog_client.model_capacities.list(
    model_format=model.model_format,  # "OpenAI"
    model_name=model.name,
    model_version=model.version,
)
```

| Pros | Cons |
|---|---|
| Minimal change — one additive field | Catalog must know Cog Services format vocabulary |
| `(format, name, version)` works everywhere | Requires Catalog team to publish and maintain the mapping |

### Option 2: Accept `assetId` in Capacities API

The Capacities API accepts the asset URI directly instead of requiring `(format, name, version)`.

```python
model = catalog_client.get_model("azureml-openai", "gpt-4.1", "2025-04-14")
capacities = cog_client.model_capacities.list(model_asset_id=model.asset_id)
```

| Pros | Cons |
|---|---|
| Zero changes to Catalog API | Cog Services API change — new parameter |
| URI is already the shared identifier | RP must parse the `azureml://` URI internally |
| Future-proof — works for any model | Breaking change if `(format, name, version)` is later deprecated |

### Option 3: SDK-side mapping table

SDK maintains a `registryName → modelFormat` lookup table internally.

```python
_REGISTRY_TO_FORMAT = {
    "azure-openai": "OpenAI",
    "azureml-anthropic": "Anthropic",
    # ...
}
capacities = cog_client.model_capacities.list_from_catalog_model(model)
```

| Pros | Cons |
|---|---|
| No API changes on either side | Fragile — breaks silently with new publishers |
| Ships immediately | Not available to REST consumers |
| | SDK release required for every new publisher |

### Option 4: `assetId` URI as canonical identity everywhere

All APIs accept the `assetId` URI as a first-class input.

```python
model_ref = "azureml://registries/azure-openai/models/gpt-4.1/versions/2025-04-14"
capacities = cog_client.model_capacities.list(model_asset_id=model_ref)
deployment = cog_client.deployments.begin_create_or_update(
    rg, account, name,
    {"model": {"asset_id": model_ref}, "sku": {"name": "GlobalStandard", "capacity": 100}},
)
```

| Pros | Cons |
|---|---|
| Single identifier across all APIs | Requires changes to every API that accepts model identity |
| No mapping tables needed | URI parsing less ergonomic than structured fields for CLI |
| Naturally extensible — new registries work immediately | Long-term convergence effort |

### Recommendation

**Option 1 + Option 2** together:
- **Short term**: Add `modelFormat` to Catalog responses. One additive field, immediately unblocks the handoff.
- **Medium term**: Make Capacities API accept `modelAssetId` as an alternative. Eliminates `format` knowledge requirement.
- **Avoid Option 3** — encodes undocumented knowledge, breaks with new publishers, unavailable to REST consumers.
- **Option 4** is the ideal end state; pursue incrementally.

---

## Unified vs. Separate Surfaces for Serverless and Managed Compute

| Dimension | Serverless (Cog Services RP) | Managed Compute (ML Registry / Foundry RP) |
|---|---|---|
| **Deployment contract** | `(format, name, version)` + SKU + TPM | `(name, version)` + accelerator type + GPU count + instance count |
| **Quota model** | Per-model, per-SKU: TPM or PTU | Per-accelerator-type: GPU count (shared across all models) |
| **Capacity check** | Model Capacities API (model-scoped) | Accelerator Capacities API (GPU-scoped, model-agnostic) |
| **SKU vocabulary** | `Standard`, `GlobalStandard`, `ProvisionedManaged`, etc. | `GlobalManagedCompute`, `DataZoneManagedCompute` |
| **Publishing** | Manual config-file update by Cog Services team | Self-service via ML Registry with automated indexing |
| **Model count** | ~345 today | 10,000+ in catalog |
| **Rate limits** | Per-model (RPM, TPM) | None (GPU-based, not token-metered) |

### Option A: Fully Unified

One SDK, one CLI, one API. A single discovery/management surface returns all models; response branches by deployment type.

**SDK:**
```python
models = client.catalog.list_models(task="chat-completion")
readiness = client.deployment_readiness.check(model)
for option in readiness.options:
    if option.sku_family == "serverless":
        print(f"  {option.sku}: {option.available_tpm} TPM in {option.region}")
    elif option.sku_family == "managed_compute":
        print(f"  {option.sku}: {option.accelerator_type}, {option.available_gpus} GPUs")
```

**CLI:**
```bash
az ai model list --task chat-completion -o table
az ai deployment-readiness check --model gpt-4.1 --version 2025-04-14 -o table
```

| Pros | Cons |
|---|---|
| Single entry point — simplest mental model | Response must branch by SKU type — union types |
| Users discover MC models they didn't know existed | Quota check hides model-scoped vs. accelerator-scoped distinction |
| Matches the portal experience (one catalog, one search bar) | Phase 2 is fundamentally different — faking unity adds complexity |

### Option B: Fully Separate

Discovery unified (one catalog), but deployment readiness / quota / management are separate per deployment contract.

**SDK:**
```python
models = catalog_client.list_models(task="chat-completion")
for model in models:
    if "GlobalStandard" in model.offers:
        capacities = cog_client.model_capacities.list(
            model_format=model.model_format, model_name=model.name,
            model_version=model.version,
        )
    if "GlobalManagedCompute" in model.offers:
        gpu_capacity = cog_client.managed_compute_capacities.list(
            offer_type="GlobalManagedCompute", accelerator_type="H100_80GB",
        )
```

**CLI:**
```bash
az ai model list --task chat-completion -o table
az cognitiveservices model capacity list \
  --model-format OpenAI --model-name gpt-4.1 --model-version 2025-04-14
az cognitiveservices managed-compute capacity list \
  --offer-type GlobalManagedCompute --accelerator-type H100_80GB
```

| Pros | Cons |
|---|---|
| APIs match actual resource model — no forced abstraction | User must know which path to take |
| Clean type system — no union types | A model supporting both requires two readiness checks |
| Each surface evolves independently | Steeper learning curve |
| Aligns with GPU Offer Spec (Proposal B) | Two CLI command groups |

### Option C: Unified Discovery, Branched Management (Recommended)

Discovery and model info are unified — one catalog, one list, one get. Deployment readiness, quota, and CRUD split into separate surfaces matching the underlying resource model.

**SDK:**
```python
model = catalog_client.get_model("azureml-meta", "Meta-Llama-3.1-405B-Instruct", "2")
print(model.offers)  # ["GlobalStandard", "GlobalManagedCompute"]

if "GlobalStandard" in model.offers:
    capacities = cog_client.model_capacities.list(
        model_format=model.model_format,
        model_name=model.name,
        model_version=model.version,
    )

if "GlobalManagedCompute" in model.offers:
    gpu_capacity = cog_client.managed_compute_capacities.list(
        offer_type="GlobalManagedCompute",
        accelerator_type="H100_80GB",
    )
```

**CLI:**
```bash
# Unified discovery
az ai model show --registry azureml-meta --name Meta-Llama-3.1-405B-Instruct --version 2

# Branched readiness — user picks based on offers in the model response
az cognitiveservices model capacity list --model-format Meta ...
az cognitiveservices managed-compute capacity list --accelerator-type H100_80GB
```

| Pros | Cons |
|---|---|
| Discovery is simple — one search, all 10,000+ models | Catalog must include MC-specific fields (accelerator specs) |
| Management APIs stay clean — no union types | User branches after discovery |
| Matches how users think: find first, then deploy | Catalog must ingest accelerator metadata from publishing pipeline |
| Aligns with GPU Offer Spec (Proposal B) | |

### Comparison

| Dimension | Option A: Fully Unified | Option B: Fully Separate | Option C: Hybrid (Recommended) |
|---|---|---|---|
| Discovery | Unified | Unified | Unified |
| Deployment readiness | Unified (branching response) | Separate per type | Separate per type |
| Type complexity | High (union types) | Low (separate types) | Low (separate types) |
| User mental model | Simplest for beginners | Requires deployment-type knowledge | Moderate — discover freely, branch when deploying |
| API evolution risk | High (changes ripple) | Low (independent) | Low (independent) |
| Quota model honesty | Low (hides model vs. accelerator distinction) | High | High |

**Recommendation: Option C.** The two deployment types share a discovery experience (same catalog, same search, same model card) but diverge at deployment time because the underlying resource models are fundamentally different:

- **Serverless quota is model-scoped** — "how many TPM of gpt-4.1 on GlobalStandard?"
- **Managed Compute quota is accelerator-scoped** — "how many H100 GPUs do I have?"

Option C gives the best experience at each phase:
1. **Discover**: One catalog, one search, all models. The model card shows available deployment types.
2. **Check readiness**: Call the API matching the deployment type — model capacities (serverless) or accelerator capacities (managed compute).
3. **Deploy**: Use the contract matching the SKU — `(format, name, version, SKU, TPM)` for serverless, `(asset ID, accelerator type, instance count)` for managed compute.
