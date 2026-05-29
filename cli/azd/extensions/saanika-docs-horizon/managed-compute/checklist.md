# Managed Compute Spec — Open Items & Decisions Checklist

Extracted from: [intro.md](intro.md), [spec-catalog.md](spec-catalog.md), [e2e-1-discover.md](e2e-1-discover.md), [e2e-2-get-model.md](e2e-2-get-model.md), [e2e-3-quota-capacity.md](e2e-3-quota-capacity.md), [e2e-4-deployments.md](e2e-4-deployments.md), [e2e-5-inference.md](e2e-5-inference.md), [spec-custom-models.md](spec-custom-models.md)

---

## Decisions Required

### Discovery & Catalog SDK/CLI

- [ ] **SDK/CLI home for Catalog API** — Where should the Catalog SDK and CLI surface live?
  - Option A: New `azure-ai-catalog` package + `az ai catalog` CLI
  - Option B: Add to `azure-ai-projects` + `az ai` CLI
  - Option C: Add to `azure-mgmt-cognitiveservices` + `az cognitiveservices catalog` CLI
  - *(Source: [spec-catalog.md](spec-catalog.md) — SDK and CLI Coverage for Discovery)*

- [ ] **Two-phase discovery SDK pattern** — How should the SDK orchestrate global discovery (Catalog, anonymous) → deployment readiness (account-scoped, authenticated)?
  - Explicit two-call (separate `catalog.list_models()` and `capacities.list()`)
  - SDK orchestration (`get_deployment_options(model)` internally calls both)
  - Catalog with precomputed capacity hints
  - Recommendation in spec: explicit two-call
  - *(Source: [spec-catalog.md](spec-catalog.md) — Two-Phase Discovery Pattern)*

### Model Identity

- [ ] **Model identity rationalization across APIs** — Bridge the `registryName` (Catalog) vs `modelFormat` (Cog Services) vocabulary mismatch
  - Option 1: Add `modelFormat` to Catalog response (short-term fix)
  - Option 2: Accept `assetId` in Capacities API (medium-term)
  - Option 3: SDK-side mapping table (fragile, avoid)
  - Option 4: `assetId` URI as canonical identity everywhere (long-term ideal)
  - Recommendation in spec: Option 1 + Option 2
  - *(Source: [spec-catalog.md](spec-catalog.md) — Rationalize Model Identity Across APIs)*

- [ ] **Dual model identity in deployment bodies** — Serverless uses `(format, name, version)`, managed compute uses `modelAssetId`. Should the deployment API accept both forms for both types, or keep them separate?
  - *(Source: [e2e-4-deployments.md](e2e-4-deployments.md) — side-by-side comparison)*

### Unified vs Separate API Surfaces

- [ ] **Unified vs separate surfaces for serverless and managed compute** — How much should the API surfaces converge?
  - Option A: Fully unified (one SDK, one CLI, one API — union types)
  - Option B: Fully separate (discovery unified, everything else separate)
  - Option C: Unified discovery, branched management (recommended)
  - *(Source: [spec-catalog.md](spec-catalog.md) — Unified vs. Separate Surfaces)*

### Quota & Capacity APIs

- [ ] **`acceleratorCapacities` API design** — What shape should the managed compute capacity API take?
  - Option A: Deployment-size breakdown (accelerator-scoped, model-agnostic) — simpler, raw pool view with fragmentation visibility
  - Option B: Model-scoped (mirrors serverless `modelCapacities`) — user-friendly, resolves template × accelerator matrix server-side
  - *(Source: [e2e-3-quota-capacity.md](e2e-3-quota-capacity.md) — Check Capacity, 4B)*

### Deployment API

- [x] **Deployment API for managed compute** — Shared or separate resource type?
  - ~~Option A: Same `deployments` API (discriminated by `sku.name = "GlobalManagedCompute"`) — unified endpoint, union type~~
  - **Option B: Separate `acceleratorDeployments` API** ✅ — clean type, separate RBAC, two endpoints
  - **Decision (Mar 23):** Going with Option B. Detailed spec: [spec-deployments.md](spec-deployments.md) with CRUD operations in [accelerator_deployments/](accelerator_deployments/).
  - *(Source: [e2e-4-deployments.md](e2e-4-deployments.md) — Option A vs Option B)*

### Inference Routing

- [ ] **Route suffix discovery for non-OpenAI-compatible models** — How do users/SDKs discover the correct route suffix (e.g., `/v1/rerank` for Cohere)?
  - Proposal: Publish `scoringPath` from deployment template as model metadata
  - Needs decision: where to surface it (Catalog API tag? Deployment response property? Both?)
  - *(Source: [e2e-5-inference.md](e2e-5-inference.md) — Case 2: Non-OpenAI-compatible models)*

- [ ] **Payload schema discovery for bespoke models** — Currently the user's responsibility to know the request/response schema. Should the platform provide schema metadata?
  - *(Source: [e2e-5-inference.md](e2e-5-inference.md) — Case 2)*

---

## Catalog API Data Quality Fixes

- [ ] **Fix offer/SKU mapping** — `azureOffers[]` uses coarse values (`standard-paygo`, `VM`) that don't map to ARM SKUs (`GlobalStandard`, `GlobalProvisionedManaged`, etc.). 97 of 114 models show `standard-paygo` but only support `GlobalStandard`. Only 6 models carry `PTU` while 171 model/versions support `Provisioned` in ARM.
  - *(Source: [spec-catalog.md](spec-catalog.md) — Offer/SKU mapping is incorrect)*

- [ ] **Rename `"VM"` → `"GlobalManagedCompute"`** in offer vocabulary for managed compute models landing in Foundry RP
  - *(Source: [e2e-1-discover.md](e2e-1-discover.md) — Catalog API fixes required)*

- [ ] **Rename `"standard-paygo"` → actual ARM SKU names** (`GlobalStandard`, `DataZoneStandard`, etc.)
  - *(Source: [e2e-1-discover.md](e2e-1-discover.md) — Catalog API fixes required)*

- [ ] **Fix null `azureOffers` on valid models** — e.g., Fireworks `FW-GPT-OSS-120B` shows `azureOffers: null` but supports `GlobalStandard` and `GlobalProvisionedManaged`
  - *(Source: [e2e-1-discover.md](e2e-1-discover.md))*

- [ ] **Complete deprecation data** — 89% of catalog models have `inferenceRetirementDate = null`; Models API has dates on all 345 models. Missing: `lifecycleStatus`, replacement/auto-upgrade policy, `replacementConfig`
  - *(Source: [spec-catalog.md](spec-catalog.md) — Deprecation is incomplete)*

- [ ] **Normalize capabilities vocabulary** — `chat-completion` (207 models) vs `chat-completions` (6) vs `messages` (6) for the same concept. Three terms each for agents and fine-tuning chat. 71% of models (213/298) have `modelCapabilities: []`.
  - *(Source: [spec-catalog.md](spec-catalog.md) — Capabilities are inconsistent)*

- [ ] **Fill sparse model cards** — 34% have `summary: null` (including `gpt-4o`, `gpt-4`). 9% have `publisher: null` (including `gpt-4`, `gpt-35-turbo`). Rich card fields only available via Get API, not list.
  - *(Source: [spec-catalog.md](spec-catalog.md) — Model card metadata is sparse)*

- [ ] **Add missing metadata** — No region/location data, no pricing, no `lifecycleStatus`, no `isDefaultVersion` in Catalog responses
  - *(Source: [spec-catalog.md](spec-catalog.md) — Missing metadata)*

- [ ] **Fix collection filter** — `"collections": ["directFromAzure"]` and `"collection": "directFromAzure"` are silently ignored. Using `collections` as a filter field returns `UserError`.
  - *(Source: [e2e-1-discover.md](e2e-1-discover.md) — Note on "Direct from Azure" filter)*

---

## New APIs to Build

- [ ] **`acceleratorUsages` API** — New quota API for managed compute (GPU count per accelerator type, model-agnostic). Backed by AzureML quota service, separate from existing `usages` API.
  - *(Source: [e2e-3-quota-capacity.md](e2e-3-quota-capacity.md) — 3B. Managed Compute Quota)*

- [ ] **`acceleratorCapacities` API** — New capacity API for managed compute (available accelerators per type). Design depends on Option A vs Option B decision above.
  - *(Source: [e2e-3-quota-capacity.md](e2e-3-quota-capacity.md) — 4B. Managed Compute Capacity)*

- [ ] **Catalog SDK** — No SDK wraps the Catalog API today. REST-only, undocumented, no published API reference.
  - *(Source: [spec-catalog.md](spec-catalog.md) — No SDK/CLI)*

- [ ] **Catalog CLI** — No CLI command wraps the Catalog API today.
  - *(Source: [spec-catalog.md](spec-catalog.md) — No SDK/CLI)*

- [ ] **ML Registry `deployment_templates.get()` SDK method** — Proposed in spec but doesn't exist today. `az ml deployment-template list` returns `[]` for the target registry.
  - *(Source: [e2e-2-get-model.md](e2e-2-get-model.md) — Fetch the Deployment Template)*

---

## API Alignment & Rationalization

- [ ] **Unify Models API + Model Capacities API** — Today answering "can I deploy model X as SKU Y in region Z with what rate limits?" requires calling both APIs and joining on `(model, version, SKU)`. A unified API should return all in one response.
  - *(Source: [spec-catalog.md](spec-catalog.md) — Model Capacities API Overlap)*

- [ ] **Align task/capability vocabulary** — Catalog uses `inferenceTasks[]` (semantic arrays); Models API uses `capabilities.*` (boolean-as-string flags). Different concepts, no shared vocabulary.
  - *(Source: [spec-catalog.md](spec-catalog.md) — Discovery and Metadata Capabilities table)*

- [ ] **Align model limits schema** — Catalog uses structured `modelLimits` object with typed ints; Models API uses string values in flat `capabilities` dict. Absent for reasoning models.
  - *(Source: [spec-catalog.md](spec-catalog.md) — Discovery and Metadata Capabilities table)*

- [ ] **Align deprecation schema** — Catalog has `inferenceRetirementDate` only (89% null). Models API has full lifecycle: dates, `lifecycleStatus`, `replacementConfig` with auto-upgrade.
  - *(Source: [spec-catalog.md](spec-catalog.md) — Discovery and Metadata Capabilities table)*

---

## Managed Compute–Specific Open Items

- [ ] **`versionUpgradeOption` for managed compute** — Only `OnceNewDefaultVersionAvailable` supported initially (platform retains right to upgrade for security patches). `NoAutoUpgrade` and `OnceCurrentVersionExpired` deferred to BYOC support. Confirm this policy.
  - *(Source: [e2e-4-deployments.md](e2e-4-deployments.md) — Option B versionUpgradeOption note)*

- [ ] **RAI policy for managed compute** — Listed as N/A (container-level filtering). Needs a defined approach for content safety on GPU deployments.
  - *(Source: [e2e-4-deployments.md](e2e-4-deployments.md) — side-by-side comparison)*

- [ ] **APIM routing logic for managed compute** — How APIM detects that a deployment name corresponds to a managed compute deployment and forks traffic to the backend compute endpoint. Described conceptually but not specified.
  - *(Source: [e2e-5-inference.md](e2e-5-inference.md) — Routing: two paths for managed compute)*

- [ ] **`accelerator_maps` field on deployment templates** — Proposed new field mapping accelerator types to GPU counts per instance. Doesn't exist today; deployment templates return `allowedInstanceType` / `defaultInstanceType` (VM SKU names). Needs to be implemented.
  - *(Source: [e2e-2-get-model.md](e2e-2-get-model.md) — Deployment template output)*

- [ ] **Self-service publishing for managed compute models** — Serverless uses manual config-file updates. Managed compute must use self-service publishing through ML Registries with automated indexing. Publishing pipeline design needed.
  - *(Source: [spec-catalog.md](spec-catalog.md) — Self-service publishing for Managed Compute)*

---

## Out of Scope (Needs Separate Specs)

- [ ] **Monitoring and billing** — Explicitly out of scope. Needs separate spec.
  - *(Source: [intro.md](intro.md) — Note after walkthrough structure table)*

- [ ] **Enterprise governance** — RBAC, Azure Policy, private networking. Explicitly out of scope. Needs separate spec.
  - *(Source: [intro.md](intro.md) — Note after walkthrough structure table)*

- [ ] **Bring-your-own-container (BYOC)** — Referenced multiple times (versionUpgradeOption, custom models) but not specified. Needs its own spec.
  - *(Source: [e2e-4-deployments.md](e2e-4-deployments.md))*

- [ ] **Custom models (BYOW) deployment compatibility** — Described at a high level in intro. Format requirements for managed compute are unclear ("specific container/runtime configurations"). Needs detailed spec.
  - *(Source: [intro.md](intro.md) — Work with Custom Models)*

- [ ] **Feature parity across SDKs and CLIs** — Multiple gaps documented: `azure-ai-projects` can't create deployments, `az cognitiveservices` has no model capacity listing, `az ml` has no deployment SKU/quota ops, etc. Needs roadmap.
  - *(Source: [intro.md](intro.md) — Feature Parity Gap)*

---

## Summary

| Category | Open Items |
|---|---|
| Decisions required | 9 |
| Catalog data quality fixes | 9 |
| New APIs to build | 5 |
| API alignment & rationalization | 4 |
| Managed compute–specific | 5 |
| Out of scope (needs separate specs) | 5 |
| **Total** | **37** |
