pwd# Foundry End-to-End Model Lifecycle Spec

## Introduction

This spec walks through the complete lifecycle of working with AI models in Azure AI Foundry — from discovery to inference. It focuses exclusively on programmatic interfaces (API, SDK, CLI); portal and UI experiences are out of scope. It exposes the current fragmentation across several dimensions: control plane vs. data plane, CLI and SDK gaps, and inconsistent API coverage across scenarios. It then proposes designs to address those gaps, with a focus on bringing Managed Compute (GPU-based deployments) into the Foundry RP alongside existing serverless deployments.

### Reference Model: `gpt-oss-120b`

The walkthrough uses **one model — `gpt-oss-120b` — deployed two ways** to make the differences and similarities between serverless and managed compute immediately visible:

- **Name**: `gpt-oss-120b` (version `4`)
- **Registry**: `azureml-openai-oss`
- **Asset ID**: `azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4`
- **Model format** (Cog Services): `OpenAI-OSS`
- **Azure offers**: `["GlobalStandard", "GlobalManagedCompute"]`

Two deployments are created from this single model:
1. **`gpt-oss-120b-serverless`** — pay-per-token, GlobalStandard, no GPU commitment
2. **`gpt-oss-120b-gpu`** — dedicated H100 GPUs, GlobalManagedCompute, full control over hardware

**Starting point**: An Azure subscription with a Foundry resource (Cog Services account) already provisioned. No deployments exist.

### Walkthrough Structure

The lifecycle is broken into five documents, each covering a stage of the workflow:

| # | Document | What it covers |
|---|---|---|
| 1 | [Discover the Model](e2e-1-discover.md) | Search the catalog by name, publisher, or task. Find `gpt-oss-120b` and its available offers. |
| 2 | [Get Model Details](e2e-2-get-model.md) | Retrieve model metadata from three APIs (Catalog, Cog Services Models, ML Registry). Understand deployment templates, accelerator maps, and the bridge between APIs. |
| 3 | [Quota and Capacity](e2e-3-quota-capacity.md) | Check quota (TPM for serverless, GPU count for managed compute) and physical capacity before deploying. Covers the new `acceleratorUsages` and `acceleratorCapacities` APIs. |
| 4 | [Deployments](e2e-4-deployments.md) | Create, list, scale, and delete deployments. Compares Option A (shared `deployments` API) vs. Option B (separate `acceleratorDeployments` API) for managed compute. |
| 5 | [Inference](e2e-5-inference.md) | Get endpoint and keys, make inference requests. Covers routing (OpenAI route vs. managed-deployments passthrough), OpenAI SDK, AI Projects SDK, and non-OpenAI-compatible models. |

> **Note**: Monitoring/billing and enterprise governance (RBAC, Azure Policy, private networking) are out of scope for this spec and will be covered separately.

### Design Assumptions

- Discovery is unified via the Catalog API
- Quota API for managed compute is new and separate: `acceleratorUsages`
- Capacity API for managed compute is new and separate: `acceleratorCapacities`
- Deployment API options are explored: Option A (same `/deployments/{name}` URL, discriminated by `sku.name`) vs. Option B (separate `/acceleratorDeployments/{name}`)
- Catalog API returns `modelFormat`

### Work with Models from the Catalog (Base Model)

The end-to-end workflow for a base model from the Azure AI Model Catalog follows five stages (discovery through inference):

#### 1. Model Discovery

Find a model that fits the use case. This includes programmatically browsing by:

- Publisher or author (OpenAI, Meta, Microsoft, Anthropic, Mistral, etc.)
- Inference task (chat completions, embeddings, image generation, audio transcription, etc.)
- Deployment offer type (Global Standard, Provisioned, Managed Compute, Marketplace, etc.)
- Billing model (Microsoft-billed, Marketplace-billed, hybrid)
- Capabilities (function calling, structured output, agent-compatible, vision, etc.)
- Collections or categories (Azure OpenAI, HuggingFace, Meta Llama, etc.)

#### 2. Get Model Metadata

Retrieve the model card, capabilities, limits, supported deployment templates, and any other metadata needed to plan a deployment:

- Model card narrative (description, evaluation, notes, license)
- Model limits (context window, max output tokens, supported modalities, languages)
- Task taxonomy (inference tasks, fine-tuning tasks)
- Deployment SKU inventory (which SKUs are available: `GlobalStandard`, `DataZoneProvisionedManaged`, `ProvisionedManaged`, `Standard`, etc.)
- SKU capacity contract (min/max/step for PTU allocations)
- Deprecation and replacement policy
- Model pricing — token-based meters that are model-specific (e.g., per-model input/output token rates) or model-agnostic SKU-level pricing (e.g., Provisioned PTU hourly rate, Managed Compute GPU hourly rate)

#### 3. Check Quota and Capacity

Before deploying, verify that all prerequisites are met:

- **Quota availability**: Does the subscription have sufficient quota for the target SKU?
- **Capacity availability**: Is there physical capacity for the chosen model + SKU + region/data-zone?
- **Regional support**: Is the model + SKU combination available in the target region? Region can mean global (no geographic constraint), a data zone (e.g., `US`, `EU`), or a specific Azure location (e.g., `eastus`, `westus2`, `swedencentral`).
- **Account access / gating**: Is the account/resource authorized for the target model? Subscription/account needs to be approved before some latest models can be used.

#### 4. Create and Manage Deployments

Create a deployment for the model, then manage its lifecycle:

- **Create deployment**: Specify model (format, name, version), SKU (name, capacity), and optional parameters (version upgrade policy, RAI policy, etc.).
- **Scale deployment**: Update capacity (within min/max/step constraints).
- **Update authentication**: Configure key-based or token-based auth.
- **Update version**: Upgrade model version or configure auto-upgrade policy.
- **Delete deployment**: Clean up when no longer needed.

#### 5. Inference

After deployment, obtain the scoring endpoint and credentials, then invoke the model:

- **Get endpoint URL**: From deployment properties.
- **Get authentication**: API keys via `AccountsOperations.list_keys()` or Microsoft Entra token from `DefaultAzureCredential`.
- **Make inference requests**: Use the OpenAI-compatible client (`openai` Python SDK with `AzureOpenAI`) or the Azure AI Inference SDK for chat completions, embeddings, image generation, etc.

### Work with Custom Models (Bring Your Own Weights)

The workflow for custom models (user-trained or fine-tuned weights) differs in the discovery and registration phases:

0. **(Optional) Fine-tune a model**: Fine-tune a base model in Foundry using the Foundry Project data-plane APIs (fine-tuning jobs), or train/fine-tune externally on another platform (e.g., HuggingFace, on-premises GPU clusters) and export the resulting weights.

1. **Register the model**: Upload model artifacts to the Foundry project as a model asset. Uses the Foundry Project data-plane APIs (model registration under project scope). Custom models are project-scoped and not globally discoverable via the catalog.

2. **Get model metadata**: Model metadata is project-scoped. Use the `azure-ai-projects` SDK to retrieve registered model details.

3. **Check deployment compatibility**: Same quota/capacity/region checks as base models, but the model must be in a format compatible with the target deployment SKU (e.g., Managed Compute requires specific container/runtime configurations).

4. **Create and manage deployments**: Same ARM management APIs as base models. For Managed Compute (GPU-centric) deployments, the deployment contract includes accelerator type and GPU count rather than PTU capacity.

5. **Fetch endpoint and score**: Same as base model inference — endpoint URL and keys from deployment properties.

Key differences from catalog-based models:

| Aspect | Catalog Base Model | Custom Model (BYOW) |
| --- | --- | --- |
| Discovery | Global catalog, cross-registry | Project-scoped asset list |
| Registration | Pre-registered in catalog | User must register via project API |
| Model card | Rich card from catalog | User-provided metadata only |
| Deployment SKU options | All standard SKUs | Typically Managed Compute (GPU) |
| Catalog visibility | Public or registry-scoped | Private to project/account |

---

## Current State

### API / SDK / CLI Overall Status

The Foundry model lifecycle spans **four distinct API planes**, **three SDKs**, and **three CLIs**, with no feature parity across them.

#### Four API Planes

| # | API Plane | Scope | Auth Model | What It Covers |
| --- | --- | --- | --- | --- |
| 1 | **Catalog APIs** | Global (no RP, no subscription required) | Anonymous or optional tenant token | Model discovery and browsing. Not control plane, not data plane. Also available without login for catalog UX when users are browsing outside scope of any RP. |
| 2 | **Foundry Project Data Plane** | Project-scoped (hits Foundry project endpoint) | Microsoft Entra token scoped to project | Agents, datasets, model assets, fine-tuning, evaluations, indexes, memory, responses API. Uses `azure-ai-projects` SDK. No CLI exists (partial `azd` coverage for fine-tuning only). |
| 3 | **Cognitive Services Management (ARM)** | Subscription / resource group / account scoped | Microsoft Entra token with ARM RBAC | Deployments CRUD, models list, quota, capacity, usages. Note: "Cog Services account" and "Foundry project" are **not** the same from RP/API standpoint — they are distinct ARM resource types under different hierarchies. Uses `azure-mgmt-cognitiveservices` SDK and `az cognitiveservices` CLI. |
| 4 | **ML Registry (ARM)** | Registry-scoped (ML services product hierarchy) | Microsoft Entra token with ARM RBAC | Publisher model registries, org-level asset sharing outside project scope, catalog publishing to registries. Uses `azure-ai-ml` (ML Client) SDK and `az ml` CLI. Not the same RP as AML workspace — this is a separate ML registry RP. |

> **Note**: ML Registry should not be confused with ML Workspace. While ML Workspace capabilities are being merged into the Foundry RP, ML Registry is a standalone RP that exclusively hosts assets — model weights, containers, environment definitions, etc. It is analogous to how Foundry Agents use ACR to pull container images. The model catalog depends on ML Registry as the backing store for publisher model artifacts; treating it as legacy or unsupported would break catalog discovery entirely.

#### Three SDKs

| # | SDK | PyPI Package | Primary API Plane | Key Operations |
| --- | --- | --- | --- | --- |
| 1 | **Azure AI Projects** | `azure-ai-projects` | Foundry Project Data Plane | Agents, datasets, evaluations, model assets, deployments (list/get only), inference, fine-tuning |
| 2 | **Cognitive Services Management** | `azure-mgmt-cognitiveservices` | Cognitive Services ARM | Deployments CRUD, models list, quota/usage, capacity, account management |
| 3 | **ML Client** | `azure-ai-ml` | ML Registry ARM | Registry operations, model registration, asset sharing, catalog publishing |

#### Three CLIs

| # | CLI | Command Group | Primary API Plane | Key Operations |
| --- | --- | --- | --- | --- |
| 1 | **Cognitive Services CLI** | `az cognitiveservices` | Cognitive Services ARM | Deployments CRUD, model list, usage list, account management |
| 2 | **ML CLI** | `az ml` | ML Registry ARM | Registry operations, model registration, workspace/registry management |
| 3 | **Azure Developer CLI** | `azd` | Mixed (Foundry + ARM) | Project scaffolding, partial fine-tuning workflows, deployment templates |

#### Feature Parity Gap

There is **no feature parity** across SDKs and CLIs:

- `azure-ai-projects` cannot create/delete deployments.
- `azure-mgmt-cognitiveservices` has no model card, no catalog discovery, no agents, no fine-tuning job management.
- `azure-ai-ml` has no deployment capacity/quota APIs or inference endpoint management.
- `az cognitiveservices` CLI has no model capacity listing command.
- `az ml` CLI has no deployment SKU or quota operations.
- `azd` has partial coverage for fine-tuning only — no deployment management, no quota checks.

### API / SDK / CLI State by Scenario

**Serverless Models** (Azure OpenAI, standard-paygo, GlobalStandard, DataZoneStandard, Provisioned, Batch — deployed via Cog Services RP):

| Scenario | API | SDK | CLI |
| --- | --- | --- | --- |
| **Discovery** | Catalog API: **Yes** (cross-registry, filters by publisher/task/offer). Cog Services Models API: **Partial** (account/location scoped, SKU metadata). | `azure-mgmt-cognitiveservices`: **Partial** (account/location scoped). No SDK wraps Catalog API. | `az cognitiveservices`: **Partial** (account/location model list). No CLI wraps Catalog API. |
| **Get model info** | Catalog API: **Yes** (model card, limits, tasks, license). Cog Services Models API: **Yes** (SKUs, capacity contracts, deprecation). Neither has both. | `azure-mgmt-cognitiveservices`: **Partial** (SKUs, deprecation; no model card). | `az cognitiveservices`: **Partial** (model list output). |
| **Deployment compatibility** | Cog Services API: **Yes** (quota/usage, model capacities). | `azure-mgmt-cognitiveservices`: **Yes** (quota, capacity). | `az cognitiveservices`: **Partial** (usage list only; no model capacity commands). |
| **Deploy** | Cog Services API: **Yes** (full CRUD). Foundry Project API: **Partial** (list/get only). | `azure-mgmt-cognitiveservices`: **Yes** (full CRUD). `azure-ai-projects`: **Partial** (list/get only). | `az cognitiveservices`: **Yes** (full CRUD). |
| **Inference** | Cog Services REST: **Yes**. Foundry Project REST: **Yes**. OpenAI-compatible REST: **Yes**. | OpenAI SDK: **Yes**. Azure AI Inference SDK: **Yes**. `azure-ai-projects`: **Yes** (via inference client). | N/A (inference is SDK/REST-driven). |

> **Note**: The table above covers Serverless models deployed via the Cognitive Services RP. **Managed Compute models (GPU/VM-based deployments) are not supported in the Foundry RP** — they remain on the ML Registry / ML Workspace RP and require `azure-ai-ml` SDK and `az ml` CLI for deployment and management. Bringing Managed Compute into the Foundry RP is a forward-looking design goal covered in the relevant proposal sections of this spec, not in this Current State section.




