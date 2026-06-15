# Custom Models (BYOW) — Spec

## Overview

This spec defines the end-to-end experience for **Bring Your Own Weights (BYOW)** — registering custom full-weight models into a Foundry project and deploying them on managed compute infrastructure.

This extends the existing accelerator deployment spec ([spec-deployments.md](spec-deployments.md)) to cover **custom models** — models that are not in the Azure AI Model Catalog but are architecturally compatible with a catalog model's deployment templates.

### Scope

This spec covers **Milestone 1: Full-Weight Models Only**:

- Registering a full-weight model (complete checkpoint with all parameters)
- Three ingestion paths: local upload, training job output, Hugging Face import
- Resolving deployment templates via the base model reference
- Deploying the registered model on managed compute

**Out of scope** (future milestones):
- LoRA adapter registration and multi-LoRA serving
- Speculative decoding (draft + target model)
- Bring Your Own Container (BYOC) — custom serving runtimes
- Quantized model import (GPTQ, AWQ, GGUF)
- Architecture-only deployment template matching (no catalog base model)

### Prerequisites — Inputs from Prior Steps

1. **Foundry account and project** — created via the control plane.
2. **Base model identified** — the user knows which catalog model their weights are derived from or architecturally compatible with (e.g., `azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4`). The base model must have approved deployment templates.
3. **Quota verified** — the user has confirmed sufficient accelerator quota via `acceleratorUsages` API ([e2e-3-quota-capacity.md](e2e-3-quota-capacity.md)).

### Key Design Decisions

- Full-weight models are registered through the **data-plane** `/models/` API with `type: "FullWeight"`.
- Every BYOW model **must reference a base model** from the catalog. The Base Model ID is the **sole authoritative mechanism** for deployment template matching. `config.json` is secondary/informational only.
- Users **cannot create** their own deployment templates — they select from existing, pre-built templates mapped via the base model.
- Deployment template compatibility is resolved **at deployment time** via the base model (Option B from PM spec). The base model is the single source of truth.
- Model registration is a **data-plane** operation. Deployment creation is a **control-plane** operation. A single BYOW workflow spans both planes.

### Terminology

| Term | Definition |
|---|---|
| **BYOW** | Bring Your Own Weights — customer uploads or references a full-weight model. |
| **Full-weight model** | A complete model checkpoint (all parameters). Registered via `PUT /models/{model}` with `type: "FullWeight"`. |
| **Base model** | A catalog model that the BYOW model is derived from or architecturally compatible with. Determines deployment template compatibility. |
| **Deployment template (DT)** | An architecture template (container environment + accelerator map) stored in MLS RP registries. Defines how a model is served. |
| **Custom model format** | The `model.format` value `"Custom"` used in the accelerator deployment request body to distinguish BYOW from catalog models. |

---

## Part A: Custom Model Registration (Data Plane)

All model registration operations target a single Foundry project via the data-plane API.

### Endpoint

```
{account}.services.ai.azure.com/api/projects/{project}
```

### Ingestion Paths

| Case | Source | Spec | Description |
|---|---|---|---|
| **Case 1** | Local machine upload | [spec-custom-models-local.md](spec-custom-models-local.md) | User uploads weight files from their local machine via SAS URI. Primary path. |
| **Case 2** | Training job output | [spec-custom-models-ft-job.md](spec-custom-models-ft-job.md) | User references a completed Foundry training job. System resolves the output checkpoint and copies weights into project storage. |
| **Case 3** | Hugging Face import | [spec-custom-models-hf.md](spec-custom-models-hf.md) | User provides a HF repo ID. System pulls weights from Hugging Face Hub. Supports public and gated/private repos. |

### Required Artifacts

| Artifact | Required | Description |
|---|---|---|
| Model weight files | **Yes** | `.safetensors` (preferred), `.bin`/`.pt` (PyTorch), or `.gguf`. Can be multi-file sharded. |
| `config.json` | **Yes** | HuggingFace-style model configuration. Architecture info, hidden size, num layers. |
| `tokenizer.json` / `tokenizer_model` | Recommended | Tokenizer files. Required by inference framework at serving time. |
| `tokenizer_config.json` | Recommended | Tokenizer configuration. Includes special tokens, chat template. |
| `special_tokens_map.json` | Optional | Special token definitions. |
| `generation_config.json` | Optional | Default generation parameters (temperature, top_p, etc.). |

> **Milestone 1 validation policy:** Validation of `config.json` and tokenizer files against the base model is **advisory** — the system warns on mismatches but does not block registration. Deeper validation may be introduced in future milestones.

### Upload-Before-Registration Pattern

Model registration follows a mandatory **upload-first** sequence for local uploads (Case 1):

1. **StartPendingUpload** — Client calls the data-plane API to initiate upload. Returns a SAS URI pointing to project-managed blob storage.
2. **Direct storage upload** — Client uploads model artifacts directly to the SAS URI via `azcopy` (recommended for large models). Bypasses Foundry services entirely.
3. **PutModel (registration)** — Client calls the model registration API. Service validates upload completed before finalizing.

For Case 2 (training job) and Case 3 (Hugging Face), registration is a **single step** — the service handles data movement internally.

---

### Operations Summary

| Operation | HTTP Method | Path | Description |
|---|---|---|---|
| **Start Pending Upload** | `POST` | `/models/{modelName}/versions/{version}:startPendingUpload` | Initiate upload session, get SAS URI (Case 1 only). |
| **Create or Update Model** | `PUT` | `/models/{modelName}/versions/{version}` | Register model after upload (Case 1) or single-step register (Cases 2, 3). |
| **Get Upload Status** | `GET` | `/models/{modelName}/versions/{version}/uploadStatus` | Check upload progress (Case 1 only). |
| **List Models** | `GET` | `/models` | List all registered models. Supports `?type=FullWeight` filter. |
| **Get Model** | `GET` | `/models/{modelName}` | Get model details, including compatible deployment templates. |
| **Delete Model** | `DELETE` | `/models/{modelName}` | Delete a registered model. Blocked if active deployments exist. |

---

### Registration — By Ingestion Path

Each ingestion path is documented in its own file with full REST, SDK, and CLI examples:

- **[Case 1: Local Upload](spec-custom-models-local.md)** — 3-step process: `startPendingUpload` → `azcopy` upload → `PutModel`. Primary path.
- **[Case 2: Training Job Output](spec-custom-models-ft-job.md)** — Single-step registration from a completed training job. Includes end-to-end Train → Register → Deploy sample.
- **[Case 3: Hugging Face Import](spec-custom-models-hf.md)** — Single-step registration from HF Hub. Supports public and gated/private repos.

---

### Listing, Getting, and Deleting Models

#### List Models

**REST API**

```http
GET {account}.services.ai.azure.com/api/projects/{project}/models?api-version=2025-06-01-preview
GET {account}.services.ai.azure.com/api/projects/{project}/models?type=FullWeight&api-version=2025-06-01-preview
Authorization: Bearer {token}
```

**SDK**

```python
# List all models
for model in client.models.list():
    print(f"{model.name}: {model.type} — {model.status}")

# List full-weight models only
for model in client.models.list(type="FullWeight"):
    print(f"{model.name}: base={model.base_model}")
```

**CLI**

```bash
{new-project-cli} model list --account my-foundry-account --project my-project
{new-project-cli} model list --account my-foundry-account --project my-project --type FullWeight
```

#### Get Model

**REST API**

```http
GET {account}.services.ai.azure.com/api/projects/{project}/models/{modelName}?api-version=2025-06-01-preview
Authorization: Bearer {token}
```

**Response**

```json
{
  "name": "my-gpt-oss-120B",
  "version": "1",
  "type": "FullWeight",
  "baseModel": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "status": "Registered",
  "createdAt": "2026-03-18T15:30:00Z",
  "storageUri": "azureml://projects/my-project/models/my-gpt-oss-120B/versions/1",
  "properties": {
    "sizeBytes": 240000000000,
    "fileCount": 15,
    "format": "safetensors"
  },
  "tags": {
    "team": "medical-ai"
  }
}
```

**SDK**

```python
model = client.models.get(name="my-gpt-oss-120B")
print(f"Name:       {model.name}")
print(f"Base model: {model.base_model}")
print(f"Status:     {model.status}")
print(f"Size:       {model.properties.size_bytes / 1e9:.1f} GB")
```

**CLI**

```bash
{new-project-cli} model show --account my-foundry-account --project my-project --name my-gpt-oss-120B
```

#### Delete Model

**REST API**

```http
DELETE {account}.services.ai.azure.com/api/projects/{project}/models/{modelName}?api-version=2025-06-01-preview
Authorization: Bearer {token}
```

**Validation:** Deletion is blocked if the model has active accelerator deployments.

| Rule | Error |
|---|---|
| Model has active deployments | `ModelInUse: Delete accelerator deployments first.` |

**SDK**

```python
client.models.delete(name="my-gpt-oss-120B")
```

**CLI**

```bash
{new-project-cli} model delete --account my-foundry-account --project my-project --name my-gpt-oss-120B
```

---

### Validation Rules (All Cases)

| Rule | Applies To | Error |
|---|---|---|
| Base model not found in catalog | All | `BaseModelNotFound: Base model '{base_model}' not found in catalog.` |
| Base model has no approved DTs | All | `BaseModelNoDTs: Base model '{base_model}' has no approved deployment templates.` |
| Base model deprecated | All | `BaseModelDeprecated: Base model '{base_model}' has been deprecated.` |
| `baseModel` field omitted | All | `BaseModelRequired: The 'baseModel' field is required.` |
| Type other than FullWeight | All | `UnsupportedModelType: Only type 'FullWeight' is supported in this milestone.` |
| Upload incomplete (partial shards) | Case 1 | `UploadIncomplete: Expected {expected} weight files, found {actual}.` |
| SAS URI expired | Case 1 | `UploadExpired: SAS URI expired. Call startPendingUpload again.` |
| Training job not found | Case 2 | `TrainingJobNotFound: Job '{jobName}' not found.` |
| Training job not completed | Case 2 | `TrainingJobNotCompleted: Job '{jobName}' is in state '{state}'.` |
| Training job output key not found | Case 2 | `OutputKeyNotFound: Output '{outputKey}' not found in job '{jobName}'.` |
| HF repo not found | Case 3 | `HuggingFaceRepoNotFound: Repository '{repo_id}' not found on HF Hub.` |
| HF authentication failed | Case 3b | `HuggingFaceAuthFailed: Token is invalid or does not have access to '{repo_id}'.` |
| HF gated model — license not accepted | Case 3b | `HuggingFaceLicenseRequired: Accept the license at https://huggingface.co/{repo_id}.` |
| HF repo has no model weights | Case 3 | `HuggingFaceNoWeights: Repository '{repo_id}' has no weight files.` |
| `config.json` mismatch | All | Advisory warning (not hard failure): `ConfigMismatch: config.json differs from base model.` |
| Model name conflict (same version, different body) | All | `ModelVersionConflict: Version '{ver}' already exists with different properties.` |

---

## Part B: Bridge — From Registered Model to Deployment Template

This section describes the **critical intermediate step** between model registration (Part A) and deployment creation (Part C). The user must resolve the deployment template ID, accelerator type, and accelerators-per-instance from the base model before calling the deployment API.

### Why This Step Exists

The accelerator deployment API ([create-or-update.md](deployments_crud/create-or-update.md)) requires three inputs:

| Input | Source |
|---|---|
| `properties.model` | The custom model reference: `projects/{project}/models/{modelName}` |
| `properties.deploymentTemplate` | Resolved from the **base model's** allowed deployment templates in the ML Registry |
| `properties.acceleratorType` | Selected from the deployment template's `accelerator_maps` |

For catalog models, the user resolves the deployment template via the ML Registry API as described in [e2e-2-get-model.md](e2e-2-get-model.md). For BYOW models, the same process applies — but the lookup goes through the **base model**, not the BYOW model itself.

### Step-by-Step: Resolve Deployment Template from Base Model

> **Note:** The steps below currently require the `az ml` CLI / `azure-ai-ml` SDK to fetch deployment templates from ML Registry. We are exploring whether the Catalog APIs can surface base model deployment templates directly, which would eliminate the ML Registry dependency and let users resolve DTs through a single SDK (`azure-ai-projects` or Catalog API) without needing `az ml`.

#### Step 1: Get the Base Model from the ML Registry

Use the `baseModel` value from the registered custom model to fetch the base model's metadata, including its `allowed_deployment_templates`.

**REST API**

```http
GET https://cert-{region}.experiments.azureml.net/genericasset/v2.0
    /subscriptions/{registrySub}/resourceGroups/{registryRg}
    /providers/Microsoft.MachineLearningServices
    /registries/{registryName}/models/{modelName}/versions/{version}
    ?api-version=2024-04-01-preview
Authorization: Bearer {token}
```

For the base model `azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4`:

**CLI (existing)**

```bash
az ml model show \
  -n gpt-oss-120B -v 4 \
  --registry-name azureml-openai-oss \
  --query "{default_dt: default_deployment_template, allowed_dts: allowed_deployment_templates}"
```

**SDK (existing)**

```python
from azure.ai.ml import MLClient

ml_client_registry = MLClient(
    credential, subscription_id,
    resource_group="rg1",
    registry_name="azureml-openai-oss",
)
base_model = ml_client_registry.models.get(name="gpt-oss-120B", version="1")
```

**Response (relevant fields)**

```json
{
  "name": "gpt-oss-120B",
  "version": "1",
  "id": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "default_deployment_template": {
    "assetId": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-vllm-throughput/versions/1"
  },
  "allowed_deployment_templates": {
    "assetIds": [
      "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-vllm-throughput/versions/1",
      "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-vllm-latency/versions/1",
      "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-sglang-throughput/versions/1"
    ]
  }
}
```

#### Step 2: Fetch a Deployment Template to Inspect Accelerator Maps

Choose a deployment template from `allowed_deployment_templates` and fetch its full specification.

**CLI (existing — preview)**

```bash
az ml deployment-template show \
  -n gpt-oss-120b-vllm-throughput -v 1 \
  --registry-name azureml-openai-oss
```

**SDK (proposed)**

```python
template = ml_client_registry.deployment_templates.get(
    name="gpt-oss-120b-vllm-throughput", version="1"
)

print(f"Template: {template.name} v{template.version}")
print(f"Framework: vLLM (throughput-optimized)")
for am in template.accelerator_maps:
    default_str = " (default)" if am.default else ""
    print(f"  {am.accelerator_type}: {am.number_of_accelerators_per_model_instance} GPUs/instance{default_str}")
```

**Output**

```
Template: gpt-oss-120b-vllm-throughput v1
Framework: vLLM (throughput-optimized)
  H100_80GB: 8 GPUs/instance (default)
  H200_141GB: 4 GPUs/instance
```

**Deployment Template Response (full)**

```json
{
  "name": "gpt-oss-120b-vllm-throughput",
  "version": "1",
  "deploymentTemplateType": "Managed",
  "description": "vLLM serving template for gpt-oss-120B — throughput-optimized, 8× H100 tensor-parallel",
  "environmentId": "azureml://registries/azureml-openai-oss/environments/gpt-oss-120b-vllm/versions/2",
  "environmentVariables": {
    "TENSOR_PARALLEL_SIZE": "8",
    "MAX_MODEL_LEN": "131072",
    "MAX_NUM_SEQS": "32",
    "CHUNKED_PREFILL_SIZE": "2048"
  },
  "requestSettings": {
    "requestTimeout": "00:02:00",
    "maxConcurrentRequestsPerInstance": 8
  },
  "scoringPath": "/v1/chat/completions",
  "scoringPort": 8000,
  "modelMountPath": "/var/azureml-app/azureml-models",
  "defaultInstanceType": "Standard_ND96isr_H100_v5",
  "allowedInstanceType": ["Standard_ND96isr_H100_v5", "Standard_ND96isr_H200_v5"],
  "accelerator_maps": [
    {
      "accelerator_type": "H100_80GB",
      "number_of_accelerators_per_model_instance": 8,
      "default": true
    },
    {
      "accelerator_type": "H200_141GB",
      "number_of_accelerators_per_model_instance": 4
    }
  ],
  "livenessProbe": {
    "initialDelay": "00:10:00",
    "period": "00:00:10",
    "timeout": "00:00:02",
    "failureThreshold": 30,
    "successThreshold": 1,
    "scheme": "http",
    "httpMethod": "GET",
    "path": "/health",
    "port": 8000
  },
  "readinessProbe": {
    "initialDelay": "00:10:00",
    "period": "00:00:10",
    "timeout": "00:00:02",
    "failureThreshold": 30,
    "successThreshold": 1,
    "scheme": "http",
    "httpMethod": "GET",
    "path": "/health",
    "port": 8000
  }
}
```

#### Step 3: Select Template and Accelerator

The user now has all three inputs required to create a deployment:

| Input | Value | Source |
|---|---|---|
| `model` | `projects/my-project/models/my-gpt-oss-120B` | Custom model registered in Part A |
| `deploymentTemplate` | `azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-vllm-throughput/versions/1` | Step 2 — selected from base model's allowed templates |
| `acceleratorType` | `H100_80GB` | Step 2 — selected from template's `accelerator_maps` |

### Multi-Framework Template Selection

Many models have deployment templates for multiple frameworks. Users select based on workload characteristics:

| DT Variant | Framework | Strengths | Best For |
|---|---|---|---|
| `{model}-vllm-throughput` | vLLM (throughput) | PagedAttention, continuous batching, high concurrency | Batch workloads, maximum tokens/sec |
| `{model}-vllm-latency` | vLLM (latency) | Lower concurrency, smaller batch, faster per-request | Interactive use, low latency |
| `{model}-sglang-throughput` | SGLang (throughput) | RadixAttention, prefix caching, high concurrency | Prefix-heavy workloads, structured output |
| `{model}-sglang-latency` | SGLang (latency) | RadixAttention, prefix caching, low concurrency | Multi-turn chat, latency-sensitive |

### Proposed: Client-Side Helper — `get_deployment_templates()`

The manual bridge flow (Steps 1–3 above) requires the user to parse the base model URI, instantiate an `MLClient` for the registry, fetch the model, then fetch each deployment template individually. This is verbose and error-prone.

We propose a **client-side convenience method** on `AIProjectClient` that encapsulates the entire bridge in a single call:

**SDK (proposed)**

```python
# One call — resolves base model → fetches DTs → returns structured result
templates = client.models.get_deployment_templates(model_name="my-gpt-oss-120B")

for t in templates:
    print(f"{t.name} v{t.version}  {'(default)' if t.is_default else ''}")
    for am in t.accelerator_maps:
        default_str = " ← default" if am.default else ""
        print(f"  {am.accelerator_type}: {am.number_of_accelerators_per_model_instance} GPUs/instance{default_str}")
```

**Output**

```
gpt-oss-120b-vllm-throughput v1  (default)
  H100_80GB: 8 GPUs/instance ← default
  H200_141GB: 4 GPUs/instance
gpt-oss-120b-vllm-latency v1
  H100_80GB: 8 GPUs/instance ← default
  H200_141GB: 4 GPUs/instance
gpt-oss-120b-sglang-throughput v1
  H100_80GB: 8 GPUs/instance ← default
  H200_141GB: 4 GPUs/instance
```

**CLI (proposed)**

```bash
# List available deployment templates for a custom model
{new-project-cli} model deployment-templates \
  --account my-foundry-account \
  --project my-project \
  --name my-gpt-oss-120B
```

**Output**

```
Template                            Version  Default  Accelerators
──────────────────────────────────  ───────  ───────  ────────────────────────────
gpt-oss-120b-vllm-throughput        1        Yes      H100_80GB (8), H200_141GB (4)
gpt-oss-120b-vllm-latency           1        No       H100_80GB (8), H200_141GB (4)
gpt-oss-120b-sglang-throughput       1        No       H100_80GB (8), H200_141GB (4)
```

**Implementation:** Internally, the helper reads the custom model's `baseModel` URI, calls the ML Registry (or Catalog API once available) to resolve `allowed_deployment_templates`, fetches each template, and returns the structured result. The user never interacts with `MLClient` or parses registry URIs.

> **Open question:** If the Catalog API is extended to surface deployment templates (see note above), this helper could be backed by a single Catalog call instead of multiple ML Registry calls — further simplifying the implementation and removing the `azure-ai-ml` dependency entirely.

### Complete Bridge Example (SDK — Manual)

```python
from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()

# ── Get the custom model to find its base model ─────────────────────────────
project_client = AIProjectClient(
    endpoint="https://my-foundry-account.services.ai.azure.com/api/projects/my-project",
    credential=credential,
)
custom_model = project_client.models.get(name="my-gpt-oss-120B")
base_model_uri = custom_model.base_model
# "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4"

# ── Parse registry name, model name, version from the base model URI ────────
# azureml://registries/{registry}/models/{model}/versions/{version}
parts = base_model_uri.replace("azureml://registries/", "").split("/")
registry_name = parts[0]    # "azureml-openai-oss"
model_name = parts[2]       # "gpt-oss-120B"
model_version = parts[4]    # "1"

# ── Fetch base model from ML Registry to get allowed deployment templates ────
ml_client = MLClient(credential, subscription_id, resource_group="rg1", registry_name=registry_name)
base_model = ml_client.models.get(name=model_name, version=model_version)

print("Available deployment templates:")
for dt_id in base_model.allowed_deployment_templates.asset_ids:
    print(f"  {dt_id}")

# ── Fetch the deployment template to see accelerator options ─────────────────
# Use the default template, or let the user choose
dt_uri = base_model.default_deployment_template.asset_id
dt_parts = dt_uri.replace("azureml://registries/", "").split("/")
dt_name = dt_parts[2]      # "gpt-oss-120b-vllm-throughput"
dt_version = dt_parts[4]   # "1"

template = ml_client.deployment_templates.get(name=dt_name, version=dt_version)

print(f"\nSelected template: {template.name} v{template.version}")
print("Accelerator options:")
for am in template.accelerator_maps:
    default_str = " ← default" if am.default else ""
    print(f"  {am.accelerator_type}: {am.number_of_accelerators_per_model_instance} GPUs/instance{default_str}")

# ── Now ready to create the deployment (Part C) ─────────────────────────────
deployment_template_id = dt_uri
accelerator_type = next(am.accelerator_type for am in template.accelerator_maps if am.default)
```

---

## Part C: Custom Model Deployment (Control Plane)

Custom model deployments use the **same** `acceleratorDeployments` ARM API as catalog models ([spec-deployments.md](spec-deployments.md), [create-or-update.md](deployments_crud/create-or-update.md)), with two differences:

1. **`model.format`** is `"Custom"` (instead of a publisher name like `"OpenAI-OSS"`).
2. **`model.source`** is `"projects/{project}/models/{modelName}"` — a data-plane model reference (instead of an ML Registry URI).

All other deployment properties (`deploymentTemplate`, `acceleratorType`, `sku`, etc.) work identically.

### Deployment Request

**REST API** (same endpoint as catalog deployments)

```http
PUT https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /acceleratorDeployments/{deploymentName}?api-version=2026-04-01-preview
Authorization: Bearer {token}
Content-Type: application/json
```

**Request Body**

```json
{
  "properties": {
    "model": {
      "format": "Custom",
      "name": "my-gpt-oss-120B",
      "source": "projects/my-project/models/my-gpt-oss-120B",
      "version": "1"
    },
    "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-vllm-throughput/versions/1",
    "acceleratorType": "H100_80GB"
  },
  "sku": {
    "name": "GlobalManagedCompute",
    "capacity": 1
  }
}
```

> **Key difference from catalog deployments:** For catalog models, `properties.model` is a string URI (`"azureml://registries/..."`). For custom models, `properties.model` is an **object** with `format`, `name`, `source`, and `version` fields. The `source` field references the data-plane model.

**Response (201 Created)**

Standard `AcceleratorDeployment` response as documented in [create-or-update.md](deployments_crud/create-or-update.md).

```json
{
  "id": "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.CognitiveServices/accounts/{account}/acceleratorDeployments/my-gpt-oss-120B-gpu",
  "name": "my-gpt-oss-120B-gpu",
  "type": "Microsoft.CognitiveServices/accounts/acceleratorDeployments",
  "properties": {
    "model": {
      "format": "Custom",
      "name": "my-gpt-oss-120B",
      "source": "projects/my-project/models/my-gpt-oss-120B",
      "version": "1"
    },
    "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-vllm-throughput/versions/1",
    "acceleratorType": "H100_80GB",
    "acceleratorsPerInstance": 8,
    "totalAccelerators": 8,
    "provisioningState": "Accepted",
    "provisioningDetails": {
      "message": "Deployment queued. Provisioning GPU resources.",
      "lastOperationTimestamp": "2026-03-30T10:00:00Z"
    },
    "routes": {
      "chatCompletionsScoringPath": "/v1/chat/completions",
      "swagger": "/swagger.json"
    }
  },
  "sku": {
    "name": "GlobalManagedCompute",
    "capacity": 1
  },
  "systemData": {}
}
```

### SDK

```python
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.mgmt.cognitiveservices.models import (
    AcceleratorDeployment, AcceleratorDeploymentProperties,
    AcceleratorDeploymentModel, Sku,
)
from azure.identity import DefaultAzureCredential

cog = CognitiveServicesManagementClient(DefaultAzureCredential(), subscription_id)

deployment = AcceleratorDeployment(
    properties=AcceleratorDeploymentProperties(
        model=AcceleratorDeploymentModel(
            format="Custom",
            name="my-gpt-oss-120B",
            version="1",
            source="projects/my-project/models/my-gpt-oss-120B",
        ),
        deployment_template="azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-vllm-throughput/versions/1",
        accelerator_type="H100_80GB",
    ),
    sku=Sku(name="GlobalManagedCompute", capacity=1),
)

poller = cog.accelerator_deployments.begin_create_or_update(
    resource_group_name="my-rg",
    account_name="my-foundry-account",
    deployment_name="my-gpt-oss-120B-gpu",
    accelerator_deployment=deployment,
)
result = poller.result()  # waits ~10-15 min
print(f"State: {result.properties.provisioning_state}")
```

### CLI

```bash
az cognitiveservices account accelerator-deployment create \
  --name my-foundry-account -g my-rg \
  --deployment-name my-gpt-oss-120B-gpu \
  --model-format Custom \
  --model-name my-gpt-oss-120B \
  --model-version 1 \
  --model-source "projects/my-project/models/my-gpt-oss-120B" \
  --deployment-template "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-vllm-throughput/versions/1" \
  --accelerator-type H100_80GB \
  --sku-name GlobalManagedCompute \
  --sku-capacity 1
```

### Deployment Validation Rules

In addition to the standard deployment validation in [create-or-update.md](deployments_crud/create-or-update.md):

| Rule | Error |
|---|---|
| Custom model not found in project | `ModelNotFound: Model '{model}' not found in project '{project}'.` |
| Custom model not in `Succeeded` state | `ModelNotReady: Model '{model}' is in state '{state}'. Wait for registration to complete.` |
| Custom model's base model has no DT matching the request | `DeploymentTemplateNotCompatible: Template '{dt}' is not in base model's allowed templates.` |
| Custom model's storage is missing/corrupt | `ModelStorageMissing: Model weight files not found in project storage.` |

### Inference

Once `provisioningState` is `Succeeded`, inference works exactly like catalog deployments. The model is exposed at the account endpoint under the deployment name:

```
https://{account}.services.ai.azure.com/managed-deployments/{deploymentName}/v1/chat/completions
```

```python
from openai import AzureOpenAI

oai = AzureOpenAI(
    azure_endpoint="https://my-foundry-account.services.ai.azure.com",
    api_key=api_key,
    api_version="2025-09-01",
)

response = oai.chat.completions.create(
    model="my-gpt-oss-120B-gpu",  # deployment name
    messages=[{"role": "user", "content": "Explain the mechanism of action of metformin."}],
)
print(response.choices[0].message.content)
```

---

## E2E Hero Scenario: Train → Register → Resolve DT → Deploy → Infer

This combines all parts into a single end-to-end walkthrough.

```python
import time
from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import (
    CommandJob, JobResourceConfiguration, PyTorchDistribution,
    Input, Output, AssetTypes, InputOutputModes,
    CustomModel, ModelSource,
)
from azure.ai.ml import MLClient
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.mgmt.cognitiveservices.models import (
    AcceleratorDeployment, AcceleratorDeploymentProperties,
    AcceleratorDeploymentModel, Sku,
)
from openai import AzureOpenAI

credential = DefaultAzureCredential()
SUBSCRIPTION_ID = "..."
RG = "my-rg"
ACCOUNT = "my-foundry-account"
PROJECT = "my-project"

project_client = AIProjectClient(
    endpoint=f"https://{ACCOUNT}.services.ai.azure.com/api/projects/{PROJECT}",
    credential=credential,
)

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: Train a model (or skip if you already have weights)
# ═══════════════════════════════════════════════════════════════════════════════
job = CommandJob(
    command="python train.py --model_name_or_path ${inputs.model_dir} "
            "--dataset_name ${inputs.dataset} "
            "--output_dir ${outputs.safetensor_model_folder}",
    environment_image_reference="mcr.microsoft.com/azureml/minimal-ubuntu22.04-py39-cuda11.8-gpu-inference",
    compute="/subscriptions/.../computes/gpu-cluster",
    code="./src",
    inputs={
        "model_dir": Input(type=AssetTypes.URI_FOLDER, path="./models/gpt-oss-120B"),
        "dataset": Input(type=AssetTypes.URI_FOLDER, path="./datasets/med_mcqa"),
    },
    outputs={
        "safetensor_model_folder": Output(type=AssetTypes.SAFETENSORS_MODEL, mode=InputOutputModes.READ_WRITE_MOUNT),
    },
    resources=JobResourceConfiguration(instance_count=1, instance_type="Standard_ND96ISR_H100_V5"),
    distribution=PyTorchDistribution(process_count_per_instance=8),
)
created_job = project_client.beta.training.jobs.create_or_update(name="grpo-med-qa", job=job)

# Wait for training
while created_job.properties.status not in ("Completed", "Failed", "Canceled"):
    time.sleep(60)
    created_job = project_client.beta.training.jobs.get(name="grpo-med-qa")
assert created_job.properties.status == "Completed"

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2: Register the trained model as a custom model
# ═══════════════════════════════════════════════════════════════════════════════
BASE_MODEL = "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4"

model = project_client.models.create_or_update(
    CustomModel(
        name="my-gpt-oss-120B",
        version="1",
        type="FullWeight",
        base_model=BASE_MODEL,
        source=ModelSource(source_type="TrainingJob", job_name="grpo-med-qa", output_key="safetensor_model_folder"),
    )
)

while model.provisioning_state not in ("Succeeded", "Failed"):
    time.sleep(30)
    model = project_client.models.get(name="my-gpt-oss-120B")
assert model.provisioning_state == "Succeeded"

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3: Resolve deployment template from the base model
# ═══════════════════════════════════════════════════════════════════════════════
# Parse base model URI → registry_name="azureml-openai-oss", model_name="gpt-oss-120B", version="1"
parts = BASE_MODEL.replace("azureml://registries/", "").split("/")
registry_name, model_name, model_version = parts[0], parts[2], parts[4]

ml_client = MLClient(credential, SUBSCRIPTION_ID, resource_group=RG, registry_name=registry_name)
base_model = ml_client.models.get(name=model_name, version=model_version)

# Select the default deployment template
dt_uri = base_model.default_deployment_template.asset_id
dt_parts = dt_uri.replace("azureml://registries/", "").split("/")
template = ml_client.deployment_templates.get(name=dt_parts[2], version=dt_parts[4])

# Pick the default accelerator
accel = next(am for am in template.accelerator_maps if am.default)
print(f"Deploying with: {template.name} on {accel.accelerator_type} ({accel.number_of_accelerators_per_model_instance} GPUs/instance)")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4: Deploy the custom model
# ═══════════════════════════════════════════════════════════════════════════════
cog = CognitiveServicesManagementClient(credential, SUBSCRIPTION_ID)

deployment = AcceleratorDeployment(
    properties=AcceleratorDeploymentProperties(
        model=AcceleratorDeploymentModel(
            format="Custom",
            name="my-gpt-oss-120B",
            version="1",
            source=f"projects/{PROJECT}/models/my-gpt-oss-120B",
        ),
        deployment_template=dt_uri,
        accelerator_type=accel.accelerator_type,
    ),
    sku=Sku(name="GlobalManagedCompute", capacity=1),
)

poller = cog.accelerator_deployments.begin_create_or_update(RG, ACCOUNT, "my-gpt-oss-120B-gpu", deployment)
result = poller.result()  # ~10-15 min
print(f"Deployment state: {result.properties.provisioning_state}")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5: Run inference
# ═══════════════════════════════════════════════════════════════════════════════
account_info = cog.accounts.get(RG, ACCOUNT)
oai = AzureOpenAI(
    azure_endpoint=account_info.properties.endpoint,
    api_key=cog.accounts.list_keys(RG, ACCOUNT).key1,
    api_version="2025-09-01",
)

response = oai.chat.completions.create(
    model="my-gpt-oss-120B-gpu",
    messages=[{"role": "user", "content": "What is the mechanism of action of metformin?"}],
)
print(response.choices[0].message.content)
```

---

## Appendix: Model Object Schema

### Custom Model (Data Plane)

| Property | Type | Required | In Response | Description |
|---|---|---|---|---|
| `name` | `string` | Yes (in URL) | Yes | Model name. Must match `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,253}$`. |
| `version` | `string` | Yes (in URL) | Yes | Version string. Opaque identifier (not necessarily sequential). |
| `type` | `string` | Yes | Yes | `"FullWeight"` for Milestone 1. |
| `baseModel` | `string` (URI) | Yes | Yes | Fully-qualified catalog model reference. |
| `description` | `string` | No | Yes | Human-readable description. |
| `tags` | `object` | No | Yes | Key-value string pairs. |
| `source` | `ModelSource` | Depends | Yes | Source information — required for Cases 2 & 3. |
| `properties` | `object` | No | Yes | Additional metadata (format, parameterCount). |
| `status` | `string` | — | Read-only | `"PendingUpload"`, `"Creating"`, `"Registered"`, `"Failed"`, `"Expired"`, `"Cancelled"`. |
| `provisioningState` | `string` | — | Read-only | `"Creating"`, `"Succeeded"`, `"Failed"`. For Cases 2 & 3 (async operations). |
| `createdAt` | `string` (ISO 8601) | — | Read-only | Creation timestamp. |
| `storageUri` | `string` | — | Read-only | Internal storage reference. |

### ModelSource

| Property | Type | Cases | Description |
|---|---|---|---|
| `sourceType` | `string` | All | `"LocalUpload"`, `"TrainingJob"`, `"HuggingFace"`. |
| `jobName` | `string` | Case 2 | Name of the completed training job. |
| `outputKey` | `string` | Case 2 | Output key in the job (e.g., `"safetensor_model_folder"`). |
| `huggingFaceRepoId` | `string` | Case 3 | HF repo ID (e.g., `"openai-oss/gpt-oss-120B"`). |
| `revision` | `string` | Case 3 | Branch, tag, or commit SHA. Defaults to `"main"`. |
| `credentials.huggingFaceToken` | `string` | Case 3b | HF API token for gated/private repos. **Not persisted.** |

### AcceleratorDeploymentModel (Control Plane — Custom Models)

For custom model deployments, the `properties.model` field is an object instead of a string URI:

| Property | Type | Required | Description |
|---|---|---|---|
| `format` | `string` | Yes | `"Custom"` for BYOW models. |
| `name` | `string` | Yes | The custom model name (e.g., `"my-gpt-oss-120B"`). |
| `version` | `string` | Yes | The custom model version. |
| `source` | `string` | Yes | Data-plane model reference: `"projects/{project}/models/{modelName}"`. |
