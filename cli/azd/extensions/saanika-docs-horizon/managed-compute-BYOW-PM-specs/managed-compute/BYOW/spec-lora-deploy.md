# LoRA Adapters — Deployment, Loading, Unloading, and Inference

> **Parent spec:** [spec-models-deploy.md](spec-models-deploy.md)
> **Companion spec:** [spec-models-register-lora.md](spec-models-register-lora.md) — registering LoRA adapter weights

## Overview

This spec defines the **deployment-level operations** for LoRA adapters on Foundry managed compute — attaching (hot-loading) registered adapters onto running deployments, detaching (unloading) them, routing inference to specific adapters, and deployment template requirements for LoRA support.

Adapter attach/detach operations are **data-plane runtime operations** on running deployments. They are fundamentally different from model registration — they manage live GPU memory, not stored weight artifacts. For adapter registration, see [spec-models-register-lora.md](spec-models-register-lora.md).

### Scope

This spec covers:

- Attaching (hot-loading) an adapter onto a running base-model deployment
- Detaching (unloading) an adapter from a running deployment
- Listing attached adapters on a deployment
- Per-request adapter routing via the `model` field in inference calls
- Multi-LoRA: multiple adapters on a single deployment
- Deployment template requirements for LoRA
- End-to-end walkthrough (register → deploy → attach → infer → detach)
- Attach/detach validation rules
- DeploymentAdapter schema

**Out of scope** (covered in [spec-models-register-lora.md](spec-models-register-lora.md)):

- Registering a LoRA adapter as a first-class model asset
- Three ingestion paths: local upload, training job output, Hugging Face import
- Listing registered adapters (via `/models` endpoint)
- Adapter registration validation rules
- Adapter model object schema (data plane)

### Prerequisites

1. **Adapter registered.** A LoRA adapter must be registered as a model asset with `weightType: "LoRA"` and `provisioningState: "Succeeded"`. See [spec-models-register-lora.md](spec-models-register-lora.md).
2. **Base model deployed.** A deployment running a base model (either a BYOW full-weight model or a catalog model) must exist before adapters can be attached. The deployment template must support LoRA (`VLLM_ENABLE_LORA: "true"` or `SGLANG_ENABLE_LORA: "true"`).
3. **Base model match.** The adapter's `baseModel` must match the deployment's base model. Architecture mismatches are rejected.

### Key Design Decisions

- **Deployment-layer operations are separate.** Attaching an adapter hot-loads weights into GPU memory. Detaching frees adapter slots. These are runtime operations with no model-API equivalent — they correctly live under `/deployments/{dep}/adapters`, not `/models/`.
- **Zero downtime.** Base model continues serving during attach/detach. Other adapters are unaffected.
- **No additional quota.** Adapter attachment uses marginal GPU memory (~50 MB per adapter) on existing instances. No new GPU allocation or quota consumed.

### Terminology

| Term | Definition |
|---|---|
| **Attach** | Hot-load adapter weights into GPU memory on a running deployment. Runtime operation via `POST /deployments/{dep}/adapters`. |
| **Detach** | Unload adapter weights from GPU memory. Frees adapter slot immediately. Runtime operation via `DELETE /deployments/{dep}/adapters/{adapter}`. |
| **Multi-LoRA** | Multiple adapters loaded simultaneously on a single base-model deployment. Requests routed to specific adapters via the `model` field. |

---

## Adapter Loading and Unloading (Deployment-Level Operations)

### Key Properties

| Property | Value |
|---|---|
| Does attach create a new deployment? | **No.** One deployment, N adapters as sub-resources. |
| GPU cost of attach | Marginal. ~50 MB VRAM per adapter (proportional to rank × target modules). No new GPU allocation. |
| Attach latency | Seconds. Adapter weights are small. No base model reload. |
| Detach latency | Immediate. Memory freed, inference routing updated. |
| Downtime on attach/detach | **Zero.** Base model continues serving. Other adapters unaffected. |
| Max adapters per deployment | Deployment template's `VLLM_MAX_LORAS` or `SGLANG_MAX_LORAS` (practical limit: 16–64 depending on GPU memory). |
| Adapter slot memory | ~50 MB per adapter (rank 16, 4 target modules on 70B model). Scales linearly with rank and target module count. |

### Endpoint

```
{account}.services.ai.azure.com/api/projects/{project}/deployments/{deploymentName}/adapters
```

### Operations Summary

| Operation | HTTP Method | Path | Description |
|---|---|---|---|
| **Attach adapter** | `POST` | `/deployments/{dep}/adapters` | Hot-load adapter weights into GPU memory. |
| **Detach adapter** | `DELETE` | `/deployments/{dep}/adapters/{adapterName}` | Unload adapter from GPU memory. Frees slot immediately. |
| **List attached adapters** | `GET` | `/deployments/{dep}/adapters` | List all adapters currently loaded on a deployment. |

---

### Attach Adapter

Loads adapter weights into GPU memory on all instances of the deployment. The adapter must be a registered model with `weightType: "LoRA"` whose `baseModel` matches the deployment's base model.

#### REST API

```http
POST {account}.services.ai.azure.com/api/projects/{project}
    /deployments/llama70b-prod/adapters?api-version=v1
Content-Type: application/json
Authorization: Bearer {token}
```

```json
{
  "adapterName": "my-llama-70b-lora-medical",
  "adapterSource": "projects/my-project/models/my-llama-70b-lora-medical"
}
```

**Response (201 Created)**

```json
{
  "adapterName": "my-llama-70b-lora-medical",
  "adapterSource": "projects/my-project/models/my-llama-70b-lora-medical",
  "status": "Loading",
  "attachedAt": "2026-04-15T10:30:00Z",
  "memorySizeMB": 48
}
```

> **Status transitions:** `Loading` → `Loaded` (ready for inference) or `Failed` (weights incompatible or OOM). Poll via `GET /deployments/{dep}/adapters` until status is `Loaded`.

#### SDK

```python
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential

client = AIProjectClient(
    endpoint="https://my-foundry-account.services.ai.azure.com/api/projects/my-project",
    credential=DefaultAzureCredential(),
)

result = client.deployments.attach_adapter(
    deployment_name="llama70b-prod",
    adapter_name="my-llama-70b-lora-medical",
)
print(f"Adapter status: {result.status}")  # "Loading" → poll until "Loaded"
```

#### CLI (azd)

```bash
azd ai models deployment attach-adapter \
  --deployment-name llama70b-prod \
  --adapter-name my-llama-70b-lora-medical
```

> The CLI blocks until the adapter status is `Loaded` (or `Failed`). Use `--no-wait` to return immediately.

---

### Detach Adapter

Unloads adapter weights from GPU memory. Frees the adapter slot immediately. No deployment restart. No downtime for the base model or other attached adapters.

#### REST API

```http
DELETE {account}.services.ai.azure.com/api/projects/{project}
    /deployments/llama70b-prod/adapters/my-llama-70b-lora-medical?api-version=v1
Authorization: Bearer {token}
```

**Response: `204 No Content`**

> After detach, inference requests with `"model": "my-llama-70b-lora-medical"` return `AdapterNotLoaded` error.

#### SDK

```python
client.deployments.detach_adapter(
    deployment_name="llama70b-prod",
    adapter_name="my-llama-70b-lora-medical",
)
# Adapter memory freed immediately.
```

#### CLI (azd)

```bash
azd ai models deployment detach-adapter \
  --deployment-name llama70b-prod \
  --adapter-name my-llama-70b-lora-medical
```

---

### List Attached Adapters

Returns all adapters currently loaded on a deployment, with status and memory usage per adapter.

#### REST API

```http
GET {account}.services.ai.azure.com/api/projects/{project}
    /deployments/llama70b-prod/adapters?api-version=v1
Authorization: Bearer {token}
```

**Response (200 OK)**

```json
{
  "value": [
    {
      "adapterName": "my-llama-70b-lora-medical",
      "adapterSource": "projects/my-project/models/my-llama-70b-lora-medical",
      "status": "Loaded",
      "attachedAt": "2026-04-15T10:30:00Z",
      "memorySizeMB": 48
    },
    {
      "adapterName": "my-llama-70b-lora-legal",
      "adapterSource": "projects/my-project/models/my-llama-70b-lora-legal",
      "status": "Loaded",
      "attachedAt": "2026-04-15T10:32:00Z",
      "memorySizeMB": 52
    }
  ],
  "deploymentName": "llama70b-prod",
  "baseModel": "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
  "maxAdapters": 32,
  "currentAdapterCount": 2
}
```

#### SDK

```python
adapters = client.deployments.list_adapters(deployment_name="llama70b-prod")
for adapter in adapters:
    print(f"{adapter.adapter_name}: {adapter.status} ({adapter.memory_size_mb} MB)")
```

#### CLI (azd)

```bash
azd ai models deployment list-adapters \
  --deployment-name llama70b-prod
```

**Output:**

```
Name                          Status    Attached             Memory
──────────────────────────────────────────────────────────────
my-llama-70b-lora-medical     Loaded    2026-04-15T10:30:00  48 MB
my-llama-70b-lora-legal       Loaded    2026-04-15T10:32:00  52 MB

Base model: Llama-3.3-70B-Instruct | Adapters: 2/32
```

---

### Attach/Detach Validation Rules

| Rule | Error |
|---|---|
| Adapter not found | `AdapterNotFound: Model '{adapter}' with weightType 'LoRA' not found in project.` |
| Adapter not ready | `AdapterNotReady: Adapter '{adapter}' is in state '{state}'. Requires provisioningState 'Succeeded'.` |
| Adapter base model ≠ deployment base model | `AdapterBaseModelMismatch: Adapter base model '{adapter_base}' does not match deployment base model '{dep_base}'.` |
| Deployment template does not support LoRA | `LoRANotSupportedByTemplate: Deployment template '{dt}' does not support LoRA (ENABLE_LORA not set).` |
| Max adapters exceeded | `MaxAdaptersExceeded: Deployment allows max {n} adapters. Currently {current} attached.` |
| Deployment not running | `DeploymentNotRunning: Deployment '{dep}' is in state '{state}'. Adapters can only be attached to running deployments.` |
| Adapter already attached | `AdapterAlreadyAttached: Adapter '{adapter}' is already attached to deployment '{dep}'.` |
| Insufficient GPU memory | `InsufficientAdapterMemory: Not enough GPU memory to load adapter '{adapter}' ({size} MB). Available: {available} MB.` |
| Adapter not attached (detach) | `AdapterNotAttached: Adapter '{adapter}' is not attached to deployment '{dep}'.` |

---

## Inference Routing

Inference calls to a deployment with attached adapters use two routing layers:

| Routing Layer | Where | What It Selects |
|---|---|---|
| URL path | `/openai/deployments/{deployment-id}/...` | Which deployment (GPU allocation, base model). Standard Azure AI Foundry routing. |
| Request body | `"model": "{value}"` | Which adapter to apply on that deployment. Passed to vLLM/SGLang, which looks up the adapter in its loaded adapter registry. |

### Resolution Rules

| `model` value | Behavior |
|---|---|
| Matches an attached adapter name | Adapter applied to base model for this request |
| Matches the deployment name | Base model used directly, no adapter applied |
| Matches the base model name | Base model used directly, no adapter applied |
| Matches nothing | Error 400: `AdapterNotLoaded: Adapter '{name}' is not attached to deployment '{dep}'. Attached adapters: [{list}].` |

### Inference with Adapter

```http
POST {account}.services.ai.azure.com/openai/deployments/llama70b-prod/chat/completions?api-version=2025-09-01
Content-Type: application/json
Authorization: Bearer {token}
```

```json
{
  "model": "my-llama-70b-lora-medical",
  "messages": [
    {"role": "user", "content": "What are the symptoms of pneumonia?"}
  ]
}
```

**Response (200 OK)**

```json
{
  "model": "my-llama-70b-lora-medical",
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": "The symptoms of pneumonia include..."
      }
    }
  ]
}
```

### Inference with Base Model (No Adapter)

```json
{
  "model": "llama70b-prod",
  "messages": [
    {"role": "user", "content": "Hello, what can you help with?"}
  ]
}
```

### SDK

```python
from openai import AzureOpenAI

oai = AzureOpenAI(
    azure_endpoint="https://my-foundry-account.services.ai.azure.com",
    api_key=api_key,
    api_version="2025-09-01",
)

# Inference with medical adapter
response = oai.chat.completions.create(
    model="llama70b-prod",                                    # deployment name (URL routing)
    extra_body={"model": "my-llama-70b-lora-medical"},        # adapter name (body routing)
    messages=[{"role": "user", "content": "What are the symptoms of pneumonia?"}],
)
print(response.choices[0].message.content)

# Inference with legal adapter (same deployment, different adapter)
response = oai.chat.completions.create(
    model="llama70b-prod",
    extra_body={"model": "my-llama-70b-lora-legal"},
    messages=[{"role": "user", "content": "Explain force majeure in contract law."}],
)

# Inference with base model (no adapter)
response = oai.chat.completions.create(
    model="llama70b-prod",
    messages=[{"role": "user", "content": "Hello, what can you help with?"}],
)
```

> **Note on SDK routing:** The Azure OpenAI SDK uses `model=` to set the deployment name in the URL path. To override the `model` field in the request body for adapter routing, use `extra_body`. This is a known ergonomic gap — see Open Questions below.

---

## Deployment Template Requirements for LoRA

Deployment templates that support LoRA must declare LoRA capability via environment variables. Not all templates support LoRA — the capability depends on the serving framework and architecture.

### LoRA-Capable DT Environment Variables

| Variable | Framework | Description |
|---|---|---|
| `VLLM_ENABLE_LORA` | vLLM | `"true"` to enable LoRA adapter serving. |
| `VLLM_MAX_LORAS` | vLLM | Maximum number of concurrent LoRA adapters. Default: `"32"`. |
| `SGLANG_ENABLE_LORA` | SGLang | `"true"` to enable LoRA adapter serving. |
| `SGLANG_MAX_LORAS` | SGLang | Maximum number of concurrent LoRA adapters. Default: `"32"`. |

> **DT selection for LoRA deployments:** The DT is determined by the **base model's** architecture. LoRA weights are loaded as an overlay — they do not change serving infrastructure. The user selects a DT via the base model's `allowed_deployment_templates`, not via the adapter.

### Architecture → LoRA Support (Build)

| Architecture | LoRA DTs Available | Notes |
|---|---|---|
| `LlamaForCausalLM` | **Yes** — all 4 Llama DTs (vLLM + SGLang, latency + throughput) | Primary LoRA architecture. Most adapters in the ecosystem. |
| `GPTBigCodeForCausalLM` | **Yes** — all 4 GPT-oss DTs | LoRA for code generation fine-tuning. |
| `DeepseekV3ForCausalLM` | **No** — deferred | MoE architecture makes LoRA complex (which experts to target). Under evaluation. |

---

## End-to-End: Register Adapter → Deploy Base Model → Attach → Infer → Detach

This walkthrough combines adapter registration ([spec-models-register-lora.md](spec-models-register-lora.md)) with deployment operations into a single script.

```python
import time
from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import Model, LoRAConfig
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
BASE_MODEL = "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2"

project_client = AIProjectClient(
    endpoint=f"https://{ACCOUNT}.services.ai.azure.com/api/projects/{PROJECT}",
    credential=credential,
)

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: Register LoRA adapter (local upload)
#         See spec-models-register-lora.md for all ingestion paths
# ═══════════════════════════════════════════════════════════════════════════════
import subprocess

upload_info = project_client.models.start_pending_upload(
    model_name="my-llama-70b-lora-medical", version="1",
    pending_upload_type="TemporaryBlobReference",
)
subprocess.run([
    "azcopy", "copy", "./adapters/lora-medical/*",
    upload_info.blob_reference.sas_uri, "--recursive",
], check=True)

adapter = project_client.models.create_or_update(
    Model(
        name="my-llama-70b-lora-medical",
        version="1",
        weight_type="LoRA",
        base_model=BASE_MODEL,
        lora_config=LoRAConfig(rank=16, alpha=32,
                               target_modules=["q_proj", "v_proj", "k_proj", "o_proj"]),
    )
)
print(f"Adapter registered: {adapter.name} v{adapter.version}")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2: Resolve deployment template from the base model (Part B)
# ═══════════════════════════════════════════════════════════════════════════════
parts = BASE_MODEL.replace("azureml://registries/", "").split("/")
registry_name, model_name, model_version = parts[0], parts[2], parts[4]

ml_client = MLClient(credential, SUBSCRIPTION_ID, resource_group=RG, registry_name=registry_name)
base_model_obj = ml_client.models.get(name=model_name, version=model_version)

dt_uri = base_model_obj.default_deployment_template.asset_id
dt_parts = dt_uri.replace("azureml://registries/", "").split("/")
template = ml_client.deployment_templates.get(name=dt_parts[2], version=dt_parts[4])

accel = next(am for am in template.accelerator_maps if am.default)
print(f"Deploying with: {template.name} on {accel.accelerator_type}")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3: Deploy the base model (control plane)
#         The deployment must exist before adapters can be attached.
# ═══════════════════════════════════════════════════════════════════════════════
cog = CognitiveServicesManagementClient(credential, SUBSCRIPTION_ID)

deployment = AcceleratorDeployment(
    properties=AcceleratorDeploymentProperties(
        model=AcceleratorDeploymentModel(
            format="Custom",
            name="my-llama-70b",
            version="1",
            source=f"projects/{PROJECT}/models/my-llama-70b",
        ),
        deployment_template=dt_uri,
        accelerator_type=accel.accelerator_type,
    ),
    sku=Sku(name="GlobalManagedCompute", capacity=1),
)

poller = cog.accelerator_deployments.begin_create_or_update(
    RG, ACCOUNT, "llama70b-prod", deployment,
)
result = poller.result()  # ~10-15 min
print(f"Base deployment ready: {result.properties.provisioning_state}")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4: Attach adapter (data plane — hot-load into GPU memory)
# ═══════════════════════════════════════════════════════════════════════════════
attach_result = project_client.deployments.attach_adapter(
    deployment_name="llama70b-prod",
    adapter_name="my-llama-70b-lora-medical",
)
print(f"Adapter status: {attach_result.status}")  # "Loading"

# Poll until loaded
while attach_result.status == "Loading":
    time.sleep(5)
    adapters = project_client.deployments.list_adapters(deployment_name="llama70b-prod")
    attach_result = next(a for a in adapters if a.adapter_name == "my-llama-70b-lora-medical")
assert attach_result.status == "Loaded"
print("Adapter loaded and ready for inference")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5: Run inference with adapter
# ═══════════════════════════════════════════════════════════════════════════════
account_info = cog.accounts.get(RG, ACCOUNT)
oai = AzureOpenAI(
    azure_endpoint=account_info.properties.endpoint,
    api_key=cog.accounts.list_keys(RG, ACCOUNT).key1,
    api_version="2025-09-01",
)

response = oai.chat.completions.create(
    model="llama70b-prod",
    extra_body={"model": "my-llama-70b-lora-medical"},
    messages=[{"role": "user", "content": "What is the mechanism of action of metformin?"}],
)
print(response.choices[0].message.content)

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6: Detach adapter (frees GPU memory immediately)
# ═══════════════════════════════════════════════════════════════════════════════
project_client.deployments.detach_adapter(
    deployment_name="llama70b-prod",
    adapter_name="my-llama-70b-lora-medical",
)
print("Adapter detached — memory freed")
```

---

## Appendix: Deployment Adapter Schemas

### DeploymentAdapter (Attach Response)

| Property | Type | Description |
|---|---|---|
| `adapterName` | `string` | Name of the attached adapter (matches registered model name). |
| `adapterSource` | `string` | Data-plane model reference: `"projects/{project}/models/{adapterName}"`. |
| `status` | `string` | `"Loading"`, `"Loaded"`, `"Failed"`. |
| `attachedAt` | `string` (ISO 8601) | Timestamp when attach was initiated. |
| `memorySizeMB` | `integer` | Estimated GPU memory consumed by this adapter. |

### DeploymentAdapterList (List Response)

| Property | Type | Description |
|---|---|---|
| `value` | `DeploymentAdapter[]` | Array of attached adapters. |
| `deploymentName` | `string` | Name of the deployment. |
| `baseModel` | `string` (URI) | The deployment's base model reference. |
| `maxAdapters` | `integer` | Maximum adapters this deployment can host (from DT config). |
| `currentAdapterCount` | `integer` | Number of adapters currently attached. |
