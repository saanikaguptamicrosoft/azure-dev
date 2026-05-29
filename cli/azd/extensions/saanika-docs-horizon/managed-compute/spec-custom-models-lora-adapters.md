# LoRA Adapters — Registration, Loading, and Unloading

> **Parent spec:** [spec-custom-models-deploy.md](spec-custom-models-deploy.md)

## Overview

This spec defines the end-to-end experience for **LoRA adapters** on Foundry managed compute — registering adapter weight artifacts, attaching (loading) them onto running deployments, routing inference to specific adapters, and detaching (unloading) them.

LoRA adapters are registered through the **same `/models/` API** as full-weight models, using the `weightType` discriminator (`"LoRAAdapter"` vs `"FullWeight"`). Deployment-level adapter operations — attach, detach, list — are **separate runtime endpoints** under `/deployments/{dep}/adapters` because they operate on live GPU memory, not stored weight artifacts.

### Scope

This spec covers:

- Registering a LoRA adapter as a first-class model asset (`weightType: "LoRAAdapter"`)
- Three ingestion paths: local upload, training job output, Hugging Face import
- Attaching (hot-loading) an adapter onto a running base-model deployment
- Detaching (unloading) an adapter from a running deployment
- Listing attached adapters on a deployment
- Per-request adapter routing via the `model` field in inference calls
- Multi-LoRA: multiple adapters on a single deployment

**Out of scope** (future milestones):

- Non-LoRA PEFT methods (IA3, ADALORA, PREFIX_TUNING, etc.)
- EAGLE/Medusa speculator registration (see [spec-custom-models-deploy.md](spec-custom-models-deploy.md#open-decisions))
- Adapter versioning (multiple versions per adapter name) — TBD
- Adapter auto-scaling (dynamic load/unload based on traffic patterns)
- LoRA on MoE architectures (DeepSeek V3) — deferred pending evaluation

### Prerequisites

1. **Base model deployed.** A deployment running a base model (either a BYOW full-weight model or a catalog model) must exist before adapters can be attached. The deployment template must support LoRA (`VLLM_ENABLE_LORA: "true"` or `SGLANG_ENABLE_LORA: "true"`).
2. **Base model identified.** The adapter's `baseModel` must reference the catalog model that the adapter was trained against. This is the same `baseModel` field used for full-weight models — a fully-qualified `azureml://` URI.
3. **Quota verified.** GPU quota for the base model deployment has been confirmed ([e2e-3-quota-capacity.md](e2e-3-quota-capacity.md)). Adapter attachment does **not** consume additional quota — it uses marginal GPU memory on existing instances.

### Key Design Decisions

- **Unified `/models/` API.** Adapters are registered through the same endpoint as full-weight models. The `weightType: "LoRAAdapter"` discriminator drives adapter-specific validation (baseModel required, loraConfig required, cannot deploy standalone). This follows the Fireworks AI pattern (`kind: "peft"` under `/v1/models`) and avoids doubling the API surface.
- **`baseModel` is required.** Every adapter must reference a catalog base model via the same `baseModel` URI format used for full-weight models. The base model determines deployment template compatibility and enables architecture validation.
- **`loraConfig` is required.** Rank, alpha, and optionally target modules must be specified at registration time (or auto-populated from `adapter_config.json`).
- **Deployment-layer operations are separate.** Attaching an adapter hot-loads weights into GPU memory. Detaching frees adapter slots. These are runtime operations with no model-API equivalent — they correctly live under `/deployments/{dep}/adapters`, not `/models/`.
- **Adapters cannot be deployed standalone.** An adapter is always attached to an existing base-model deployment. There is no `acceleratorDeployments` create call for adapters.
- **SafeTensors enforcement.** Same as full-weight models — only `.safetensors` adapter files are accepted for Build.

### Terminology

| Term | Definition |
|---|---|
| **LoRA adapter** | A lightweight delta-weight artifact (Low-Rank Adaptation) that modifies specific layers of a base model. Registered via `PUT /models/{model}` with `weightType: "LoRAAdapter"`. |
| **Base model** | The foundation model that the adapter was trained against. Can be a catalog model referenced by `azureml://` URI. Must have LoRA-supporting deployment templates. |
| **Attach** | Hot-load adapter weights into GPU memory on a running deployment. Runtime operation via `POST /deployments/{dep}/adapters`. |
| **Detach** | Unload adapter weights from GPU memory. Frees adapter slot immediately. Runtime operation via `DELETE /deployments/{dep}/adapters/{adapter}`. |
| **Multi-LoRA** | Multiple adapters loaded simultaneously on a single base-model deployment. Requests routed to specific adapters via the `model` field. |
| **loraConfig** | Adapter-specific metadata: rank (r), alpha (α), target modules, dropout. Drives serving engine configuration. |

---

## Part 1: Adapter Registration (Data Plane)

Adapter registration uses the **same** `/models/` endpoint and the same 3-step upload pattern as full-weight models. The `weightType: "LoRAAdapter"` discriminator activates adapter-specific validation.

### Endpoint

```
{account}.services.ai.azure.com/api/projects/{project}
```

### Required Artifacts

| Artifact | Required | Description |
|---|---|---|
| `adapter_model.safetensors` | **Yes** | LoRA delta weights. Typically a single file, 10s–100s of MB. Multi-file shards are supported but uncommon. |
| `adapter_config.json` | **Recommended** | PEFT/HuggingFace-format config specifying rank, alpha, target modules, base model architecture. Auto-populates `loraConfig` fields if present. |
| `tokenizer.json` / `tokenizer_config.json` | Optional (if modified) | Only needed if the adapter modifies the tokenizer (e.g., added special tokens). |

> **Size comparison:** Full-weight models are typically 10s–100s of GB. LoRA adapters are typically 10–500 MB (proportional to rank × number of target modules). This makes adapters suitable for browser upload in the UI, unlike full-weight models.

### Adapter-Specific Fields

The following fields are **required** when `weightType: "LoRAAdapter"` and **ignored/absent** when `weightType: "FullWeight"`:

| Field | Type | Required | Description |
|---|---|---|---|
| `loraConfig.rank` | `integer` | Yes* | LoRA rank (r). Common values: 8, 16, 32, 64. |
| `loraConfig.alpha` | `integer` | Yes* | LoRA scaling factor (α). Typically 2× the rank. |
| `loraConfig.targetModules` | `string[]` | No | Which model layers the adapter modifies (e.g., `["q_proj", "v_proj", "k_proj", "o_proj"]`). Inferred from `adapter_config.json` if omitted. |
| `loraConfig.dropout` | `float` | No | Dropout used during training. Informational for reproducibility. |

> **\*Required unless auto-populated.** `loraConfig` (including `rank` and `alpha`) can be omitted entirely from the request body if `adapter_config.json` is present in the uploaded files. See auto-population note below.

> **Auto-population:** If `adapter_config.json` is present in the uploaded files, the system auto-populates `loraConfig` fields that the user did not explicitly provide. User-provided values take precedence over auto-detected values.

### Ingestion Paths

| Case | Source | Description |
|---|---|---|
| **Case 1** | Local machine upload | User uploads adapter files from their local machine via SAS URI. Same 3-step pattern as full-weight models: `startPendingUpload` → upload → `PutModel`. |
| **Case 2** | Training job output | User declares a LoRA adapter output in the job spec. The training job auto-creates and auto-registers the adapter during job finalize. Same pattern as full-weight model auto-registration. |
| **Case 3** | Hugging Face import | User provides a HF repo ID for a LoRA adapter repo. System pulls adapter weights from HF Hub. Supports public and gated/private repos. |

### Case 1: Local Upload

Three-step flow identical to full-weight models, with `weightType: "LoRAAdapter"` and adapter-specific fields.

#### REST API

**Step 1: Start Pending Upload**

```http
POST {account}.services.ai.azure.com/api/projects/{project}
    /models/my-llama-70b-lora-medical/versions/1:startPendingUpload?api-version=2025-06-01-preview
Content-Type: application/json
Authorization: Bearer {token}
```

**Request Body**

```json
{
  "pendingUploadType": "TemporaryBlobReference"
}
```

**Response (200 OK)**

```json
{
  "pendingUploadId": "pu-adapter-abc123",
  "pendingUploadType": "TemporaryBlobReference",
  "blobReference": {
    "sasUri": "https://projstorage.blob.core.windows.net/models/my-llama-70b-lora-medical/v1?sp=rcw&se=2026-04-20T...",
    "containerPath": "models/my-llama-70b-lora-medical/v1/",
    "expiresOn": "2026-04-20T20:00:00Z"
  }
}
```

**Step 2: Upload adapter files**

```bash
# Adapters are small — azcopy still recommended for consistency, but direct upload is also viable
azcopy copy "./adapters/lora-medical/*" \
  "https://projstorage.blob.core.windows.net/models/my-llama-70b-lora-medical/v1?sp=rcw&se=..." \
  --recursive
```

**Step 3: PutModel (commit registration)**

```http
PUT {account}.services.ai.azure.com/api/projects/{project}
    /models/my-llama-70b-lora-medical/versions/1?api-version=2025-06-01-preview
Content-Type: application/json
Authorization: Bearer {token}
```

```json
{
  "weightType": "LoRAAdapter",
  "baseModel": "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
  "description": "LoRA adapter fine-tuned for medical Q&A",
  "loraConfig": {
    "rank": 16,
    "alpha": 32,
    "targetModules": ["q_proj", "v_proj", "k_proj", "o_proj"],
    "dropout": 0.05
  },
  "tags": {
    "domain": "medical",
    "training-data": "med-mcqa"
  }
}
```

> **Note:** `name` and `version` are provided in the URL path, not in the request body.

**Response (201 Created)**

The response follows the standard model object schema with adapter-specific fields. Since `PUT /models` is asynchronous, the initial response shows `provisioningState: "Creating"`. Poll `GET /models/{name}/versions/{version}` until `provisioningState: "Succeeded"`.

```json
{
  "name": "my-llama-70b-lora-medical",
  "version": "1",
  "weightType": "LoRAAdapter",
  "baseModel": "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
  "description": "LoRA adapter fine-tuned for medical Q&A",
  "loraConfig": {
    "rank": 16,
    "alpha": 32,
    "targetModules": ["q_proj", "v_proj", "k_proj", "o_proj"],
    "dropout": 0.05
  },
  "provisioningState": "Creating",
  "createdAt": "2026-04-15T10:30:00Z",
  "tags": {
    "domain": "medical",
    "training-data": "med-mcqa"
  }
}
```

#### SDK

```python
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import CustomModel, LoRAConfig
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
client = AIProjectClient(
    endpoint="https://my-foundry-account.services.ai.azure.com/api/projects/my-project",
    credential=credential,
)

# Step 1: Start pending upload
upload_info = client.models.start_pending_upload(
    model_name="my-llama-70b-lora-medical",
    version="1",
    pending_upload_type="TemporaryBlobReference",
)

# Step 2: Upload adapter files via azcopy
import subprocess
subprocess.run([
    "azcopy", "copy",
    "./adapters/lora-medical/*",
    upload_info.blob_reference.sas_uri,
    "--recursive",
], check=True)

# Step 3: Commit registration
adapter = client.models.create_or_update(
    CustomModel(
        name="my-llama-70b-lora-medical",
        version="1",
        weight_type="LoRAAdapter",
        base_model="azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
        description="LoRA adapter fine-tuned for medical Q&A",
        lora_config=LoRAConfig(
            rank=16,
            alpha=32,
            target_modules=["q_proj", "v_proj", "k_proj", "o_proj"],
            dropout=0.05,
        ),
        tags={"domain": "medical"},
    )
)
print(f"Adapter registered: {adapter.name} v{adapter.version} ({adapter.weight_type})")
```

#### CLI (azd)

```bash
# Register LoRA adapter from local files
azd ai models custom create \
  --name my-llama-70b-lora-medical \
  --source ./adapters/lora-medical/ \
  --base-model "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2" \
  --weight-type LoRAAdapter \
  --lora-rank 16 \
  --lora-alpha 32 \
  --description "LoRA adapter fine-tuned for medical Q&A"
```

> After running `azd ai models init`, no additional flags are needed. Without init, pass `-e` and `-s` explicitly.

> **Open:** The `--weight-type`, `--lora-rank`, and `--lora-alpha` flags are spec-level requirements. Whether the CLI surfaces these flags in its current design iteration requires confirmation with CLI team (Radhika/Amit).

### Case 2: Training Job Output

Training jobs can auto-register LoRA adapters the same way they auto-register full-weight models. The user declares a model output with `type: "LoRAAdapter"` in the job spec.

#### Job Spec

```python
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import (
    CommandJob, JobResourceConfiguration,
    Input, Output, AssetTypes, InputOutputModes,
)
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
client = AIProjectClient(
    endpoint="https://my-foundry-account.services.ai.azure.com/api/projects/my-project",
    credential=credential,
)

job = CommandJob(
    command="python finetune_lora.py "
            "--model_name_or_path ${inputs.base_model_dir} "
            "--dataset ${inputs.dataset} "
            "--output_dir ${outputs.lora_adapter} "
            "--lora_rank 16 --lora_alpha 32",
    environment_image_reference="mcr.microsoft.com/azureml/minimal-ubuntu22.04-py39-cuda11.8-gpu-inference",
    compute="/subscriptions/.../computes/gpu-cluster",
    code="./src",
    inputs={
        "base_model_dir": Input(
            type=AssetTypes.URI_FOLDER,
            path="azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
            mode=InputOutputModes.READ_ONLY_MOUNT,
        ),
        "dataset": Input(
            type=AssetTypes.URI_FOLDER,
            path="./datasets/med_mcqa",
            mode=InputOutputModes.READ_ONLY_MOUNT,
        ),
    },
    outputs={
        "lora_adapter": Output(
            type=AssetTypes.SAFETENSORS_MODEL,
            mode=InputOutputModes.READ_WRITE_MOUNT,
            model_name="my-llama-70b-lora-medical",
            weight_type="LoRAAdapter",
            base_model="azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
        ),
    },
    resources=JobResourceConfiguration(instance_count=1, instance_type="Standard_NC48ads_A100_v4"),
)
created_job = client.beta.training.jobs.create_or_update(name="lora-medical-qa", job=job)
print(f"Job submitted: {created_job.name} (status: {created_job.properties.status})")
```

> **Note:** Training jobs are submitted via the training API, not through `azd ai models`. The adapter is auto-registered on job completion during the common runtime finalize step. See [spec-custom-models-create-ft-job.md](spec-custom-models-create-ft-job.md) for the full auto-registration pattern.

After the job completes, verify the auto-registered adapter:

```bash
# Verify auto-registered adapter from job output
azd ai models custom list --source-job-id lora-medical-qa

# Show adapter details
azd ai models custom show --name my-llama-70b-lora-medical
```

#### REST API (Job Spec)

```http
PUT {account}.services.ai.azure.com/api/projects/{project}
    /training/jobs/lora-medical-qa?api-version=2025-06-01-preview
Content-Type: application/json
Authorization: Bearer {token}
```

```json
{
  "command": "python finetune_lora.py --model_name_or_path ${inputs.base_model_dir} --output_dir ${outputs.lora_adapter} --lora_rank 16 --lora_alpha 32",
  "outputs": {
    "lora_adapter": {
      "type": "Model",
      "modelName": "my-llama-70b-lora-medical",
      "weightType": "LoRAAdapter",
      "baseModel": "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2"
    }
  }
}
```

### Case 3: Hugging Face Import

Single-step registration. The service pulls adapter weights from Hugging Face Hub.

#### REST API

```http
PUT {account}.services.ai.azure.com/api/projects/{project}
    /models/my-glora-medical/versions/1?api-version=2025-06-01-preview
Content-Type: application/json
Authorization: Bearer {token}
```

**Case 3a: Public HF repo**

```json
{
  "weightType": "LoRAAdapter",
  "baseModel": "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
  "source": {
    "sourceType": "HuggingFace",
    "huggingFaceRepoId": "predibase/glora-medical-llama-3",
    "revision": "main"
  },
  "loraConfig": {
    "rank": 16,
    "alpha": 32
  }
}
```

> **Note:** If the HF repo contains `adapter_config.json`, the system auto-populates `loraConfig` fields (rank, alpha, targetModules) that the user did not explicitly provide. User-provided values take precedence.

**Case 3b: Gated/private HF repo**

```json
{
  "weightType": "LoRAAdapter",
  "baseModel": "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
  "source": {
    "sourceType": "HuggingFace",
    "huggingFaceRepoId": "my-org/clinical-lora-llama-70b",
    "revision": "main",
    "credentials": {
      "huggingFaceToken": "<hf_token>"
    }
  },
  "loraConfig": {
    "rank": 32,
    "alpha": 64
  }
}
```

> The `huggingFaceToken` is used only during the pull operation and is **NOT persisted** in the model metadata. See [spec-custom-models-create-hf.md](spec-custom-models-create-hf.md) for full HF import details.

#### SDK

```python
# Case 3a: Public HF adapter
from azure.ai.projects.models import CustomModel, ModelSource, LoRAConfig

adapter = client.models.create_or_update(
    CustomModel(
        name="my-glora-medical",
        version="1",
        weight_type="LoRAAdapter",
        base_model="azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
        source=ModelSource(
            source_type="HuggingFace",
            hugging_face_repo_id="predibase/glora-medical-llama-3",
            revision="main",
        ),
        lora_config=LoRAConfig(rank=16, alpha=32),
    )
)
print(f"Adapter state: {adapter.provisioning_state}")  # "Creating" — pull in progress
```

#### CLI (azd)

```bash
# Case 3a: Public HF adapter
azd ai models custom create \
  --name my-glora-medical \
  --source "https://huggingface.co/predibase/glora-medical-llama-3" \
  --base-model "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2" \
  --weight-type LoRAAdapter \
  --lora-rank 16 \
  --lora-alpha 32
```

> **Open:** HF import for gated repos requires a token. The current CLI design spec does not include `--huggingface-token`. Gated adapter imports are SDK/REST only until CLI flags are designed. See [spec-custom-models-create-hf.md](spec-custom-models-create-hf.md) for the open item.

---

### Listing Adapters

Adapters are listed through the same `/models` endpoint. Filtering by `weightType` uses the Index Service pattern (not REST query parameters).

#### REST API

```http
GET {account}.services.ai.azure.com/api/projects/{project}/models?api-version=2025-06-01-preview
Authorization: Bearer {token}
```

> **Note (Item 13):** REST query parameters for filtering are **not used**. Use Index Service for search and filtering. The list endpoint returns all models (both full-weight and adapters). Client-side filtering by `weightType` is the interim pattern until Index Service integration is available.

#### SDK

```python
# List all models (includes both full-weight and adapters)
for model in client.models.list():
    print(f"{model.name}: {model.weight_type} — {model.status}")

# Filter client-side for adapters
adapters = [m for m in client.models.list() if m.weight_type == "LoRAAdapter"]
for a in adapters:
    print(f"Adapter: {a.name} (base: {a.base_model})")
```

#### CLI (azd)

```bash
# List all models (includes adapters)
azd ai models custom list

# Show a specific adapter
azd ai models custom show --name my-llama-70b-lora-medical
```

---

### Validation Rules (Adapter Registration)

| Rule | Error |
|---|---|
| `baseModel` omitted | `BaseModelRequired: The 'baseModel' field is required for LoRA adapters.` |
| Base model not found in catalog | `BaseModelNotFound: Base model '{base_model}' not found in catalog.` |
| Base model has no LoRA-supporting DTs | `LoRANotSupported: No deployment templates for base model '{base_model}' support LoRA serving.` |
| `loraConfig` omitted and no `adapter_config.json` | `MissingLoraConfig: 'loraConfig' (rank, alpha) is required for LoRA adapters. Provide in request body or include adapter_config.json in upload.` |
| Weight files not `.safetensors` | `UnsupportedWeightFormat: Only SafeTensors (.safetensors) files are accepted.` |
| No adapter weight files found | `NoAdapterWeights: No adapter weight files (adapter_model.safetensors) found in upload.` |
| `weightType` is `"LoRAAdapter"` but `loraConfig.rank` ≤ 0 | `InvalidLoraConfig: loraConfig.rank must be a positive integer.` |

See [spec-custom-models-deploy.md](spec-custom-models-deploy.md#validation-rules-all-cases) for validation rules common to all model types.

---

## Part 2: Adapter Loading and Unloading (Deployment-Level Operations)

Adapter attach/detach operations are **data-plane runtime operations** on running deployments. They are fundamentally different from model CRUD — they manage live GPU memory, not stored weight artifacts.

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

Loads adapter weights into GPU memory on all instances of the deployment. The adapter must be a registered model with `weightType: "LoRAAdapter"` whose `baseModel` matches the deployment's base model.

#### REST API

```http
POST {account}.services.ai.azure.com/api/projects/{project}
    /deployments/llama70b-prod/adapters?api-version=2025-06-01-preview
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
    /deployments/llama70b-prod/adapters/my-llama-70b-lora-medical?api-version=2025-06-01-preview
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
    /deployments/llama70b-prod/adapters?api-version=2025-06-01-preview
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
| Adapter not found | `AdapterNotFound: Model '{adapter}' with weightType 'LoRAAdapter' not found in project.` |
| Adapter not ready | `AdapterNotReady: Adapter '{adapter}' is in state '{state}'. Requires provisioningState 'Succeeded'.` |
| Adapter base model ≠ deployment base model | `AdapterBaseModelMismatch: Adapter base model '{adapter_base}' does not match deployment base model '{dep_base}'.` |
| Deployment template does not support LoRA | `LoRANotSupportedByTemplate: Deployment template '{dt}' does not support LoRA (ENABLE_LORA not set).` |
| Max adapters exceeded | `MaxAdaptersExceeded: Deployment allows max {n} adapters. Currently {current} attached.` |
| Deployment not running | `DeploymentNotRunning: Deployment '{dep}' is in state '{state}'. Adapters can only be attached to running deployments.` |
| Adapter already attached | `AdapterAlreadyAttached: Adapter '{adapter}' is already attached to deployment '{dep}'.` |
| Insufficient GPU memory | `InsufficientAdapterMemory: Not enough GPU memory to load adapter '{adapter}' ({size} MB). Available: {available} MB.` |
| Adapter not attached (detach) | `AdapterNotAttached: Adapter '{adapter}' is not attached to deployment '{dep}'.` |

---

## Part 3: Inference Routing

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

## Part 4: Deployment Template Requirements for LoRA

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

```python
import time
from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import CustomModel, LoRAConfig
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
    CustomModel(
        name="my-llama-70b-lora-medical",
        version="1",
        weight_type="LoRAAdapter",
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

## Appendix: Adapter Model Object Schema

### LoRA Adapter (Data Plane)

Extends the base model schema from [spec-custom-models-deploy.md](spec-custom-models-deploy.md#appendix-model-object-schema) with adapter-specific fields.

| Property | Type | Required | In Response | Description |
|---|---|---|---|---|
| `name` | `string` | Yes | Yes | Adapter name. Same naming rules as full-weight models: `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,253}$`. |
| `version` | `string` | Yes | Yes | Version string. |
| `weightType` | `string` (enum) | Yes | Yes | `"LoRAAdapter"`. Case-insensitive at ingestion. |
| `baseModel` | `string` (URI) | **Yes** | Yes | Fully-qualified catalog model reference. **Required** for adapters (unlike full-weight models where it references the architecture-compatible model). |
| `loraConfig` | `LoRAConfig` | Yes* | Yes | Adapter-specific configuration. Can be omitted if `adapter_config.json` is present in uploaded files (auto-populated). See LoRAConfig table below. |
| `description` | `string` | No | Yes | Human-readable description. |
| `tags` | `object` | No | Yes | Key-value string pairs. User-controlled only. |
| `source` | `ModelSource` | Depends | Yes | Source information — required for Cases 2 & 3. Same schema as full-weight models. |
| `properties` | `object` | No | Yes | Additional metadata (sizeBytes, fileCount, format). |
| `status` | `string` | — | Read-only | `"Creating"`, `"Registered"`, `"Failed"`, `"Cancelled"`. |
| `provisioningState` | `string` | — | Read-only | `"Creating"`, `"Succeeded"`, `"Failed"`. |
| `createdAt` | `string` (ISO 8601) | — | Read-only | Creation timestamp. |
| `storageUri` | `string` | — | Read-only | Internal storage reference. |

### LoRAConfig

| Property | Type | Required | Description |
|---|---|---|---|
| `rank` | `integer` | Yes | LoRA rank (r). Must be a positive integer. Common values: 8, 16, 32, 64. |
| `alpha` | `integer` | Yes | LoRA scaling factor (α). Typically 2× the rank. |
| `targetModules` | `string[]` | No | Model layers modified by the adapter. Auto-detected from `adapter_config.json` if omitted. Examples: `["q_proj", "v_proj", "k_proj", "o_proj"]`. |
| `dropout` | `float` | No | Dropout rate used during training. Informational — not used at serving time. |

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

---

