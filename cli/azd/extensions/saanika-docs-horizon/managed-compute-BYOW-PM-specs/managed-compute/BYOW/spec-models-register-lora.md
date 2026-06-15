# LoRA Adapters — Registration

> **Parent spec:** [spec-models-deploy.md](spec-models-deploy.md)
> **Companion spec:** [spec-lora-deploy.md](spec-lora-deploy.md) — deploying, attaching, detaching, and inference routing with LoRA adapters
> **See also:** [spec-artifact-classification.md](spec-artifact-classification.md) — artifact classification and `artifactProfile` field

## Overview

This spec defines how to **register LoRA adapter weight artifacts** as first-class model assets in Foundry managed compute. It covers the three ingestion paths (local upload, training job output, Hugging Face import), listing adapters, and registration validation rules.

LoRA adapters are registered through the **same `/models/` API** as full-weight models, using the `weightType` discriminator (`"LoRA"` vs `"FullWeight"`). Once registered, adapters can be attached to running deployments — see [spec-lora-deploy.md](spec-lora-deploy.md) for deployment-level adapter operations.

### Scope

This spec covers:

- Registering a LoRA adapter as a first-class model asset (`weightType: "LoRA"`)
- Three ingestion paths: local upload, training job output, Hugging Face import
- Listing registered adapters
- Adapter registration validation rules
- Adapter model object schema (data plane)

**Out of scope** (covered in [spec-lora-deploy.md](spec-lora-deploy.md)):

- Attaching (hot-loading) an adapter onto a running base-model deployment
- Detaching (unloading) an adapter from a running deployment
- Listing attached adapters on a deployment
- Per-request adapter routing via the `model` field in inference calls
- Multi-LoRA: multiple adapters on a single deployment
- Deployment template requirements for LoRA
- End-to-end walkthrough (register → deploy → attach → infer → detach)

**Out of scope** (future milestones):

- Non-LoRA PEFT methods (IA3, ADALORA, PREFIX_TUNING, etc.)
- EAGLE/Medusa speculator registration (see [spec-models-deploy.md](spec-models-deploy.md#open-decisions))
- Adapter versioning (multiple versions per adapter name) — TBD
- Adapter auto-scaling (dynamic load/unload based on traffic patterns)
- LoRA on MoE architectures (DeepSeek V3) — deferred pending evaluation

### Prerequisites

1. **Base model identified.** The adapter's `baseModel` must reference the catalog model that the adapter was trained against. This is the same `baseModel` field used for full-weight models — a fully-qualified `azureml://` URI.
2. **Quota verified.** GPU quota for the base model deployment has been confirmed ([e2e-3-quota-capacity.md](e2e-3-quota-capacity.md)). Adapter attachment does **not** consume additional quota — it uses marginal GPU memory on existing instances.

### Key Design Decisions

- **Unified `/models/` API.** Adapters are registered through the same endpoint as full-weight models. The `weightType: "LoRA"` discriminator drives adapter-specific validation (baseModel required, loraConfig required, cannot deploy standalone). This follows the Fireworks AI pattern (`kind: "peft"` under `/v1/models`) and avoids doubling the API surface.
- **`baseModel` is required.** Every adapter must reference a catalog base model via the same `baseModel` URI format used for full-weight models. The base model determines deployment template compatibility and enables architecture validation.
- **`loraConfig` is required.** Rank, alpha, and optionally target modules must be specified at registration time (or auto-populated from `adapter_config.json`).
- **Adapters cannot be deployed standalone.** An adapter is always attached to an existing base-model deployment. There is no `acceleratorDeployments` create call for adapters.
- **SafeTensors enforcement.** Same as full-weight models — only `.safetensors` adapter files are accepted for Build.

### Terminology

| Term | Definition |
|---|---|
| **LoRA adapter** | A lightweight delta-weight artifact (Low-Rank Adaptation) that modifies specific layers of a base model. Registered via `PUT /models/{model}` with `weightType: "LoRA"`. |
| **Base model** | The foundation model that the adapter was trained against. Can be a catalog model referenced by `azureml://` URI. Must have LoRA-supporting deployment templates. |
| **loraConfig** | Adapter-specific metadata: rank (r), alpha (α), target modules, dropout. Drives serving engine configuration. |

---

## Part 1: Adapter Registration (Data Plane)

Adapter registration uses the **same** `/models/` endpoint and the same 3-step upload pattern as full-weight models. The `weightType: "LoRA"` discriminator activates adapter-specific validation.

### Endpoint

```
{account}.services.ai.azure.com/api/projects/{project}
```

### Required Artifacts

| Artifact | Required | Description |
|---|---|---|
| `adapter_model.safetensors` | **Yes** | LoRA delta weights. Typically a single file, 10s–100s of MB. Multi-file shards are supported but uncommon. |
| `adapter_model.bin` | **Alternative** | PyTorch-format LoRA delta weights. Accepted as an alternative to `.safetensors`. See Fireworks integration note below. |
| `tokenizer.json` / `tokenizer_config.json` | Optional (if modified) | Only needed if the adapter modifies the tokenizer (e.g., added special tokens). |

**Additional files required for the Fireworks integration: 
| `adapter_config.json` | **Recommended** | PEFT/HuggingFace-format config specifying rank, alpha, target modules, base model architecture. Auto-populates `loraConfig` fields if present. |
| `fireworks.json` | Optional | Fireworks-specific configuration for overriding generation defaults (stop tokens, max_tokens, temperature). Passed through to the Fireworks backend at deployment time. Not parsed or validated by the Foundry registration service. See Fireworks integration note below. |

> **Size comparison:** Full-weight models are typically 10s–100s of GB. LoRA adapters are typically 10–500 MB (proportional to rank × number of target modules). This makes adapters suitable for browser upload in the UI, unlike full-weight models.

> **Fireworks integration note — `.bin` file support and `fireworks.json`.**
>
> **`.bin` files:** Fireworks accepts both `adapter_model.bin` (PyTorch) and `adapter_model.safetensors` as LoRA weight formats. The spec accepts both for LoRA adapters to avoid forcing customers to convert formats before registration. SafeTensors is preferred for security (no arbitrary code execution) and performance (memory-mapped loading), but `.bin` is still widely produced by PEFT/HuggingFace training pipelines. Full-weight models remain `.safetensors`-only. If a backend does not support `.bin`, the RP should convert or reject at deployment time, not at registration.
>
> **`fireworks.json`:** This is an optional Fireworks-specific file that overrides generation defaults (`has_lora: true`, stop tokens, temperature, etc.). The Foundry registration service stores it as an opaque artifact alongside the adapter weights — it is not parsed, validated, or exposed in the model schema. The RP passes it through to Fireworks during the `POST /v1/accounts/{account_id}/models` call. If a customer does not include it, Fireworks uses the base model's generation defaults.

### Adapter-Specific Fields

The following fields are **required** when `weightType: "LoRA"` and **ignored/absent** when `weightType: "FullWeight"`:

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
| **Case 2** | Azure Storage URI | User provides a SAS URL to adapter files already in Azure Blob Storage. Single-step registration — no local upload needed. |
| **Case 3** | Training job output | User declares a LoRA adapter output in the job spec. The training job auto-creates and auto-registers the adapter during job finalize. Same pattern as full-weight model auto-registration. |
| **Case 4** | Hugging Face import | User provides a HF repo ID for a LoRA adapter repo. System pulls adapter weights from HF Hub. Supports public and gated/private repos. |

### Case 1: Local Upload

Three-step flow identical to full-weight models, with `weightType: "LoRA"` and adapter-specific fields.

#### REST API

**Step 1: Start Pending Upload**

```http
POST {account}.services.ai.azure.com/api/projects/{project}
    /models/my-llama-70b-lora-medical/versions/1/startPendingUpload?api-version=v1
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
    /models/my-llama-70b-lora-medical/versions/1?api-version=v1
Content-Type: application/json
Authorization: Bearer {token}
```

```json
{
  "weightType": "LoRA",
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
  "weightType": "LoRA",
  "baseModel": "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
  "description": "LoRA adapter fine-tuned for medical Q&A",
  "loraConfig": {
    "rank": 16,
    "alpha": 32,
    "targetModules": ["q_proj", "v_proj", "k_proj", "o_proj"],
    "dropout": 0.05
  },
  "provisioningState": "Creating",
  "systemData": {
    "createdAt": "2026-04-15T10:30:00Z"
  },
  "artifactProfile": {
    "category": "DataOnly",
    "signals": []
  },
  "tags": {
    "domain": "medical",
    "training-data": "med-mcqa"
  }
}
```

> **Artifact classification:** `artifactProfile` is populated synchronously at the registration commit step for local uploads. See [spec-artifact-classification.md](spec-artifact-classification.md).

#### SDK

```python
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import Model, LoRAConfig
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
    Model(
        name="my-llama-70b-lora-medical",
        version="1",
        weight_type="LoRA",
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
print(f"Artifact profile: {adapter.artifact_profile.category}")  # e.g. "DataOnly"
```

#### CLI (azd)

```bash
# Register LoRA adapter from local files
azd ai models create \
  --name my-llama-70b-lora-medical \
  --source ./adapters/lora-medical/ \
  --base-model "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2" \
  --weight-type LoRA \
  --lora-rank 16 \
  --lora-alpha 32 \
  --description "LoRA adapter fine-tuned for medical Q&A"
```

> After running `azd ai models init`, no additional flags are needed. Without init, pass `-e` and `-s` explicitly.

> **Open:** The `--weight-type`, `--lora-rank`, and `--lora-alpha` flags are spec-level requirements. Whether the CLI surfaces these flags in its current design iteration requires confirmation with CLI team (Radhika/Amit).

### Case 2: Azure Storage URI

Single-step registration for adapter files that already exist in Azure Blob Storage. The user provides a SAS URL pointing to the blob container with the adapter artifacts — no local upload or `startPendingUpload` step needed.

#### REST API

```http
PUT {account}.services.ai.azure.com/api/projects/{project}
    /models/my-llama-70b-lora-medical/versions/1?api-version=v1
Content-Type: application/json
Authorization: Bearer {token}
```

```json
{
  "weightType": "LoRA",
  "baseModel": "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
  "description": "LoRA adapter fine-tuned for medical Q&A",
  "source": {
    "sourceType": "AzureBlob",
    "sasUri": "https://mystorage.blob.core.windows.net/adapters/lora-medical?sp=r&se=2026-05-01T..."
  },
  "loraConfig": {
    "rank": 16,
    "alpha": 32,
    "targetModules": ["q_proj", "v_proj", "k_proj", "o_proj"]
  }
}
```

> **Note:** The SAS URI must grant read access to the blob container. The service copies adapter files from the user's storage into project-managed storage during registration. The original blob is not modified.

**Response (201 Created)**

```json
{
  "name": "my-llama-70b-lora-medical",
  "version": "1",
  "weightType": "LoRA",
  "baseModel": "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
  "source": {
    "sourceType": "AzureBlob"
  },
  "loraConfig": {
    "rank": 16,
    "alpha": 32,
    "targetModules": ["q_proj", "v_proj", "k_proj", "o_proj"]
  },
  "provisioningState": "Creating",
  "systemData": {
    "createdAt": "2026-04-15T10:30:00Z"
  }
}
```

> **Artifact classification:** `artifactProfile` is not set in the initial `Creating` response for non-local sources. It is populated during the service's finalization step — before `provisioningState` transitions to `Succeeded`. See [spec-artifact-classification.md](spec-artifact-classification.md).

> Registration is async. Poll `GET /models/{name}/versions/{version}` until `provisioningState: "Succeeded"`.

#### SDK

```python
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import Model, ModelSource, LoRAConfig
from azure.identity import DefaultAzureCredential

client = AIProjectClient(
    endpoint="https://my-foundry-account.services.ai.azure.com/api/projects/my-project",
    credential=DefaultAzureCredential(),
)

adapter = client.models.create_or_update(
    Model(
        name="my-llama-70b-lora-medical",
        version="1",
        weight_type="LoRA",
        base_model="azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
        source=ModelSource(
            source_type="AzureBlob",
            sas_uri="https://mystorage.blob.core.windows.net/adapters/lora-medical?sp=r&se=...",
        ),
        lora_config=LoRAConfig(rank=16, alpha=32,
                               target_modules=["q_proj", "v_proj", "k_proj", "o_proj"]),
    )
)
print(f"Adapter state: {adapter.provisioning_state}")  # "Creating" — copy in progress
```

#### CLI (azd)

```bash
# Register LoRA adapter from Azure Blob Storage
azd ai models create \
  --name my-llama-70b-lora-medical \
  --source "https://mystorage.blob.core.windows.net/adapters/lora-medical?sp=r&se=..." \
  --base-model "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2" \
  --weight-type LoRA \
  --lora-rank 16 \
  --lora-alpha 32
```

> The CLI detects that `--source` is a `https://` URL (not a local path) and uses the Azure Blob ingestion path instead of the local upload flow.

### Case 3: Training Job Output

Training jobs can auto-register LoRA adapters the same way they auto-register full-weight models. The user declares a model output with `type: "LoRA"` in the job spec.

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
            weight_type="LoRA",
            base_model="azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
        ),
    },
    resources=JobResourceConfiguration(instance_count=1, instance_type="Standard_NC48ads_A100_v4"),
)
created_job = client.beta.training.jobs.create_or_update(name="lora-medical-qa", job=job)
print(f"Job submitted: {created_job.name} (status: {created_job.properties.status})")
```

> **Note:** Training jobs are submitted via the training API, not through `azd ai models`. The adapter is auto-registered on job completion during the common runtime finalize step. See [spec-models-register-training-job.md](spec-models-register-training-job.md) for the full auto-registration pattern.

After the job completes, verify the auto-registered adapter:

```bash
# Verify auto-registered adapter from job output
azd ai models list --source-job-id lora-medical-qa

# Show adapter details
azd ai models show --name my-llama-70b-lora-medical
```

#### REST API (Job Spec)

```http
PUT {account}.services.ai.azure.com/api/projects/{project}
    /training/jobs/lora-medical-qa?api-version=v1
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
      "weightType": "LoRA",
      "baseModel": "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2"
    }
  }
}
```

### Case 4: Hugging Face Import

Single-step registration. The service pulls adapter weights from Hugging Face Hub.

#### REST API

```http
PUT {account}.services.ai.azure.com/api/projects/{project}
    /models/my-glora-medical/versions/1?api-version=v1
Content-Type: application/json
Authorization: Bearer {token}
```

**Case 4a: Public HF repo**

```json
{
  "weightType": "LoRA",
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

**Case 4b: Gated/private HF repo**

```json
{
  "weightType": "LoRA",
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

> The `huggingFaceToken` is used only during the pull operation and is **NOT persisted** in the model metadata. See [spec-models-register-hf.md](spec-models-register-hf.md) for full HF import details.

#### SDK

```python
# Case 4a: Public HF adapter
from azure.ai.projects.models import Model, ModelSource, LoRAConfig

adapter = client.models.create_or_update(
    Model(
        name="my-glora-medical",
        version="1",
        weight_type="LoRA",
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
# Case 4a: Public HF adapter
azd ai models create \
  --name my-glora-medical \
  --source "https://huggingface.co/predibase/glora-medical-llama-3" \
  --base-model "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2" \
  --weight-type LoRA \
  --lora-rank 16 \
  --lora-alpha 32
```

> **Open:** HF import for gated repos requires a token. The current CLI design spec does not include `--huggingface-token`. Gated adapter imports are SDK/REST only until CLI flags are designed. See [spec-models-register-hf.md](spec-models-register-hf.md) for the open item.

---

### Listing Adapters

Adapters are listed through the `ListLatestModels` operation (`GET /models`), which returns the latest version of each registered model as a paginated list of `FoundryModelDto`. Filtering by `weightType` uses the Index Service pattern (not REST query parameters).

#### REST API

```http
GET {account}.services.ai.azure.com/api/projects/{project}/models?api-version=v1
Authorization: Bearer {token}
```

> **Note (Item 13):** REST query parameters for filtering are **not used**. Use Index Service for search and filtering. The list endpoint returns all models (both full-weight and adapters). Client-side filtering by `weightType` is the interim pattern until Index Service integration is available.

#### SDK

```python
# List all models (includes both full-weight and adapters)
for model in client.models.list():
    print(f"{model.name}: {model.weight_type}")

# Filter client-side for adapters
adapters = [m for m in client.models.list() if m.weight_type == "LoRA"]
for a in adapters:
    print(f"Adapter: {a.name} (base: {a.base_model})")
```

#### CLI (azd)

```bash
# List all models (includes adapters)
azd ai models list

# Show a specific adapter
azd ai models show --name my-llama-70b-lora-medical
```

---

### Validation Rules (Adapter Registration)

| Rule | Error |
|---|---|
| `baseModel` omitted | `BaseModelRequired: The 'baseModel' field is required for LoRA adapters.` |
| Base model not found in catalog | `BaseModelNotFound: Base model '{base_model}' not found in catalog.` |
| Base model is a BYOW model that is not in `Registered` / `Succeeded` state | `BaseModelNotReady: Base model '{base_model}' is in state '{state}'. The base model must be in 'Registered' (Succeeded) state before a LoRA adapter referencing it can be registered.` |
| Base model has no LoRA-supporting DTs | `LoRANotSupported: No deployment templates for base model '{base_model}' support LoRA serving.` |
| `loraConfig` omitted and no `adapter_config.json` | `MissingLoraConfig: 'loraConfig' (rank, alpha) is required for LoRA adapters. Provide in request body or include adapter_config.json in upload.` |
| Weight files not `.safetensors` or `.bin` (LoRA only) | `UnsupportedWeightFormat: Only SafeTensors (.safetensors) and PyTorch (.bin) adapter files are accepted for LoRA. Full-weight models require SafeTensors only.` |
| No adapter weight files found | `NoAdapterWeights: No adapter weight files (adapter_model.safetensors or adapter_model.bin) found in upload.` |
| `weightType` is `"LoRA"` but `loraConfig.rank` ≤ 0 | `InvalidLoraConfig: loraConfig.rank must be a positive integer.` |
| `loraConfig.alpha` ≤ 0 | `InvalidLoraConfig: loraConfig.alpha must be a positive integer.` |
| `loraConfig.targetModules` is empty array | `InvalidLoraConfig: loraConfig.targetModules must be a non-empty array of strings if provided.` |
| `loraConfig.targetModules` contains empty or blank strings | `InvalidLoraConfig: Each entry in loraConfig.targetModules must be a non-empty string.` |
| `loraConfig.rank` provided but `loraConfig.alpha` missing (and no `adapter_config.json`) | `MissingLoraAlpha: loraConfig.alpha is required when loraConfig.rank is provided.` |

> **Design note — backend-specific constraints are not enforced at registration time.** Registration validates only structural correctness (rank is a positive integer, required fields present, valid file formats). Backend-specific constraints — such as Fireworks' requirement that rank be between 4 and 64, or allowed `targetModules` values — are enforced at **deployment time** by the RP when uploading to the backend. This keeps the Foundry model registry backend-agnostic: a rank=2 or rank=128 adapter can be registered and stored even if a specific serving backend doesn't support it today. The same principle applies to `targetModules` — any valid string array is accepted at registration; module compatibility is validated against the serving engine at deployment.

See [spec-models-deploy.md](spec-models-deploy.md#validation-rules-all-cases) for validation rules common to all model types.

---

## Next Steps

After adapter registration completes:
- [Deploy, attach, and run inference with LoRA adapters](spec-lora-deploy.md)
- [Part B: Bridge — Resolve Deployment Template](spec-models-deploy.md#part-b-bridge--from-registered-model-to-deployment-template)

---

## Appendix: Adapter Model Object Schema

### LoRA Adapter (Data Plane)

Extends the base model schema from [spec-models-deploy.md](spec-models-deploy.md#appendix-model-object-schema) with adapter-specific fields.

| Property | Type | Required | In Response | Description |
|---|---|---|---|---|
| `name` | `string` | Yes | Yes | Adapter name. Same naming rules as full-weight models: `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,253}$`. |
| `version` | `string` | Yes | Yes | Version string. |
| `weightType` | `string` (enum) | Yes | Yes | `"LoRA"`. Case-insensitive at ingestion. |
| `baseModel` | `string` (URI) | **Yes** | Yes | Fully-qualified catalog model reference. **Required** for adapters (unlike full-weight models where it references the architecture-compatible model). |
| `loraConfig` | `LoRAConfig` | Yes* | Yes | Adapter-specific configuration. Can be omitted if `adapter_config.json` is present in uploaded files (auto-populated). See LoRAConfig table below. |
| `description` | `string` | No | Yes | Human-readable description. |
| `tags` | `object` | No | Yes | Key-value string pairs. User-controlled only. |
| `source` | `ModelSource` | Depends | Yes | Source information — required for Cases 2 & 3. Same schema as full-weight models. |
| `properties` | `object` | No | Yes | Additional metadata (format). |
| `provisioningState` | `string` | — | Read-only | `"Creating"`, `"Succeeded"`, `"Failed"`. |
| `createdAt` | `string` (ISO 8601) | — | Read-only | Creation timestamp. Available via `systemData.createdAt`. |
| `blobUri` | `string` | — | Read-only | Blob storage URI for the model weights. |

### LoRAConfig

| Property | Type | Required | Description |
|---|---|---|---|
| `rank` | `integer` | Yes | LoRA rank (r). Must be a positive integer. Common values: 8, 16, 32, 64. |
| `alpha` | `integer` | Yes | LoRA scaling factor (α). Typically 2× the rank. |
| `targetModules` | `string[]` | No | Model layers modified by the adapter. Auto-detected from `adapter_config.json` if omitted. Examples: `["q_proj", "v_proj", "k_proj", "o_proj"]`. |
| `dropout` | `float` | No | Dropout rate used during training. Informational — not used at serving time. |

### RP Mapping: Foundry `loraConfig` → Fireworks `peftDetails`

> **Audience:** RP team. This section documents how the Foundry-facing `loraConfig` fields map to the Fireworks `peftDetails` object when the RP calls `POST /v1/accounts/{account_id}/models` to create the adapter on the Fireworks side.

| Foundry field (`loraConfig`) | Fireworks field (`peftDetails`) | Notes |
|---|---|---|
| `loraConfig.rank` | `peftDetails.r` | Direct mapping. Fireworks requires 4 ≤ r ≤ 64 — validated at deployment time by RP, not at registration |
| `loraConfig.targetModules` | `peftDetails.targetModules` | Direct mapping. Fireworks restricts to 8 allowed values — validated at deployment time by RP |
| `loraConfig.alpha` | *(no Fireworks equivalent)* | Foundry-only. Informational for reproducibility; not sent to Fireworks |
| `loraConfig.dropout` | *(no Fireworks equivalent)* | Foundry-only. Informational; not sent to Fireworks |
| *(not exposed to user)* | `peftDetails.mergeAddonModelName` | RP-internal. Left empty for live merge; used for offline persistent merging (future) |
| `weightType: "LoRA"` | `kind: "HF_PEFT_ADDON"` | Discriminator mapping. `"FullWeight"` maps to `kind: "HF_BASE_MODEL"` |
| `baseModel` | `peftDetails.baseModel` | RP resolves the Foundry catalog URI to a Fireworks resource name (`accounts/{acct}/models/{baseId}`) |
