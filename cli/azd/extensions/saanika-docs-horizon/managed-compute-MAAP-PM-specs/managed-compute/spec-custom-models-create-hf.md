# Custom Model Registration — Hugging Face Import

> **Parent spec:** [spec-custom-models.md](spec-custom-models.md)

The user provides a Hugging Face repo ID. The system pulls weights directly from HF Hub into project storage. This is a **single-step** registration — the service handles the download internally.

Supports both public repos (no authentication) and gated/private repos (requires HF API token).

---

## Case 3a: Public Hugging Face Model

No authentication required.

**REST API**

```http
PUT {account}.services.ai.azure.com/api/projects/{project}
    /models/my-gpt-oss-120B/versions/1?api-version=2025-06-01-preview
Content-Type: application/json
Authorization: Bearer {token}
```

**Request Body**

```json
{
  "type": "FullWeight",
  "baseModel": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "source": {
    "sourceType": "HuggingFace",
    "huggingFaceRepoId": "openai-oss/gpt-oss-120B",
    "revision": "main"
  },
  "properties": {
    "format": "safetensors"
  }
}
```

**Response (201 Created)**

```json
{
  "name": "my-gpt-oss-120B",
  "version": "1",
  "type": "FullWeight",
  "baseModel": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "provisioningState": "Creating",
  "source": {
    "sourceType": "HuggingFace",
    "huggingFaceRepoId": "openai-oss/gpt-oss-120B",
    "revision": "main"
  }
}
```

**SDK**

```python
from azure.ai.projects.models import CustomModel, ModelSource

model = client.models.create_or_update(
    CustomModel(
        name="my-gpt-oss-120B",
        version="1",
        type="FullWeight",
        base_model="azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
        source=ModelSource(
            source_type="HuggingFace",
            hugging_face_repo_id="openai-oss/gpt-oss-120B",
            revision="main",
        ),
    )
)
print(f"Model state: {model.provisioning_state}")  # "Creating" — pull in progress
```

**CLI**

```bash
{new-project-cli} model create \
  --account my-foundry-account \
  --project my-project \
  --name my-gpt-oss-120B \
  --type FullWeight \
  --base-model "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4" \
  --huggingface-repo-id openai-oss/gpt-oss-120B \
  --huggingface-revision main
```

---

## Case 3b: Gated or Private Hugging Face Model

Requires a Hugging Face API token. The user must have accepted the model's license on huggingface.co.

**REST API**

```http
PUT {account}.services.ai.azure.com/api/projects/{project}
    /models/my-gpt-oss-120B/versions/1?api-version=2025-06-01-preview
Content-Type: application/json
Authorization: Bearer {token}
```

**Request Body**

```json
{
  "type": "FullWeight",
  "baseModel": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "source": {
    "sourceType": "HuggingFace",
    "huggingFaceRepoId": "openai-oss/gpt-oss-120B",
    "revision": "main",
    "credentials": {
      "huggingFaceToken": "<hf_token>"
    }
  },
  "properties": {
    "format": "safetensors"
  }
}
```

> **Security:** The `huggingFaceToken` is used only during the pull operation and is **not persisted** in model metadata. The response does not include the token.

**SDK**

```python
model = client.models.create_or_update(
    CustomModel(
        name="my-gpt-oss-120B",
        version="1",
        type="FullWeight",
        base_model="azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
        source=ModelSource(
            source_type="HuggingFace",
            hugging_face_repo_id="openai-oss/gpt-oss-120B",
            revision="main",
            hugging_face_token="<hf_token>",  # NOT persisted; used only during pull
        ),
    )
)
print(f"Model state: {model.provisioning_state}")  # "Creating" — authenticated pull in progress
```

**CLI**

```bash
{new-project-cli} model create \
  --account my-foundry-account \
  --project my-project \
  --name my-gpt-oss-120B \
  --type FullWeight \
  --base-model "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4" \
  --huggingface-repo-id openai-oss/gpt-oss-120B \
  --huggingface-revision main \
  --huggingface-token <hf_token>
```

> The `--huggingface-token` can also be provided via the `HUGGING_FACE_HUB_TOKEN` environment variable.

---

## Provisioning Status Flow

| Source | Status Flow |
|---|---|
| Public HF | `Creating` (pull from HF) → `Succeeded` / `Failed` |
| Gated HF | `Creating` (authenticated pull) → `Succeeded` / `Failed` |

---

## Validation Rules (Hugging Face Import)

| Rule | Error |
|---|---|
| HF repo not found | `HuggingFaceRepoNotFound: Repository '{repo_id}' not found on HF Hub.` |
| HF authentication failed | `HuggingFaceAuthFailed: Token is invalid or does not have access to '{repo_id}'.` |
| HF gated model — license not accepted | `HuggingFaceLicenseRequired: Accept the license at https://huggingface.co/{repo_id}.` |
| HF repo has no model weights | `HuggingFaceNoWeights: Repository '{repo_id}' has no weight files.` |

See [spec-custom-models.md](spec-custom-models.md#validation-rules-all-cases) for validation rules common to all ingestion paths.

---

## Next Steps

After model registration completes, resolve the deployment template and deploy:
- [Part B: Bridge — Resolve Deployment Template](spec-custom-models.md#part-b-bridge--from-registered-model-to-deployment-template)
- [Part C: Deploy the Custom Model](spec-custom-models.md#part-c-custom-model-deployment-control-plane)
