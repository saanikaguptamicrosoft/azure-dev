# Model Registration — Hugging Face Import

> **Parent spec:** [spec-models-deploy.md](spec-models-deploy.md)
> **See also:** [spec-artifact-classification.md](spec-artifact-classification.md) — artifact classification and `artifactProfile` field

The user provides a Hugging Face repo ID. The system pulls weights directly from HF Hub into project storage. This is a **single-step** registration — the service handles the download internally.

Supports both public repos (no authentication) and gated/private repos (requires HF API token)

---

## Out of Scope for Build
This scenario is not in scope for Build

---

## Case 3a: Public Hugging Face Model

No authentication required.

**REST API**

```http
PUT {account}.services.ai.azure.com/api/projects/{project}
    /models/my-gpt-oss-120B/versions/1?api-version=v1
Content-Type: application/json
Authorization: Bearer {token}
```

**Request Body**

```json
{
  "weightType": "FullWeight",
  "baseModel": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "source": {
    "sourceType": "HuggingFace",
    "huggingFaceRepoId": "openai-oss/gpt-oss-120B",
    "revision": "main"
  }
}
```

**Response (201 Created)**

```json
{
  "name": "my-gpt-oss-120B",
  "version": "1",
  "weightType": "FullWeight",
  "baseModel": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "provisioningState": "Creating",
  "source": {
    "sourceType": "HuggingFace",
    "huggingFaceRepoId": "openai-oss/gpt-oss-120B",
    "revision": "main"
  }
}
```

> **Artifact classification:** `artifactProfile` is not set in the initial `Creating` response. It is populated during the service's finalization step — before `provisioningState` transitions to `Succeeded`. See [spec-artifact-classification.md](spec-artifact-classification.md).

**SDK**

```python
model = client.models.create_or_update(
    model_name="my-gpt-oss-120B",
    version="1",
    weight_type="FullWeight",
    base_model="azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
    source_type="HuggingFace",
    hugging_face_repo_id="openai-oss/gpt-oss-120B",
    revision="main",
)
print(f"Model state: {model.provisioning_state}")  # "Creating" — pull in progress
```

**CLI (azd)**

```bash
azd ai models create \
  --name my-gpt-oss-120B \
  --source "https://huggingface.co/openai-oss/gpt-oss-120B" \
  --base-model "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4"
```

> **Note:** The `azd ai models create` command accepts `--source` as a local path or remote URL. For public HF models, the HF repo URL can be passed directly. The CLI design spec does not currently define HF-specific flags (`--huggingface-revision`, `--huggingface-token`). See the open item below.

> After running `azd ai models init`, the `--project-endpoint` (`-e`) and `--subscription` (`-s`) flags are not needed. Without init, pass them explicitly:

```bash
azd ai models create \
  -e "https://my-account.services.ai.azure.com/api/projects/my-project" \
  -s "8861a79b-1234-5678-abcd-1234567890ab" \
  --name my-gpt-oss-120B \
  --source "https://huggingface.co/openai-oss/gpt-oss-120B" \
  --base-model FW-GPT-OSS-120B
```

---

## Case 3b: Gated or Private Hugging Face Model

Requires a Hugging Face API token. The user must have accepted the model's license on huggingface.co.

**REST API**

```http
PUT {account}.services.ai.azure.com/api/projects/{project}
    /models/my-gpt-oss-120B/versions/1?api-version=v1
Content-Type: application/json
Authorization: Bearer {token}
```

**Request Body**

```json
{
  "weightType": "FullWeight",
  "baseModel": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "source": {
    "sourceType": "HuggingFace",
    "huggingFaceRepoId": "openai-oss/gpt-oss-120B",
    "revision": "main",
    "credentials": {
      "huggingFaceToken": "<hf_token>"
    }
  }
}
```

> **Security:** The `huggingFaceToken` is used only during the pull operation and is **not persisted** in model metadata. The response does not include the token.

**SDK**

```python
model = client.models.create_or_update(
    model_name="my-gpt-oss-120B",
    version="1",
    weight_type="FullWeight",
    base_model="azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
    source_type="HuggingFace",
    hugging_face_repo_id="openai-oss/gpt-oss-120B",
    revision="main",
    hugging_face_token="<hf_token>",  # NOT persisted; used only during pull
)
print(f"Model state: {model.provisioning_state}")  # "Creating" — authenticated pull in progress
```

**CLI (azd)**

> **Open:** The CLI design spec does not currently define HF-specific flags for gated repos. The `--source` flag accepts remote URLs, but authentication (HF token) and revision selection require dedicated flags that are not yet in the CLI surface. Until resolved, gated HF imports are SDK/REST only.

```bash
# Proposed — pending CLI design review
azd ai models create \
  --name my-gpt-oss-120B \
  --source "https://huggingface.co/openai-oss/gpt-oss-120B" \
  --base-model FW-GPT-OSS-120B
  # --huggingface-token <hf_token>  ← not yet in CLI design spec
```

> The `--huggingface-token` can also be provided via the `HUGGING_FACE_HUB_TOKEN` environment variable once supported.

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

See [spec-models-deploy.md](spec-models-deploy.md#validation-rules-all-cases) for validation rules common to all ingestion paths.

---

## Next Steps

After model registration completes, resolve the deployment template and deploy:
- [Part B: Bridge — Resolve Deployment Template](spec-models-deploy.md#part-b-bridge--from-registered-model-to-deployment-template)
- [Part C: Deploy the Model](spec-models-deploy.md#part-c-model-deployment-control-plane)
