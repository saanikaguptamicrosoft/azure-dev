# Custom Model Registration — Local Upload

> **Parent spec:** [spec-custom-models.md](spec-custom-models.md)

This is the **primary ingestion path** for BYOW models. The user uploads weight files from their local machine to project-managed blob storage via SAS URI, then finalizes registration.

Model registration follows a mandatory **upload-first** sequence:

1. **StartPendingUpload** — Client calls the data-plane API to initiate upload. Returns a SAS URI pointing to project-managed blob storage.
2. **Direct storage upload** — Client uploads model artifacts directly to the SAS URI via `azcopy` (recommended for large models). Bypasses Foundry services entirely.
3. **PutModel (registration)** — Client calls the model registration API. Service validates upload completed before finalizing.

---

## Step 1: Start Pending Upload

**REST API**

```http
POST {account}.services.ai.azure.com/api/projects/{project}
    /models/{modelName}/versions/{version}:startPendingUpload?api-version=2025-06-01-preview
Content-Type: application/json
Authorization: Bearer {token}
```

**Request Body**

```json
{
  "type": "FullWeight",
  "description": "gpt-oss-120B fine-tuned on internal corpus",
  "baseModel": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "source": {
    "sourceType": "LocalUpload"
  },
  "properties": {
    "parameterCount": "120000000000",
    "format": "safetensors"
  }
}
```

**Response (200 OK)**

```json
{
  "uploadId": "upload-abc123",
  "modelName": "my-gpt-oss-120B",
  "version": "1",
  "type": "FullWeight",
  "baseModel": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "status": "PendingUpload",
  "upload": {
    "sasUri": "https://projstorage.blob.core.windows.net/models/my-gpt-oss-120B/v1?sp=rcw&se=2026-03-19T...",
    "containerPath": "models/my-gpt-oss-120B/v1/",
    "expiresOn": "2026-03-19T20:00:00Z",
    "supportedUploadMethods": ["azcopy", "blockBlob"]
  }
}
```

**SDK**

```python
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential

client = AIProjectClient(
    endpoint="https://my-foundry-account.services.ai.azure.com/api/projects/my-project",
    credential=DefaultAzureCredential(),
)

upload_info = client.models.start_pending_upload(
    model_name="my-gpt-oss-120B",
    version="1",
    model_type="FullWeight",
    base_model="azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
)
print(f"SAS URI: {upload_info.upload.sas_uri}")
print(f"Expires: {upload_info.upload.expires_on}")
```

**CLI**

```bash
{new-project-cli} model upload-start \
  --account my-foundry-account \
  --project my-project \
  --name my-gpt-oss-120B \
  --version 1 \
  --type FullWeight \
  --base-model "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4"
```

---

## Step 2: Upload Weight Files

Client uploads directly to blob storage using `azcopy`. This bypasses Foundry services entirely.

```bash
azcopy copy "./model-weights/*" \
  "https://projstorage.blob.core.windows.net/models/my-gpt-oss-120B/v1?sp=rcw&se=..." \
  --recursive
```

> `azcopy` is recommended for full-weight models (hundreds of GBs). It handles extremely large transfers, supports resumable uploads, and fully utilizes available bandwidth.

---

## Step 3: Complete Registration (PutModel)

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
  "description": "Custom fine-tuned gpt-oss-120B for medical Q&A",
  "tags": {
    "team": "medical-ai",
    "baseCheckpoint": "openai-oss/gpt-oss-120B"
  }
}
```

**Response (200 OK)**

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
    "fileCount": 15
  }
}
```

**SDK**

```python
import subprocess

# Step 2: Upload via azcopy
subprocess.run([
    "azcopy", "copy",
    "./model-weights/",
    upload_info.upload.sas_uri,
    "--recursive"
], check=True)

# Step 3: Complete registration
model = client.models.create_or_update(
    model_name="my-gpt-oss-120B",
    version="1",
    model_type="FullWeight",
    base_model="azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
    description="Custom fine-tuned gpt-oss-120B for medical Q&A",
    tags={"team": "medical-ai"},
)
print(f"Model registered: {model.name} (status: {model.status})")
```

---

## Get Upload Status

**REST API**

```http
GET {account}.services.ai.azure.com/api/projects/{project}
    /models/my-gpt-oss-120B/versions/1/uploadStatus?api-version=2025-06-01-preview
Authorization: Bearer {token}
```

**Response**

```json
{
  "uploadId": "upload-abc123",
  "status": "Uploading",
  "bytesUploaded": 75161927680,
  "expiresOn": "2026-03-19T20:00:00Z",
  "filesDetected": 22
}
```

Upload status values: `"PendingUpload"` | `"Uploading"` | `"Completed"` | `"Expired"` | `"Cancelled"`

---

## Validation Rules (Local Upload)

| Rule | Error |
|---|---|
| Upload incomplete (partial shards) | `UploadIncomplete: Expected {expected} weight files, found {actual}.` |
| SAS URI expired | `UploadExpired: SAS URI expired. Call startPendingUpload again.` |

See [spec-custom-models.md](spec-custom-models.md#validation-rules-all-cases) for validation rules common to all ingestion paths.

---

## Next Steps

After model registration completes, resolve the deployment template and deploy:
- [Part B: Bridge — Resolve Deployment Template](spec-custom-models.md#part-b-bridge--from-registered-model-to-deployment-template)
- [Part C: Deploy the Custom Model](spec-custom-models.md#part-c-custom-model-deployment-control-plane)
