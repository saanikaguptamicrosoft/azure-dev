# Custom Model Registration — Local Upload

> **Parent spec:** [spec-custom-models.md](spec-custom-models.md)

This is the **primary ingestion path** for BYOW models. The user uploads weight files from their local machine to project-managed blob storage via SAS URI, then finalizes registration.

Model registration follows a mandatory **upload-first** sequence:

1. **StartPendingUpload** — Client calls the data-plane API to initiate upload. Returns a SAS URI pointing to project-managed blob storage.
2. **Direct storage upload** — Client uploads model artifacts directly to the SAS URI via `azcopy` (recommended for large models). Bypasses Foundry services entirely.
3. **PutModel (registration)** — Client calls the model registration API. Service validates upload completed before finalizing.

---

## Versioning Requirements

### Decision

The local upload path **MUST** support both user-specified version and auto-versioning:

| Mode | Behavior |
|---|---|
| **Explicit version** | User provides `version` in the request path. If the version already exists with different content, a `ModelVersionConflict` error is returned. |
| **Auto-version** | User omits version (or passes a sentinel like `"auto"`). The system assigns the next available version (best-effort monotonic increment). |

> **Requirement:** Both CLI and SDK must support both modes. Auto-versioning is critical for CI/CD pipelines and iterative experimentation workflows.

> **UX requirement:** A version conflict **SHOULD** enable recovery without forcing the user to re-upload weights when feasible. Re-uploading hundreds of GBs due to a version race is unacceptable.

### Open Design Questions (Versioning)

| # | Question | Context | Owner |
|---|---|---|---|
| V-1 | Should version be determined at StartPendingUpload time vs at PutModel commit time? | The "naive desirable" approach is versionless pending uploads with version assigned at commit. But this has race-condition and cleanup implications — pending uploads currently key off model name + version in the path. | Anthony |
| V-2 | How to prevent forcing re-upload on version conflict? | If two users upload to the same name/version concurrently, the losing user should be able to re-bind the already-uploaded bytes to a new version without re-uploading. Feasibility depends on V-1 answer. | Anthony |
| V-3 | Does auto-versioning interact correctly with the reaping/cleanup logic? | Reaping uses the association between pending uploads and committed assets. If pending uploads are versionless, the reaping key changes — needs code validation. | Anthony |

---

## Race Condition / Conflict Handling

When two users attempt to register the same model name + version concurrently (both upload, one commits first):

### Expected Behavior

1. First user to call PutModel (commit) **succeeds** — model asset is created.
2. Second user's PutModel call **fails** with a clear, actionable error:

```json
{
  "error": {
    "code": "ModelVersionConflict",
    "message": "Version '1' of model 'my-gpt-oss-120B' already exists with different properties. Use a different version or call startPendingUpload with a new version."
  }
}
```

3. The losing user **MUST NOT** have their uploaded bytes silently discarded.

### Recovery Path (Open)

| Option | Description | Status |
|---|---|---|
| **(a) Re-bind to new version** | Allow the losing user to re-commit the same pending upload ID to a different version without re-uploading. | Preferred but feasibility depends on storage layer — **open investigation**. |
| **(b) New pending upload** | Losing user must call StartPendingUpload again with a new version, then re-upload. | Fallback; undesirable for large models. |

> **Minimum requirement:** The system must not silently discard uploaded bytes. The error response must provide a clear recovery action.

---

## Pending Upload Cleanup (Reaping)

Pending uploads that are never committed to a model asset must be cleaned up to avoid orphaned storage containers.

### Current Behavior

- The reaping logic uses the association between pending uploads and committed assets to determine which uncommitted containers can be deleted.
- Reaping currently keys off the **model name + version** bound at StartPendingUpload time.

### Constraints

- Any change to version semantics (e.g., versionless pending uploads for auto-versioning) **MUST** preserve safe cleanup semantics.
- If pending uploads become versionless, the reaping logic must be updated to key off `pendingUploadId` instead of name + version — this requires code validation.
- Orphaned containers from failed or abandoned uploads must still be reaped on a predictable schedule.

> **Action required:** Validate in code whether the reaping logic keys off version only vs pending upload ID. This is a prerequisite for deciding V-1 (versionless pending uploads). Owner: Anthony.

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
  "pendingUploadType": "TemporaryBlobReference"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `pendingUploadType` | `string` | Yes | Must be `"TemporaryBlobReference"`. Indicates the upload will use a temporary SAS-based blob container. |

**Response (200 OK — PendingUploadResponseDto)**

```json
{
  "pendingUploadId": "upload-abc123",
  "pendingUploadType": "TemporaryBlobReference",
  "blobReference": {
    "sasUri": "https://projstorage.blob.core.windows.net/models/my-gpt-oss-120B/v1?sp=rcw&se=2026-03-19T...",
    "containerPath": "models/my-gpt-oss-120B/v1/",
    "expiresOn": "2026-03-19T20:00:00Z"
  }
}
```

| Field | Type | Description |
|---|---|---|
| `pendingUploadId` | `string` | Opaque ID for this pending upload session. Used for listing and cleanup tracking. |
| `pendingUploadType` | `string` | `"TemporaryBlobReference"`. |
| `blobReference.sasUri` | `string` | SAS URI for uploading weight files via `azcopy` or block blob API. |
| `blobReference.containerPath` | `string` | Path prefix within the container. |
| `blobReference.expiresOn` | `string` (ISO 8601) | SAS expiration timestamp. |

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
    pending_upload_type="TemporaryBlobReference",
)
print(f"Upload ID: {upload_info.pending_upload_id}")
print(f"SAS URI: {upload_info.blob_reference.sas_uri}")
print(f"Expires: {upload_info.blob_reference.expires_on}")
```

**CLI**

The `azd ai models custom create` command handles the full upload flow (StartPendingUpload → upload → commit) as a single command. There is no separate low-level `upload-start` CLI command — the three-step sequence is internal to the CLI.

```bash
# Full upload + registration in one command
azd ai models custom create \
  --name my-gpt-oss-120B \
  --source ./model-weights/ \
  --base-model "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4"
```

> After `azd ai models init`, the `--project-endpoint` and `--subscription` flags are not needed.


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
  "weightType": "FullWeight",
  "baseModel": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "description": "Custom fine-tuned gpt-oss-120B for medical Q&A",
  "tags": {
    "team": "medical-ai",
    "baseCheckpoint": "openai-oss/gpt-oss-120B"
  }
}
```

> **Note:** PUT /models is an **async** operation. The response will include a polling URL. Clients must poll for completion.

**Response (202 Accepted)**

```json
{
  "name": "my-gpt-oss-120B",
  "version": "1",
  "weightType": "FullWeight",
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
    weight_type="FullWeight",
    base_model="azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
    description="Custom fine-tuned gpt-oss-120B for medical Q&A",
    tags={"team": "medical-ai"},
)
print(f"Model registered: {model.name} (status: {model.status})")
```

---

## Get Upload Status

> **De-scoped for Build.** Upload status APIs are post-Build. Upload status is only meaningful for HF imports that happen backend-side. For local uploads, users monitor `azcopy` directly.

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

## List Pending Uploads

Returns all active (uncommitted) pending uploads for a given model, with enough metadata to map them to the provisioned storage.

**REST API**

```http
GET {account}.services.ai.azure.com/api/projects/{project}
    /models/{modelName}/pendingUploads?api-version=2025-06-01-preview
Authorization: Bearer {token}
```

**Response (200 OK)**

```json
{
  "value": [
    {
      "pendingUploadId": "upload-abc123",
      "pendingUploadType": "TemporaryBlobReference",
      "version": "1",
      "blobReference": {
        "containerPath": "models/my-gpt-oss-120B/v1/",
        "expiresOn": "2026-03-19T20:00:00Z"
      },
      "createdAt": "2026-03-18T14:00:00Z"
    }
  ]
}
```

**SDK**

```python
pending = client.models.list_pending_uploads(model_name="my-gpt-oss-120B")
for p in pending:
    print(f"{p.pending_upload_id} → version={p.version}, expires={p.blob_reference.expires_on}")
```
---

## CLI UX (Build)

The CLI must provide a streamlined experience for the full local upload workflow. For Build, the CLI performs the three-step sequence internally.

### CLI Flow

```
1. CLI calls StartPendingUpload → receives SAS URI
2. CLI uploads weight files to SAS URI (via azcopy or native upload)
3. CLI calls PutModel (commit) with model metadata → model registered
```

### Single-Command Experience (azd)

**Proposal: To ensure alignment between API/SDK/CLI surfaces, `azd ai models custom create` should be changed to `azd ai models create`**

The `azd ai models custom create` command wraps the full three-step flow into a single operation. The CLI uses `azcopy` internally for file upload, showing a real-time progress bar:

```bash
# Explicit version
azd ai models custom create \
  --name my-gpt-oss-120B \
  --version 1 \
  --source ./model-weights/ \
  --base-model "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4" \
  --description "Fine-tuned for medical Q&A"

# Auto-version (omit --version; defaults to "1")
azd ai models custom create \
  --name my-gpt-oss-120B \
  --source ./model-weights/ \
  --base-model "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/1"


```

**Expected output:**

```
Creating custom model: my-gpt-oss-120B (version 1)
✓ Upload location ready
Step 2/3: Uploading model files...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% (240.0 GB / 240.0 GB) | 53.0 MB/s | Elapsed: 1h 15m | ETA: done
✓ Upload complete
✓ Model registered successfully!
  Name:        my-gpt-oss-120B
  Version:     1
  Description: Fine-tuned for medical Q&A
```
---

## Validation Rules (Local Upload)

| Rule | Error |
|---|---|
| Upload incomplete (partial shards) | `UploadIncomplete: Expected {expected} weight files, found {actual}.` |
| SAS URI expired | `UploadExpired: SAS URI expired. Call startPendingUpload again.` |
| Version conflict (concurrent commit) | `ModelVersionConflict: Version '{ver}' of model '{name}' already exists with different properties. Use a different version or call startPendingUpload with a new version.` |
| Pending upload not found | `PendingUploadNotFound: No pending upload found for model '{name}' version '{ver}'.` |

See [spec-custom-models.md](spec-custom-models.md#validation-rules-all-cases) for validation rules common to all ingestion paths.

---

## Next Steps

After model registration completes, resolve the deployment template and deploy:
- [Part B: Bridge — Resolve Deployment Template](spec-custom-models.md#part-b-bridge--from-registered-model-to-deployment-template)
- [Part C: Deploy the Custom Model](spec-custom-models.md#part-c-custom-model-deployment-control-plane)
