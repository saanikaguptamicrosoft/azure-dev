# Troubleshooting Guide - SDK (Python)

This guide is for SDK flows that map to both training and models scenarios in Foundry.

---

## Scope

Applies to:
- `azure-ai-projects` (`AIProjectClient`, `project_client.get_openai_client()`)
- Training jobs via `project_client.beta.jobs`
- Fine-tuning jobs via `openai_client.fine_tuning.jobs`
- BYOW models via `project_client.beta.models`

---

## Quick triage (customer-facing)

1. Capture the exception class and HTTP status code (if any).
2. If the failure happened before any network call, run local job validation first.
3. If it is an HTTP error, treat `401/403` as identity or RBAC, `404` as wrong resource IDs/names, `409` as state/conflict, and `5xx` as service/transient.

---

## Errors surfaced and what to do

### 1) Client construction / environment

#### `ValueError: Parameter 'endpoint' must not be None.`
Cause: SDK client was created without endpoint.
Fix: Pass a valid project endpoint in the form `https://<account>.services.ai.azure.com/api/projects/<project>`.

#### `ValueError: Parameter 'credential' must not be None.`
Cause: SDK client was created without a credential.
Fix: Provide a valid Azure credential (for example `DefaultAzureCredential`).

#### `KeyError: 'FOUNDRY_PROJECT_ENDPOINT'` (from samples)
Cause: Required environment variable is missing.
Fix: Set `FOUNDRY_PROJECT_ENDPOINT` before running.

#### `FileNotFoundError` when opening training/validation file (from samples)
Cause: Path in `TRAINING_FILE_PATH` or `VALIDATION_FILE_PATH` is wrong.
Fix: Verify file paths; sample defaults resolve under `samples/finetuning/data/`.

---

### 2) Local validation errors (no network call) for `project_client.beta.jobs`

These are returned by `project_client.beta.jobs.validate(job)` and also raised during `create_or_update(...)` when validation is enabled.

#### `'command' is required and cannot be empty for a CommandJob.`
Cause: Missing or blank `command`.
Fix: Provide a non-empty command string.

#### `'environment_image_reference' is required and cannot be empty for a CommandJob.`
Cause: Missing container image reference.
Fix: Set a valid environment image reference.

#### `'compute' is required and cannot be empty for a CommandJob.`
Cause: Missing compute target.
Fix: Set a valid compute target name.

#### `'code' cannot be an empty string. Omit it or provide a valid local path or datastore URI.`
Cause: `code` is present but blank.
Fix: Remove `code` or provide a valid path/URI.

#### `Invalid code value: <value>. Git paths are not supported.`
Cause: `code` used `git://` or `git+...`.
Fix: Use a local path or supported datastore URI.

#### `Local path '<path>' does not exist.`
Cause: Local `code`, `inputs.<name>.path`, or `outputs.<name>.path` not found.
Fix: Correct the path. Relative paths are resolved from the job file location when loaded from YAML.

---

### 3) YAML/job loading errors

#### `ImportError: PyYAML (>=6.0) is required to load jobs from YAML files.`
Cause: PyYAML not installed.
Fix: Install with `pip install "pyyaml>=6.0"`.

#### `ValueError: Unsupported job type: '<type>'. Supported types: ['command']`
Cause: YAML `type` is not supported by SDK job loader.
Fix: Use `type: command`.

---

### 4) Streaming and download errors (`project_client.beta.jobs`)

#### `RuntimeError: Exception : <service error payload>` from `jobs.stream(...)`
Cause: Job reached `Failed` status; SDK raises with server error payload.
Fix: Inspect payload and run logs, then resolve model/data/compute/config issue and resubmit.

#### `ValueError: Specify either 'output_name' or 'all=True', not both.`
Cause: Both output selectors were set for `jobs.download(...)`.
Fix: Use only one.

#### `ValueError: Job '<name>' is in state '<state>'. Download is allowed only when the job is in a terminal state: [...]`
Cause: Download attempted while job still running.
Fix: Wait for terminal state (`completed`, `failed`, `canceled`, `notresponding`, `paused`, `unknown`).

#### `ValueError: Job '<name>' has no output named '<output_name>'. Available outputs: [...]`
Cause: Invalid output name.
Fix: Use one of the listed output names.

#### `ValueError: Job '<name>' run is missing 'experimentId'; cannot list artifacts.`
Cause: Service did not provide required run metadata for artifact listing.
Fix: Retry; if persistent, treat as service-side issue and open support ticket with run ID.

#### `ValueError: Blob reference is missing a SAS URI credential.`
#### `ValueError: Blob reference is missing the blob URI.`
Cause: Service returned incomplete artifact blob reference.
Fix: Retry; if persistent, open support ticket with job ID.

#### `ValueError: Output '<name>' has unsupported jobOutputType '<type>'. Supported types: uri_file, uri_folder, safetensors_model.`
Cause: Output type is not supported by current SDK download helper.
Fix: Use supported output types or download artifacts via service-native path.

---

### 5) Service-side HTTP errors during fine-tuning/training/models

All service-call failures are surfaced as HTTP exceptions (`HttpResponseError` and OpenAI HTTP errors). Focus on status code first.

#### `401 Unauthorized` / `AuthenticationError`
Cause: Token/key invalid, wrong endpoint, or missing required data action.
Observed training baseline example: missing `AIServices/agents/write` data action.
Fix: Verify endpoint, identity, token validity, and required permissions.

#### `403 Forbidden` / `PermissionDeniedError`
Cause: Identity authenticated but lacks required RBAC/data actions.
Observed training baseline example: missing `Microsoft.MachineLearningServices/workspaces/agents/action`.
Fix: Grant required role/permissions on project/workspace/account scope, then retry.

#### `404 Not Found`
Cause: Wrong job/model/file/resource identifier or resource removed.
Fix: Re-check IDs and project/account context.

#### `409 Conflict`
Cause: Resource state conflict (already exists, invalid transition, or concurrent op conflict).
Fix: Use unique names/versions, wait for active operations to finish, then retry.

#### `429 Too Many Requests`
Cause: Throttling or quota pressure.
Fix: Backoff/retry and reduce concurrency; verify quota.

#### `5xx` (for example `500 Internal Server Error`)
Cause: Service transient or backend issue.
Fix: Retry with correlation/request IDs; escalate with run/job IDs if persistent.

---

### 6) BYOW models registration and management (`project_client.beta.models`)

#### `ValueError: \`name\` must be a non-empty string.`
Cause: Empty model name passed to models create helper.
Fix: Pass a non-empty `name`.

#### `ValueError: \`version\` must be a non-empty string.`
Cause: Empty model version passed to models create helper.
Fix: Pass a non-empty `version`.

#### `ValueError: Upload source does not exist: <path>`
Cause: Local model source path is invalid.
Fix: Point `source` to an existing local file or directory.

#### `ValueError: Upload source directory is empty: <path>`
Cause: Source directory has no files.
Fix: Add model files (weights/config) to the folder before create.

#### `ValueError: Upload source file is empty: <path>`
Cause: Source file exists but has zero bytes.
Fix: Replace with a valid non-empty file.

#### `ValueError: \`polling_timeout\` must be > 0 when \`wait_for_commit\` is True.`
#### `ValueError: \`polling_interval\` must be > 0 when \`wait_for_commit\` is True.`
Cause: Invalid polling arguments in models create helper.
Fix: Use positive values.

#### `RuntimeError: \`azcopy\` was not found on PATH...` (sync create helper)
Cause: Sync models create helper requires AzCopy for upload.
Fix: Install AzCopy and ensure it is on PATH, or pass `azcopy_path`.

#### `RuntimeError: azcopy exited with code <N> ...`
Cause: File upload to pending container failed.
Fix: Validate source path, connectivity, and SAS validity; retry.

#### `ValueError: Could not locate SAS URI / blob URI in pending_upload response: ...`
Cause: Service returned incomplete pending-upload payload.
Fix: Retry. If persistent, open support ticket with request/correlation IDs.

#### `RuntimeError: \`azure-storage-blob\` is required for the async \`create\` helper...`
Cause: Async helper uses `azure.storage.blob.aio` for uploads and dependency is missing.
Fix: Install with `pip install azure-storage-blob aiohttp`.

#### `RuntimeError: Model '<name>'@'<version>' did not appear within <timeout>s after pending_create_version.`
Cause: Commit accepted but model version not observable before timeout.
Fix: Increase timeout and retry; if still failing, escalate with model name/version and timestamps.

#### `ResourceNotFoundError` / `HttpResponseError` on `get(name, version)`
Cause: Model version does not exist, is in wrong project endpoint, or was deleted.
Fix: Verify endpoint and exact `name`/`version`, then list versions to confirm.

#### `HttpResponseError` on delete even when operation succeeded (status 200)
Cause: Service may return `200 OK` for delete while generated operation expects `204`.
Fix: Treat delete as successful if backend status is 200 and subsequent `get` returns not found.

---

## Recommended support payload

When escalating, include:
- Endpoint (redact sensitive segments if needed)
- Job ID / fine-tuning job ID
- Exception type, HTTP status code, and full error message
- UTC timestamp
- Correlation/request ID headers
- Minimal repro snippet
