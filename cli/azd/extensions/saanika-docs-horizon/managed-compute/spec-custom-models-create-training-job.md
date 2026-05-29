# Custom Model Registration — Training Job Output

> **Parent spec:** [spec-custom-models-deploy.md](spec-custom-models-deploy.md)

## Build Scope — Closed Decisions

The following decisions are **closed for Build** (confirmed in BYOW Training review, April 2026):

**In scope (Build):**
- User specifies a named model output (type `Model`) in the job spec
- User specifies a base model with model name in the job spec
- Job completion auto-creates and auto-registers the model asset for outputs of type `Model`
- Model assets are stored in project-managed storage (deep copy, including from BYOS training outputs)
- Users can query the Models API and filter models created by a specific job (via `source.jobId` lineage)

**Out of scope (Build):**
- User manually creates a model asset from a dataset folder output
- Model assets saved by reference to BYOS storage paths
- Checkpoint-to-model-asset promotion (open design, post-Build)

---

## Core Requirement: Auto-Registration on Job Completion

When a training job declares an output of type `Model`, the training service **MUST** create and register the corresponding model asset as part of the job completion (post-job finalize) step. This is a **normative requirement**, not optional behavior.

### Rules

1. **Auto-create is mandatory.** If the job spec declares `outputs.{key}.type = AssetTypes.SAFETENSORS_MODEL` (or equivalent model output type), the training runtime **WILL** create a model asset in the project's Models API upon successful job completion.
2. **No separate "register" call required.** The user does NOT need to call `PUT /models` or `client.models.create_or_update()` for this scenario. The training job's finalize step handles registration.
3. **Base model is required in the job spec.** When declaring a model output, the user **MUST** provide the `baseModel` reference in the job specification. The base model becomes a required part of the created model asset's metadata. This ensures deployability and provenance guarantees.
4. **Model name and version.** The user specifies the model name via the output key or a dedicated field in the job spec. The system assigns a version.
5. **SafeTensors enforcement.** The auto-created model asset follows the same SafeTensors-only validation as all other ingestion paths (Build scope).

### When Is the Model Asset Available?

The model asset is created **on job completion** — specifically, during the common runtime finalize step. It does NOT exist at job submission time.

| Event | Model asset exists? | Status signal |
|---|---|---|
| Job submitted | **No** | Poll `job.status` |
| Job running | **No** | Poll `job.status` |
| Job completed (finalize runs) | **Yes** — created during finalize | `job.status == "Completed"` |
| Job failed / canceled | **No** | `job.status == "Failed"` / `"Canceled"` |

> **Important:** There is no requirement to poll model status for auto-registered training outputs. The **job status is the authoritative status signal.** Once `job.status == "Completed"`, the model asset exists and is ready. Do not implement "placeholder model asset" semantics — the model asset and the job completion are a single atomic event from the user's perspective.

### Storage Semantics

Model assets auto-created from training jobs are stored in **project-managed storage** (Build scope).

- If the training job ran on project-managed compute with managed storage, the finalize step registers the output directly.
- If the training job ran on **BYOS (Bring Your Own Storage)**, the finalize step performs a **deep copy** of the model artifacts into project-managed storage before creating the model asset.

> **Out of scope (Build):** Model assets saved by reference to BYOS paths are NOT supported. The model asset always lives in managed storage. This avoids fragile dependencies on user-controlled storage that could be deleted or modified after model creation.

### Implementation Note: Common Runtime Finalize

The model asset creation occurs in the **common runtime finalize** step — the post-job lifecycle hook that runs after the training script exits successfully. The finalize step:

1. Mounts or reads the job's model output artifacts
2. Copies artifacts to project-managed storage (if BYOS, this is a deep copy)
3. Calls the internal model registration API to create the model asset with all required metadata (name, version, weightType, baseModel, source lineage)
4. Updates the job's output metadata to include the created model asset ID

This responsibility lives in the common runtime, NOT in the job service or SDK client. The SDK/CLI/UI do not need to call a separate "register" API for this scenario.

---

## Lineage

The training job output path establishes **bidirectional lineage** between jobs and models.

### Forward: Job → Model

The job's output metadata **MUST** include the model asset ID for the created model output:

```json
// GET /training/jobs/grpo-med-qa (after completion)
{
  "name": "grpo-med-qa",
  "status": "Completed",
  "outputs": {
    "trained_model": {
      "type": "Model",
      "assetId": "projects/my-project/models/my-gpt-oss-120B/versions/1"
    }
  }
}
```

This enables: _"I ran a job — what model did it produce?"_

### Reverse: Model → Job

The model asset **MUST** store a back-pointer to the training job via `source.jobId`:

```json
// GET /models/my-gpt-oss-120B/versions/1
{
  "name": "my-gpt-oss-120B",
  "version": "1",
  "weightType": "FullWeight",
  "baseModel": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "status": "Registered",
  "source": {
    "sourceType": "TrainingJob",
    "jobId": "grpo-med-qa"
  },
  "storageUri": "azureml://projects/my-project/models/my-gpt-oss-120B/versions/1"
}
```

This enables: _"Show me all models derived from this job."_

### Query Models by Job ID

The Models API **MUST** support filtering models by lineage:

```http
GET {account}.services.ai.azure.com/api/projects/{project}/models?source.jobId=grpo-med-qa&api-version=2025-06-01-preview
Authorization: Bearer {token}
```

```python
# SDK
models = project_client.models.list(source_job_id="grpo-med-qa")
for model in models:
    print(f"{model.name} v{model.version}: {model.weight_type}")
```

```bash
# CLI (azd) — list models created by a specific training job
azd ai models custom list --source-job-id grpo-med-qa
```

> **Note:** This lineage filter is an exception to the general "use Index Service for filtering" guidance. Filtering by `source.jobId` is a core workflow requirement for the training → deploy path and **MUST** be supported directly on the Models API for Build.

---

## Job Spec: Model Output Declaration

When submitting a training job, the user declares a model output with the base model reference.

### REST API

```http
PUT {account}.services.ai.azure.com/api/projects/{project}
    /training/jobs/grpo-med-qa?api-version=2025-06-01-preview
Content-Type: application/json
Authorization: Bearer {token}
```

**Request Body (relevant excerpt)**

```json
{
  "command": "python train.py --model_name_or_path ${inputs.model_dir} --output_dir ${outputs.trained_model}",
  "outputs": {
    "trained_model": {
      "type": "Model",
      "modelName": "my-gpt-oss-120B",
      "baseModel": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
      "weightType": "FullWeight"
    }
  }
}
```

### SDK

```python
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import (
    CommandJob, JobResourceConfiguration, PyTorchDistribution,
    Input, Output, AssetTypes, InputOutputModes,
)
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
project_client = AIProjectClient(
    endpoint="https://my-foundry-account.services.ai.azure.com/api/projects/my-project",
    credential=credential,
)

job = CommandJob(
    command="python train.py --model_name_or_path ${inputs.model_dir} "
            "--dataset_name ${inputs.dataset} "
            "--output_dir ${outputs.trained_model}",
    environment_image_reference="mcr.microsoft.com/azureml/minimal-ubuntu22.04-py39-cuda11.8-gpu-inference",
    compute="/subscriptions/.../computes/gpu-cluster",
    code="./src",
    inputs={
        "model_dir": Input(type=AssetTypes.URI_FOLDER, path="./models/gpt-oss-120B", mode=InputOutputModes.READ_ONLY_MOUNT),
        "dataset": Input(type=AssetTypes.URI_FOLDER, path="./datasets/med_mcqa", mode=InputOutputModes.READ_ONLY_MOUNT),
    },
    outputs={
        "trained_model": Output(
            type=AssetTypes.SAFETENSORS_MODEL,
            mode=InputOutputModes.READ_WRITE_MOUNT,
            model_name="my-gpt-oss-120B",
            base_model="azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
        ),
    },
    resources=JobResourceConfiguration(instance_count=1, instance_type="Standard_ND96ISR_H100_V5"),
    distribution=PyTorchDistribution(process_count_per_instance=8),
)
created_job = project_client.beta.training.jobs.create_or_update(name="grpo-med-qa", job=job)
print(f"Job submitted: {created_job.name} (status: {created_job.properties.status})")
```

### CLI (azd)

> **Note:** Training jobs are submitted via the training API (SDK / REST), not through the `azd ai models` CLI extension. The `azd ai models` extension is used for model CRUD operations **after** the job completes. There is no `create-from-job` CLI command — model registration happens automatically on the server side during job finalize.

After the job completes, verify the auto-registered model:

```bash
# Verify auto-registered model from job output
azd ai models list --source-job-id grpo-med-qa

# Show model details (defaults to latest version)
azd ai models show --name my-gpt-oss-120B

# Show specific version
azd ai models show --name my-gpt-oss-120B --version 1
```

> After running `azd ai models init`, no additional flags are needed. Without init, pass `-e` and `-s` explicitly.

> **Open:** The `--source-job-id` filter on `custom list` is a spec-level requirement for the Models API. Whether the CLI surfaces this filter flag in its current design iteration is TBD — confirm with CLI team (Radhika/Amit).

---

## Validation Rules (Training Job Output)

| Rule | Error |
|---|---|
| `baseModel` omitted in model output spec | `BaseModelRequired: The 'baseModel' field is required when declaring a model output.` |
| Base model not found in catalog | `BaseModelNotFound: Base model '{base_model}' not found in catalog.` |
| Base model has no approved DTs | `BaseModelNoDTs: Base model '{base_model}' has no approved deployment templates.` |
| Training job failed (no model created) | N/A — model asset is not created; job status is the signal |
| Weight files not `.safetensors` | `UnsupportedWeightFormat: Only SafeTensors (.safetensors) files are accepted.` |

See [spec-custom-models-deploy.md](spec-custom-models-deploy.md#validation-rules-all-cases) for validation rules common to all ingestion paths.

---

## De-scoped for Build

### Checkpoint Registration

Checkpoint-to-model-asset promotion is **not committed for Build**. Checkpoints are intermediate training artifacts that are not guaranteed to be deployable. The checkpoint scenario remains open design; iteration continues post-Build.

> The Build-scope path is: user declares a model output type → job completes → model asset is auto-created from the final output. Intermediate checkpoints are not promoted to model assets.

### Manual Registration from Dataset Output

Creating a model asset manually from a dataset folder output (e.g., `client.models.create_or_update(...)` with `source_type="TrainingJob"`) is **out of scope for Build**. The auto-registration path is the only supported mechanism for training job outputs.

> For Build, training → model is a single automated path. There is no manual "register from job output" API surface. This may be added in future milestones if needed.

### Model Assets by Reference to BYOS Paths

Creating a model asset that references weights in user-provided (BYOS) storage — without copying them to managed storage — is **out of scope for Build**. All model assets created from training jobs live in project-managed storage.

> Reference-based model assets introduce fragile dependencies (storage deletion, permission changes) that complicate deployability. Managed storage is the source of truth for Build.

---

## End-to-End: Train → Register → Deploy

This shows the full workflow from submitting a training job through to deploying the trained model:

```python
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import (
    CommandJob, JobResourceConfiguration, PyTorchDistribution,
    Input, Output, AssetTypes, InputOutputModes,
    CustomModel, ModelSource,
)
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.mgmt.cognitiveservices.models import (
    AcceleratorDeployment, AcceleratorDeploymentProperties, Sku,
)
from azure.identity import DefaultAzureCredential
import time

credential = DefaultAzureCredential()
project_client = AIProjectClient(
    endpoint="https://my-foundry-account.services.ai.azure.com/api/projects/my-project",
    credential=credential,
)

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: Submit training job with model output declaration
#         Base model is REQUIRED in the job spec for model outputs.
# ═══════════════════════════════════════════════════════════════════════════════
job = CommandJob(
    command="python train.py --model_name_or_path ${inputs.model_dir} "
            "--dataset_name ${inputs.dataset} "
            "--output_dir ${outputs.trained_model}",
    environment_image_reference="mcr.microsoft.com/azureml/minimal-ubuntu22.04-py39-cuda11.8-gpu-inference",
    compute="/subscriptions/.../computes/gpu-cluster",
    code="./src",
    inputs={
        "model_dir": Input(type=AssetTypes.URI_FOLDER, path="./models/gpt-oss-120B", mode=InputOutputModes.READ_ONLY_MOUNT),
        "dataset": Input(type=AssetTypes.URI_FOLDER, path="./datasets/med_mcqa", mode=InputOutputModes.READ_ONLY_MOUNT),
    },
    outputs={
        "trained_model": Output(
            type=AssetTypes.SAFETENSORS_MODEL,
            mode=InputOutputModes.READ_WRITE_MOUNT,
            model_name="my-gpt-oss-120B",
            base_model=BASE_MODEL,
        ),
    },
    resources=JobResourceConfiguration(instance_count=1, instance_type="Standard_ND96ISR_H100_V5"),
    distribution=PyTorchDistribution(process_count_per_instance=8),
)
created_job = project_client.beta.training.jobs.create_or_update(name="grpo-med-qa", job=job)

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2: Wait for job completion
#         The model asset is auto-created during job finalize.
#         Job status is the ONLY status signal — do NOT poll model status.
# ═══════════════════════════════════════════════════════════════════════════════
while created_job.properties.status not in ("Completed", "Failed", "Canceled"):
    time.sleep(60)
    created_job = project_client.beta.training.jobs.get(name="grpo-med-qa")
assert created_job.properties.status == "Completed"

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3: Read model asset ID from job outputs
#         The model asset now exists — created during finalize.
# ═══════════════════════════════════════════════════════════════════════════════
model_asset_id = created_job.properties.outputs["trained_model"].asset_id
# e.g., "projects/my-project/models/my-gpt-oss-120B/versions/1"
print(f"Model auto-registered: {model_asset_id}")

# Alternative: query models by job ID
model = list(project_client.models.list(source_job_id="grpo-med-qa"))[0]
print(f"Model: {model.name} v{model.version} (source job: {model.source.job_id})")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4: Resolve deployment template from the base model
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
# STEP 5: Deploy the auto-registered model
#         Pass the model asset ID from step 3 into the deployment API.
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
# STEP 6: Run inference
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

### Deploy Flow Sequence (Summary)

The deployment flow for training job outputs follows this mandatory ordering:

```
1. Submit job (with model output + baseModel in job spec)
       ↓
2. Wait for job completion (poll job.status — NOT model status)
       ↓
3. Job finalize auto-creates model asset in managed storage
       ↓
4. Read model assetId from job outputs (or query by source.jobId)
       ↓
5. Resolve deployment template from base model (Part B)
       ↓
6. Pass model assetId into deployment API (Part C)
```

---

## Next Steps

After model registration completes, resolve the deployment template and deploy:
- [Part B: Bridge — Resolve Deployment Template](spec-custom-models.md#part-b-bridge--from-registered-model-to-deployment-template)
- [Part C: Deploy the Custom Model](spec-custom-models.md#part-c-custom-model-deployment-control-plane)
