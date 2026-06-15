# Custom Model Registration — Training Job Output

> **Parent spec:** [spec-custom-models.md](spec-custom-models.md)

The user has completed a custom training job and wants to register the output checkpoint as a deployable model. This is a **single-step** registration — the service copies weights from the job output location into project-managed model storage.

The training job must have produced output weights using `AssetTypes.SAFETENSORS_MODEL` output type. The system resolves the job's output location and copies weights into project-managed storage.

**Supported source references:**

| Reference | Format | Description |
|---|---|---|
| Training job name | `training/jobs/{jobName}` | System resolves the job's output model or best checkpoint. |
| Job output with specific output key | `training/jobs/{jobName}/outputs/{outputKey}` | Use when the job has multiple outputs. |

---

## Register Model from Training Job

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
  "description": "gpt-oss-120B fine-tuned on internal medical Q&A data via GRPO",
  "source": {
    "sourceType": "TrainingJob",
    "jobName": "grpo-reasoning-training-job",
    "outputKey": "safetensor_model_folder"
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
    "sourceType": "TrainingJob",
    "jobName": "grpo-reasoning-training-job",
    "outputKey": "safetensor_model_folder"
  }
}
```

> The service copies weights from the job output location into project-managed model storage asynchronously. Poll `GET /models/my-gpt-oss-120B` until `provisioningState` is `Succeeded`.

**SDK**

```python
from azure.ai.projects.models import CustomModel, ModelSource

model = client.models.create_or_update(
    CustomModel(
        name="my-gpt-oss-120B",
        version="1",
        type="FullWeight",
        base_model="azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
        description="gpt-oss-120B fine-tuned on internal medical Q&A data via GRPO",
        source=ModelSource(
            source_type="TrainingJob",
            job_name="grpo-reasoning-training-job",
            output_key="safetensor_model_folder",
        ),
    )
)
print(f"Model state: {model.provisioning_state}")  # "Creating" — copy in progress
```

**CLI**

```bash
{new-project-cli} model create \
  --account my-foundry-account \
  --project my-project \
  --name my-gpt-oss-120B \
  --type FullWeight \
  --base-model "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4" \
  --training-job grpo-reasoning-training-job \
  --training-output-key safetensor_model_folder
```

### Provisioning Status Flow

`Creating` (weight copy from job output) → `Succeeded` / `Failed`

---

## Validation Rules (Training Job)

| Rule | Error |
|---|---|
| Training job not found | `TrainingJobNotFound: Job '{jobName}' not found.` |
| Training job not completed | `TrainingJobNotCompleted: Job '{jobName}' is in state '{state}'.` |
| Training job output key not found | `OutputKeyNotFound: Output '{outputKey}' not found in job '{jobName}'.` |

See [spec-custom-models.md](spec-custom-models.md#validation-rules-all-cases) for validation rules common to all ingestion paths.

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

# ── Step 1: Submit training job ──────────────────────────────────────────────
job = CommandJob(
    command="python train.py --model_name_or_path ${inputs.model_dir} "
            "--dataset_name ${inputs.dataset} "
            "--output_dir ${outputs.safetensor_model_folder}",
    environment_image_reference="mcr.microsoft.com/azureml/minimal-ubuntu22.04-py39-cuda11.8-gpu-inference",
    compute="/subscriptions/.../computes/gpu-cluster",
    code="./src",
    inputs={
        "model_dir": Input(type=AssetTypes.URI_FOLDER, path="./models/gpt-oss-120B", mode=InputOutputModes.READ_ONLY_MOUNT),
        "dataset": Input(type=AssetTypes.URI_FOLDER, path="./datasets/med_mcqa", mode=InputOutputModes.READ_ONLY_MOUNT),
    },
    outputs={
        "safetensor_model_folder": Output(type=AssetTypes.SAFETENSORS_MODEL, mode=InputOutputModes.READ_WRITE_MOUNT),
    },
    resources=JobResourceConfiguration(instance_count=1, instance_type="Standard_ND96ISR_H100_V5"),
    distribution=PyTorchDistribution(process_count_per_instance=8),
)
created_job = project_client.beta.training.jobs.create_or_update(name="grpo-med-qa", job=job)

# Wait for training to complete
while created_job.properties.status not in ("Completed", "Failed", "Canceled"):
    time.sleep(60)
    created_job = project_client.beta.training.jobs.get(name="grpo-med-qa")
print(f"Training status: {created_job.properties.status}")

# ── Step 2: Register trained model ───────────────────────────────────────────
model = project_client.models.create_or_update(
    CustomModel(
        name="my-gpt-oss-120B",
        version="1",
        type="FullWeight",
        base_model="azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
        source=ModelSource(
            source_type="TrainingJob",
            job_name="grpo-med-qa",
            output_key="safetensor_model_folder",
        ),
    )
)

# Wait for model registration to complete (weight copy)
while model.provisioning_state not in ("Succeeded", "Failed"):
    time.sleep(30)
    model = project_client.models.get(name="my-gpt-oss-120B")
print(f"Model registered: {model.name} (state: {model.provisioning_state})")

# ── Step 3: Resolve deployment template (see Part B) ─────────────────────────
# See spec-custom-models.md Part B: Bridge

# ── Step 4: Deploy (see Part C) ──────────────────────────────────────────────
# See spec-custom-models.md Part C: Custom Model Deployment
```

---

## Next Steps

After model registration completes, resolve the deployment template and deploy:
- [Part B: Bridge — Resolve Deployment Template](spec-custom-models.md#part-b-bridge--from-registered-model-to-deployment-template)
- [Part C: Deploy the Custom Model](spec-custom-models.md#part-c-custom-model-deployment-control-plane)
