# Accelerator Deployments — SDK/CLI Spec

- TypeSpec: TBD (new resource type under `Microsoft.CognitiveServices`)
- PM One Pager: [spec-deployments.md](spec-deployments.md)
- Tech Spec: [spec-deployments.md](spec-deployments.md) + [deployments_crud/](deployments_crud/)
- TypeSpec owners: TBD
- Service Owners: TBD
- SDK Owners: TBD

## What is the goal of this feature?

Enable users to deploy GPU-based (managed compute) models from the Azure AI Model Catalog onto dedicated accelerator infrastructure through a first-class ARM API, SDK, and CLI — with full lifecycle management (create, scale, monitor, debug, delete).

**Success metric:** 100+ unique subscriptions with active accelerator deployments within 3 months of GA.

Users get dedicated GPU capacity for open-source and first-party models (e.g., Llama, Phi, Mistral) with the same account-level endpoint, auth, and networking they already use for serverless deployments — no new infrastructure to learn.

## What is the problem being solved?

1. **No first-class ARM API for GPU deployments.** Today, managed compute model deployments go through Azure ML's MIR (Managed Inference Runtime) with a separate endpoint per deployment, separate auth/keys, and no integration with Foundry's account-level networking. Users manage two disconnected systems.

2. **Union-type API complexity.** Serverless and managed compute have fundamentally different properties. Sharing the existing `deployments` API would create a union type where 50%+ of fields are irrelevant for any given deployment, confusing SDK users and breaking type safety.

3. **No deployment debugging.** Users can't inspect container logs, infrastructure provisioning events, or per-instance health — they file support tickets for issues they could self-diagnose.

## Requirements

After we deliver the accelerator deployments feature, users will be able to:

1. **Create** a managed compute deployment by specifying a model, deployment template, and accelerator type — all discovered from the catalog
2. **Get** deployment details including provisioning state, instance count, and inference routes
3. **List** all managed compute deployments on an account (type-safe — no serverless deployments mixed in)
4. **Update** a deployment: scale instances, change accelerator type, or update the deployment template
5. **Delete** a deployment and release GPU resources
6. **Get logs** from inference server containers, model downloader init containers, or infrastructure-level events — per-instance for multi-instance deployments
7. All operations use **ARM auth** (Microsoft Entra ID) and standard ARM RBAC
8. Create/Update/Delete are **long-running operations** (10–15 min) with standard ARM async polling

## E2E Code Samples

### Deploy a model from catalog and run inference

```python
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.mgmt.cognitiveservices.models import (
    AcceleratorDeployment, AcceleratorDeploymentProperties, Sku,
)
from azure.identity import DefaultAzureCredential
from openai import AzureOpenAI

credential = DefaultAzureCredential()
cog = CognitiveServicesManagementClient(credential, subscription_id)
rg, account = "my-rg", "my-foundry-account"

# Step 1: Create deployment (inputs from catalog discovery)
deployment = AcceleratorDeployment(
    properties=AcceleratorDeploymentProperties(
        model="azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
        deployment_template="azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
        accelerator_type="H100_80GB",
    ),
    sku=Sku(name="GlobalManagedCompute", capacity=1),
)
poller = cog.accelerator_deployments.begin_create_or_update(rg, account, "gpt-oss-120b-gpu", deployment)
result = poller.result()  # waits ~10-15 min

# Step 2: Use the deployment for inference (same account endpoint as serverless)
account_info = cog.accounts.get(rg, account)
client = AzureOpenAI(
    azure_endpoint=account_info.properties.endpoint,
    api_key=cog.accounts.list_keys(rg, account).key1,
    api_version="2025-09-01",
)
response = client.chat.completions.create(
    model="gpt-oss-120b-gpu",  # deployment name
    messages=[{"role": "user", "content": "Hello"}],
)
print(response.choices[0].message.content)
```

### Debug a failed deployment

```python
from azure.mgmt.cognitiveservices.models import DeploymentLogsRequest

# Check infrastructure logs — what happened during provisioning?
logs = cog.accelerator_deployments.get_logs(
    resource_group_name=rg,
    account_name=account,
    deployment_name="gpt-oss-120b-gpu",
    body=DeploymentLogsRequest(log_source="Infrastructure", tail=200),
)
print(logs.infrastructure)

# Check model download logs — did weights fail to download?
logs = cog.accelerator_deployments.get_logs(
    resource_group_name=rg,
    account_name=account,
    deployment_name="gpt-oss-120b-gpu",
    body=DeploymentLogsRequest(log_source="ModelDownloader"),
)
for instance in logs.instances:
    print(f"--- Instance {instance.index} ({instance.instance_id}) ---")
    print(instance.content)
```

## CRUD APIs

### Create or Update

**REST API**

```
PUT https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /acceleratorDeployments/{deploymentName}?api-version=2026-04-01-preview
```

**SDK**

```python
deployment = AcceleratorDeployment(
    properties=AcceleratorDeploymentProperties(
        model="azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
        deployment_template="azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
        accelerator_type="H100_80GB",
    ),
    sku=Sku(name="GlobalManagedCompute", capacity=1),
)

# LRO — returns poller
poller = cog.accelerator_deployments.begin_create_or_update(rg, account, "gpt-oss-120b-gpu", deployment)
result = poller.result()
```

**CLI**

```bash
az cognitiveservices account accelerator-deployment create \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --model "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4" \
  --deployment-template "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1" \
  --accelerator-type H100_80GB \
  --sku-name GlobalManagedCompute --sku-capacity 1
```

### Get

**REST API**

```
GET https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /acceleratorDeployments/{deploymentName}?api-version=2026-04-01-preview
```

**SDK**

```python
dep = cog.accelerator_deployments.get(rg, account, "gpt-oss-120b-gpu")
print(f"State: {dep.properties.provisioning_state}")
print(f"GPUs:  {dep.properties.total_accelerators}")
print(f"Routes: {dep.properties.routes}")
```

**CLI**

```bash
az cognitiveservices account accelerator-deployment show \
  --name $ACCOUNT -g $RG --deployment-name gpt-oss-120b-gpu
```

### List

**REST API**

```
GET https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /acceleratorDeployments?api-version=2026-04-01-preview
```

**SDK**

```python
for dep in cog.accelerator_deployments.list(rg, account):
    print(f"{dep.name}: {dep.properties.accelerator_type} × {dep.sku.capacity} inst")
```

**CLI**

```bash
az cognitiveservices account accelerator-deployment list --name $ACCOUNT -g $RG -o table
```

### Update (Scale)

Uses `PUT` (same as create). Only changed fields are required.

**SDK**

```python
scale = AcceleratorDeployment(sku=Sku(name="GlobalManagedCompute", capacity=2))
cog.accelerator_deployments.begin_create_or_update(rg, account, "gpt-oss-120b-gpu", scale).result()
```

**CLI**

```bash
az cognitiveservices account accelerator-deployment create \
  --name $ACCOUNT -g $RG --deployment-name gpt-oss-120b-gpu --sku-capacity 2
```

### Delete

**REST API**

```
DELETE https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /acceleratorDeployments/{deploymentName}?api-version=2026-04-01-preview
```

**SDK**

```python
cog.accelerator_deployments.begin_delete(rg, account, "gpt-oss-120b-gpu").result()
```

**CLI**

```bash
az cognitiveservices account accelerator-deployment delete \
  --name $ACCOUNT -g $RG --deployment-name gpt-oss-120b-gpu
```

### Get Logs

**REST API**

```
POST https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /acceleratorDeployments/{deploymentName}/getLogs?api-version=2026-04-01-preview
```

**SDK**

```python
# All instance logs
logs = cog.accelerator_deployments.get_logs(
    rg, account, "gpt-oss-120b-gpu",
    body=DeploymentLogsRequest(log_source="InferenceServer", tail=100),
)
for inst in logs.instances:
    print(f"--- {inst.instance_id} (index {inst.index}) ---\n{inst.content}")

# Infrastructure logs
logs = cog.accelerator_deployments.get_logs(
    rg, account, "gpt-oss-120b-gpu",
    body=DeploymentLogsRequest(log_source="Infrastructure"),
)
print(logs.infrastructure)
```

**CLI**

```bash
# Inference server logs (all instances)
az cognitiveservices account accelerator-deployment get-logs \
  --name $ACCOUNT -g $RG --deployment-name gpt-oss-120b-gpu

# Infrastructure logs
az cognitiveservices account accelerator-deployment get-logs \
  --name $ACCOUNT -g $RG --deployment-name gpt-oss-120b-gpu \
  --log-source Infrastructure

# Specific instance
az cognitiveservices account accelerator-deployment get-logs \
  --name $ACCOUNT -g $RG --deployment-name gpt-oss-120b-gpu \
  --instance-id xk9f2a --tail 50
```
