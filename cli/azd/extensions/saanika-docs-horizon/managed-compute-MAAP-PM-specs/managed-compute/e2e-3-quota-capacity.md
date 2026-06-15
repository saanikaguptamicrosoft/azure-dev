# Quota and Capacity

The user now decides to deploy the model both ways. Quota for serverless is model-scoped (TPM); quota for managed compute is accelerator-scoped (GPU count). These are fundamentally different pools — checking one tells you nothing about the other.

---

## Check Quota

### 3A. Serverless Quota

"How many tokens per minute of `gpt-oss-120b` on GlobalStandard do I have?"

**REST (existing):**
```http
GET https://management.azure.com/subscriptions/{sub}/providers/Microsoft.CognitiveServices
    /locations/global/usages?api-version=2025-09-01
Authorization: Bearer {token}
```

**Output** (filtered to `gpt-oss-120b`):

```json
{
  "currentValue": 0,
  "limit": 5000,
  "name": {
    "localizedValue": "Tokens Per Minute (thousands) - gpt-oss-120b",
    "value": "AIServices.GlobalStandard.gpt-oss-120b"
  },
  "scopeId": "global",
  "scopeType": "Regional",
  "unit": "Count"
}
```

> Quota is **per this specific model** — deploying another model doesn't consume it. `currentValue: 0` means no TPM allocated yet; `limit: 5000` is the subscription ceiling (in thousands).

**SDK (existing):**
```python
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.identity import DefaultAzureCredential

cog = CognitiveServicesManagementClient(DefaultAzureCredential(), subscription_id)

usages = cog.usages.list(location="global")
for u in usages:
    if "gpt-oss-120b" in u.name.value and "GlobalStandard" in u.name.value:
        print(f"{u.name.value}: {u.current_value}/{u.limit} TPM")
        # OpenAI-OSS.GlobalStandard.gpt-oss-120b: 0/500000 TPM
```

**CLI (existing):**
```bash
az cognitiveservices usage list --location global --subscription $SUB \
  --query "[?contains(name.value, 'gpt-oss-120b')]" -o table
```

### 3B. Managed Compute Quota

"How many H100 GPUs do I have globally?" — this is model-agnostic; the same GPU pool serves any model.

**REST (proposed):**
```http
GET https://management.azure.com/subscriptions/{sub}/providers/Microsoft.CognitiveServices
    /acceleratorUsages?api-version=2026-01-01-preview
    &acceleratorSku=H100_80G&offer=GlobalManagedCompute&location=global
Authorization: Bearer {token}
```

**Output** (proposed):

```json
{
  "currentValue": 0,
  "limit": 64,
  "name": {
    "localizedValue": "H100_80G GPU Count",
    "value": "H100_80G.GlobalManagedCompute.global"
  },
  "acceleratorSku": "H100_80G",
  "offer": "GlobalManagedCompute",
  "location": "global",
  "unit": "GPUs"
}
```

> Quota is **shared across ALL models** on H100 — deploying any model on H100s reduces the same pool. `limit: 64` means 64 GPUs total for the subscription.

**SDK (proposed):**
```python
acc_usages = cog.accelerator_usages.list(
    accelerator_sku="H100_80G",
    offer="GlobalManagedCompute",
    location="global",
)
for u in acc_usages:
    print(f"{u.accelerator_sku} ({u.offer}, {u.location}): "
          f"{u.current_value}/{u.limit} GPUs")
    # H100_80G (GlobalManagedCompute, global): 0/64 GPUs → 64 GPUs available
```

**CLI (proposed):**
```bash
az cognitiveservices accelerator usage list \
  --subscription $SUB --accelerator-sku H100_80G \
  --offer GlobalManagedCompute --location global -o table
```

### Why `acceleratorUsages` is a new API (not added to `usages`)

The existing `usages` API already mixes model-specific and model-agnostic entries in the same response — for example, `AIServices.GlobalStandard.gpt-oss-120b` is per-model while `OpenAI.GlobalProvisionedManaged` is a shared pool across all models. This inconsistency makes the API harder to reason about: the caller has to parse the key name to know whether a quota entry applies to one model or all of them. Adding accelerator quota to this same list would compound the problem.

More importantly, the underlying quota store for managed compute accelerators is the AzureML quota service, not the Cog Services quota store. Exposing accelerator quota through a separate `acceleratorUsages` endpoint maps cleanly to the backing service — the Foundry RP proxies directly to AzureML's quota records rather than trying to merge two different quota backends into one response.

A separate `acceleratorUsages` endpoint keeps the contract clean: callers querying serverless quota get exactly what they expect from the existing `usages` API, and callers querying GPU quota get a purpose-built response backed by the AzureML quota service with the right fields (`acceleratorSku`, `offer`, `location`, GPU-denominated units).

---

## Check Capacity

"Is there physical infrastructure available to serve my deployment?"

### 4A. Serverless Capacity

Per-model, per-SKU, per-region. "Where can I run `gpt-oss-120b` on GlobalStandard, and how much capacity is there?"

**REST (existing):**
```http
GET https://management.azure.com/subscriptions/{sub}/providers/Microsoft.CognitiveServices
    /modelCapacities?api-version=2025-09-01
    &modelFormat=OpenAI-OSS&modelName=gpt-oss-120b&modelVersion=1
Authorization: Bearer {token}
```

**Output** (one entry per region × SKU):

```json
{
  "location": "EastUS",
  "properties": {
    "model": {
      "format": "OpenAI-OSS",
      "name": "gpt-oss-120b",
      "version": "1"
    },
    "skuName": "GlobalStandard",
    "availableCapacity": 45000,
    "availableFinetuneCapacity": 0
  }
}
```

> Capacity is **per this model, per region**. Other regions return their own entries (westus2: 38000, swedencentral: 29000, etc.).

**SDK (existing):**
```python
capacities = cog.model_capacities.list(
    model_format="OpenAI-OSS",   # from catalog's modelFormat field
    model_name="gpt-oss-120b",
    model_version="1",
)
for c in capacities:
    if c.properties.sku_name == "GlobalStandard":
        print(f"  {c.location}: {c.properties.available_capacity} TPM")
        # eastus: 45000 TPM, westus2: 38000 TPM, swedencentral: 29000 TPM, ...
```

**CLI (existing):**
```bash
az cognitiveservices model capacity list \
  --model-format OpenAI-OSS --model-name gpt-oss-120b --model-version 1 \
  --query "[?properties.skuName=='GlobalStandard']" -o table
```

### 4B. Managed Compute Capacity

"How many accelerators are available for my deployment?" Two design options:

#### Option A: Deployment-size breakdown (accelerator-scoped)

Per-accelerator, per-offer, per-location. Model-agnostic. Returns capacity broken down by deployment size, showing how many model instances can be deployed at each accelerator count.

**REST (proposed):**
```http
GET https://management.azure.com/subscriptions/{sub}/providers/Microsoft.CognitiveServices
    /acceleratorCapacities?api-version=2026-01-01-preview
    &acceleratorSku=H100_80G&offer=GlobalManagedCompute&location=global
Authorization: Bearer {token}
```

**Output** (proposed):

```json
{
  "acceleratorSku": "H100_80G",
  "offer": "GlobalManagedCompute",
  "location": "global",
  "availableAccelerators": 240,
  "deploymentSizeCapacities": [
    { "modelInstanceAcceleratorCount": 1, "totalAvailableCapacity": 240, "largestDeploymentCapacity": 240 },
    { "modelInstanceAcceleratorCount": 2, "totalAvailableCapacity": 120, "largestDeploymentCapacity": 96 },
    { "modelInstanceAcceleratorCount": 4, "totalAvailableCapacity": 60, "largestDeploymentCapacity": 48 },
    { "modelInstanceAcceleratorCount": 8, "totalAvailableCapacity": 30, "largestDeploymentCapacity": 24 }
  ]
}
```

> Capacity is **model-agnostic** — any model on H100s draws from the same pool. `largestDeploymentCapacity` shows the biggest contiguous block; fragmentation matters here (unlike serverless). For `gpt-oss-120b` short-context needing 4 accelerators per instance, there are 48 deployments' worth of contiguous capacity. Long-context needing 8 would have 24.

**SDK (proposed):**
```python
acc_caps = cog.accelerator_capacities.list(
    accelerator_sku="H100_80G",
    offer="GlobalManagedCompute",
    location="global",
)
for cap in acc_caps:
    print(f"{cap.accelerator_sku} ({cap.offer}, {cap.location}): "
          f"{cap.available_accelerators} total accelerators")
    for size in cap.deployment_size_capacities:
        print(f"  {size.model_instance_accelerator_count} accelerators/instance: "
              f"total={size.total_available_capacity}, "
              f"largest_block={size.largest_deployment_capacity}")
    # H100_80G (GlobalManagedCompute, global): 240 total accelerators
    #   1 accelerators/instance: total=240, largest_block=240
    #   2 accelerators/instance: total=120, largest_block=96
    #   4 accelerators/instance: total=60, largest_block=48   ← gpt-oss-120b short-context
    #   8 accelerators/instance: total=30, largest_block=24   ← gpt-oss-120b long-context
```

**CLI (proposed):**
```bash
az cognitiveservices accelerator capacity list \
  --subscription $SUB --accelerator-sku H100_80G \
  --offer GlobalManagedCompute --location global -o table
```

#### Option B: Model-scoped (mirrors serverless `modelCapacities`)

Per-model, per-offer, per-location. The user passes a model ID and the system resolves the deployment template × accelerator combinations, returning capacity in instance count — the unit the user specifies at deployment time.

**REST (proposed):**
```http
GET https://management.azure.com/subscriptions/{sub}/providers/Microsoft.CognitiveServices
    /acceleratorCapacities?api-version=2026-01-01-preview
    &modelId=azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4
    &offer=GlobalManagedCompute&location=global
Authorization: Bearer {token}
```

**Output** (proposed):

```json
{
  "modelId": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
  "offer": "GlobalManagedCompute",
  "location": "global",
  "capacities": [
    {
      "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
      "acceleratorType": "H100_80GB",
      "acceleratorsPerInstance": 4,
      "availableAccelerators": 240,
      "availableInstances": 60
    },
    {
      "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
      "acceleratorType": "H200_141GB",
      "acceleratorsPerInstance": 2,
      "availableAccelerators": 180,
      "availableInstances": 90
    },
    {
      "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-long-context/versions/1",
      "acceleratorType": "H100_80GB",
      "acceleratorsPerInstance": 8,
      "availableAccelerators": 240,
      "availableInstances": 30
    },
    {
      "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-long-context/versions/1",
      "acceleratorType": "H200_141GB",
      "acceleratorsPerInstance": 4,
      "availableAccelerators": 180,
      "availableInstances": 45
    }
  ]
}
```

> The RP resolves model → deployment templates → accelerator maps server-side. Each entry shows a deployable configuration with capacity in both raw accelerator count and instance count. The user sees exactly which configurations are available and picks one for deployment.

**SDK (proposed):**
```python
acc_caps = cog.accelerator_capacities.list(
    model_id="azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
    offer="GlobalManagedCompute",
    location="global",
)
for cap in acc_caps:
    for c in cap.capacities:
        print(f"  {c.accelerator_type} × {c.accelerators_per_instance}: "
              f"{c.available_instances} instances ({c.available_accelerators} accelerators)")
    # short-context / H100_80GB × 4: 60 instances (240 accelerators)
    # short-context / H200_141GB × 2: 90 instances (180 accelerators)
    # long-context / H100_80GB × 8: 30 instances (240 accelerators)
    # long-context / H200_141GB × 4: 45 instances (180 accelerators)
    # ...
```

**CLI (proposed):**
```bash
az cognitiveservices accelerator capacity list \
  --subscription $SUB \
  --model-id azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4 \
  --offer GlobalManagedCompute --location global -o table
```

#### Option A vs. Option B

| | Option A: Deployment-size breakdown | Option B: Model-scoped |
|---|---|---|
| **User query** | "How many H100s are available, by deployment size?" | "What can I deploy for this model?" |
| **Mirrors** | Raw pool view with size breakdown | Serverless `modelCapacities` |
| **Unit** | Accelerator count per size bucket | Instance count (ready to use) |
| **Model-aware** | No | Yes |
| **Template-aware** | No | Yes (cross-product resolved server-side) |
| **Backend complexity** | Low | Higher (joins ML Registry + capacity) |

Option A is simpler and exposes the shared pool directly with fragmentation visibility. Option B is more user-friendly — it answers the deployment question directly in the unit the user will specify at deployment time, and it pre-resolves the template × accelerator matrix so the user doesn't need to fetch templates separately.

Both pre-flight checks pass. We have quota headroom and physical capacity for both deployment types. Now deploy.
