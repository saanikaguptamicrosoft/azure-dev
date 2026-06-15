# Capacity APIs — `modelCapacities` vs `acceleratorCapacities`

Side-by-side comparison of the capacity APIs for serverless and managed compute deployments.

Both APIs answer "is there physical infrastructure available?" — but against fundamentally different pools.

---

## Overview

| Dimension | Serverless (`modelCapacities`) | Managed Compute (`acceleratorCapacities`) |
|---|---|---|
| **API** | `GET .../modelCapacities` | `GET .../acceleratorCapacities` |
| **API version** | `2025-09-01` (GA) | `2026-04-01-preview` (proposed) |
| **Status** | Existing — shipping today | Proposed — needs to be built |
| **Backing service** | Cog Services capacity store | AzureML capacity service |
| **Pool type** | Per-model, per-SKU, per-region | Per-accelerator type, model-agnostic (shared GPU pool) |
| **Capacity unit** | Tokens per minute (TPM) | Accelerator (GPU) count or instance count |
| **Fragmentation** | Not applicable (token throughput is fungible) | Matters — contiguous GPU blocks needed for multi-GPU models |

---

## Schema

### Serverless — `modelCapacities` response

```jsonc
{
  "location": "EastUS",
  "properties": {
    "model": {
      "format": "OpenAI-OSS",
      "name": "gpt-oss-120b",
      "version": "1"
    },
    "skuName": "GlobalStandard",
    "availableCapacity": 45000,          // TPM available in this region
    "availableFinetuneCapacity": 0
  }
}
```

### Managed Compute — `acceleratorCapacities` response (Option A: accelerator-scoped)

```jsonc
{
  "acceleratorSku": "H100_80G",
  "offer": "GlobalManagedCompute",
  "location": "global",
  "availableAccelerators": 240,           // total GPUs in pool
  "deploymentSizeCapacities": [
    // breakdown by deployment size — shows fragmentation
    { "modelInstanceAcceleratorCount": 1, "totalAvailableCapacity": 240, "largestDeploymentCapacity": 240 },
    { "modelInstanceAcceleratorCount": 2, "totalAvailableCapacity": 120, "largestDeploymentCapacity": 96 },
    { "modelInstanceAcceleratorCount": 4, "totalAvailableCapacity": 60,  "largestDeploymentCapacity": 48 },
    { "modelInstanceAcceleratorCount": 8, "totalAvailableCapacity": 30,  "largestDeploymentCapacity": 24 }
  ]
}
```

### Managed Compute — `acceleratorCapacities` response (Option B: model-scoped)

```jsonc
{
  "modelId": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
  "offer": "GlobalManagedCompute",
  "location": "global",
  "capacities": [
    {
      "deploymentTemplate": "azureml://…/gpt-oss-120b-short-context/versions/1",
      "acceleratorType": "H100_80GB",
      "acceleratorsPerInstance": 4,
      "availableAccelerators": 240,       // raw GPUs
      "availableInstances": 60            // ready-to-use unit for sku.capacity
    },
    {
      "deploymentTemplate": "azureml://…/gpt-oss-120b-long-context/versions/1",
      "acceleratorType": "H100_80GB",
      "acceleratorsPerInstance": 8,
      "availableAccelerators": 240,
      "availableInstances": 30
    }
  ]
}
```

---

## Key Differences

| Dimension | Serverless | Managed Compute |
|---|---|---|
| **Query parameters** | `modelFormat`, `modelName`, `modelVersion` | Option A: `acceleratorSku`, `offer`, `location`; Option B: `modelId`, `offer`, `location` |
| **Scoping** | Per model × per SKU × per region | Option A: per accelerator × per offer × per location (model-agnostic); Option B: per model (resolved server-side) |
| **What it tells you** | "You can add up to 45,000 TPM more of gpt-oss-120b in EastUS on GlobalStandard" | Option A: "You have 240 H100 GPUs available, here's how they break down by deployment size"; Option B: "For gpt-oss-120b, you can deploy up to 60 short-context instances or 30 long-context instances" |
| **Pools are shared** | No — each model has its own capacity allocation | Yes — all models on H100 draw from the same GPU pool |
| **Fragmentation** | N/A (throughput is fungible) | Visible — `largestDeploymentCapacity` shows largest contiguous block vs `totalAvailableCapacity` |
| **Template awareness** | N/A | Option A: no; Option B: yes — resolves model → template → accelerator maps server-side |
| **Backend complexity** | Low — single lookup in Cog Services capacity store | Option A: low (raw pool query); Option B: higher (joins ML Registry + capacity) |

---

## REST API Comparison

### Serverless

```http
GET https://management.azure.com/subscriptions/{sub}/providers/Microsoft.CognitiveServices
    /modelCapacities?api-version=2025-09-01
    &modelFormat=OpenAI-OSS&modelName=gpt-oss-120b&modelVersion=1
Authorization: Bearer {token}
```

Response (one entry per region × SKU):

```jsonc
{
  "value": [
    {
      "location": "EastUS",
      "properties": {
        "model": { "format": "OpenAI-OSS", "name": "gpt-oss-120b", "version": "1" },
        "skuName": "GlobalStandard",
        "availableCapacity": 45000                // TPM available
      }
    },
    {
      "location": "WestUS2",
      "properties": {
        "model": { "format": "OpenAI-OSS", "name": "gpt-oss-120b", "version": "1" },
        "skuName": "GlobalStandard",
        "availableCapacity": 38000
      }
    }
  ]
}
```

### Managed Compute (Option A — accelerator-scoped)

```http
GET https://management.azure.com/subscriptions/{sub}/providers/Microsoft.CognitiveServices
    /acceleratorCapacities?api-version=2026-04-01-preview
    &acceleratorSku=H100_80G&offer=GlobalManagedCompute&location=global
Authorization: Bearer {token}
```

Response:

```jsonc
{
  "value": [
    {
      "acceleratorSku": "H100_80G",
      "offer": "GlobalManagedCompute",
      "location": "global",
      "availableAccelerators": 240,
      "deploymentSizeCapacities": [
        { "modelInstanceAcceleratorCount": 1, "totalAvailableCapacity": 240, "largestDeploymentCapacity": 240 },
        { "modelInstanceAcceleratorCount": 4, "totalAvailableCapacity": 60,  "largestDeploymentCapacity": 48 },
        { "modelInstanceAcceleratorCount": 8, "totalAvailableCapacity": 30,  "largestDeploymentCapacity": 24 }
      ]
    }
  ]
}
```

### Managed Compute (Option B — model-scoped)

```http
GET https://management.azure.com/subscriptions/{sub}/providers/Microsoft.CognitiveServices
    /acceleratorCapacities?api-version=2026-04-01-preview
    &modelId=azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4
    &offer=GlobalManagedCompute&location=global
Authorization: Bearer {token}
```

Response:

```jsonc
{
  "value": [
    {
      "modelId": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
      "offer": "GlobalManagedCompute",
      "location": "global",
      "capacities": [
        {
          "deploymentTemplate": "azureml://…/gpt-oss-120b-short-context/versions/1",
          "acceleratorType": "H100_80GB",
          "acceleratorsPerInstance": 4,
          "availableAccelerators": 240,
          "availableInstances": 60
        },
        {
          "deploymentTemplate": "azureml://…/gpt-oss-120b-long-context/versions/1",
          "acceleratorType": "H100_80GB",
          "acceleratorsPerInstance": 8,
          "availableAccelerators": 240,
          "availableInstances": 30
        }
      ]
    }
  ]
}
```

---

## SDK / CLI Comparison

### Serverless

```python
capacities = cog.model_capacities.list(
    model_format="OpenAI-OSS",
    model_name="gpt-oss-120b",
    model_version="1",
)
for c in capacities:
    if c.properties.sku_name == "GlobalStandard":
        print(f"  {c.location}: {c.properties.available_capacity} TPM")
```

```bash
az cognitiveservices model capacity list \
  --model-format OpenAI-OSS --model-name gpt-oss-120b --model-version 1 \
  --query "[?properties.skuName=='GlobalStandard']" -o table
```

### Managed Compute (Option A)

```python
acc_caps = cog.accelerator_capacities.list(
    accelerator_sku="H100_80G",
    offer="GlobalManagedCompute",
    location="global",
)
for cap in acc_caps:
    print(f"{cap.accelerator_sku}: {cap.available_accelerators} total GPUs")
    for size in cap.deployment_size_capacities:
        print(f"  {size.model_instance_accelerator_count} GPUs/instance: "
              f"total={size.total_available_capacity}, "
              f"largest_block={size.largest_deployment_capacity}")
```

```bash
az cognitiveservices accelerator capacity list \
  --accelerator-sku H100_80G --offer GlobalManagedCompute --location global -o table
```

### Managed Compute (Option B)

```python
acc_caps = cog.accelerator_capacities.list(
    model_id="azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
    offer="GlobalManagedCompute",
    location="global",
)
for cap in acc_caps:
    for c in cap.capacities:
        print(f"  {c.accelerator_type} × {c.accelerators_per_instance}: "
              f"{c.available_instances} instances ({c.available_accelerators} GPUs)")
```

```bash
az cognitiveservices accelerator capacity list \
  --model-id azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4 \
  --offer GlobalManagedCompute --location global -o table
```

---

## Open Decision: Option A vs Option B

| | Option A: Accelerator-scoped | Option B: Model-scoped |
|---|---|---|
| **User question** | "How many H100s are available?" | "What can I deploy for this model?" |
| **Model-aware** | No | Yes |
| **Template-aware** | No | Yes (cross-product resolved server-side) |
| **Fragmentation** | Visible via `deploymentSizeCapacities` | Not directly exposed |
| **Backend complexity** | Low | Higher (joins ML Registry + capacity) |
| **Mirrors** | Raw infrastructure view | Serverless `modelCapacities` pattern |
