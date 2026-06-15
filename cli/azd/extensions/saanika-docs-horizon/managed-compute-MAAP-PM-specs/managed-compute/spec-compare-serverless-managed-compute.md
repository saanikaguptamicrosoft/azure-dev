# Serverless vs Managed Compute — Side-by-Side Comparison

Quick-reference comparison of the two deployment types under `Microsoft.CognitiveServices/accounts`.

---

## Schema

| Dimension | Serverless (`deployments`) | Managed Compute (`acceleratorDeployments`) |
|---|---|---|
| **ARM resource type** | `Microsoft.CognitiveServices/accounts/deployments` | `Microsoft.CognitiveServices/accounts/acceleratorDeployments` |
| **API version** | `2025-09-01` (GA) | `2026-04-01-preview` |
| **Type model** | Shared type, discriminated by `sku.name` | Dedicated type — `AcceleratorDeploymentProperties` |

### Serverless — Resource JSON

```jsonc
{
  "type": "Microsoft.CognitiveServices/accounts/deployments",
  "name": "gpt-oss-120b-serverless",
  "properties": {
    "model": {
      "format": "OpenAI-OSS",
      "name": "gpt-oss-120b",
      "version": "1"
    },
    "versionUpgradeOption": "OnceNewDefaultVersionAvailable",
    "raiPolicyName": "Microsoft.DefaultV2"
  },
  "sku": {
    "name": "GlobalStandard",
    "capacity": 100                              // tokens per minute (TPM)
  }
}
```

### Managed Compute — Resource JSON

```jsonc
{
  "type": "Microsoft.CognitiveServices/accounts/acceleratorDeployments",
  "name": "gpt-oss-120b-gpu",
  "properties": {
    // --- required on create ---
    "model": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
    "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
    "acceleratorType": "H100_80GB",
    "versionUpgradeOption": "OnceNewDefaultVersionAvailable",
    // raiPolicyName: N/A — content safety is container-level

    // --- read-only (returned in response) ---
    "acceleratorsPerInstance": 4,
    "totalAccelerators": 4,
    "provisioningState": "Succeeded",
    "provisioningDetails": {
      "message": "Deployment is healthy and serving traffic.",
      "lastOperationTimestamp": "2026-03-23T14:45:00Z"
    },
    "routes": {
      "chatCompletionsScoringPath": "/v1/chat/completions",
      "swagger": "/swagger.json",
      "messagesApiScoringPath": "/v1/messages"
    }
  },
  "sku": {
    "name": "GlobalManagedCompute",
    "capacity": 1                                // model instances (replicas), NOT GPUs
  }
}
```

---

## Type / Properties / Identity

| Property | Serverless | Managed Compute |
|---|---|---|
| **Model identity** | Tuple: `{ format, name, version }` (Cog Services registry) | URI: `azureml://registries/{reg}/models/{m}/versions/{v}` (ML Registry) |
| **Deployment template** | N/A | Required — ML Registry template URI defining container, runtime, accelerator maps |
| **Accelerator type** | N/A | Required — e.g. `H100_80GB`, `H200_141GB`, `A100_80GB` |
| **Accelerators per instance** | N/A | Read-only — GPUs consumed by each replica (from template) |
| **Total accelerators** | N/A | Read-only — `capacity × acceleratorsPerInstance` |
| **RAI policy** | `raiPolicyName` — platform-level content filtering | N/A — content safety is container-level (TBD) |
| **Version upgrade option** | `NoAutoUpgrade`, `OnceCurrentVersionExpired`, `OnceNewDefaultVersionAvailable` | Only `OnceNewDefaultVersionAvailable` (others deferred to BYOC) |
| **Provisioning state** | Returned (usually instant) | Returned — critical for 10–15 min LRO polling |
| **Provisioning details** | N/A | `{ message, lastOperationTimestamp }` — progress info during LRO |
| **Inference routes** | N/A (known OpenAI-compatible paths) | Read-only `routes` object with relative paths (`chatCompletionsScoringPath`, `swagger`, `messagesApiScoringPath`) — varies by model |

---

## Capacity / Limits

| Dimension | Serverless | Managed Compute |
|---|---|---|
| **SKU names** | `GlobalStandard`, `DataZoneStandard`, `GlobalProvisionedManaged`, etc. | `GlobalManagedCompute`, `DataZoneManagedCompute` |
| **`sku.capacity` unit** | **Tokens per minute (TPM)** | **Model instances (replicas)** |
| **Capacity example** | `capacity: 100` → 100K TPM | `capacity: 2` → 2 instances × 4 GPUs = 8 GPUs |
| **Quota resource** | `usages` API (TPM per model per SKU) | `acceleratorUsages` API (GPU count per accelerator type) |
| **Capacity resource** | `modelCapacities` API (available TPM per model per region) | `acceleratorCapacities` API (available GPUs per accelerator type) |
| **Scaling granularity** | TPM increments (e.g. +50 TPM) | Whole instance increments (each consumes N GPUs) |
| **Scaling speed** | Near-instant | 10–15 min LRO (provisions GPU resources) |
| **Quota enforcement** | `requestedTPM ≤ availableTPM` | `capacity × acceleratorsPerInstance ≤ availableGPUs` |

---

## Naming

| Dimension | Serverless | Managed Compute |
|---|---|---|
| **Resource type name** | `deployments` | `acceleratorDeployments` |
| **Deployment name rules** | 2–64 chars, `^[a-zA-Z0-9][a-zA-Z0-9_.-]*$` | Same: 2–64 chars, `^[a-zA-Z0-9][a-zA-Z0-9_.-]*$` |
| **Uniqueness scope** | Unique within account (across both resource types) | Same — unique across both `deployments` and `acceleratorDeployments` |
| **SDK class** | `Deployment`, `DeploymentProperties`, `DeploymentModel` | `AcceleratorDeployment`, `AcceleratorDeploymentProperties` |
| **CLI noun** | `az cognitiveservices account deployment` | `az cognitiveservices account accelerator-deployment` |
| **ARM permission prefix** | `.../deployments/read\|write\|delete` | `.../acceleratorDeployments/read\|write\|delete` |

---

## REST API Comparison

### Create Deployment

#### Serverless

```http
PUT https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /deployments/gpt-oss-120b-serverless?api-version=2025-09-01
Authorization: Bearer {token}
Content-Type: application/json

{
  "properties": {
    "model": {
      "format": "OpenAI-OSS",
      "name": "gpt-oss-120b",
      "version": "1"
    },
    "versionUpgradeOption": "OnceNewDefaultVersionAvailable",
    "raiPolicyName": "Microsoft.DefaultV2"
  },
  "sku": { "name": "GlobalStandard", "capacity": 100 }
}
```

#### Managed Compute

```http
PUT https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /acceleratorDeployments/gpt-oss-120b-gpu?api-version=2026-04-01-preview
Authorization: Bearer {token}
Content-Type: application/json

{
  "properties": {
    "model": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
    "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
    "acceleratorType": "H100_80GB"
  },
  "sku": { "name": "GlobalManagedCompute", "capacity": 1 }
}
```

### List Deployments

#### Serverless

```http
GET https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /deployments?api-version=2025-09-01
Authorization: Bearer {token}
```

#### Managed Compute

```http
GET https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /acceleratorDeployments?api-version=2026-04-01-preview
Authorization: Bearer {token}
```

### Scale

#### Serverless — increase TPM

```http
PUT https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /deployments/gpt-oss-120b-serverless?api-version=2025-09-01
Authorization: Bearer {token}
Content-Type: application/json

{ "sku": { "name": "GlobalStandard", "capacity": 500 } }
```

#### Managed Compute — add instances

```http
PUT https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /acceleratorDeployments/gpt-oss-120b-gpu?api-version=2026-04-01-preview
Authorization: Bearer {token}
Content-Type: application/json

{ "sku": { "name": "GlobalManagedCompute", "capacity": 2 } }
```

### Delete

#### Serverless

```http
DELETE https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /deployments/gpt-oss-120b-serverless?api-version=2025-09-01
Authorization: Bearer {token}
```

#### Managed Compute

```http
DELETE https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /acceleratorDeployments/gpt-oss-120b-gpu?api-version=2026-04-01-preview
Authorization: Bearer {token}
```

### Inference

#### Serverless

```http
POST https://{account}.openai.azure.com/openai/deployments/gpt-oss-120b-serverless/chat/completions?api-version=2025-09-01
api-key: {key}
Content-Type: application/json

{ "messages": [{"role": "user", "content": "Hello"}], "max_tokens": 100 }
```

#### Managed Compute

```http
POST https://{account}.openai.azure.com/managed-deployments/gpt-oss-120b-gpu/v1/chat/completions
api-key: {key}
Content-Type: application/json

{ "messages": [{"role": "user", "content": "Hello"}], "max_tokens": 100 }
```

---

## Summary — Key Differences at a Glance

```
                          SERVERLESS                MANAGED COMPUTE
                          ──────────                ───────────────
Resource type             deployments               acceleratorDeployments
Model reference           (format, name, version)   azureml:// asset URI
Deployment template       N/A                       Required (container + runtime)
Accelerator               N/A                       Required (H100, H200, A100…)
SKU                       GlobalStandard / etc.     GlobalManagedCompute / DataZone
Capacity unit             TPM                       Model instances (replicas)
Provisioning time         Seconds                   10–15 minutes (LRO)
RAI policy                Platform-level            Container-level (TBD)
Inference URL prefix      /openai/deployments/      /managed-deployments/
Route discovery           Fixed (OpenAI-compat)     From routes object (per model)
Logs API                  N/A                       getLogs (per-instance + infra)
RBAC                      Shared with other deps    Separate resource = separate perms
```
