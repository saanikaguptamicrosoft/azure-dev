# Accelerator Deployments — Create or Update

Creates or updates a managed compute (accelerator) deployment under a Foundry resource (Cognitive Services account).

This is a **long-running operation (LRO)**. The initial response returns `201 Created` with `provisioningState: "Accepted"`. The deployment typically takes **10–15 minutes** to become ready. Use `GET` on the deployment URL or the `Azure-AsyncOperation` header URL to poll for completion.

---

## HTTP Request

```http
PUT https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}
    /providers/Microsoft.CognitiveServices/accounts/{accountName}
    /acceleratorDeployments/{deploymentName}?api-version=2026-04-01-preview
Authorization: Bearer {token}
Content-Type: application/json
```

## URI Parameters

| Name | In | Required | Type | Description |
|---|---|---|---|---|
| `subscriptionId` | path | Yes | `string` (UUID) | The ID of the target subscription. |
| `resourceGroupName` | path | Yes | `string` (1–90 chars) | The name of the resource group. Case-insensitive. |
| `accountName` | path | Yes | `string` (2–64 chars, `^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`) | The name of the Cognitive Services / Foundry account. |
| `deploymentName` | path | Yes | `string` (2–64 chars, `^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`) | The name for this accelerator deployment. Must be unique within the account. This name is used in the inference URL path. |
| `api-version` | query | Yes | `string` | API version. Minimum: `2026-04-01-preview`. |

---

## Request Body

### `AcceleratorDeployment`

| Name | Type | Required | Description |
|---|---|---|---|
| `properties` | [`AcceleratorDeploymentProperties`](#acceleratordeploymentproperties) | Yes | Properties of the accelerator deployment. |
| `sku` | [`Sku`](#sku) | Yes | The SKU definition — name and instance count. |
| `tags` | `object` | No | Resource tags (key-value string pairs). |

### `AcceleratorDeploymentProperties`

| Name | Type | Required | Description |
|---|---|---|---|
| `model` | `string` (URI) **or** [`AcceleratorDeploymentModel`](#acceleratordeploymentmodel-custom-models) (object) | Yes | **Catalog models:** ML Registry asset ID string — `azureml://registries/{registryName}/models/{modelName}/versions/{version}`. **Custom models (BYOW):** An object with `format`, `name`, `version`, and `source` fields. See [spec-custom-models.md](../spec-custom-models.md) for details. |
| `deploymentTemplate` | `string` (URI) | Yes | The ML Registry asset ID of the deployment template defining the serving container, accelerator maps, and runtime configuration. Format: `azureml://registries/{registryName}/deploymenttemplates/{templateName}/versions/{version}`. Obtained from the model's `allowed_deployment_templates` in the ML Registry. |
| `acceleratorType` | `string` | Yes | The accelerator type to use for this deployment. Must match one of the entries in the deployment template's `accelerator_maps`. Examples: `H100_80GB`, `H200_141GB`, `A100_80GB`. |
| `versionUpgradeOption` | [`DeploymentVersionUpgradeOption`](#deploymentversionupgradeoption) | No | Controls when the platform upgrades the deployment template (container image, serving runtime). Default: `OnceNewDefaultVersionAvailable`. Only `OnceNewDefaultVersionAvailable` is supported initially — the platform retains the right to upgrade containers for security patches, CVE remediation, and runtime fixes. `NoAutoUpgrade` and `OnceCurrentVersionExpired` will be enabled when BYOC is supported. |

### `Sku`

| Name | Type | Required | Description |
|---|---|---|---|
| `name` | `string` | Yes | The SKU name. Must be `GlobalManagedCompute` or `DataZoneManagedCompute`. |
| `capacity` | `integer` (int32) | Yes | The number of **model instances** (replicas) to deploy — **not** the number of accelerators. Each instance consumes `acceleratorsPerInstance` accelerators (defined by the deployment template). Total accelerators consumed = `capacity × acceleratorsPerInstance`. Minimum: `1`. Example: for a model requiring 4 × H100 per instance, `capacity: 2` provisions 2 instances using 8 accelerators total. |

> **Design note — why `sku.capacity`, not `properties.modelInstances`:**
>
> We considered a custom `properties.modelInstances` field, but `sku.capacity` is the correct choice:
>
> - **ARM standard contract.** The `Sku` type is a fixed ARM schema (`{ name, tier, size, family, capacity }`). Every Azure resource that scales uses `sku.capacity` — App Service (instances), Event Hubs (throughput units), Cosmos DB (RU/s), VMSS (VM count). Custom fields are not allowed in the `Sku` object.
> - **Autoscale integration.** ARM autoscale rules target `sku.capacity`. A custom property name would lose automatic autoscale support.
> - **Azure Policy.** Policy aliases for `sku.capacity` are registered automatically. A custom property would require manual alias registration.
> - **Tooling compatibility.** Bicep, Terraform, ARM templates, and the Azure Portal "Scale" blade all understand `sku.capacity` out of the box.
> - **Cost Management.** Azure Cost Management correlates `sku.capacity` with billing automatically.
> - **Redundancy risk.** ARM requires `sku.capacity` on SKU'd resources, so a `properties.modelInstances` field would create two fields meaning the same thing — or require a dummy `sku.capacity` value.
>
> The unit of `sku.capacity` is **model instances (replicas)**, not accelerators. Users think in terms of throughput scaling ("I need 3 copies of this model"), not GPU arithmetic. The accelerator math is system-internal: `totalAccelerators = capacity × acceleratorsPerInstance`. This prevents invalid configurations (e.g., requesting 5 GPUs for a model that requires 4-GPU multiples) and keeps the create/scale UX simple. Quota enforcement operates on total accelerators consumed, so the platform validates `capacity × acceleratorsPerInstance ≤ availableQuota` on each create or scale operation.

---

## Request Body Example

```json
{
  "properties": {
    "model": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
    "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
    "acceleratorType": "H100_80GB",
    "versionUpgradeOption": "OnceNewDefaultVersionAvailable"
  },
  "sku": {
    "name": "GlobalManagedCompute",
    "capacity": 1
  },
  "tags": {
    "environment": "production",
    "team": "nlp"
  }
}
```

---

## Responses

| Status Code | Type | Description |
|---|---|---|
| `200 OK` | [`AcceleratorDeployment`](#response-body) | Update operation completed synchronously (e.g., tag-only update). |
| `201 Created` | [`AcceleratorDeployment`](#response-body) | Create operation accepted. Deployment provisioning has started. Poll via `Azure-AsyncOperation` header or `GET` on resource URL. |
| Other | [`ErrorResponse`](#errorresponse) | Error response describing why the operation failed. |

### Response Headers (201 Created)

| Header | Type | Description |
|---|---|---|
| `Azure-AsyncOperation` | `string` (URL) | URL to poll for operation status. Returns `{ "status": "InProgress" | "Succeeded" | "Failed" }`. |
| `Retry-After` | `integer` | Suggested polling interval in seconds. Typically `30`. |

### Response Body

Full `AcceleratorDeployment` resource, including read-only properties populated by the service.

```json
{
  "id": "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.CognitiveServices/accounts/{account}/acceleratorDeployments/gpt-oss-120b-gpu",
  "name": "gpt-oss-120b-gpu",
  "type": "Microsoft.CognitiveServices/accounts/acceleratorDeployments",
  "etag": "\"0x8D..."  ,
  "properties": {
    "model": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
    "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
    "acceleratorType": "H100_80GB",
    "acceleratorsPerInstance": 4,
    "totalAccelerators": 4,
    "versionUpgradeOption": "OnceNewDefaultVersionAvailable",
    "provisioningState": "Accepted",
    "provisioningDetails": {
      "message": "Deployment queued. Provisioning GPU resources.",
      "lastOperationTimestamp": "2026-03-23T14:30:00Z"
    },
    "routes": {
      "chatCompletionsScoringPath": "/v1/chat/completions",
      "swagger": "/swagger.json",
      "messagesApiScoringPath": "/v1/messages"
    }
  },
  "sku": {
    "name": "GlobalManagedCompute",
    "capacity": 1
  },
  "tags": {
    "environment": "production",
    "team": "nlp"
  },
  "systemData": {
    "createdBy": "user@contoso.com",
    "createdByType": "User",
    "createdAt": "2026-03-23T14:30:00Z",
    "lastModifiedBy": "user@contoso.com",
    "lastModifiedByType": "User",
    "lastModifiedAt": "2026-03-23T14:30:00Z"
  }
}
```

### Read-Only Response Properties

These fields are returned in the response but **cannot** be set in the request body:

| Name | Type | Description |
|---|---|---|
| `id` | `string` | Fully qualified ARM resource ID. |
| `name` | `string` | Deployment name. |
| `type` | `string` | Resource type: `Microsoft.CognitiveServices/accounts/acceleratorDeployments`. |
| `etag` | `string` | Resource ETag for concurrency control. |
| `properties.acceleratorsPerInstance` | `integer` | Accelerators consumed per model instance, sourced from the deployment template's accelerator map. |
| `properties.totalAccelerators` | `integer` | Total accelerators allocated: `capacity × acceleratorsPerInstance`. |
| `properties.provisioningState` | [`ProvisioningState`](#provisioningstate) | Current provisioning state. |
| `properties.provisioningDetails` | `object` | Human-readable status message and timestamp. |
| `properties.routes` | [`InferenceRoutes`](#inferenceroutes) | Inference route paths (relative to `<account-endpoint>/managed-deployments/<deployment-name>`). Populated when `provisioningState` is `Succeeded`. Sourced from the deployment template. |
| `systemData` | [`SystemData`](#systemdata) | ARM system metadata (creation/modification tracking). |

---

## Updating an Existing Deployment

`PUT` is also used to update. Supported update operations:

| Update | Fields to set | Behavior |
|---|---|---|
| **Scale out/in** | `sku.capacity` | Changes instance count. Long-running — scale-out provisions new GPUs (10–15 min); scale-in is faster. |
| **Change accelerator type** | `properties.acceleratorType` | Requires full redeployment. Existing instances are drained and new ones provisioned on the target accelerator. |
| **Change deployment template** | `properties.deploymentTemplate` | Triggers rolling update of the serving container. May cause brief availability gaps during rollout. |
| **Update tags** | `tags` | Synchronous (returns 200). |

> Changing `model` is **not supported** — delete and recreate the deployment instead.

---

## SDK

```python
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.mgmt.cognitiveservices.models import (
    AcceleratorDeployment,
    AcceleratorDeploymentProperties,
    Sku,
)
from azure.identity import DefaultAzureCredential

cog = CognitiveServicesManagementClient(DefaultAzureCredential(), subscription_id)

deployment = AcceleratorDeployment(
    properties=AcceleratorDeploymentProperties(
        model="azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
        deployment_template="azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
        accelerator_type="H100_80GB",
        version_upgrade_option="OnceNewDefaultVersionAvailable",
    ),
    sku=Sku(name="GlobalManagedCompute", capacity=1),
    tags={"environment": "production"},
)

# begin_ prefix — this is a long-running operation
poller = cog.accelerator_deployments.begin_create_or_update(
    resource_group_name="my-rg",
    account_name="my-foundry-account",
    deployment_name="gpt-oss-120b-gpu",
    accelerator_deployment=deployment,
)

# Option 1: Block until done (10-15 min)
result = poller.result()
print(f"State: {result.properties.provisioning_state}")

# Option 2: Poll periodically
while not poller.done():
    print(f"Status: {poller.status()}...")
    time.sleep(30)
result = poller.result()
```

---

## CLI

```bash
# Create deployment (async — returns immediately, polls automatically)
az cognitiveservices account accelerator-deployment create \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --model "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4" \
  --deployment-template "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1" \
  --accelerator-type H100_80GB \
  --sku-name GlobalManagedCompute \
  --sku-capacity 1 \
  --tags environment=production team=nlp

# Create deployment (no-wait — returns immediately, don't poll)
az cognitiveservices account accelerator-deployment create \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --model "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4" \
  --deployment-template "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1" \
  --accelerator-type H100_80GB \
  --sku-name GlobalManagedCompute \
  --sku-capacity 1 \
  --no-wait

# Check provisioning state
az cognitiveservices account accelerator-deployment show \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --query "properties.provisioningState" -o tsv

# Scale to 2 instances
az cognitiveservices account accelerator-deployment create \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --sku-capacity 2
```

---

## Definitions

### `AcceleratorDeploymentModel` (Custom Models)

For custom (BYOW) models, the `model` property is an object instead of a string URI:

| Name | Type | Required | Description |
|---|---|---|---|
| `format` | `string` | Yes | `"Custom"` for BYOW models. For catalog models, this is the publisher name (e.g., `"OpenAI-OSS"`). |
| `name` | `string` | Yes | The custom model name as registered in the project data plane. |
| `version` | `string` | Yes | The custom model version. |
| `source` | `string` | Yes | Data-plane model reference: `"projects/{project}/models/{modelName}"`. |

**Example (custom model):**

```json
{
  "model": {
    "format": "Custom",
    "name": "my-deepseek-v3",
    "source": "projects/my-project/models/my-deepseek-v3",
    "version": "1"
  }
}
```

> For the full BYOW workflow (model registration → deployment template resolution → deployment), see [spec-custom-models.md](../spec-custom-models.md).

---

### `DeploymentVersionUpgradeOption`

| Value | Description |
|---|---|
| `OnceNewDefaultVersionAvailable` | Platform upgrades the deployment template when a new default version is published. **Only supported option initially.** |
| `OnceCurrentVersionExpired` | Platform upgrades only when the current template version reaches end-of-life. *(Future — requires BYOC support.)* |
| `NoAutoUpgrade` | No automatic upgrades. Customer owns container lifecycle. *(Future — requires BYOC support.)* |

### `ProvisioningState`

| Value | Description |
|---|---|
| `Accepted` | Request accepted, deployment queued. |
| `Creating` | GPU resources being provisioned and container starting. |
| `Updating` | Deployment being scaled or template being updated. |
| `Succeeded` | Deployment is healthy and serving traffic. |
| `Failed` | Deployment failed. Check `provisioningDetails.message` for error. |
| `Deleting` | Deployment is being torn down. |
| `Canceled` | Operation was canceled. |

### `InferenceRoutes`

A key-value map of inference route paths exposed by the deployment's serving container. The keys and values are sourced from the deployment template. To construct a full inference URL, combine: `<account-endpoint>/managed-deployments/<deployment-name><route-path>`.

> **Design note — why routes, not full URLs:**
> The inference endpoint is the Foundry project/account endpoint (e.g., `https://{account}.services.ai.azure.com`). It is not per-deployment — it's shared across all deployments and configured at the account level (auth, networking, private link). The deployment resource only needs to advertise the route suffixes the container runtime exposes. The user already knows the account endpoint from the account resource.

| Key (example) | Value (example) | Description |
|---|---|---|
| `chatCompletionsScoringPath` | `/v1/chat/completions` | Chat completions route. Present for OpenAI-compatible models. |
| `messagesApiScoringPath` | `/v1/messages` | Messages API route. Present for models supporting the messages API. |
| `swagger` | `/swagger.json` | OpenAPI spec for the model's inference API. |

The set of keys depends on the model's runtime. Examples:
- **OpenAI-compatible model** (gpt-oss-120b via vLLM): `{ "chatCompletionsScoringPath": "/v1/chat/completions", "swagger": "/swagger.json" }`
- **Reranker model** (Cohere Rerank): `{ "rerankScoringPath": "/v1/rerank", "swagger": "/swagger.json" }`
- **Embedding model**: `{ "embeddingsScoringPath": "/v1/embeddings", "swagger": "/swagger.json" }`

> **OpenAI-compatible route:** Models with OpenAI-compatible runtimes are also accessible via the standard OpenAI SDK route: `<account-endpoint>/openai/deployments/<deployment-name>/chat/completions`. This route is handled by APIM and does not appear in `routes` — it's an implicit capability of the platform for any model that declares OpenAI compatibility in its deployment template.

### `SystemData`

| Name | Type | Description |
|---|---|---|
| `createdAt` | `string` (date-time) | Timestamp of resource creation (UTC). |
| `createdBy` | `string` | Identity that created the resource. |
| `createdByType` | `string` | Type of identity: `User`, `Application`, `ManagedIdentity`, `Key`. |
| `lastModifiedAt` | `string` (date-time) | Timestamp of last modification (UTC). |
| `lastModifiedBy` | `string` | Identity that last modified the resource. |
| `lastModifiedByType` | `string` | Type of identity. |

### `ErrorResponse`

| Name | Type | Description |
|---|---|---|
| `error.code` | `string` | Error code (e.g., `QuotaExceeded`, `InvalidModel`, `CapacityUnavailable`). |
| `error.message` | `string` | Human-readable error description. |
| `error.target` | `string` | The property that caused the error. |
| `error.details` | `ErrorDetail[]` | Additional error details. |

### Common Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `QuotaExceeded` | `409 Conflict` | Subscription does not have enough accelerator quota for the requested capacity. |
| `CapacityUnavailable` | `409 Conflict` | No physical capacity available for the requested accelerator type and instance count. |
| `InvalidModel` | `400 Bad Request` | The `model` does not resolve to a valid model in the ML Registry. |
| `InvalidDeploymentTemplate` | `400 Bad Request` | The `deploymentTemplate` is not in the model's `allowed_deployment_templates`. |
| `AcceleratorTypeMismatch` | `400 Bad Request` | The `acceleratorType` is not listed in the deployment template's `accelerator_maps`. |
| `DeploymentNameConflict` | `409 Conflict` | A serverless deployment with the same name already exists on this account. Deployment names must be unique across both `deployments` and `acceleratorDeployments`. |
| `ModelImmutable` | `400 Bad Request` | Cannot change `model` on an existing deployment. Delete and recreate instead. |
