# Managed Compute Deployments — Update (PATCH)

Updates the SKU of a managed compute (accelerator) deployment. This is the preferred method for **scaling** (changing instance count).

This is a **long-running operation (LRO)**. Scale-out provisions new GPU resources (10–15 minutes); scale-in is typically faster.

> **Why PATCH instead of PUT for scaling?** ARM best practices recommend PATCH for partial updates. The PATCH operation only accepts the `sku` field (via `PatchResourceSku`), making it more efficient and less error-prone than resending the full resource via PUT. PUT is still supported for updates that require the full resource body (e.g., changing `deploymentTemplate`).

---

## HTTP Request

```http
PATCH https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}
    /providers/Microsoft.CognitiveServices/accounts/{accountName}
    /managedComputeDeployments/{deploymentName}?api-version=2026-03-15-preview
Authorization: Bearer {token}
Content-Type: application/json
```

## URI Parameters

| Name | In | Required | Type | Description |
|---|---|---|---|---|
| `subscriptionId` | path | Yes | `string` (UUID) | The ID of the target subscription. |
| `resourceGroupName` | path | Yes | `string` (1–90 chars) | The name of the resource group. Case-insensitive. |
| `accountName` | path | Yes | `string` (2–64 chars) | The name of the Cognitive Services / Foundry account. |
| `deploymentName` | path | Yes | `string` | The name of the managed compute deployment to update. |
| `api-version` | query | Yes | `string` | API version. Minimum: `2026-03-15-preview`. |

---

## Request Body

### `PatchResourceSku`

Only the `sku` field is accepted. All other fields are ignored.

| Name | Type | Required | Description |
|---|---|---|---|
| `sku` | [`Sku`](#sku) | Yes | The updated SKU definition. |

### `Sku`

| Name | Type | Required | Description |
|---|---|---|---|
| `name` | `string` | Yes | The SKU name. Must be `GlobalManagedCompute` or `DataZoneManagedCompute`. Must match the existing deployment's SKU name. |
| `capacity` | `integer` (int32) | Yes | The new number of model instances (replicas). Minimum: `1`. Total accelerators consumed = `capacity × acceleratorsPerInstance`. |

---

## Request Body Example

```json
{
  "sku": {
    "name": "GlobalManagedCompute",
    "capacity": 2
  }
}
```

---

## Responses

| Status Code | Type | Description |
|---|---|---|
| `200 OK` | [`ManagedComputeDeployment`](#response-body) | Update completed synchronously (rare — typically only if capacity is unchanged). |
| `202 Accepted` | *(empty body)* | Scale operation accepted. GPU resources are being provisioned or released. Poll via `Azure-AsyncOperation` header or `GET` on resource URL. |
| Other | [`ErrorResponse`](#errorresponse) | Error response describing why the operation failed. |

### Response Headers (202 Accepted)

| Header | Type | Description |
|---|---|---|
| `Azure-AsyncOperation` | `string` (URL) | URL to poll for operation status. Returns `{ "status": "InProgress" | "Succeeded" | "Failed" }`. |
| `Retry-After` | `integer` | Suggested polling interval in seconds. Typically `30`. |
| `Location` | `string` (URL) | URL that returns `200` when the update is complete, `202` while in progress. |

### Response Body

For `200 OK` — full `ManagedComputeDeployment` resource with updated `sku.capacity` and `provisioningState`.

For `202 Accepted` — no response body. Use the `Azure-AsyncOperation` or `Location` header to poll for completion, then `GET` the resource for the updated state.

#### 200 OK Example

```json
{
  "id": "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.CognitiveServices/accounts/{account}/managedComputeDeployments/gpt-oss-120b-gpu",
  "name": "gpt-oss-120b-gpu",
  "type": "Microsoft.CognitiveServices/accounts/managedComputeDeployments",
  "etag": "\"0x8D...\"",
  "properties": {
    "model": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
    "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
    "acceleratorType": "H100_80GB",
    "acceleratorsPerInstance": 4,
    "totalAccelerators": 8,
    "versionUpgradeOption": "OnceNewDefaultVersionAvailable",
    "provisioningState": "Succeeded",
    "provisioningDetails": {
      "message": "Scale operation completed successfully.",
      "lastOperationTimestamp": "2026-03-23T15:15:00Z"
    },
    "routes": {
      "chatCompletionsScoringPath": "/managedComputeDeployments/gpt-oss-120b-gpu/chat/completions",
      "swagger": "/managedComputeDeployments/gpt-oss-120b-gpu/swagger.json",
      "messagesApiScoringPath": "/managedComputeDeployments/gpt-oss-120b-gpu/messages"
    }
  },
  "sku": {
    "name": "GlobalManagedCompute",
    "capacity": 2
  }
}
```

---

## Behavior

- **SKU-only update**: PATCH only accepts the `sku` field. To update other properties (e.g., `deploymentTemplate`, `tags`), use [PUT (Create or Update)](create-or-update.md).
- **SKU name must match**: The `sku.name` in the PATCH body must match the existing deployment's SKU name. Changing SKU name is not supported via PATCH.
- **Existing traffic preserved**: During scale-out, existing instances continue serving traffic. New instances are added to the traffic pool once healthy. During scale-in, instances are drained before removal.
- **Quota validation**: The platform validates `newCapacity × acceleratorsPerInstance ≤ availableQuota` before accepting the scale operation.

---

## SDK

```python
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.mgmt.cognitiveservices.models import Sku
from azure.identity import DefaultAzureCredential

cog = CognitiveServicesManagementClient(DefaultAzureCredential(), subscription_id)

# Scale from 1 to 2 instances
poller = cog.managed_compute_deployments.begin_update(
    resource_group_name="my-rg",
    account_name="my-foundry-account",
    deployment_name="gpt-oss-120b-gpu",
    managed_compute_deployment={"sku": {"name": "GlobalManagedCompute", "capacity": 2}},
)

result = poller.result()
print(f"Scaled to {result.sku.capacity} instances ({result.properties.total_accelerators} GPUs)")
```

---

## CLI

```bash
# Scale to 2 instances (PATCH)
az cognitiveservices account managed-compute-deployment update \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --sku-name GlobalManagedCompute \
  --sku-capacity 2

# Scale without waiting
az cognitiveservices account managed-compute-deployment update \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --sku-capacity 2 \
  --no-wait
```

---

## Common Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `QuotaExceeded` | `409 Conflict` | Subscription does not have enough accelerator quota for the requested capacity. |
| `CapacityUnavailable` | `409 Conflict` | No physical capacity available for the additional instances. |
| `InvalidSkuUpdate` | `400 Bad Request` | SKU name in PATCH body does not match the existing deployment's SKU name. |
| `DeploymentNotFound` | `404 Not Found` | The specified deployment does not exist. |
