# Managed Compute Deployments — Delete

Deletes a managed compute (accelerator) deployment and releases all associated GPU resources.

This is a **long-running operation (LRO)**. Resources (GPU allocations, containers, endpoints) are released asynchronously. The `provisioningState` transitions to `Deleting` immediately; the resource is removed when cleanup completes.

---

## HTTP Request

```http
DELETE https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}
    /providers/Microsoft.CognitiveServices/accounts/{accountName}
    /managedComputeDeployments/{deploymentName}?api-version=2026-03-15-preview
Authorization: Bearer {token}
```

## URI Parameters

| Name | In | Required | Type | Description |
|---|---|---|---|---|
| `subscriptionId` | path | Yes | `string` (UUID) | The ID of the target subscription. |
| `resourceGroupName` | path | Yes | `string` (1–90 chars) | The name of the resource group. Case-insensitive. |
| `accountName` | path | Yes | `string` (2–64 chars) | The name of the Cognitive Services / Foundry account. |
| `deploymentName` | path | Yes | `string` | The name of the managed compute deployment to delete. |
| `api-version` | query | Yes | `string` | API version. Minimum: `2026-03-15-preview`. |

---

## Responses

| Status Code | Type | Description |
|---|---|---|
| `200 OK` | *(empty body)* | Delete completed synchronously. |
| `202 Accepted` | *(empty body)* | Delete operation accepted. GPU resources are being released. |
| `204 No Content` | *(empty body)* | Deployment does not exist (already deleted or never existed). Idempotent. |
| Other | `ErrorResponse` | Error response. |

### Response Headers (202 Accepted)

| Header | Type | Description |
|---|---|---|
| `Azure-AsyncOperation` | `string` (URL) | URL to poll for delete completion. |
| `Retry-After` | `integer` | Suggested polling interval in seconds. |
| `Location` | `string` (URL) | URL that returns `200` when the delete is complete, `202` while in progress. |

---

## Behavior

- **Idempotent**: Deleting a deployment that does not exist returns `204 No Content`.
- **Immediate effect**: Inference requests to the deployment's endpoint will begin failing as soon as the delete is accepted. The endpoint is deregistered from APIM routing immediately.
- **Quota release**: Accelerator quota is released when the underlying GPU resources are freed (not immediately on `202`). Use `acceleratorUsages` API to confirm quota release.
- **No cascading deletes**: Deleting a deployment does not affect the model, deployment template, or any other deployments.

---

## SDK

```python
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.identity import DefaultAzureCredential

cog = CognitiveServicesManagementClient(DefaultAzureCredential(), subscription_id)

# begin_ prefix — long-running operation
poller = cog.managed_compute_deployments.begin_delete(
    resource_group_name="my-rg",
    account_name="my-foundry-account",
    deployment_name="gpt-oss-120b-gpu",
)

# Block until delete completes
poller.result()
print("Deployment deleted and GPUs released.")
```

---

## CLI

```bash
# Delete (waits for completion by default)
az cognitiveservices account managed-compute-deployment delete \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu

# Delete without waiting
az cognitiveservices account managed-compute-deployment delete \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --no-wait

# Confirm deletion
az cognitiveservices account managed-compute-deployment show \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu 2>/dev/null \
  && echo "Still exists" || echo "Deleted"
```
