# Managed Compute Deployments — Get

Retrieves the properties of a managed compute (accelerator) deployment.

Use this operation to check provisioning status, get endpoint URLs, and read deployment configuration.

---

## HTTP Request

```http
GET https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}
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
| `deploymentName` | path | Yes | `string` | The name of the managed compute deployment. |
| `api-version` | query | Yes | `string` | API version. Minimum: `2026-03-15-preview`. |

---

## Responses

| Status Code | Type | Description |
|---|---|---|
| `200 OK` | `ManagedComputeDeployment` | The deployment resource. |
| `404 Not Found` | `ErrorResponse` | Deployment does not exist. |
| Other | `ErrorResponse` | Error response. |

### Response Body

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
    "totalAccelerators": 4,
    "versionUpgradeOption": "OnceNewDefaultVersionAvailable",
    "provisioningState": "Succeeded",
    "provisioningDetails": {
      "message": "Deployment is healthy and serving traffic.",
      "lastOperationTimestamp": "2026-03-23T14:45:00Z"
    },
    "routes": {
      "chatCompletionsScoringPath": "/managedComputeDeployments/gpt-oss-120b-gpu/chat/completions",
      "swagger": "/managedComputeDeployments/gpt-oss-120b-gpu/swagger.json",
      "messagesApiScoringPath": "/managedComputeDeployments/gpt-oss-120b-gpu/messages"
    }
  },
  "sku": {
    "name": "GlobalManagedCompute",
    "capacity": 1
  }
}
```

### Polling for Provisioning Completion

Deployments take 10–15 minutes. Use `GET` to poll `provisioningState`:

```
Accepted → Creating → Succeeded
                   └→ Failed
```

Poll every 30 seconds until `provisioningState` is `Succeeded` or `Failed`.

---

## SDK

```python
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.identity import DefaultAzureCredential

cog = CognitiveServicesManagementClient(DefaultAzureCredential(), subscription_id)

deployment = cog.managed_compute_deployments.get(
    resource_group_name="my-rg",
    account_name="my-foundry-account",
    deployment_name="gpt-oss-120b-gpu",
)

print(f"State:    {deployment.properties.provisioning_state}")
print(f"SKU:      {deployment.sku.name} × {deployment.sku.capacity} instances")
print(f"Accel:    {deployment.properties.accelerator_type} "
      f"× {deployment.properties.accelerators_per_instance}/instance")
print(f"Total:    {deployment.properties.total_accelerators} accelerators")

if deployment.properties.provisioning_state == "Succeeded":
    print(f"Routes: {deployment.properties.routes}")
```

---

## CLI

```bash
# Get full deployment details
az cognitiveservices account managed-compute-deployment show \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu

# Get just provisioning state
az cognitiveservices account managed-compute-deployment show \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --query "properties.provisioningState" -o tsv

# Get inference routes (only available when Succeeded)
az cognitiveservices account managed-compute-deployment show \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --query "properties.routes" -o json

# Wait for deployment to complete (poll until Succeeded or Failed)
az cognitiveservices account managed-compute-deployment wait \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --created
```
