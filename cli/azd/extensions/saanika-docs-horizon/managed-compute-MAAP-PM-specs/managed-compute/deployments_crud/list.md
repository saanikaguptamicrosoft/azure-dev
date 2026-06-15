# Accelerator Deployments — List

Lists all managed compute (accelerator) deployments under a Foundry resource (Cognitive Services account).

This returns **only** accelerator deployments. Serverless deployments are listed via the separate `deployments` API.

---

## HTTP Request

```http
GET https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}
    /providers/Microsoft.CognitiveServices/accounts/{accountName}
    /acceleratorDeployments?api-version=2026-04-01-preview
Authorization: Bearer {token}
```

## URI Parameters

| Name | In | Required | Type | Description |
|---|---|---|---|---|
| `subscriptionId` | path | Yes | `string` (UUID) | The ID of the target subscription. |
| `resourceGroupName` | path | Yes | `string` (1–90 chars) | The name of the resource group. Case-insensitive. |
| `accountName` | path | Yes | `string` (2–64 chars) | The name of the Cognitive Services / Foundry account. |
| `api-version` | query | Yes | `string` | API version. Minimum: `2026-04-01-preview`. |

---

## Responses

| Status Code | Type | Description |
|---|---|---|
| `200 OK` | `AcceleratorDeploymentListResult` | Paginated list of accelerator deployments. |
| Other | `ErrorResponse` | Error response. |

### Response Body

```json
{
  "value": [
    {
      "id": "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.CognitiveServices/accounts/{account}/acceleratorDeployments/gpt-oss-120b-gpu",
      "name": "gpt-oss-120b-gpu",
      "type": "Microsoft.CognitiveServices/accounts/acceleratorDeployments",
      "properties": {
        "model": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
        "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
        "acceleratorType": "H100_80GB",
        "acceleratorsPerInstance": 4,
        "totalAccelerators": 4,
        "versionUpgradeOption": "OnceNewDefaultVersionAvailable",
        "provisioningState": "Succeeded",
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
      "tags": { "environment": "production" },
      "systemData": { "createdAt": "2026-03-23T14:30:00Z", "createdBy": "user@contoso.com", "createdByType": "User" }
    },
    {
      "id": "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.CognitiveServices/accounts/{account}/acceleratorDeployments/llama-3-gpu",
      "name": "llama-3-gpu",
      "type": "Microsoft.CognitiveServices/accounts/acceleratorDeployments",
      "properties": {
        "model": "azureml://registries/azureml-meta/models/Meta-Llama-3.1-405B-Instruct/versions/2",
        "deploymentTemplate": "azureml://registries/azureml-meta/deploymenttemplates/llama-3-405b-fp8/versions/1",
        "acceleratorType": "H100_80GB",
        "acceleratorsPerInstance": 8,
        "totalAccelerators": 16,
        "versionUpgradeOption": "OnceNewDefaultVersionAvailable",
        "provisioningState": "Succeeded",
        "routes": {
          "chatCompletionsScoringPath": "/v1/chat/completions",
          "swagger": "/swagger.json"
        }
      },
      "sku": {
        "name": "GlobalManagedCompute",
        "capacity": 2
      },
      "tags": { "environment": "staging" },
      "systemData": { "createdAt": "2026-03-20T09:15:00Z", "createdBy": "admin@contoso.com", "createdByType": "User" }
    }
  ],
  "nextLink": null
}
```

### `AcceleratorDeploymentListResult`

| Name | Type | Description |
|---|---|---|
| `value` | `AcceleratorDeployment[]` | Array of accelerator deployments. |
| `nextLink` | `string` or `null` | URL for the next page of results. `null` if no more pages. |

---

## SDK

```python
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.identity import DefaultAzureCredential

cog = CognitiveServicesManagementClient(DefaultAzureCredential(), subscription_id)

for dep in cog.accelerator_deployments.list(
    resource_group_name="my-rg",
    account_name="my-foundry-account",
):
    print(f"{dep.name:25s}  {dep.properties.accelerator_type:12s}  "
          f"{dep.sku.capacity} inst  "
          f"{dep.properties.total_accelerators} GPUs  "
          f"{dep.properties.provisioning_state}")
```

Output:
```
gpt-oss-120b-gpu           H100_80GB     1 inst  4 GPUs   Succeeded
llama-3-gpu                H100_80GB     2 inst  16 GPUs  Succeeded
```

---

## CLI

```bash
# List all accelerator deployments
az cognitiveservices account accelerator-deployment list \
  --name $ACCOUNT -g $RG -o table

# List with specific fields
az cognitiveservices account accelerator-deployment list \
  --name $ACCOUNT -g $RG \
  --query "[].{Name:name, Accelerator:properties.acceleratorType, Instances:sku.capacity, State:properties.provisioningState}" \
  -o table
```

Output:
```
Name              Accelerator  Instances  State
────────────────  ───────────  ─────────  ─────────
gpt-oss-120b-gpu  H100_80GB    1          Succeeded
llama-3-gpu       H100_80GB    2          Succeeded
```
