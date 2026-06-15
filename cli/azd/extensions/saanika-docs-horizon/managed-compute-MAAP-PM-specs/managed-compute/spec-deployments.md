# Accelerator Deployments — Spec

## Overview

This spec defines the `acceleratorDeployments` resource type — a **new ARM resource** under `Microsoft.CognitiveServices/accounts` for managed compute (GPU-based) model deployments in Azure AI Foundry.

This is a **separate API from the existing `deployments` resource** (used for serverless models). The decision to create a separate resource type was made based on the following rationale:

- Managed compute deployments have fundamentally different properties (model asset ID, deployment template, accelerator type, instance count) that do not apply to serverless deployments.
- A shared `deployments` API would become a union type with 50%+ of fields irrelevant for any given deployment type.
- Future managed-compute-only features (LoRA adapters, speculative decoding, BYOC) would further bloat a shared type.
- Separate resource types enable separate ARM RBAC permissions (e.g., "can create serverless but not managed compute").
- Type-safe list operations: `acceleratorDeployments.list()` returns only managed compute deployments.

### Scope

This spec covers **catalog model deployments only** — models discovered through the Azure AI Model Catalog and deployed using publisher-provided deployment templates.

**Out of scope** (covered in separate specs):
- **Bring-your-own-weights (BYOW) deployments** — see [spec-custom-models.md](spec-custom-models.md) for custom model registration and deployment
- Draft model deployments
- LoRA adapter management
- Bring-your-own-container (BYOC) deployments

### Prerequisites — Inputs from Prior Steps

This spec assumes the user has completed the prior steps in the model lifecycle:

1. **Model discovery** ([e2e-1-discover.md](e2e-1-discover.md)) — The user has found the model in the catalog and has the `model`:
   ```
   azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4
   ```

2. **Model details** ([e2e-2-get-model.md](e2e-2-get-model.md)) — The user has retrieved the deployment template from the ML Registry and selected a `deploymentTemplate` and `acceleratorType`:
   ```
   azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1
   acceleratorType: H100_80GB (4 accelerators per instance)
   ```

3. **Quota and capacity** ([e2e-3-quota-capacity.md](e2e-3-quota-capacity.md)) — The user has verified:
   - Sufficient accelerator quota via `acceleratorUsages` API
   - Physical capacity via `acceleratorCapacities` API

All three inputs (`model`, `deploymentTemplate`, `acceleratorType`) are **required** for creating an accelerator deployment.

---

## Resource Model

### ARM Resource Type

```
Microsoft.CognitiveServices/accounts/acceleratorDeployments
```

### Base URL

```
https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}
    /providers/Microsoft.CognitiveServices/accounts/{accountName}
    /acceleratorDeployments/{deploymentName}?api-version=2026-04-01-preview
```

### API Version

```
2026-04-01-preview
```

---

## Operations Summary

| Operation | HTTP Method | URL Suffix | LRO | Description |
|---|---|---|---|---|
| [**Create or Update**](deployments_crud/create-or-update.md) | `PUT` | `/{deploymentName}` | Yes (10–15 min) | Create a new deployment or update an existing one (scale, change template). |
| [**Get**](deployments_crud/get.md) | `GET` | `/{deploymentName}` | No | Retrieve deployment properties, provisioning state, and inference routes. |
| [**List**](deployments_crud/list.md) | `GET` | `/` | No | List all accelerator deployments under the account. |
| [**Delete**](deployments_crud/delete.md) | `DELETE` | `/{deploymentName}` | Yes | Delete a deployment and release GPU resources. |
| [**Get Logs**](deployments_crud/get-logs.md) | `POST` | `/{deploymentName}/getLogs` | No | Retrieve per-instance container logs or infrastructure logs (provisioning, scaling, routing). |

See the dedicated files in the [`deployments_crud/`](deployments_crud/) folder for full request/response schemas, SDK samples, and CLI commands.

---

## Resource Schema

### `AcceleratorDeployment`

```json
{
  "id": "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.CognitiveServices/accounts/{account}/acceleratorDeployments/{name}",
  "name": "{deploymentName}",
  "type": "Microsoft.CognitiveServices/accounts/acceleratorDeployments",
  "etag": "\"0x8D...\"",
  "properties": {
    "model": "azureml://registries/{registry}/models/{model}/versions/{version}",
    "deploymentTemplate": "azureml://registries/{registry}/deploymenttemplates/{template}/versions/{version}",
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
      "chatCompletionsScoringPath": "/v1/chat/completions",
      "swagger": "/swagger.json",
      "messagesApiScoringPath": "/v1/messages"
    }
  },
  "sku": {
    "name": "GlobalManagedCompute",
    "capacity": 1
  },
  "tags": {},
  "systemData": {}
}
```

### Request vs Response Properties

| Property | Set on Request | Returned in Response | Description |
|---|---|---|---|
| `properties.model` | **Required** | Yes | ML Registry model asset URI (catalog models) or `AcceleratorDeploymentModel` object (custom models). Immutable after creation. See [spec-custom-models.md](spec-custom-models.md) for custom model format. |
| `properties.deploymentTemplate` | **Required** | Yes | ML Registry deployment template URI. Can be updated (triggers rolling update). |
| `properties.acceleratorType` | **Required** | Yes | Accelerator type (e.g., `H100_80GB`). Can be changed (triggers redeployment). |
| `properties.versionUpgradeOption` | Optional | Yes | Template auto-upgrade policy. Default: `OnceNewDefaultVersionAvailable`. |
| `sku.name` | **Required** | Yes | Must be `GlobalManagedCompute` or `DataZoneManagedCompute`. |
| `sku.capacity` | **Required** | Yes | Number of model instances (replicas) — **not** accelerator count. Total accelerators consumed = `capacity × acceleratorsPerInstance`. Can be updated (scale in/out). |
| `tags` | Optional | Yes | Resource tags. |
| `properties.acceleratorsPerInstance` | — | **Read-only** | Number of accelerators (GPUs) consumed by each model instance, sourced from the deployment template. |
| `properties.totalAccelerators` | — | **Read-only** | Total accelerators allocated: `sku.capacity` (instances) × `acceleratorsPerInstance`. |
| `properties.provisioningState` | — | **Read-only** | Current state: `Accepted`, `Creating`, `Updating`, `Succeeded`, `Failed`, `Deleting`, `Canceled`. |
| `properties.provisioningDetails` | — | **Read-only** | Status message and timestamp. |
| `properties.routes` | — | **Read-only** | Inference route paths (relative to `<account-endpoint>/managed-deployments/<deployment-name>`). Sourced from deployment template. Populated when `Succeeded`. |
| `id`, `name`, `type`, `etag` | — | **Read-only** | ARM resource metadata. |
| `systemData` | — | **Read-only** | Creation/modification tracking. |

---

## Long-Running Operations

GPU-based deployments take **10–15 minutes** to provision. All mutating operations (Create, Update, Delete) are long-running.

### Async Pattern

1. Client sends `PUT` or `DELETE`.
2. Service returns `201 Created` (or `202 Accepted` for delete) with:
   - `Azure-AsyncOperation` header — poll URL for operation status.
   - `Retry-After` header — suggested interval (typically 30 seconds).
3. Client polls `Azure-AsyncOperation` URL until `status` is `Succeeded` or `Failed`.
4. Client does a final `GET` on the resource URL to fetch the completed state.

### SDK Handling

The Azure SDK's `begin_` prefix methods return an `LROPoller`:

```python
# Non-blocking — returns immediately
poller = cog.accelerator_deployments.begin_create_or_update(rg, account, name, deployment)

# Option A: Block until done
result = poller.result()  # waits 10-15 min

# Option B: Poll manually
while not poller.done():
    print(f"Status: {poller.status()}")
    time.sleep(30)
result = poller.result()
```

### CLI Handling

```bash
# Default: waits for completion (shows progress dots)
az cognitiveservices account accelerator-deployment create ...

# --no-wait: returns immediately
az cognitiveservices account accelerator-deployment create ... --no-wait

# Check state later
az cognitiveservices account accelerator-deployment show ... --query "properties.provisioningState"

# Block until a specific state
az cognitiveservices account accelerator-deployment wait ... --created
```

---

## Authentication and Authorization

### Authentication

Accelerator deployment management uses **ARM authentication** — Microsoft Entra ID (Azure AD) tokens scoped to `https://management.azure.com/.default`.

| Method | Details |
|---|---|
| **Microsoft Entra (AAD)** | Obtain a token via `DefaultAzureCredential`, service principal, or managed identity. Scope: `https://management.azure.com/.default`. This is the recommended approach. |
| **Key-based** | Not supported for ARM management operations. API keys are used for **inference** only (via the account endpoint), not for deployment CRUD. |

#### Entra Token Acquisition

```python
from azure.identity import DefaultAzureCredential

# Works with user login, service principal, managed identity
credential = DefaultAzureCredential()
cog = CognitiveServicesManagementClient(credential, subscription_id)
```

```bash
# CLI uses the logged-in identity automatically
az login
az cognitiveservices account accelerator-deployment create ...

# Service principal
az login --service-principal -u $CLIENT_ID -p $CLIENT_SECRET --tenant $TENANT_ID
```

### Authorization (RBAC)

Accelerator deployments are a separate ARM resource type, enabling fine-grained RBAC:

| Action | Permission | Description |
|---|---|---|
| Create / Update | `Microsoft.CognitiveServices/accounts/acceleratorDeployments/write` | Create or update a managed compute deployment. |
| Get | `Microsoft.CognitiveServices/accounts/acceleratorDeployments/read` | Read deployment details. |
| List | `Microsoft.CognitiveServices/accounts/acceleratorDeployments/read` | List all managed compute deployments. |
| Delete | `Microsoft.CognitiveServices/accounts/acceleratorDeployments/delete` | Delete a managed compute deployment. |

#### Built-in Roles

| Role | Deployments/write | Deployments/read | Deployments/delete |
|---|---|---|---|
| **Owner** | ✅ | ✅ | ✅ |
| **Contributor** | ✅ | ✅ | ✅ |
| **Cognitive Services Contributor** | ✅ | ✅ | ✅ |
| **Reader** | ❌ | ✅ | ❌ |
| **Cognitive Services Accelerator Deployment Operator** *(proposed)* | ✅ | ✅ | ✅ |

The proposed **Cognitive Services Accelerator Deployment Operator** role grants full CRUD access to accelerator deployments only. This enables separation of duties — teams managing GPU infrastructure can operate independently.

#### Custom Role Example

```json
{
  "Name": "Accelerator Deployment Operator",
  "Description": "Can manage managed compute deployments but not serverless deployments",
  "Actions": [
    "Microsoft.CognitiveServices/accounts/acceleratorDeployments/read",
    "Microsoft.CognitiveServices/accounts/acceleratorDeployments/write",
    "Microsoft.CognitiveServices/accounts/acceleratorDeployments/delete"
  ],
  "NotActions": [],
  "AssignableScopes": [
    "/subscriptions/{subscriptionId}"
  ]
}
```

---

## Azure Policy Integration

Azure Policy can be used to enforce governance rules on accelerator deployments. Policies evaluate the `PUT` request body and can `deny`, `audit`, or `modify` deployments.

### Policy Alias Paths

| Alias | Type | Description |
|---|---|---|
| `Microsoft.CognitiveServices/accounts/acceleratorDeployments/properties.model` | `string` | The model being deployed. Use to allowlist/denylist specific models. |
| `Microsoft.CognitiveServices/accounts/acceleratorDeployments/properties.acceleratorType` | `string` | The accelerator type. Use to restrict which GPU types are allowed. |
| `Microsoft.CognitiveServices/accounts/acceleratorDeployments/sku.name` | `string` | The SKU name (`GlobalManagedCompute`, `DataZoneManagedCompute`). Use to restrict deployment to data-zone-scoped SKUs only. |
| `Microsoft.CognitiveServices/accounts/acceleratorDeployments/sku.capacity` | `integer` | Number of model instances (not accelerator count). Use to set max instance limits. To cap total GPU consumption, factor in the model's `acceleratorsPerInstance`. |
| `Microsoft.CognitiveServices/accounts/acceleratorDeployments/properties.deploymentTemplate` | `string` | The deployment template. Use to enforce approved templates. |

### Example Policies

#### 1. Only allow approved models

Restrict which models can be deployed on managed compute.

```json
{
  "if": {
    "allOf": [
      {
        "field": "type",
        "equals": "Microsoft.CognitiveServices/accounts/acceleratorDeployments"
      },
      {
        "field": "Microsoft.CognitiveServices/accounts/acceleratorDeployments/properties.model",
        "notIn": [
          "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
          "azureml://registries/azureml-meta/models/Meta-Llama-3.1-405B-Instruct/versions/2"
        ]
      }
    ]
  },
  "then": {
    "effect": "deny"
  }
}
```

#### 2. Restrict accelerator types

Only allow H100 GPUs — block older generation accelerators.

```json
{
  "if": {
    "allOf": [
      {
        "field": "type",
        "equals": "Microsoft.CognitiveServices/accounts/acceleratorDeployments"
      },
      {
        "field": "Microsoft.CognitiveServices/accounts/acceleratorDeployments/properties.acceleratorType",
        "notIn": ["H100_80GB", "H200_141GB"]
      }
    ]
  },
  "then": {
    "effect": "deny"
  }
}
```

#### 3. Enforce maximum instance count

Limit the number of model instances per deployment. Note: `sku.capacity` is the instance count, not accelerator count — total GPU consumption depends on the model's `acceleratorsPerInstance`.

```json
{
  "if": {
    "allOf": [
      {
        "field": "type",
        "equals": "Microsoft.CognitiveServices/accounts/acceleratorDeployments"
      },
      {
        "field": "Microsoft.CognitiveServices/accounts/acceleratorDeployments/sku.capacity",
        "greater": 4
      }
    ]
  },
  "then": {
    "effect": "deny"
  }
}
```

#### 4. Enforce data-zone-only deployments

For compliance — ensure all managed compute stays within a data zone.

```json
{
  "if": {
    "allOf": [
      {
        "field": "type",
        "equals": "Microsoft.CognitiveServices/accounts/acceleratorDeployments"
      },
      {
        "field": "Microsoft.CognitiveServices/accounts/acceleratorDeployments/sku.name",
        "notEquals": "DataZoneManagedCompute"
      }
    ]
  },
  "then": {
    "effect": "deny"
  }
}
```

#### 5. Audit all managed compute deployments

Log all deployments without blocking them.

```json
{
  "if": {
    "field": "type",
    "equals": "Microsoft.CognitiveServices/accounts/acceleratorDeployments"
  },
  "then": {
    "effect": "audit"
  }
}
```

---

## Activity Log (Azure Monitor)

Customers can monitor deployment lifecycle events via the standard ARM activity log. All deployment operations emit entries under the `Microsoft.CognitiveServices` provider.

### Activity Log Events

| Event | Operation Name | Status | When |
|---|---|---|---|
| Create initiated | `Microsoft.CognitiveServices/accounts/acceleratorDeployments/write` | `Started` | `PUT` request accepted. |
| Create succeeded | `Microsoft.CognitiveServices/accounts/acceleratorDeployments/write` | `Succeeded` | Deployment healthy and serving. |
| Create failed | `Microsoft.CognitiveServices/accounts/acceleratorDeployments/write` | `Failed` | Provisioning failed. |
| Scale initiated | `Microsoft.CognitiveServices/accounts/acceleratorDeployments/write` | `Started` | `PUT` with new `sku.capacity`. |
| Scale succeeded | `Microsoft.CognitiveServices/accounts/acceleratorDeployments/write` | `Succeeded` | Scale-out/in complete. |
| Delete initiated | `Microsoft.CognitiveServices/accounts/acceleratorDeployments/delete` | `Started` | `DELETE` request accepted. |
| Delete completed | `Microsoft.CognitiveServices/accounts/acceleratorDeployments/delete` | `Succeeded` | Resources released. |

### Diagnostic Settings

Customers can route activity logs to Log Analytics, Event Hubs, or Storage via [Azure Monitor Diagnostic Settings](https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/diagnostic-settings):

```bash
# Route accelerator deployment logs to Log Analytics
az monitor diagnostic-settings create \
  --resource "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.CognitiveServices/accounts/{account}" \
  --name "mc-deployment-logs" \
  --workspace "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.OperationalInsights/workspaces/{workspace}" \
  --logs '[{"category": "Audit", "enabled": true}]'
```

### Customer Kusto Queries

```kusto
// All accelerator deployment operations in the last 7 days
AzureActivity
| where ResourceProvider == "MICROSOFT.COGNITIVESERVICES"
| where OperationNameValue has "acceleratorDeployments"
| where TimeGenerated > ago(7d)
| project TimeGenerated, OperationNameValue, ActivityStatusValue, Caller,
          Properties = parse_json(Properties)
| order by TimeGenerated desc

// Average provisioning time by accelerator type
AzureActivity
| where ResourceProvider == "MICROSOFT.COGNITIVESERVICES"
| where OperationNameValue has "acceleratorDeployments/write"
| where ActivityStatusValue == "Succeeded"
| extend props = parse_json(Properties)
| summarize AvgDurationSec = avg(toreal(props.provisioningDurationSeconds))
    by tostring(props.acceleratorType)

// Failed deployments with error details
AzureActivity
| where ResourceProvider == "MICROSOFT.COGNITIVESERVICES"
| where OperationNameValue has "acceleratorDeployments/write"
| where ActivityStatusValue == "Failed"
| extend props = parse_json(Properties)
| project TimeGenerated, Caller,
          Model = tostring(props.model),
          Error = tostring(props.failureReason)
```

---

## Service Telemetry

Internal telemetry emitted by the RP and infrastructure to track **product usage, adoption, retention, and reliability**. This data feeds internal dashboards, SLO tracking, and business reviews — it is not exposed to customers.

### Event Schema

Every deployment lifecycle event emits a structured telemetry record:

| Property | Type | Description |
|---|---|---|
| `subscriptionId` | `string` | Subscription ID. |
| `resourceGroupName` | `string` | Resource group name. |
| `accountName` | `string` | Foundry account name. |
| `deploymentName` | `string` | Deployment name. |
| `model` | `string` | Full model asset URI (registry, model, version). |
| `deploymentTemplate` | `string` | Deployment template URI. |
| `acceleratorType` | `string` | Accelerator type (e.g., `H100_80GB`). |
| `skuName` | `string` | SKU name (`GlobalManagedCompute`, `DataZoneManagedCompute`). |
| `instanceCount` | `integer` | Number of model instances (`sku.capacity`). |
| `acceleratorsPerInstance` | `integer` | GPUs per instance (from template). |
| `totalAccelerators` | `integer` | Total GPUs allocated. |
| `provisioningDurationSeconds` | `number` | Time from `Accepted` to `Succeeded` (or `Failed`). |
| `operationType` | `string` | `Create`, `Update`, `Scale`, `Delete`. |
| `resultStatus` | `string` | `Succeeded`, `Failed`, `Canceled`. |
| `failureReason` | `string` | Error code if failed (e.g., `QuotaExceeded`, `CapacityUnavailable`). |
| `callerIdentity` | `string` | Identity (UPN or object ID) that initiated the operation. |
| `correlationId` | `string` | ARM correlation ID for tracing across services. |
| `region` | `string` | Azure region where the deployment is provisioned. |
| `tenantId` | `string` | Entra tenant ID of the caller. |
| `timestamp` | `datetime` | UTC timestamp of the event. |

### Usage Metrics

Track what customers are deploying, how much, and where.

| Metric | Grain | Definition | Use |
|---|---|---|---|
| **Active deployments** | Hourly snapshot | Count of deployments with `provisioningState = Succeeded` at snapshot time. | Product usage dashboard, capacity planning. |
| **Total accelerators allocated** | Hourly snapshot | Sum of `totalAccelerators` across all active deployments. | GPU utilization tracking, cost attribution. |
| **Deployments created** | Daily/weekly/monthly | Count of `operationType = Create` events with `resultStatus = Succeeded`. | Growth tracking. |
| **Deployments deleted** | Daily/weekly/monthly | Count of `operationType = Delete` events. | Churn tracking. |
| **Net deployments** | Daily/weekly/monthly | Created − Deleted. | Net growth indicator. |
| **Models deployed** | Daily snapshot | Distinct `model` values across active deployments. | Model breadth, catalog coverage. |
| **Accelerator type distribution** | Daily snapshot | Breakdown of active deployments by `acceleratorType`. | Hardware demand mix. |
| **SKU distribution** | Daily snapshot | Breakdown by `skuName` (Global vs. DataZone). | Data residency adoption. |

### Adoption Metrics

Track customer onboarding and breadth.

| Metric | Grain | Definition | Use |
|---|---|---|---|
| **Unique subscriptions** | Daily/weekly/monthly | Distinct `subscriptionId` values with at least one active deployment. | Customer breadth, adoption funnel. |
| **Unique accounts** | Daily/weekly/monthly | Distinct `accountName` values with at least one active deployment. | Account-level adoption. |
| **Unique tenants** | Monthly | Distinct `tenantId` values with at least one active deployment. | Org-level adoption. |
| **New subscriptions** | Weekly/monthly | Subscriptions that created their first-ever accelerator deployment in the period. | New customer acquisition. |
| **First deployment time-to-value** | Per subscription | Duration from first ARM API call (any `acceleratorDeployments` operation) to first `provisioningState = Succeeded`. | Onboarding friction. |
| **Models per account** | Monthly snapshot | Distinct `model` values per account. | Depth of adoption within accounts. |

### Retention Metrics

Track whether customers keep using managed compute over time.

| Metric | Grain | Definition | Use |
|---|---|---|---|
| **Subscription retention (D7/D28)** | Weekly/monthly cohort | % of subscriptions that had an active deployment on day 0 and still have one on day 7/28. | Stickiness. |
| **Deployment lifespan** | Per deployment | Duration from `Create Succeeded` to `Delete Succeeded`. | How long deployments live — short-lived = experimentation, long-lived = production. |
| **Deployment survival curve** | Cohort | % of deployments from a weekly cohort still active at day 1, 7, 14, 28. | Drop-off patterns. |
| **Scale events per deployment** | Per deployment lifetime | Count of `operationType = Scale` events. | Engagement depth — scaling = active tuning. |
| **Returning accounts** | Monthly | Accounts that deleted all deployments in month N and created a new one in month N+1. | Win-back signal. |

### Reliability Metrics

Track service health and operational quality.

| Metric | Grain | Definition | SLO Target |
|---|---|---|---|
| **Provisioning success rate** | Daily | `Create Succeeded / (Create Succeeded + Create Failed)` × 100. | ≥ 99.5% |
| **P50/P95/P99 provisioning duration** | Daily | Percentiles of `provisioningDurationSeconds` for `Create Succeeded` events. | P95 ≤ 15 min |
| **Scale success rate** | Daily | `Scale Succeeded / (Scale Succeeded + Scale Failed)` × 100. | ≥ 99.5% |
| **Delete success rate** | Daily | `Delete Succeeded / (Delete Succeeded + Delete Failed)` × 100. | ≥ 99.9% |
| **Failure breakdown** | Daily | Count of failures by `failureReason` (e.g., `QuotaExceeded`, `CapacityUnavailable`, `InternalError`). | — |
| **Capacity-related failures** | Daily | Failures where `failureReason = CapacityUnavailable`. | ≤ 5% of create attempts |
| **ARM API latency (sync)** | Daily | P50/P95 latency for `GET` and `LIST` operations. | P95 ≤ 500ms |
| **ARM API error rate** | Daily | 5xx responses / total requests for all `acceleratorDeployments` operations. | ≤ 0.1% |

### Key Dashboards

| Dashboard | Audience | Primary Metrics |
|---|---|---|
| **Managed Compute Overview** | PM, Engineering leads | Active deployments, total GPUs, deployments created/deleted, net growth, model mix. |
| **Adoption Funnel** | PM, GTM | Unique subscriptions, new subscriptions, first deployment time-to-value, models per account. |
| **Retention & Engagement** | PM | D7/D28 retention, deployment lifespan distribution, scale events per deployment. |
| **Reliability & SLOs** | Engineering, SRE | Provisioning success rate, P95 duration, failure breakdown, API latency, error rate. |
| **Capacity Planning** | Infrastructure | Accelerator allocation by region/type, capacity-related failure rate, demand forecast. |

---

## Deployment Name Rules

Deployment names must be **unique across both `deployments` and `acceleratorDeployments`** on the same account. This is because inference routing uses the deployment name in the URL path — collisions would create ambiguous routes.

| Rule | Constraint |
|---|---|
| Length | 2–64 characters |
| Characters | Alphanumeric, underscore, hyphen, period: `^[a-zA-Z0-9][a-zA-Z0-9_.-]*$` |
| Uniqueness | Unique within the account, across both `deployments` and `acceleratorDeployments`. |
| Used in inference URL | `<endpoint>/managed-deployments/<deployment-name>/<path>` |

---

## End-to-End Example

Full workflow from the user's perspective — assumes prior steps have yielded the three required inputs.

### REST

```bash
# 1. Create deployment
curl -X PUT \
  "https://management.azure.com/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.CognitiveServices/accounts/$ACCOUNT/acceleratorDeployments/gpt-oss-120b-gpu?api-version=2026-04-01-preview" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "properties": {
      "model": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
      "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
      "acceleratorType": "H100_80GB"
    },
    "sku": { "name": "GlobalManagedCompute", "capacity": 1 }
  }'

# 2. Poll until ready
while true; do
  STATE=$(curl -s \
    "https://management.azure.com/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.CognitiveServices/accounts/$ACCOUNT/acceleratorDeployments/gpt-oss-120b-gpu?api-version=2026-04-01-preview" \
    -H "Authorization: Bearer $TOKEN" \
    | jq -r '.properties.provisioningState')
  echo "State: $STATE"
  [[ "$STATE" == "Succeeded" || "$STATE" == "Failed" ]] && break
  sleep 30
done

# 3. Get account endpoint and scoring route
ACCOUNT_ENDPOINT=$(az cognitiveservices account show --name $ACCOUNT -g $RG \
  --query "properties.endpoint" -o tsv)
SCORING_ROUTE=$(curl -s \
  "https://management.azure.com/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.CognitiveServices/accounts/$ACCOUNT/acceleratorDeployments/gpt-oss-120b-gpu?api-version=2026-04-01-preview" \
  -H "Authorization: Bearer $TOKEN" \
  | jq -r '.properties.routes.chatCompletionsScoringPath')

# 4. Make inference request
API_KEY=$(az cognitiveservices account keys list --name $ACCOUNT -g $RG --query "key1" -o tsv)
curl -s "${ACCOUNT_ENDPOINT}/managed-deployments/gpt-oss-120b-gpu${SCORING_ROUTE}" \
  -H "api-key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Hello"}],"max_tokens":100}'

# 5. Scale to 2 instances
curl -X PUT \
  "https://management.azure.com/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.CognitiveServices/accounts/$ACCOUNT/acceleratorDeployments/gpt-oss-120b-gpu?api-version=2026-04-01-preview" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{ "sku": { "name": "GlobalManagedCompute", "capacity": 2 } }'

# 6. Delete deployment
curl -X DELETE \
  "https://management.azure.com/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.CognitiveServices/accounts/$ACCOUNT/acceleratorDeployments/gpt-oss-120b-gpu?api-version=2026-04-01-preview" \
  -H "Authorization: Bearer $TOKEN"
```

### SDK

```python
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.mgmt.cognitiveservices.models import (
    AcceleratorDeployment, AcceleratorDeploymentProperties, Sku,
)
from azure.identity import DefaultAzureCredential
import time

cog = CognitiveServicesManagementClient(DefaultAzureCredential(), subscription_id)
rg, account = "my-rg", "my-foundry-account"

# 1. Create
deployment = AcceleratorDeployment(
    properties=AcceleratorDeploymentProperties(
        model="azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
        deployment_template="azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
        accelerator_type="H100_80GB",
    ),
    sku=Sku(name="GlobalManagedCompute", capacity=1),
)
poller = cog.accelerator_deployments.begin_create_or_update(rg, account, "gpt-oss-120b-gpu", deployment)
result = poller.result()
print(f"Routes: {result.properties.routes}")

# 2. List
for dep in cog.accelerator_deployments.list(rg, account):
    print(f"{dep.name}: {dep.properties.accelerator_type} × {dep.sku.capacity}")

# 3. Scale
scale = AcceleratorDeployment(sku=Sku(name="GlobalManagedCompute", capacity=2))
cog.accelerator_deployments.begin_create_or_update(rg, account, "gpt-oss-120b-gpu", scale).result()

# 4. Delete
cog.accelerator_deployments.begin_delete(rg, account, "gpt-oss-120b-gpu").result()
```

### CLI

```bash
# 1. Create
az cognitiveservices account accelerator-deployment create \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --model "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4" \
  --deployment-template "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1" \
  --accelerator-type H100_80GB \
  --sku-name GlobalManagedCompute --sku-capacity 1

# 2. List
az cognitiveservices account accelerator-deployment list --name $ACCOUNT -g $RG -o table

# 3. Scale
az cognitiveservices account accelerator-deployment create \
  --name $ACCOUNT -g $RG --deployment-name gpt-oss-120b-gpu --sku-capacity 2

# 4. Delete
az cognitiveservices account accelerator-deployment delete \
  --name $ACCOUNT -g $RG --deployment-name gpt-oss-120b-gpu
```
