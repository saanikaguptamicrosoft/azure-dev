# Accelerator Deployments — Get Logs

Retrieves logs from a managed compute (accelerator) deployment. Supports three log sources: inference server containers, model downloader init containers, and infrastructure-level ARM events (instance provisioning, scaling, traffic routing).

This is modeled after the [Azure ML Online Deployments — Get Logs](https://learn.microsoft.com/en-us/rest/api/azureml/online-deployments/get-logs) API, extended with multi-instance and infrastructure log support.

---

## Delivery Options

Two options for how logs are returned. Final choice depends on engineering feasibility — both are specced below.

### Option A: Inline JSON Response

Logs are returned directly in the response body as JSON with embedded newline strings.

| Aspect | Details |
|---|---|
| **Transport** | Standard ARM `POST` → `200 OK` with JSON body. |
| **Response size** | Bounded by `tail` parameter (default 500 lines per instance). For a 3-instance deployment at 500 lines each, response is ~150–300 KB. |
| **Pros** | Simple — one request, one response, fully testable with `curl`. No storage dependency. Matches AzureML's existing `getLogs` pattern. SDK returns a typed object (iterate `instances[]`, print `content`). |
| **Cons** | Not suitable for very large log volumes (e.g., full container lifetime logs). ARM has a practical response size limit (~4 MB). Multi-instance with high `tail` values could be slow. |
| **Best for** | Interactive debugging ("show me last 100 lines"), SDK/CLI workflows, CI/CD scripts. The 90% use case. |

### Option B: Download as ZIP

Service writes logs to a temporary blob, returns a download URL. Client downloads a ZIP containing one log file per instance + infrastructure log.

| Aspect | Details |
|---|---|
| **Transport** | `POST` returns `202 Accepted` with `Location` header → poll until `200 OK` with `{ "downloadUrl": "https://...?sv=...&sig=..." }`. Download URL is a time-limited SAS (15 min expiry). |
| **Response structure** | ZIP archive containing:<br>`infrastructure.log`<br>`instances/0-xk9f2a-inference-server.log`<br>`instances/1-m7p3bz-inference-server.log`<br>`instances/0-xk9f2a-model-downloader.log`<br>`instances/1-m7p3bz-model-downloader.log` |
| **Pros** | No size limit — can return full lifetime logs. ZIP contains all log sources in one download. Good for support bundles and post-mortem analysis. |
| **Cons** | More complex: requires staging blob storage, SAS generation, async polling. Two round trips (POST → poll → download). Harder to use in scripts without `unzip`. SDK needs a `download_logs()` method that streams to disk. |
| **Best for** | Full log export, support ticket attachments, post-mortem analysis. The 10% use case. |

### Recommendation

Implement **Option A first** — it covers the primary debugging workflow and matches the AzureML pattern. Option B can be added later as a separate `/exportLogs` action if customers need full log dumps for support scenarios.

If both ship, they would be separate operations:
- `POST .../getLogs` — Option A (inline, interactive)
- `POST .../exportLogs` — Option B (ZIP download, bulk export)

---

## HTTP Request

```http
POST https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}
    /providers/Microsoft.CognitiveServices/accounts/{accountName}
    /acceleratorDeployments/{deploymentName}/getLogs?api-version=2026-04-01-preview
Authorization: Bearer {token}
Content-Type: application/json
```

## URI Parameters

| Name | In | Required | Type | Description |
|---|---|---|---|---|
| `subscriptionId` | path | Yes | `string` (UUID) | The ID of the target subscription. |
| `resourceGroupName` | path | Yes | `string` (1–90 chars) | The name of the resource group. Case-insensitive. |
| `accountName` | path | Yes | `string` (2–64 chars) | The name of the Cognitive Services / Foundry account. |
| `deploymentName` | path | Yes | `string` | The name of the accelerator deployment. |
| `api-version` | query | Yes | `string` | API version. Minimum: `2026-04-01-preview`. |

---

## Request Body

### `DeploymentLogsRequest`

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `logSource` | [`LogSource`](#logsource) | No | `InferenceServer` | Which log source to retrieve from. |
| `instanceId` | `string` | No | *(all instances)* | Filter logs to a specific instance by its stable instance ID (e.g., `xk9f2a`). Omit to get logs from all instances. Ignored when `logSource` is `Infrastructure`. |
| `tail` | `integer` (int32) | No | `500` | Maximum number of lines to return per instance (most recent lines). For `Infrastructure` logs, this is the total line count. |

### Request Body Examples

```json
// Get inference server logs from all instances (default)
{
  "logSource": "InferenceServer",
  "tail": 100
}
```

```json
// Get logs from a specific instance
{
  "logSource": "InferenceServer",
  "instanceId": "xk9f2a",
  "tail": 50
}
```

```json
// Get infrastructure logs (instance creation, scaling, routing)
{
  "logSource": "Infrastructure",
  "tail": 200
}
```

---

## Responses

| Status Code | Type | Description |
|---|---|---|
| `200 OK` | [`DeploymentLogs`](#deploymentlogs) | Logs retrieved successfully. |
| `404 Not Found` | `ErrorResponse` | Deployment or instance does not exist. |
| Other | `ErrorResponse` | Error response. |

### Response Body — Container Logs (all instances)

When `instanceId` is omitted, logs are returned per-instance. Each entry includes the instance's stable ID, ordinal index, and log content.

```json
{
  "instances": [
    {
      "instanceId": "xk9f2a",
      "index": 0,
      "content": "2026-03-23T14:31:02Z [INFO] Starting vLLM inference server...\n2026-03-23T14:31:03Z [INFO] Loading model: gpt-oss-120b\n2026-03-23T14:31:15Z [INFO] Model loaded successfully on 4x H100_80GB\n2026-03-23T14:31:16Z [INFO] Ready to serve requests\n"
    },
    {
      "instanceId": "m7p3bz",
      "index": 1,
      "content": "2026-03-23T14:35:10Z [INFO] Starting vLLM inference server...\n2026-03-23T14:35:11Z [INFO] Loading model: gpt-oss-120b\n2026-03-23T14:35:22Z [INFO] Model loaded successfully on 4x H100_80GB\n2026-03-23T14:35:23Z [INFO] Ready to serve requests\n"
    }
  ]
}
```

### Response Body — Container Logs (single instance)

When `instanceId` is specified, response contains a single instance entry.

```json
{
  "instances": [
    {
      "instanceId": "xk9f2a",
      "index": 0,
      "content": "2026-03-23T14:31:02Z [INFO] Starting vLLM inference server...\n2026-03-23T14:31:15Z [INFO] Model loaded successfully on 4x H100_80GB\n2026-03-23T14:31:16Z [INFO] Ready to serve requests\n"
    }
  ]
}
```

### Response Body — Infrastructure Logs

Infrastructure logs are deployment-scoped (not per-instance). They show ARM-level lifecycle events.

```json
{
  "instances": [],
  "infrastructure": "2026-03-23T14:30:00Z [ARM] Deployment gpt-oss-120b-gpu: PUT accepted, provisioningState=Accepted\n2026-03-23T14:30:01Z [INFRA] Requesting 4x H100_80GB from capacity pool region=eastus2\n2026-03-23T14:30:15Z [INFRA] GPU allocation succeeded, instanceId=xk9f2a\n2026-03-23T14:30:16Z [INFRA] Pulling container image: mcr.microsoft.com/aifoundry/vllm:2026.03\n2026-03-23T14:31:00Z [INFRA] Container started on instance xk9f2a\n2026-03-23T14:31:16Z [INFRA] Health check passed on instance xk9f2a\n2026-03-23T14:31:16Z [ROUTING] Instance xk9f2a added to traffic pool\n2026-03-23T14:31:16Z [ARM] provisioningState=Succeeded\n2026-03-23T14:45:00Z [ARM] Scale event: sku.capacity 1→2\n2026-03-23T14:45:01Z [INFRA] Requesting 4x H100_80GB from capacity pool region=eastus2\n2026-03-23T14:45:20Z [INFRA] GPU allocation succeeded, instanceId=m7p3bz\n2026-03-23T14:45:21Z [INFRA] Pulling container image: mcr.microsoft.com/aifoundry/vllm:2026.03\n2026-03-23T14:46:10Z [INFRA] Container started on instance m7p3bz\n2026-03-23T14:46:22Z [INFRA] Health check passed on instance m7p3bz\n2026-03-23T14:46:22Z [ROUTING] Instance m7p3bz added to traffic pool, active instances: 2\n2026-03-23T14:46:22Z [ARM] Scale event completed, provisioningState=Succeeded\n"
}
```

---

## Instance Identification

Each model instance in a deployment has two identifiers:

| Identifier | Type | Stability | Example | Use |
|---|---|---|---|---|
| `instanceId` | `string` (6-char alphanumeric) | **Stable** — unique to the physical instance for its lifetime. New instance = new ID. | `xk9f2a` | Primary key for log retrieval, debugging, and correlation. |
| `index` | `integer` (0-based) | **Ordinal** — assigned by creation order. When an instance is replaced (scale event, rolling update), the new instance inherits the index. | `0`, `1`, `2` | Human-friendly reference ("check instance 0's logs"). |

> **Design note — why short IDs, not GUIDs:**
> GUIDs are stable but unwieldy for CLI and verbal reference ("check instance `a4f2e8c1-3b7d-...`" — no one does this). Sequential ordinal numbers (1, 2, 3) are human-friendly but unstable — scale-in removes instance 3, scale-out creates a different instance 3, and during rolling updates the same index maps to a different physical instance. Short IDs (6-char, scoped to the deployment) give both stability and readability. Collision risk is negligible within a single deployment's instance count.

### Instance Lifecycle

```
Scale capacity: 1 → 2 → 1 → 2

Instances over time:
  index 0: xk9f2a ─────────────────────────────────
  index 1:          m7p3bz ──────── (removed)
  index 1:                                   q2w4rt ─── (new instance, new ID, same index)
```

When instance `m7p3bz` (index 1) is removed during scale-in and a new instance is added during scale-out, the new instance `q2w4rt` gets index 1 but a fresh `instanceId`. Infrastructure logs show this transition.

---

## Behavior

- **Read-only**: This operation does not modify the deployment.
- **Tail semantics**: Returns the last `tail` lines per instance. Logs reflect the current container's stdout/stderr buffer from the underlying compute infrastructure and are not persisted indefinitely.
- **Multi-instance**: Logs are returned for all active instances by default. Use `instanceId` to filter to a specific instance.
- **Infrastructure logs**: Deployment-scoped (not per-instance). Show ARM operations, GPU provisioning, container lifecycle, and traffic routing events. Useful for debugging provisioning failures, slow scale-outs, or routing issues.
- **Provisioning states**: Container logs (`InferenceServer`, `ModelDownloader`) are available when `provisioningState` is `Creating`, `Updating`, `Succeeded`, or `Failed`. Infrastructure logs are available in all states except `Accepted`.
- **Authorization**: Requires `Microsoft.CognitiveServices/accounts/acceleratorDeployments/read` permission.

---

## SDK

```python
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.mgmt.cognitiveservices.models import DeploymentLogsRequest
from azure.identity import DefaultAzureCredential

cog = CognitiveServicesManagementClient(DefaultAzureCredential(), subscription_id)
rg, account = "my-rg", "my-foundry-account"

# Get inference server logs from all instances
logs = cog.accelerator_deployments.get_logs(
    resource_group_name=rg,
    account_name=account,
    deployment_name="gpt-oss-120b-gpu",
    body=DeploymentLogsRequest(
        log_source="InferenceServer",
        tail=100,
    ),
)

for instance in logs.instances:
    print(f"--- Instance {instance.index} ({instance.instance_id}) ---")
    print(instance.content)
```

```python
# Get logs from a specific instance
logs = cog.accelerator_deployments.get_logs(
    resource_group_name=rg,
    account_name=account,
    deployment_name="gpt-oss-120b-gpu",
    body=DeploymentLogsRequest(
        log_source="InferenceServer",
        instance_id="xk9f2a",
        tail=50,
    ),
)

print(logs.instances[0].content)
```

```python
# Get infrastructure logs — debug a scaling issue
logs = cog.accelerator_deployments.get_logs(
    resource_group_name=rg,
    account_name=account,
    deployment_name="gpt-oss-120b-gpu",
    body=DeploymentLogsRequest(
        log_source="Infrastructure",
        tail=200,
    ),
)

print(logs.infrastructure)
```

```python
# Debug a failed deployment — check model download logs
logs = cog.accelerator_deployments.get_logs(
    resource_group_name=rg,
    account_name=account,
    deployment_name="gpt-oss-120b-gpu",
    body=DeploymentLogsRequest(
        log_source="ModelDownloader",
    ),
)

for instance in logs.instances:
    print(f"--- Instance {instance.index} ({instance.instance_id}) ---")
    print(instance.content)
```

---

## CLI

```bash
# Get inference server logs from all instances (default)
az cognitiveservices account accelerator-deployment get-logs \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu

# Get last 50 lines from a specific instance
az cognitiveservices account accelerator-deployment get-logs \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --instance-id xk9f2a \
  --tail 50

# Get infrastructure logs (ARM events, scaling, routing)
az cognitiveservices account accelerator-deployment get-logs \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --log-source Infrastructure

# Get model downloader logs to debug startup failure
az cognitiveservices account accelerator-deployment get-logs \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --log-source ModelDownloader

# Search all instance logs for errors
az cognitiveservices account accelerator-deployment get-logs \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --tail 1000 -o json \
  | jq -r '.instances[] | "--- \(.instanceId) ---\n\(.content)"' \
  | grep -i "error\|exception\|fatal"
```

---

## Definitions

### `LogSource`

The source to retrieve logs from.

| Value | Description |
|---|---|
| `InferenceServer` | The model serving container (e.g., vLLM, TGI, Triton). This is the primary container that handles inference requests. **Default.** Per-instance. |
| `ModelDownloader` | The init container that downloads model weights from the ML Registry to local storage before the inference server starts. Useful for debugging model download failures or slow startup. Per-instance. |
| `Infrastructure` | ARM and infrastructure-level events: instance provisioning, GPU allocation, container lifecycle, health checks, scaling operations, traffic routing, and rolling update progress. Deployment-scoped (not per-instance). |

### `DeploymentLogs`

| Name | Type | Description |
|---|---|---|
| `instances` | [`InstanceLogs[]`](#instancelogs) | Array of per-instance log entries. One entry per active instance (or one if `instanceId` was specified). Empty when `logSource` is `Infrastructure`. |
| `infrastructure` | `string` | Infrastructure-level logs as a single string with embedded newlines. Present only when `logSource` is `Infrastructure`. `null` otherwise. |

### `InstanceLogs`

| Name | Type | Description |
|---|---|---|
| `instanceId` | `string` | Stable 6-character alphanumeric identifier for this instance. Unique within the deployment. Does not change for the lifetime of the instance. |
| `index` | `integer` | 0-based ordinal index assigned by creation order. When an instance is replaced, the new instance inherits the index but gets a new `instanceId`. |
| `content` | `string` | Log content for this instance as a single string with embedded newlines (`\n`). Empty string if no logs are available. |

### `DeploymentLogsRequest`

| Name | Type | Default | Description |
|---|---|---|---|
| `logSource` | [`LogSource`](#logsource) | `InferenceServer` | Which log source to retrieve from. |
| `instanceId` | `string` | *(all instances)* | Filter to a specific instance. Ignored for `Infrastructure` logs. Returns `404` if the instance ID does not exist. |
| `tail` | `integer` (int32) | `500` | Maximum number of lines per instance (or total for `Infrastructure`). |
