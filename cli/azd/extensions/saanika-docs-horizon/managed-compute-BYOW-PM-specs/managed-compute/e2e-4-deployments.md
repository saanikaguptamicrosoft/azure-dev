# Deployments — Create, List, Scale, Delete

Both pre-flight checks pass. We have quota headroom and physical capacity for both deployment types. Now deploy.

---

## Create Deployments

Same URL for both: `PUT .../accounts/{account}/deployments/{name}`. The body tells the RP which kind of deployment to create.

### 5A. Serverless — `gpt-oss-120b-serverless`

**REST:**
```http
PUT https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /deployments/gpt-oss-120b-serverless?api-version=2025-09-01
Authorization: Bearer {token}

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
  "sku": {
    "name": "GlobalStandard",
    "capacity": 100
  }
}
```

**SDK:**
```python
from azure.mgmt.cognitiveservices.models import (
    Deployment, DeploymentProperties, DeploymentModel, Sku,
)

serverless_dep = Deployment(
    properties=DeploymentProperties(
        model=DeploymentModel(
            format="OpenAI-OSS",
            name="gpt-oss-120b",
            version="1",
        ),
        version_upgrade_option="OnceNewDefaultVersionAvailable",
        rai_policy_name="Microsoft.DefaultV2",
    ),
    sku=Sku(name="GlobalStandard", capacity=100),
)

cog.deployments.begin_create_or_update(
    rg, account, "gpt-oss-120b-serverless", serverless_dep,
).result()
```

**CLI:**
```bash
az cognitiveservices account deployment create \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-serverless \
  --model-format OpenAI-OSS --model-name gpt-oss-120b --model-version 1 \
  --sku-name GlobalStandard --sku-capacity 100
```

### 5B. Managed Compute — `gpt-oss-120b-gpu`

Two options for creating managed compute deployments:

#### Option A: Same `deployments` API (discriminated by `sku.name`)

Uses the same URL as serverless: `PUT .../accounts/{account}/deployments/{name}`. The body carries managed-compute-specific fields; the RP discriminates by `sku.name = "GlobalManagedCompute"`.

**REST:**
```http
PUT https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /deployments/gpt-oss-120b-gpu?api-version=2026-01-01-preview
Authorization: Bearer {token}

{
  "properties": {
    "modelAssetId": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
    "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
    "acceleratorType": "H100_80G"
  },
  "sku": {
    "name": "GlobalManagedCompute",
    "capacity": 1
  }
}
```

> `sku.capacity` is **instance count** for managed compute — the number of model instances to deploy. Each instance uses 4 H100 accelerators (defined by the deployment template). Total accelerators consumed = `capacity × acceleratorsPerInstance` = 1 × 4 = 4 GPUs. This mirrors how serverless uses `sku.capacity` for TPM — it's always the user-facing unit for that deployment type.

**SDK:**
```python
mc_dep = Deployment(
    properties=DeploymentProperties(
        model_asset_id=(
            "azureml://registries/azureml-openai-oss"
            "/models/gpt-oss-120b/versions/4"
        ),
        deployment_template=(
            "azureml://registries/azureml-openai-oss"
            "/deploymenttemplates/gpt-oss-120b-short-context/versions/1"
        ),
        accelerator_type="H100_80G",
    ),
    sku=Sku(name="GlobalManagedCompute", capacity=1),  # 1 instance = 4 H100s
)

cog.deployments.begin_create_or_update(
    rg, account, "gpt-oss-120b-gpu", mc_dep,
).result()
```

**CLI:**
```bash
az cognitiveservices account deployment create \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --model-id azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4 \
  --sku-name GlobalManagedCompute --sku-capacity 1 \
  --accelerator-type H100_80G \
  --deployment-template azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1
```

**Pros:**
- Unified endpoint — one URL for all deployment types. List, delete, and get operations work identically.
- Existing SDK clients (e.g., `cog.deployments.begin_create_or_update`) work without new methods.
- Consistent with how the RP already handles `GlobalStandard` vs `GlobalProvisionedManaged` via `sku.name`.

**Cons:**
- `DeploymentProperties` becomes a union type — `model.(format, name, version)` for serverless, `modelAssetId` for managed compute. Half the fields are irrelevant for any given deployment.
- Managed compute adds new fields (`deploymentTemplate`, `acceleratorType`) that are meaningless for serverless, and `sku.capacity` changes semantics (instance count vs TPM). Validation must reject them contextually.
- Future extensibility (LoRA adapters, speculative decoding) will add more managed-compute-only fields, further bloating the union.

#### Option B: Separate `acceleratorDeployments` API

A new resource type under the same account. Managed compute deployments live at `.../accounts/{account}/acceleratorDeployments/{name}` — a clean separation.

**REST:**
```http
PUT https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /acceleratorDeployments/gpt-oss-120b-gpu?api-version=2026-01-01-preview
Authorization: Bearer {token}

{
  "properties": {
    "modelAssetId": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
    "deploymentTemplate": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
    "acceleratorType": "H100_80G",
    "versionUpgradeOption": "OnceNewDefaultVersionAvailable"
  },
  "sku": {
    "name": "GlobalManagedCompute",
    "capacity": 1
  }
}
```

> **`versionUpgradeOption`**: Controls when the platform upgrades the deployment template (container image, serving runtime). Valid values are `NoAutoUpgrade`, `OnceCurrentVersionExpired`, and `OnceNewDefaultVersionAvailable`. **Only `OnceNewDefaultVersionAvailable` is supported initially** — the platform must retain the right to upgrade containers for security patches, CVE remediation, and runtime fixes. `NoAutoUpgrade` and `OnceCurrentVersionExpired` will be enabled when bring-your-own-container (BYOC) is supported, at which point the customer owns the container lifecycle and accepts the security responsibility.

**SDK:**
```python
from azure.mgmt.cognitiveservices.models import (
    AcceleratorDeployment, AcceleratorDeploymentProperties, Sku,
)

mc_dep = AcceleratorDeployment(
    properties=AcceleratorDeploymentProperties(
        model_asset_id=(
            "azureml://registries/azureml-openai-oss"
            "/models/gpt-oss-120b/versions/4"
        ),
        deployment_template=(
            "azureml://registries/azureml-openai-oss"
            "/deploymenttemplates/gpt-oss-120b-short-context/versions/1"
        ),
        accelerator_type="H100_80G",
        version_upgrade_option="OnceNewDefaultVersionAvailable",
    ),
    sku=Sku(name="GlobalManagedCompute", capacity=1),  # 1 instance = 4 H100s
)

cog.accelerator_deployments.begin_create_or_update(
    rg, account, "gpt-oss-120b-gpu", mc_dep,
).result()
```

**CLI:**
```bash
az cognitiveservices account accelerator-deployment create \
  --name $ACCOUNT -g $RG \
  --deployment-name gpt-oss-120b-gpu \
  --model-id azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4 \
  --sku-name GlobalManagedCompute --sku-capacity 1 \
  --accelerator-type H100_80G \
  --deployment-template azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1
```

**Pros:**
- Clean type — `AcceleratorDeploymentProperties` has only managed-compute fields. No union, no contextual validation.
- Future extensibility is straightforward — adding LoRA adapter management, speculative decoding, or other GPU-specific operations goes on this resource type without affecting serverless deployments.
- List operations are type-safe: `acceleratorDeployments.list()` returns only managed compute deployments; no need to filter by `sku.name`.
- ARM RBAC can scope permissions separately — "can create serverless deployments but not managed compute" becomes a natural resource-type distinction.

**Cons:**
- Two endpoints to learn. Users deploying both types must know `deployments` and `acceleratorDeployments`.
- List-all-deployments requires two calls (or a unified view at the account level).
- Inference routing must map both resource types to the same endpoint (account-level key + deployment name).

#### Option A vs. Option B

| | Option A: Same `deployments` API | Option B: `acceleratorDeployments` API |
|---|---|---|
| **URL** | `PUT .../deployments/{name}` | `PUT .../acceleratorDeployments/{name}` |
| **Type model** | Union (`DeploymentProperties` with conditional fields) | Separate (`AcceleratorDeploymentProperties`) |
| **List** | One call, filter by `sku.name` | Two calls (or unified account-level view) |
| **Validation** | Contextual (reject serverless fields on MC, vice versa) | Type-safe (each resource has only relevant fields) |
| **Extensibility** | New MC fields bloat the shared type | New MC fields go on the dedicated type |
| **RBAC** | Same resource type, same permissions | Separate resource type, separate permissions |
| **Inference** | Same account endpoint, deployment name routing | Same (no change to inference path) |

### Side-by-side: Same model, different deployment bodies

```
                    SERVERLESS                        MANAGED COMPUTE
                    ──────────                        ───────────────
Deployment name:    gpt-oss-120b-serverless            gpt-oss-120b-gpu

URL (Option A):     PUT .../deployments/{name}         PUT .../deployments/{name}
                    (same endpoint)                    (same endpoint)
URL (Option B):     PUT .../deployments/{name}         PUT .../acceleratorDeployments/{name}
                                                       (separate endpoint)

Model identity:     format: "OpenAI-OSS"               modelAssetId:
                    name: "gpt-oss-120b"                  "azureml://registries/azureml
                    version: "1"                          -openai-oss/models/gpt-oss
                                                          -120b/versions/4"

Deployment          N/A                                "azureml://registries/azureml
template:                                                -openai-oss/deploymenttemplates
                                                         /gpt-oss-120b-short-context/versions/1"

Accelerator:        N/A                                H100_80G, 4 accelerators × 1 instance

SKU:                GlobalStandard                     GlobalManagedCompute
Capacity:           100 (TPM)                          1 (instance count)

Version upgrade:    OnceNewDefaultVersionAvailable     OnceNewDefaultVersionAvailable
                                                       (only option until BYOC)
RAI policy:         Microsoft.DefaultV2                N/A (container-level filtering)
```

Key observation: the same model (`gpt-oss-120b v4`) is referenced two different ways. The serverless deployment uses `(format, name, version)` — the Cog Services identity tuple. The managed compute deployment uses the `assetId` URI — the ML Registry identity. Both point to the same model artifact. The deployment template (only needed for managed compute) specifies the container and serving runtime that the RP will provision on the allocated GPUs.

---

## List All Deployments

Both deployments on the same account. How they appear depends on which deployment API option was chosen above.

#### Option A: Single list (shared `deployments` API)

Both serverless and managed compute deployments live under the same resource type, so one list call returns everything.

**REST:**
```http
GET https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /deployments?api-version=2026-01-01-preview
Authorization: Bearer {token}
```

**SDK:**
```python
for dep in cog.deployments.list(resource_group_name=rg, account_name=account):
    if dep.sku.name in ("GlobalManagedCompute", "DataZoneManagedCompute"):
        print(f"[MC]  {dep.name}: {dep.properties.accelerator_type} "
              f"× {dep.sku.capacity} instances")
    else:
        print(f"[SL]  {dep.name}: {dep.properties.model.name} "
              f"on {dep.sku.name} @ {dep.sku.capacity} TPM")
```

**CLI:**
```bash
az cognitiveservices account deployment list --name $ACCOUNT -g $RG -o table
```

**Output:**
```
Type  Name                     SKU                    Capacity  Details
────  ───────────────────────  ─────────────────────  ────────  ──────────────────────────
[SL]  gpt-oss-120b-serverless   GlobalStandard         100 TPM   OpenAI-OSS/gpt-oss-120b/4
[MC]  gpt-oss-120b-gpu          GlobalManagedCompute   1 inst    H100_80G × 4 accel/inst
```

Same model, two rows, one list call. The caller must check `sku.name` to distinguish deployment types.

#### Option B: Two lists (separate `acceleratorDeployments` API)

Serverless and managed compute are different resource types. Each has its own list operation.

**REST:**
```http
# Serverless deployments
GET https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /deployments?api-version=2026-01-01-preview
Authorization: Bearer {token}

# Managed compute deployments
GET https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}
    /providers/Microsoft.CognitiveServices/accounts/{account}
    /acceleratorDeployments?api-version=2026-01-01-preview
Authorization: Bearer {token}
```

**SDK:**
```python
# Serverless
for dep in cog.deployments.list(resource_group_name=rg, account_name=account):
    print(f"[SL]  {dep.name}: {dep.properties.model.name} "
          f"on {dep.sku.name} @ {dep.sku.capacity} TPM")

# Managed compute
for dep in cog.accelerator_deployments.list(resource_group_name=rg, account_name=account):
    print(f"[MC]  {dep.name}: {dep.properties.accelerator_type} "
          f"× {dep.sku.capacity} instances")
```

**CLI:**
```bash
# Serverless
az cognitiveservices account deployment list --name $ACCOUNT -g $RG -o table

# Managed compute
az cognitiveservices account accelerator-deployment list --name $ACCOUNT -g $RG -o table
```

**Output (serverless list):**
```
Name                     SKU             Capacity  Model
───────────────────────  ──────────────  ────────  ──────────────────────
gpt-oss-120b-serverless  GlobalStandard  100 TPM   OpenAI-OSS/gpt-oss-120b/4
```

**Output (managed compute list):**
```
Name              SKU                   Capacity  Accelerator  Template
────────────────  ────────────────────  ────────  ───────────  ────────────────────────
gpt-oss-120b-gpu  GlobalManagedCompute  1 inst    H100_80G     gpt-oss-120b-short-context/1
```

Two calls, but each returns only the relevant type — no filtering needed. The serverless list never shows GPU fields, and the managed compute list never shows TPM fields.

---

## Scale

### Serverless — increase TPM

```python
cog.deployments.begin_create_or_update(
    rg, account, "gpt-oss-120b-serverless",
    Deployment(sku=Sku(name="GlobalStandard", capacity=500)),
).result()
```

```bash
az cognitiveservices account deployment create \
  --name $ACCOUNT -g $RG --deployment-name gpt-oss-120b-serverless \
  --sku-name GlobalStandard --sku-capacity 500
```

### Managed Compute — add instances

```python
# Scale from 1 to 2 instances (4 GPUs each → 8 GPUs total)
cog.deployments.begin_create_or_update(
    rg, account, "gpt-oss-120b-gpu",
    Deployment(
        sku=Sku(name="GlobalManagedCompute", capacity=2),
    ),
).result()
```

```bash
az cognitiveservices account deployment create \
  --name $ACCOUNT -g $RG --deployment-name gpt-oss-120b-gpu \
  --sku-name GlobalManagedCompute --sku-capacity 2
```

---

## Delete

Same API, same command, no difference.

### SDK

```python
cog.deployments.begin_delete(rg, account, "gpt-oss-120b-serverless").result()
cog.deployments.begin_delete(rg, account, "gpt-oss-120b-gpu").result()
```

### CLI

```bash
az cognitiveservices account deployment delete \
  --name $ACCOUNT -g $RG --deployment-name gpt-oss-120b-serverless
az cognitiveservices account deployment delete \
  --name $ACCOUNT -g $RG --deployment-name gpt-oss-120b-gpu
```
