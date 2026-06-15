# Quota APIs — `usages` vs `acceleratorUsages`

Side-by-side comparison of the quota (usage) APIs for serverless and managed compute deployments.

Both APIs answer "how much of my quota have I consumed?" — but against fundamentally different pools with different units.

---

## Overview

| Dimension | Serverless (`usages`) | Managed Compute (`acceleratorUsages`) |
|---|---|---|
| **API** | `GET .../locations/{location}/usages` | `GET .../acceleratorUsages` |
| **API version** | `2025-09-01` (GA) | `2026-04-01-preview` (proposed) |
| **Status** | Existing — shipping today | Proposed — needs to be built |
| **Backing service** | Cog Services quota store | AzureML quota service |
| **Pool type** | Per-model, per-SKU (each model has its own quota) | Per-accelerator type, model-agnostic (shared GPU pool) |
| **Quota unit** | Tokens per minute (TPM) | GPU count |
| **Key insight** | Deploying model A does not consume model B's quota | Deploying _any_ model on H100 consumes the same H100 quota pool |

---

## Schema

### Serverless — `usages` response

```jsonc
{
  "currentValue": 0,                             // TPM currently allocated
  "limit": 5000,                                 // subscription ceiling (in thousands)
  "name": {
    "localizedValue": "Tokens Per Minute (thousands) - gpt-oss-120b",
    "value": "AIServices.GlobalStandard.gpt-oss-120b"
  },
  "scopeId": "global",
  "scopeType": "Regional",
  "unit": "Count"
}
```

### Managed Compute — `acceleratorUsages` response

```jsonc
{
  "currentValue": 0,                             // GPUs currently allocated
  "limit": 64,                                   // subscription ceiling
  "name": {
    "localizedValue": "H100_80G GPU Count",
    "value": "H100_80G.GlobalManagedCompute.global"
  },
  "acceleratorSku": "H100_80G",
  "offer": "GlobalManagedCompute",
  "location": "global",
  "unit": "GPUs"                                 // not tokens — physical accelerators
}
```

---

## Key Differences

| Dimension | Serverless (`usages`) | Managed Compute (`acceleratorUsages`) |
|---|---|---|
| **Query path** | `GET .../locations/global/usages` | `GET .../acceleratorUsages?acceleratorSku=H100_80G&offer=GlobalManagedCompute&location=global` |
| **Quota key** | `{provider}.{SKU}.{model}` — e.g. `AIServices.GlobalStandard.gpt-oss-120b` | `{acceleratorSku}.{offer}.{location}` — e.g. `H100_80G.GlobalManagedCompute.global` |
| **Scope** | Per model — deploying gpt-4o does not affect gpt-oss-120b quota | Per accelerator — deploying _any_ model on H100 consumes the same pool |
| **Unit** | TPM (thousands) | GPUs |
| **`currentValue` meaning** | TPM currently allocated to deployments of this model | GPUs currently allocated across all managed compute deployments on this accelerator type |
| **`limit` meaning** | Max TPM for this model in this subscription | Max GPUs of this accelerator type in this subscription |
| **What consumes quota** | `sku.capacity` (TPM) on serverless deployment create/scale | `sku.capacity × acceleratorsPerInstance` on managed compute deployment create/scale |
| **Quota check formula** | `requestedTPM ≤ limit - currentValue` | `(capacity × acceleratorsPerInstance) ≤ limit - currentValue` |

---

## REST API Comparison

### Serverless

```http
GET https://management.azure.com/subscriptions/{sub}/providers/Microsoft.CognitiveServices
    /locations/global/usages?api-version=2025-09-01
Authorization: Bearer {token}
```

Response (filtered to relevant model):

```jsonc
{
  "value": [
    {
      "currentValue": 0,
      "limit": 5000,
      "name": {
        "localizedValue": "Tokens Per Minute (thousands) - gpt-oss-120b",
        "value": "AIServices.GlobalStandard.gpt-oss-120b"
      },
      "scopeId": "global",
      "scopeType": "Regional",
      "unit": "Count"
    }
  ]
}
```

### Managed Compute

```http
GET https://management.azure.com/subscriptions/{sub}/providers/Microsoft.CognitiveServices
    /acceleratorUsages?api-version=2026-04-01-preview
    &acceleratorSku=H100_80G&offer=GlobalManagedCompute&location=global
Authorization: Bearer {token}
```

Response:

```jsonc
{
  "value": [
    {
      "currentValue": 0,
      "limit": 64,
      "name": {
        "localizedValue": "H100_80G GPU Count",
        "value": "H100_80G.GlobalManagedCompute.global"
      },
      "acceleratorSku": "H100_80G",
      "offer": "GlobalManagedCompute",
      "location": "global",
      "unit": "GPUs"
    }
  ]
}
```

---

## SDK / CLI Comparison

### Serverless

```python
usages = cog.usages.list(location="global")
for u in usages:
    if "gpt-oss-120b" in u.name.value and "GlobalStandard" in u.name.value:
        print(f"{u.name.value}: {u.current_value}/{u.limit} TPM")
        # AIServices.GlobalStandard.gpt-oss-120b: 0/5000 TPM
```

```bash
az cognitiveservices usage list --location global --subscription $SUB \
  --query "[?contains(name.value, 'gpt-oss-120b')]" -o table
```

### Managed Compute

```python
acc_usages = cog.accelerator_usages.list(
    accelerator_sku="H100_80G",
    offer="GlobalManagedCompute",
    location="global",
)
for u in acc_usages:
    print(f"{u.accelerator_sku} ({u.offer}): {u.current_value}/{u.limit} GPUs")
    # H100_80G (GlobalManagedCompute): 0/64 GPUs
```

```bash
az cognitiveservices accelerator usage list \
  --subscription $SUB --accelerator-sku H100_80G \
  --offer GlobalManagedCompute --location global -o table
```

---

## Why a Separate API

The existing `usages` API mixes model-specific and model-agnostic entries in the same response (e.g., `AIServices.GlobalStandard.gpt-oss-120b` is per-model while `OpenAI.GlobalProvisionedManaged` is a shared pool). Adding accelerator quota to this list would compound the inconsistency.

More importantly, the backing store is different:

| | Serverless (`usages`) | Managed Compute (`acceleratorUsages`) |
|---|---|---|
| **Backend** | Cog Services quota store | AzureML quota service |
| **Quota management** | Cog Services RP | Foundry RP proxies to AzureML |
| **Quota increase request** | Azure Portal → Cog Services | Azure Portal → AzureML (different workflow) |

A separate `acceleratorUsages` endpoint keeps the contract clean — serverless callers get exactly what they expect, and GPU callers get a purpose-built response backed by the AzureML quota service with the right fields (`acceleratorSku`, `offer`, GPU-denominated units).
