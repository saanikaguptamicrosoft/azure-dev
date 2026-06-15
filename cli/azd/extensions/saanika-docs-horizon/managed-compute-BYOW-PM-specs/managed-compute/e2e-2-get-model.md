# Get Model Details

The Catalog API response from the discovery step is the starting point. From here, two downstream APIs provide deployment-specific details:

- **Serverless:** Cog Services Models API — SKUs, rate limits, PTU capacity (Section 2.2)
- **Managed compute:** ML Registry API — deployment templates, accelerator SKUs (Section 2.3)

The bridge between these APIs is analyzed at the end of this section.

## 2.1 Get Model from Catalog API

**REST (existing):**

```http
GET https://api.catalog.azureml.ms/asset-gallery/v1.0/azureml-openai-oss/models/gpt-oss-120b/version/4
```

```json
{
  "name": "gpt-oss-120B",
  "version": "4",
  "publisher": "OpenAI",
  "registryName": "azureml-openai-oss",
  "displayName": "gpt-oss-120b",
  "azureOffers": ["standard-paygo", "VM"],
  "assetId": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
  "inferenceTasks": ["chat-completion"],
  "modelCapabilities": ["agentsV2", "reasoning", "streaming"],
  "modelLimits": {
    "textLimits": {
      "inputContextWindow": 131072,
      "maxOutputTokens": 131072
    },
    "supportedLanguages": ["en"],
    "supportedInputModalities": ["text"],
    "supportedOutputModalities": ["text"]
  },
  "keywords": ["Reasoning", "Multilingual", "Coding"],
  "license": "custom",
  "deprecation": { "inferenceRetirementDate": null },
  "summary": "Push the open model frontier with GPT-OSS models, released under the permissive Apache 2.0 license, allowing anyone to use, modify, and deploy them freely."
}
```

**SDK (proposed):**

```python
model = catalog.get_model("azureml-openai-oss", "gpt-oss-120b", "4")
print(f"Context window: {model.model_limits.text_limits.input_context_window}")
print(f"Max output:     {model.model_limits.text_limits.max_output_tokens}")
print(f"Tasks:          {model.inference_tasks}")
print(f"Capabilities:   {model.model_capabilities}")
print(f"License:        {model.license}")
```

**CLI (proposed):**

```bash
az ai catalog model show --registry azureml-openai-oss --name gpt-oss-120b --version 4 -o json
```

## 2.2 Get Model from Cog Services Models API

The Cog Services Models API is the ARM-side view of the same model. It confirms the deployment SKUs (`GlobalStandard`, `GlobalProvisionedManaged`), minimum and step counts for PTU capacity, and per-SKU rate limits. All downstream deployment, quota, and capacity operations go through ARM using identifiers from this API.

Finding which **regions** support this model should ideally be answered by the Models API itself, but today that requires a separate call to the `modelCapacities` API — covered in the [Quota and Capacity](e2e-3-quota-capacity.md) section.

To look up this model, the URL format is `{format}.{name}.{version}`:

**REST (existing):**

```http
GET https://management.azure.com/subscriptions/{sub}/providers/Microsoft.CognitiveServices
    /locations/eastus/models/OpenAI-OSS.gpt-oss-120b.1?api-version=2025-04-01-preview
Authorization: Bearer {token}
```

```json
{
  "id": "/subscriptions/{sub}/providers/Microsoft.CognitiveServices/locations/EastUS/models/OpenAI-OSS.gpt-oss-120b.1",
  "type": "Microsoft.CognitiveServices/locations/models",
  "name": "OpenAI-OSS.gpt-oss-120b.1",
  "location": "EastUS",
  "kind": "AIServices",
  "skuName": "S0",
  "model": {
    "format": "OpenAI-OSS",
    "name": "gpt-oss-120b",
    "version": "1",
    "isDefaultVersion": false,
    "skus": [
      {
        "name": "GlobalStandard",
        "usageName": "AIServices.GlobalStandard.gpt-oss-120b",
        "capacity": {
          "maximum": 1000000,
          "default": 500
        },
        "deprecationDate": "2099-12-31T00:00:00Z",
        "rateLimits": [
          { "key": "request", "renewalPeriod": 60, "count": 1 },
          { "key": "token", "renewalPeriod": 60, "count": 1000 }
        ]
      }
    ],
    "maxCapacity": 3,
    "capabilities": {
      "chatCompletion": "true",
      "agentsV2": "true"
    },
    "deprecation": { "inference": "2099-12-31T00:00:00Z" },
    "lifecycleStatus": "Stable",
    "systemData": {
      "createdBy": "Microsoft",
      "createdAt": "2025-08-05T00:00:00Z",
      "lastModifiedBy": "MaaSModelConverter",
      "lastModifiedAt": "2025-08-05T00:00:00Z"
    }
  }
}
```

> Second entry is identical with `"kind": "MaaS"`.

## 2.3 Get Model from ML Registry API

The ML Registry API provides the deployment template and supported Foundry Accelerator SKUs for managed compute. The Catalog's `assetId` maps directly to this API.

**REST (existing):**
```http
GET https://cert-eastus.experiments.azureml.net/mferp/managementfrontend
    /subscriptions/d4d34678-c0d7-4d69-a257-366e3cb4a7d8
    /resourceGroups/registry-builtin-openai-oss-eastus
    /providers/Microsoft.MachineLearningServices
    /registries/azureml-openai-oss/models/gpt-oss-120B/versions/4
    ?api-version=2021-10-01-dataplanepreview
Authorization: Bearer {token}
```

> The registry's subscription and resource group are not the user's — they belong to the registry itself. Discover them via `GET https://eastus.api.azureml.ms/registrymanagement/v1.0/registries/azureml-openai-oss/discovery`.

**CLI (existing):**
```bash
az ml model show -n gpt-oss-120B -v 4 --registry-name azureml-openai-oss
```

**Output (proposed):**

```json
{
  "name": "gpt-oss-120b",
  "version": "4",
  "id": "azureml://registries/azureml-openai-oss/models/gpt-oss-120b/versions/4",
  "type": "custom_model",
  "default_deployment_template": {                                             
    "assetId": "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1"
  },
  "allowed_deployment_templates": {                                             
    "assetIds": [
      "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-short-context/versions/1",
      "azureml://registries/azureml-openai-oss/deploymenttemplates/gpt-oss-120b-long-context/versions/1"
    ]
  },
  "properties": {
    "SharedComputeCapacityEnabled": "True",
  },
  "tags": {
    "isDirectFromAzure": "true",
  }
}
```

**SDK (existing):**
```python
from azure.ai.ml import MLClient

ml_client_registry = MLClient(
    credential, subscription_id,
    resource_group="rg1",
    registry_name="azureml-openai-oss",
)
model = ml_client_registry.models.get(name="gpt-oss-120B", version="4")

```

> **Note:** The deployment template is only needed for managed compute (GlobalManagedCompute). Serverless deployments (GlobalStandard) use `(format, name, version)` from the Cog Services Models API and do not require a template.

### Fetch the Deployment Template

Once the model object surfaces `allowed_deployment_templates`, the user can fetch the deployment template to see the full infrastructure specification — container image, serving routes, allowed VM SKUs, and accelerator mappings.

**REST (existing):**
```http
GET https://cert-eastus.experiments.azureml.net/genericasset/v2.0
    /subscriptions/d4d34678-c0d7-4d69-a257-366e3cb4a7d8
    /resourceGroups/registry-builtin-openai-oss-eastus
    /providers/Microsoft.MachineLearningServices
    /registries/azureml-openai-oss
    /deploymenttemplates/gpt-oss-120b-short-context/versions/1
    ?api-version=2024-04-01-preview
Authorization: Bearer {token}
```

**CLI (existing — preview):**
```bash
az ml deployment-template show \
    -n gpt-oss-120b-short-context -v 1 \
    --registry-name azureml-openai-oss
```

**SDK (proposed):**
```python
from azure.ai.ml import MLClient

ml_client_registry = MLClient(
    credential, subscription_id,
    resource_group="rg1",
    registry_name="azureml-openai-oss",
)
template = ml_client_registry.deployment_templates.get(
    name="gpt-oss-120b-short-context", version="1"
)
for am in template.accelerator_maps:
    print(f"{am.accelerator_type}: {am.number_of_accelerators_per_model_instance} GPUs"
          f"{' (default)' if am.default else ''}")
```

> Today `az ml deployment-template list --registry-name azureml-openai-oss` returns `[]` — no templates have been published for this registry yet. The output below is **fabricated** based on the deployment template spec and adapted for `gpt-oss-120B`.

**Deployment template output (fabricated):**
```json
{
  "name": "gpt-oss-120b-short-context",
  "version": "1",
  "type": "deploymenttemplates",
  "description": "vLLM serving template for gpt-oss-120B — short context (32k), 4× H100 tensor-parallel",
  "deploymentTemplateType": "Managed",
  "environmentId": "azureml://registries/azureml-openai-oss/environments/gpt-oss-env/versions/3",
  "environmentVariables": {
    "MODEL_NAME": "gpt-oss-120b",
    "TENSOR_PARALLEL_SIZE": "4",
    "MAX_MODEL_LEN": "131072",
    "ENABLE_INFERENCESERVER_DIAGNOSTICSLOG_AML_VISIBILITY": "true"
  },
  "requestSettings": {
    "requestTimeout": "00:02:00",
    "maxConcurrentRequestsPerInstance": 64
  },
  "scoringPath": "/v1/chat/completions",
  "scoringPort": 8000,
  "livenessProbe": {
    "initialDelay": "00:10:00",
    "period": "00:00:10",
    "timeout": "00:00:02",
    "failureThreshold": 30,
    "successThreshold": 1,
    "scheme": "http",
    "httpMethod": "GET",
    "path": "/health",
    "port": 8000
  },
  "readinessProbe": {
    "initialDelay": "00:10:00",
    "period": "00:00:10",
    "timeout": "00:00:02",
    "failureThreshold": 30,
    "successThreshold": 1,
    "scheme": "http",
    "httpMethod": "GET",
    "path": "/health",
    "port": 8000
  },
  "modelMountPath": "/var/azureml-app/azureml-models",
  "allowedInstanceType": [
    "Standard_ND96isr_H100_v5",
    "Standard_ND96isr_H200_v5"
  ],
  "defaultInstanceType": "Standard_ND96isr_H100_v5",
  "instanceCount": 1,
  "accelerator_maps": [
    {
      "accelerator_type": "H100_80GB",
      "number_of_accelerators_per_model_instance": 4,
      "default": true
    },
    {
      "accelerator_type": "H200_141GB",
      "number_of_accelerators_per_model_instance": 2
    }
  ]
}
```

**What this template provides:**

- **`allowedInstanceType` / `defaultInstanceType`** — VM SKU names (`Standard_ND96isr_H100_v5`). This is the AzureML-era field, retained for backward compatibility with existing `az ml online-deployment create` flows that take `instance_type`.
- **`accelerator_maps`** (proposed) — A list of `AcceleratorMap` entries, each specifying an accelerator type (e.g., `H100_80GB`, `H200_141GB`) and how many accelerators the model needs per instance. The `default: true` entry is used when the user doesn't specify a preference. This is the bridge to GlobalManagedCompute quota and capacity APIs — it tells the user exactly which accelerator type and count to request. VM SKU names (`Standard_ND96isr_H100_v5`) are not exposed here; the RP maps accelerator types to VM SKUs internally.
- **`environmentId`** — Points to the container image (vLLM serving runtime). Today this same value is buried in `tags.inference_environment_asset_id` as an unstructured string.
- **Serving routes** — `scoringPath`, `livenessProbe`, `readinessProbe` define the vLLM HTTP interface. Today these are hardcoded in the RP; with the template, they are declarative and visible to the user.

## Side-by-Side: Catalog API vs. Cog Services Models API

| Field | Catalog API | Cog Services Models API |
| --- | --- | --- |
| **Name** | `gpt-oss-120B` | `gpt-oss-120b` (format: `OpenAI-OSS`) |
| **Publisher** | `OpenAI` | N/A (no publisher field) |
| **Version** | `4` (latest; v1–v4 exist) | `1` only |
| **Offers / SKUs** | `["standard-paygo", "VM"]` | `["GlobalStandard"]` |
| **Context window** | 131072 | N/A (not in Models API) |
| **Max output tokens** | 131072 | N/A |
| **Capabilities** | `agentsV2, reasoning, streaming` | `chatCompletion: "true", agentsV2: "true"` |
| **License** | `custom` (Apache 2.0) | N/A |
| **Deprecation** | `null` | `2099-12-31` |
| **Lifecycle status** | N/A | `Stable` |
| **Rate limits** | N/A | 1 req/min, 1000 tok/min |
| **Location-scoped** | No (global catalog) | Yes (`eastus` shown) |
| **Duplicate entries** | No | Yes (`AIServices` + `MaaS` kind) |

## Bridge analysis: Catalog → downstream APIs

The Catalog API must bridge to two downstream systems depending on deployment type:

| Path | Bridge mechanism | Status |
| --- | --- | --- |
| Catalog → Cog Services Models API (serverless) | Requires `format`, matching version, matching offer names | **Broken** — 3 gaps (see below) |
| Catalog → ML Registry API (managed compute) | `assetId` from Catalog maps directly to ML Registry model | **Works** — `assetId` is a well-formed URI |
| ML Registry model → Deployment template | `allowed_deployment_templates.assetIds` → `az ml deployment-template show` | **Proposed** — field not populated today |

### Serverless bridge gaps (Catalog → Cog Services Models API)

1. **`format` is missing from the Catalog API.** The Models API namespaces every model as `{format}.{name}.{version}` — for this model, `OpenAI-OSS.gpt-oss-120b.1`. But the Catalog API does not return `format` (here `OpenAI-OSS`) anywhere in its response. The user has no programmatic way to construct the Models API URL from the Catalog response.

2. **Version mismatch.** The Catalog returns version `4` as the latest. The Models API only has version `1`. If the user takes version `4` from the Catalog and plugs it into the Models API URL (`OpenAI-OSS.gpt-oss-120b.4`), the call will 404. There is no mapping between Catalog versions and ARM versions.

3. **Offer names don't match.** The Catalog says `standard-paygo` and `VM`. The Models API says `GlobalStandard`. There is no translation table exposed by either API.

### Managed compute bridge gaps (Catalog → ML Registry → Deployment Template)

The Catalog → ML Registry bridge works via `assetId`. The remaining gap: the ML Registry model must populate `default_deployment_template` and `allowed_deployment_templates` (today empty/absent). Once populated, the full chain becomes:

```
Catalog (assetId) → ML Registry model (allowed_deployment_templates) → deployment template (accelerator types, env, probes)
```

The deployment template's `accelerator_maps` (proposed) lists the supported accelerator types (e.g., `H100_80GB`, `H200_141GB`) and how many each model instance needs. This bridges directly to the GlobalManagedCompute quota and capacity APIs.

Discovery is done. **From here, the paths diverge.**
