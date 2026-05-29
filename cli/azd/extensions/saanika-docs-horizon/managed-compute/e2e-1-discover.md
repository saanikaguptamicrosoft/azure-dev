# Discover the Model

The user searches the catalog for "gpt-oss-120b" by name. They don't know the registry, version, or asset ID yet — just the model name.

## REST (existing)

```bash
curl -s 'https://api.catalog.azureml.ms/asset-gallery/v1.0/models' \
  -H 'Content-Type: application/json' \
  -d '{
    "filters": [
      {"field": "name", "operator": "contains", "values": ["gpt-oss-120b"]},
      {"field": "labels", "operator": "eq", "values": ["latest"]}
    ],
    "order": [{"field": "Popularity", "direction": "Desc"}],
    "pageSize": 5
  }'
```

**Output:**

```json
{
  "totalCount": 7,
  "summaries": [
    {
      "popularity": 71.28,
      "name": "FW-GPT-OSS-120B",
      "version": "1",
      "publisher": "Fireworks",
      "registryName": "azureml-fireworks",
      "azureOffers": null,
      "assetId": "azureml://registries/azureml-fireworks/models/FW-GPT-OSS-120B/versions/1",
      "inferenceTasks": ["chat-completion"]
    },
    {
      "popularity": 68.95,
      "name": "gpt-oss-120B",
      "version": "4",
      "publisher": "OpenAI",
      "registryName": "azureml-openai-oss",
      "azureOffers": ["standard-paygo", "VM"],
      "assetId": "azureml://registries/azureml-openai-oss/models/gpt-oss-120B/versions/4",
      "inferenceTasks": ["chat-completion"]
    },
    {
      "popularity": 68.6,
      "name": "gpt-oss-safeguard-120b",
      "version": "5",
      "publisher": "OpenAI",
      "registryName": "azureml-openai-oss",
      "azureOffers": ["VM"],
      "assetId": "azureml://registries/azureml-openai-oss/models/gpt-oss-safeguard-120b/versions/5",
      "inferenceTasks": ["chat-completion"]
    },
    {
      "popularity": 1.0,
      "name": "openai-gpt-oss-120b",
      "version": "2",
      "publisher": "Hugging Face",
      "registryName": "HuggingFace",
      "azureOffers": ["VM"],
      "assetId": "azureml://registries/HuggingFace/models/openai-gpt-oss-120b/versions/2",
      "inferenceTasks": ["chat-completion"]
    },
    {
      "popularity": 1.0,
      "name": "openai-gpt-oss-safeguard-120b",
      "version": "1",
      "publisher": "Hugging Face",
      "registryName": "HuggingFace",
      "azureOffers": ["VM"],
      "assetId": "azureml://registries/HuggingFace/models/openai-gpt-oss-safeguard-120b/versions/1",
      "inferenceTasks": ["chat-completion"]
    }
  ]
}
```

**Catalog API fixes required for this sample** (see discovery.md Section 3 for exhaustive set of Catalog API issues):

- **`"standard-paygo"` → `"GlobalStandard"`**: Offer names are out of sync with the Models API. `standard-paygo` conflates `Standard`, `GlobalStandard`, and `DataZoneStandard` — it needs to be renamed to match ARM SKU names.
- **Fireworks variant has `null` offers**: `FW-GPT-OSS-120B` shows `azureOffers: null`, but the model apparently supports `GlobalStandard` and `GlobalProvisionedManaged` deployments. The catalog entry is not tagged properly.
- **`"VM"` → `"GlobalManagedCompute"`**: `VM` refers to current AzureML managed compute deployments. We will introduce `GlobalManagedCompute` as the offer name when these models are supported in the Foundry RP.

> **Note:** The UI has a "Direct from Azure" collection filter. We tested `"collections": ["directFromAzure"]` and `"collection": "directFromAzure"` as top-level request fields — both were silently ignored (still returned all 7 results). Using `collections` as a filter field returns a `UserError` — it is not in the list of valid filter fields. A `Publisher ne` filter can be used as a workaround.

## SDK (proposed)

```python
from azure.ai.catalog import CatalogClient

catalog = CatalogClient()  # anonymous — no credential

models = catalog.list_models(
    filters=[
        {"field": "name", "operator": "contains", "values": ["gpt-oss-120b"]},
        {"field": "labels", "operator": "eq", "values": ["latest"]},
    ],
    order_by="popularity",
    page_size=5,
)
for model in models:
    print(f"{model.name:30s} v{model.version:5s} pub={model.publisher:15s} "
          f"reg={model.registry_name:25s} offers={model.azure_offers}")
```

## CLI (proposed)

```bash
az ai catalog model list --filter "name contains gpt-oss-120b" --labels latest --page-size 5 -o table
```
