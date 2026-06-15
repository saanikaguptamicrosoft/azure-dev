Original link: https://github.com/jayantjha/agent-first-sdk/tree/jayant/compute/specs/compute

# Spec Template

- TypeSpec: [Foundry Compute - Typespec](https://github.com/Azure/azure-rest-api-specs/pull/41101)
- PM One Pager: [Compute for Foundry Horizon - 1 Pager](https://microsoft.sharepoint.com/:w:/t/91AzureAIPM/IQD1QfsZxPFvTIxabWAp8wc2AbiuEp8G1jTxmnKaME5qCXQ?e=cflOrg)
- Tech Spec: Implementation spec (detailed engineering spec)
- TypeSpec owners: Nupur Kinger, Jayesh Tanna, Jayant Kumar
- Service Owners: Rajat Garg
- SDK Owners: Nupur Kinger, Jayesh Tanna

## What is the goal of this feature?

We want users to be able to create and manage compute clusters within a Cognitive Services Foundry account so they can run custom training jobs (e.g., BYOM workflows) without leaving the Foundry ecosystem. Success means users can create compute, run training, and deploy custom models entirely within the Foundry ecosystem.

## What is the problem being solved?

Foundry Horizon integrates Azure Machine Learning capabilities into AI Foundry, enabling end-to-end training, fine-tuning, serving, and evaluation from a single control plane. Instead of managing VMs, customers select a GPU class and attach Foundry Compute Quota to access serverless, managed dedicated, or secure AKS compute. Workloads run elastically across Microsoft's GPU fleet and customer-owned GPUs, with automatic job placement optimization for utilization and cost efficiency.

## Requirements

After we deliver the compute management feature, users will be able to:
1. Create a compute cluster under a Cognitive Services account (PUT)
2. Get details of an existing compute cluster (GET)
3. List all compute clusters in an account (LIST)
4. Update a compute cluster (PATCH) — e.g., modify tags or scale pools
5. Delete a compute cluster (DELETE)
6. Reference the created compute cluster when submitting custom training jobs

### Resource model

- **Compute** is a child resource of **Account** at path:
  `subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.CognitiveServices/accounts/{accountName}/computes/{computeName}`
- Each compute contains one or more **Pools**, each specifying a VM instance type, node count, and priority (Regular or Spot)
- All create/update/delete operations are async

### Payload summary

| Model | Fields |
|-------|--------|
| **ComputeProperties** | `pools` (required), `subnetArmId`, `provisioningState` (read-only), `errors` (read-only), `creationTime` (read-only) |
| **Pool** | `name` (required), `instanceType` (required), `nodeCount` (required), `vmPriority` (optional: Regular/Spot) |
| **ComputeProvisioningState** | Accepted, Succeeded, Failed, Canceled, Deleting, Scaling, Disabled |

## E2E Code Samples

### Scenario: Create compute, submit training job, monitor job

```python
from azure.identity import DefaultAzureCredential
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
from azure.ai.projects import AIProjectClient

credential = DefaultAzureCredential()
subscription_id = "<subscription-id>"
resource_group = "<resource-group>"
account_name = "<account-name>"
project_endpoint = "<project-endpoint>"

# Management client — used for compute create, update, delete
cogsvc_client = CognitiveServicesManagementClient(credential, subscription_id)

# Project client — used for job operations, and also supports compute list/get
project_client = AIProjectClient(endpoint=project_endpoint, credential=credential)

# ---------------------------------------------------------------------------
# Step 1: Create a compute cluster (management client)
# ---------------------------------------------------------------------------
compute_name = "my-training-cluster"
compute = {
    "properties": {
        "location": "eastus2",
        "pools": [
            {
                "name": "gpu-pool",
                "instanceType": "Standard_NC6s_v3",
                "nodeCount": 2,
                "vmPriority": "Regular",
            }
        ]
    }
}

compute_poller = cogsvc_client.computes.begin_create_or_update(
    resource_group_name=resource_group,
    account_name=account_name,
    compute_name=compute_name,
    resource=compute,
)
compute_result = compute_poller.result()  # blocks until provisioning completes
print(f"Compute state: {compute_result.properties.provisioning_state}")

# ---------------------------------------------------------------------------
# Step 2: Submit a training job on the compute (project client)
# ---------------------------------------------------------------------------
job_poller = project_client.jobs.begin_create(
    job_name="my-training-job",
    body={
        "properties": {
            "computeName": compute_name,
            "commandToRun": ["-c", "echo 'Training started'; sleep 30; echo 'done'"],
        }
    },
)
job_result = job_poller.result()  # blocks until job completes
print(f"Job finished with status: {job_result.properties.status}")

# ---------------------------------------------------------------------------
# Step 3 (optional): Cleanup — delete the compute cluster (management client)
# ---------------------------------------------------------------------------
compute_poller = cogsvc_client.computes.begin_delete(
    resource_group_name=resource_group,
    account_name=account_name,
    compute_name=compute_name,
)
compute_poller.result()
print("Compute deleted")
```

## CRUD APIs

### Create or update a compute

**REST API**
```
PUT https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.CognitiveServices/accounts/{accountName}/computes/{computeName}?api-version=2025-10-01-preview
```

Request body:
```json
{
  "properties": {
    "location": "eastus2",
    "pools": [
      {
        "name": "gpu-pool",
        "instanceType": "Standard_NC6s_v3",
        "nodeCount": 4,
        "vmPriority": "Regular"
      }
    ],
    "subnetArmId": "/subscriptions/.../subnets/default"
  },
  "tags": {
    "environment": "training"
  }
}
```

**SDK**
```python
poller = cogsvc_client.computes.begin_create_or_update(
    resource_group_name="my-rg",
    account_name="my-account",
    compute_name="my-cluster",
    resource={
        "properties": { "location": "eastus2",
            "pools": [{"name": "pool1", "instanceType": "Standard_NC6s_v3", "nodeCount": 4, "vmPriority": "Regular"}]
        }
    },
)
result = poller.result()
```

### Get a compute

**REST API**
```
GET https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.CognitiveServices/accounts/{accountName}/computes/{computeName}?api-version=2025-10-01-preview
```

**SDK**
```python
compute = cogsvc_client.computes.get(
    resource_group_name="my-rg",
    account_name="my-account",
    compute_name="my-cluster",
)
print(compute.properties.provisioning_state)
```

### List computes

**REST API**
```
GET https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.CognitiveServices/accounts/{accountName}/computes?api-version=2025-10-01-preview
```

**SDK**
```python
computes = cogsvc_client.computes.list(
    resource_group_name="my-rg",
    account_name="my-account",
)
for c in computes:
    print(f"{c.name}: {c.properties.provisioning_state}")
```

### Delete a compute

**REST API**
```
DELETE https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.CognitiveServices/accounts/{accountName}/computes/{computeName}?api-version=2025-10-01-preview
```

**SDK**
```python
poller = cogsvc_client.computes.begin_delete(
    resource_group_name="my-rg",
    account_name="my-account",
    compute_name="my-cluster",
)
poller.result()
```
