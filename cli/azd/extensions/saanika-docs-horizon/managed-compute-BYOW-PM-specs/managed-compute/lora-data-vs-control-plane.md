# LoRA Adapter Load/Unload: Data Plane vs Control Plane — Trade-off Analysis

> **Context:** The current spec ([spec-custom-models-lora-adapters.md](spec-custom-models-lora-adapters.md)) places adapter attach/detach as **data-plane** operations under `/deployments/{dep}/adapters`. This document evaluates that decision against the alternative of making them **control-plane** (ARM) operations, grounded in the actual platform architecture and capabilities.

---

## Background: How the Planes Are Split Today

| Plane | URL Pattern | Auth | Operations | Latency |
|---|---|---|---|---|
| **Control Plane (ARM)** | `management.azure.com/.../acceleratorDeployments/{name}` | Microsoft Entra ID (ARM scope) | Deployment CRUD (create, scale, delete). LRO — 10–15 min. | Minutes |
| **Data Plane** | `{account}.services.ai.azure.com/api/projects/{project}/...` | Microsoft Entra ID (data-plane scope) or API key | Model registration, inference, adapter ops | Seconds |

**Deployment creation** is a control-plane ARM resource (`Microsoft.CognitiveServices/accounts/acceleratorDeployments`). It provisions GPU infrastructure, pulls model weights, starts serving containers — a 10–15 min LRO.

**Model registration** is a data-plane operation (`PUT /models/{name}/versions/{version}`). It writes model metadata and uploads weights to project-scoped storage.

**Adapter load/unload** (as spec'd) is a data-plane operation (`POST /deployments/{dep}/adapters`). It hot-loads ~50 MB of delta weights into already-running GPU memory.

---

## Option A: Data Plane (Current Spec)

Adapter attach/detach lives at:
```
POST  {account}.services.ai.azure.com/api/projects/{project}/deployments/{dep}/adapters
DELETE {account}.services.ai.azure.com/api/projects/{project}/deployments/{dep}/adapters/{name}
```

### Pros

| # | Pro | Evidence / Rationale |
|---|---|---|
| **A1** | **Matches operational reality — seconds, not minutes.** Adapter attach takes seconds (load ~50 MB into VRAM). ARM LROs are designed for 10–15 min infrastructure provisioning. Wrapping a 3-second GPU memory operation in ARM's async polling machinery (`Azure-AsyncOperation` header, `Retry-After: 30`, final GET) would impose unnecessary overhead and confuse users who expect ARM LROs to indicate heavy infrastructure work. | Deployment create is 10–15 min LRO per [spec-deployments.md](spec-deployments.md#long-running-operations). Adapter attach is seconds per spec — weights are 10–500 MB, no container restart, no GPU allocation. |
| **A2** | **No infrastructure change.** Attach/detach modifies live GPU memory on existing instances — it does not provision VMs, allocate GPUs, or change networking. ARM resource operations should reflect infrastructure state changes. Adapter load is analogous to loading a configuration into a running service, not deploying new infrastructure. | Per spec: "Does attach create a new deployment? **No.** One deployment, N adapters as sub-resources." GPU cost is marginal (~50 MB VRAM per adapter). No new quota consumed. |
| **A3** | **Same auth boundary as inference and model registration.** Adapters are project-scoped data assets. The data plane already handles model CRUD (`/models/`) and inference (`/openai/deployments/`). Adapter load/unload operates on the same project-scoped resources with the same identity model. Users don't need ARM permissions to manage adapters — only data-plane access to the project. | Model registration uses data-plane auth per [spec-custom-models-deploy.md](spec-custom-models-deploy.md): `{account}.services.ai.azure.com/api/projects/{project}/models/`. Inference uses the same endpoint. |
| **A4** | **Enables API-key-based workflows.** Data-plane operations can be authenticated with API keys (from `accounts.list_keys`). This enables adapter management from notebooks, scripts, and CI/CD pipelines without requiring ARM service principal setup. Control-plane operations require Entra ID tokens scoped to `management.azure.com`. | Per [e2e-5-inference.md](e2e-5-inference.md): inference uses `api-key: {key}`. Per [spec-deployments.md](spec-deployments.md#authentication-and-authorization): ARM management ops require Entra tokens, API keys are for inference/data-plane only. |
| **A5** | **Separation of duties matches organizational roles.** Platform teams create deployments (ARM, control plane — requires `acceleratorDeployments/write`). ML engineers manage adapters and run inference (data plane — requires project-level access). This avoids giving every ML engineer ARM write access to GPU infrastructure. | Per [spec-deployments.md](spec-deployments.md#authorization-rbac): `Microsoft.CognitiveServices/accounts/acceleratorDeployments/write` controls deployment CRUD. Data-plane permissions are project-scoped and separate from ARM RBAC. |
| **A6** | **Consistent with industry pattern.** Fireworks AI, Together AI, and Anyscale all expose adapter management as API-level (data-plane-equivalent) operations, not infrastructure provisioning operations. vLLM and SGLang natively support hot-loading adapters via API calls to the running engine — the operation is inherently a runtime/data-plane action. | vLLM adapter loading is a runtime engine operation (sends adapter weights to the running process). SGLang similarly manages adapters at the engine level. The spec references `VLLM_ENABLE_LORA` and `SGLANG_ENABLE_LORA` as serving engine env vars. |
| **A7** | **Faster iteration and independent versioning.** Data-plane APIs version independently from ARM APIs (`api-version=2025-06-01-preview` vs ARM's `2026-04-01-preview`). Adapter operations can evolve (new fields, new statuses) without going through the ARM API review process, TypeSpec codegen pipeline, and multi-month SDK release cycles. | ARM API requires TypeSpec definition, ARM API review board approval, swagger generation, SDK codegen for every language. Data-plane APIs have shorter review cycles per the Foundry API governance model. |
| **A8** | **No ARM resource explosion.** With N adapters on M deployments, control-plane approach would create N×M ARM sub-resources (or N adapter ARM resources + M attachment join resources). ARM resource counts affect subscription limits, Azure Resource Graph query performance, and billing pipeline processing. Data-plane keeps adapters as lightweight runtime state. | ARM subscriptions have resource count limits. Per [spec-deployments.md](spec-deployments.md#resource-model): `Microsoft.CognitiveServices/accounts/acceleratorDeployments` is already a new ARM resource type. Adding `/adapters` as another ARM sub-resource compounds the problem. |

### Cons

| # | Con | Evidence / Rationale |
|---|---|---|
| **A9** | **No ARM RBAC for adapter operations.** You cannot create a custom role that grants "attach adapter to deployment X" without granting broader data-plane project access. ARM RBAC is per-resource-type and per-action (`read`/`write`/`delete`). Data-plane RBAC is coarser — typically project-level or endpoint-level. | Per [spec-deployments.md](spec-deployments.md#authorization-rbac): ARM enables granular per-resource-type permissions like `acceleratorDeployments/write`. Data-plane auth doesn't have an equivalent `deployments/adapters/write` permission in ARM RBAC. |
| **A10** | **Not visible in Azure Resource Graph (ARG).** ARM resources are queryable via ARG for compliance, inventory, and cost tracking. Data-plane adapter state is invisible to ARG queries, Azure Policy, and governance tools. If an org needs to enforce "no more than 5 adapters per deployment" via policy, they can't do it through ARM Policy. | ARM resources are indexed in Azure Resource Graph. Per [spec-deployments.md](spec-deployments.md): accelerator deployments are ARM resources visible in ARG. Adapters as data-plane state would not be. |
| **A11** | **No Azure Activity Log for attach/detach.** ARM operations automatically emit entries in the Azure Activity Log (Monitor → Activity Log). Data-plane operations require explicit diagnostic settings and custom logging to achieve equivalent auditability. | ARM operations are auto-logged in Activity Log with caller identity, timestamp, and operation result. Data-plane audit logging depends on the service implementing it explicitly. |
| **A12** | **No Bicep/Terraform/ARM template support.** Adapters cannot be declared in infrastructure-as-code (IaC). Users cannot define "deployment X with adapters Y, Z" in a Bicep file and deploy atomically. The adapter state becomes imperative (API calls) rather than declarative (IaC). | ARM resources can be defined in Bicep: `resource dep 'Microsoft.CognitiveServices/accounts/acceleratorDeployments@2026-04-01-preview'`. Data-plane operations have no Bicep equivalent — they require post-deployment scripts. |
| **A13** | **Split-plane workflow complexity.** A single BYOW+LoRA workflow spans three operations across two planes: (1) Register model — data plane, (2) Create deployment — control plane ARM, (3) Attach adapter — data plane. Users switch between two different endpoints, two auth mechanisms, and two SDK clients (`CognitiveServicesManagementClient` for ARM, `AIProjectClient` for data plane). | Per [spec-custom-models-deploy.md](spec-custom-models-deploy.md): "Model registration is a data-plane operation. Deployment creation is a control-plane operation. A single BYOW workflow spans both planes." Adding adapter attach as data plane maintains the existing split but adds a third step. |
| **A14** | **Consistency gap with deployment scaling.** Changing `sku.capacity` (instance count) is an ARM control-plane operation. But loading an adapter — which also changes the deployment's runtime behavior and memory footprint — is data-plane. The conceptual boundary blurs: both affect what a deployment does and how much resource it consumes. | Per [spec-compare-serverless-managed-compute.md](spec-compare-serverless-managed-compute.md): scaling is via ARM PUT with updated `sku.capacity`. Adapter load changes GPU memory utilization on the same deployment but lives on a different plane. |

---

## Option B: Control Plane (ARM)

Adapter attach/detach would be ARM sub-resource operations:
```
PUT    management.azure.com/.../acceleratorDeployments/{dep}/adapters/{name}
DELETE management.azure.com/.../acceleratorDeployments/{dep}/adapters/{name}
GET    management.azure.com/.../acceleratorDeployments/{dep}/adapters
```

### Pros

| # | Pro | Evidence / Rationale |
|---|---|---|
| **B1** | **Full ARM RBAC.** `Microsoft.CognitiveServices/accounts/acceleratorDeployments/adapters/write` enables fine-grained access control. Custom roles can grant "manage adapters" without granting "delete deployment." | Per [spec-deployments.md](spec-deployments.md#authorization-rbac): ARM supports per-action permissions. A proposed `Cognitive Services Accelerator Deployment Operator` role already exists — extending it to `/adapters` sub-resources is natural. |
| **B2** | **Azure Policy enforcement.** Organizations can create Azure Policy rules like "max 5 adapters per deployment" or "only allow adapters tagged with 'approved'." ARM Policy evaluates at resource creation time — no custom enforcement code needed. | Azure Policy evaluates ARM PUT operations natively. Data-plane operations require custom policy enforcement logic in the service backend. |
| **B3** | **Infrastructure-as-Code.** Adapters can be declared in Bicep/Terraform alongside the deployment, enabling full declarative deployment: "deploy model X with adapters A, B, C." Reproducible, version-controlled, CI/CD-friendly. | ARM resources are Bicep-native. Example: `resource adapter 'Microsoft.CognitiveServices/accounts/acceleratorDeployments/adapters@2026-04-01-preview'`. |
| **B4** | **Azure Activity Log and Resource Graph.** Attach/detach events appear in Activity Log automatically. Adapters are queryable via ARG for compliance and inventory. "Show me all loaded adapters across all deployments in subscription X" is a single ARG query. | ARM operations auto-emit Activity Log entries. ARM resources are indexed in ARG within minutes. |
| **B5** | **Single auth model for all deployment operations.** Users already use ARM auth (`CognitiveServicesManagementClient`) for deployment CRUD. Adapter management would use the same client, same token, same RBAC model. No context switching between planes. | Currently the E2E flow already requires both `CognitiveServicesManagementClient` (ARM) and `AIProjectClient` (data plane) per the E2E sample in the LoRA spec. |

### Cons

| # | Con | Evidence / Rationale |
|---|---|---|
| **B6** | **ARM LRO overhead for a seconds-scale operation.** ARM async operations include `Azure-AsyncOperation` polling, `Retry-After` headers (typically 30s), and final GET. A 3-second adapter load would still require polling machinery. ARM LROs are not designed for sub-minute operations — they're built for provisioning. | Per [spec-deployments.md](spec-deployments.md#long-running-operations): ARM LROs use 30-second `Retry-After`. The fastest ARM LRO roundtrip (even if the operation itself is instant) is constrained by the ARM RP contract. |
| **B7** | **ARM API velocity.** ARM APIs require TypeSpec definitions, ARM API review board approval, swagger generation, and multi-language SDK codegen. Adding fields, changing response shapes, or introducing new statuses (e.g., `"Warming"`) requires the full ARM release pipeline. Data-plane APIs iterate faster. | ARM API changes follow the Azure API review process. Data-plane APIs under `services.ai.azure.com` have independent versioning and lighter review cycles. |
| **B8** | **Forces ARM auth for ML practitioners.** Data scientists running notebooks typically have API keys or project-scoped Entra tokens. ARM auth requires `management.azure.com` scope, which many data-science environments don't configure. Requiring ARM auth for adapter load/unload raises the barrier for the primary user persona. | Per [e2e-5-inference.md](e2e-5-inference.md): inference uses API keys. Per [spec-deployments.md](spec-deployments.md#authentication): ARM requires Entra tokens with `management.azure.com/.default` scope. |
| **B9** | **ARM resource count scaling.** A deployment with 32 adapters (the max per the spec's `VLLM_MAX_LORAS` default) creates 32 ARM sub-resources. Across 10 deployments, that's 320 ARM resources for adapter state alone. ARM subscriptions have resource count limits and resource operations rate limits (1200 writes/5 min per subscription). Frequent adapter cycling (load/unload for A/B testing) could hit ARM throttling. | ARM throttling limits: 1200 writes per 5 minutes per subscription. vLLM supports up to 64 concurrent adapters. High-frequency adapter cycling (e.g., automated adapter evaluation pipelines) would create significant ARM write pressure. |
| **B10** | **Impedance mismatch with serving engine.** vLLM and SGLang manage adapters as in-memory runtime state, not as infrastructure resources. The ARM resource model implies durability — if the deployment restarts, ARM expects the sub-resources to still exist. But adapter load state is ephemeral (adapters must be re-loaded after container restart). This creates a consistency challenge: ARM says adapter exists, but GPU says it's not loaded. | vLLM/SGLang adapter state is in-process memory. Container restarts lose loaded adapters. ARM sub-resources would need reconciliation logic to re-load adapters on restart, adding complexity. |
| **B11** | **Conceptual misfit.** ARM resources represent infrastructure — VMs, NICs, storage accounts, deployments. An adapter is a ~50 MB memory overlay on a running process. Making it an ARM resource elevates ephemeral runtime state to the level of infrastructure provisioning, which confuses the resource model and sets a precedent for other runtime state (e.g., KV cache configuration, batch sizes) to become ARM resources. | Per [spec-deployments.md](spec-deployments.md): the rationale for `acceleratorDeployments` as a separate ARM type was "fundamentally different properties" and "separate ARM RBAC permissions." Adapters don't have fundamentally different infrastructure properties — they share the deployment's infrastructure. |
| **B12** | **Deployment template restart risk.** If ARM manages adapters as sub-resources, the ARM RP must orchestrate with the serving engine. A PUT to update an adapter sub-resource could potentially trigger the ARM RP's update path (which for deployments involves rolling updates, container restarts). Carefully scoping the ARM RP to avoid this is non-trivial. | Per [deployments_crud/create-or-update.md](deployments_crud/create-or-update.md): deployment updates (changing template, accelerator type) trigger redeployment. ARM RP update paths are designed for infrastructure changes. Adapter sub-resource updates would need to bypass this entirely. |

---

## Summary Matrix

| Criterion | Data Plane (Option A) | Control Plane (Option B) |
|---|---|---|
| **Operation latency** | Seconds (native) | Seconds (but ARM LRO overhead) |
| **Auth model** | API key + Entra (data plane) | Entra only (ARM scope) |
| **RBAC granularity** | Project-level | Per-resource-type + per-action |
| **Azure Policy** | Not supported | Supported |
| **IaC (Bicep/Terraform)** | Not supported | Supported |
| **Activity Log** | Requires explicit implementation | Automatic |
| **Resource Graph visibility** | Not visible | Visible |
| **API iteration speed** | Fast (data-plane versioning) | Slow (ARM review pipeline) |
| **Serving engine alignment** | Natural (runtime operation) | Impedance mismatch (ephemeral state as ARM resource) |
| **Resource count impact** | None (lightweight state) | 16–64 ARM sub-resources per deployment |
| **Persona fit (ML engineers)** | Natural (same plane as model reg + inference) | Requires ARM auth setup |
| **Industry alignment** | Consistent (Fireworks, Together, vLLM native) | No known precedent |
| **Org governance** | Weaker (no Policy, no ARG) | Stronger (Policy, ARG, Activity Log) |

---

## Recommendation

**Data Plane (Option A) is more feasable**, for three primary reasons:

1. **Operational nature.** Adapter load/unload is a runtime GPU memory operation — seconds-scale, no infrastructure change, ephemeral state. It is fundamentally different from deployment creation (which provisions VMs, allocates GPUs, and sets up networking). The ARM resource model is designed for durable infrastructure, not ephemeral runtime state.

2. **Persona alignment.** The primary users of adapter load/unload are ML engineers iterating on fine-tuned models. They work in notebooks, use API keys, and interact with the data plane. Requiring ARM auth and ARM RBAC for adapter management creates unnecessary friction for the core workflow.

3. **Serving engine reality.** vLLM and SGLang manage adapters as in-process memory state. There is no infrastructure to provision. Wrapping this in ARM creates an impedance mismatch that requires reconciliation logic (re-load adapters after restart, handle ARM-says-loaded-but-GPU-says-not discrepancies).

### Mitigations for Data-Plane Gaps

| Gap | Mitigation |
|---|---|
| **No ARM RBAC for adapters** | Implement data-plane RBAC at the project level (e.g., `projects/{project}/deployments/adapters/write` permission). Azure AI Foundry already has project-level role assignments. |
| **No Activity Log** | Emit adapter attach/detach events to Azure Monitor diagnostic logs. Configure via diagnostic settings on the Cognitive Services account. |
| **No IaC support** | Provide a post-deployment script pattern: Bicep deploys the base model, a `deploymentScript` resource calls the data-plane API to attach adapters. Document this as a recommended pattern. |
| **No Azure Policy** | Enforce adapter limits server-side in the data-plane API (max adapters per deployment is already enforced via `VLLM_MAX_LORAS`). For org-level policy, use Foundry project-level governance. |
| **No Resource Graph visibility** | Expose adapter state via the data-plane list API (`GET /deployments/{dep}/adapters`). For cross-deployment queries, use Index Service (already planned for model search). |
