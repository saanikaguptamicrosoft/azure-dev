# Artifact Classification — `artifactProfile`

> **Parent spec:** [spec-models-deploy.md](spec-models-deploy.md)
> **Applies to:** All model types registered through the `/models/` API 

## Overview

When a user registers a model, the platform inspects the uploaded artifact files and produces a **artifact profile** — a service-computed classification that describes what the artifact package contains and what loading it requires. This classification drives downstream behavior: deployment infrastructure routing, governance policy, and (when relevant) user-facing messaging.

This spec defines:

1. The `artifactProfile` field on the model object
2. When and how classification happens
3. The `runtimeHandling` field on the deployment response
4. Validation and early-feedback behavior
5. User experience across portal, CLI, SDK, and error messaging
6. Governance and policy integration
7. Extensibility contract

### Problem Statement

Not all model artifacts are equal. A safetensors file is static data — the platform reads bytes into memory with no code execution. A pickle `.bin` file embeds Python objects that execute during deserialization. A model with custom `.py` files requires importing and running user-authored code.

Today, the platform has no way to express this distinction. Users upload artifacts, and the platform either runs everything in the same path or rejects formats outright. This creates two bad outcomes:

- **Late failure:** User uploads a model, registration succeeds, but deployment fails hours later because the backend cannot safely load the artifact type.
- **Blanket restriction:** Platform blocks all non-safetensors formats, even when the infrastructure could handle them safely in an isolated path.

artifact profile solves this by classifying artifacts at registration time and routing deployment infrastructure accordingly.

### Key Design Decisions

- **Service-computed, not user-declared.** The platform sets `artifactProfile` based on artifact inspection. Users cannot write to it. This prevents gaming and ensures classification accuracy.
- **Classification at registration, enforcement at deployment.** The model record carries the classification. The deployment response carries the infrastructure decision. These are separate because the same model may get different handling on different backends, SKUs, or policy environments.
- **Early feedback, not early blocking.** Classification happens synchronously at registration commit. The user sees the result immediately on the model object. But registration does not fail based on classification — a `RuntimeDependent` model still registers successfully. Blocking happens at deployment time if no compatible infrastructure path exists.
- **Build: only `DataOnly` models are deployable.** For Build, no isolated runtime path exists. Only models classified as `DataOnly` (safetensors weights, JSON configs, tokenizer files — no pickle, no custom code, no native binaries) can be deployed. Models classified as `RuntimeDependent` or `Unknown` can be registered and stored but **deployment will be rejected**. This is a platform constraint, not a policy choice — the managed compute infrastructure does not support sandboxed execution at Build.
- **Invisible when irrelevant.** Most users uploading safetensors models never need to see or think about this field. It becomes visible only when the classification causes a deployment-time difference (cost, latency, path restriction) or when a governance policy references it.

### Terminology

| Term | Definition |
|---|---|
| **artifact profile** | Service-computed metadata on the model object describing what the artifact package contains and what loading it requires. |
| **Category** | Summary classification: `DataOnly`, `RuntimeDependent`, or `Unknown`. |
| **Signal** | A specific technical finding (e.g., `pickleDeserialization`) that contributed to the category. |
| **Runtime handling** | The infrastructure decision made at deployment time: `Standard` or `Isolated`. Appears on the deployment response, not the model. |

---

## Part 1: Schema

### Model Object — `artifactProfile`

Added to the model object returned by `GET /models/{name}/versions/{version}`. Present on all model types (`FullWeight`, `LoRA`, `DraftModel`).

| Property | Type | Required | Writable | Description |
|---|---|---|---|---|
| `artifactProfile` | `object` | — | **No** (read-only, service-computed) | Artifact classification metadata. Populated after upload commit. |
| `artifactProfile.category` | `string` (enum) | — | No | Summary classification. See values below. |
| `artifactProfile.signals` | `string[]` | — | No | Technical findings that contributed to the category. Empty array when `DataOnly`. |

#### `category` Values

| Value | Meaning | When assigned |
|---|---|---|
| `DataOnly` | Every file in the artifact package is static data. Weight tensors in safe formats (safetensors, GGUF), JSON configs, tokenizer files. The platform can load all files by reading bytes into memory. No code executes during loading. | All weight files are `.safetensors` or other known-safe formats, and no `.py`, `.so`, `.dll`, `.pkl`, or pickle-based files are present. |
| `RuntimeDependent` | At least one file requires the platform to execute or interpret code to load the model. The artifact is not malicious by definition, but loading is not side-effect-free. | Any signal is detected: pickle files, Python source, native binaries, dynamic ops markers. |
| `Unknown` | The service cannot confidently classify the artifact package. Files are present that the classifier does not recognize. | Unrecognized file types, corrupted manifests, or packaging layouts the classifier has no rule for. |

#### `signals` Values

Signals are additive-only. New signals can be introduced in future API versions without breaking existing consumers. An empty array `[]` means no signals were detected (the model is `DataOnly`).

| Signal | Meaning | Trigger |
|---|---|---|
| `pickleDeserialization` | Artifact contains files that use Python's pickle serialization protocol. Pickle can embed arbitrary Python objects that execute during deserialization. | `.bin`, `.pkl`, `.pt`, `.pth` files detected. |
| `customPythonCode` | Artifact contains Python source files that must be imported to construct the model architecture. | `.py` files detected (e.g., `modeling_*.py`, `configuration_*.py`, `tokenization_*.py`). |
| `dynamicOps` | Model uses operations resolved at runtime rather than statically defined in a standard framework graph. | Custom CUDA kernels, `torch.autograd.Function` subclasses, `eval()`/`exec()` patterns detected in code or config. |
| `nativeBinary` | Artifact contains compiled shared libraries or WebAssembly modules. Opaque machine code that must be loaded into process memory. | `.so`, `.dll`, `.dylib`, `.wasm` files detected. |
| `unknownFormat` | Artifact contains files the classifier does not recognize. Not inherently dangerous, but the platform cannot prove they are inert. | Unrecognized file extensions or structures. |

### Deployment Response — `runtimeHandling`

Added to the deployment response from the control-plane accelerator deployment API. **Not present on the model object.**

| Property | Type | Description |
|---|---|---|
| `runtimeHandling` | `string` (enum) | How the platform is running this model. |

| Value | Meaning |
|---|---|
| `Standard` | Model is loaded in the normal managed compute path. No additional isolation. |
| `Isolated` | Model is loaded in a sandboxed or restricted execution environment. The specific isolation mechanism is an implementation detail. |

`runtimeHandling` lives on the deployment, not the model, because:
- The same model may get `Standard` on one backend and `Isolated` on another.
- The handling depends on the deployment target, SKU, backend capabilities, and organization policy — not just the artifact contents.
- The model record describes **what the artifact is**. The deployment response describes **what the platform did about it**.

---

## Part 2: When Classification Happens

### Timing: Synchronous at Registration Commit

Classification runs **synchronously** as part of the registration commit step (`PUT /models/{name}/versions/{version}`). It does not run during upload (`startPendingUpload`) or as a background job.

**Why synchronous:**
- If async, every model starts as `Unknown` until the background job completes. Users learn to ignore the field.
- Classification is a file-extension and header scan, not a deep static analysis. It completes in milliseconds for typical artifacts.
- The user gets the classification result in the same response that confirms registration.

**Sequence:**

```
1. User uploads artifacts (startPendingUpload → azcopy → blob storage)
2. User calls PUT /models/{name}/versions/{version} (commit registration)
3. Service validates required fields (baseModel, weightType, etc.)
4. Service scans uploaded artifacts:
   a. Enumerate all files by extension
   b. For known-risky extensions (.bin, .pkl, .pt, .py, .so), record signals
   c. For unrecognized extensions, record unknownFormat
   d. If no signals: category = DataOnly
   e. If any signal: category = RuntimeDependent
   f. If only unknownFormat: category = Unknown
5. Service writes artifactProfile to model record
6. Service returns 201 Created with artifactProfile in response body
```

For non-local ingestion paths (HF import, training job output, Azure Storage URI), classification runs after the service finishes pulling/copying the artifacts and before the model transitions to `Succeeded`.

### What Classification Does NOT Do

- **Does not block registration.** A `RuntimeDependent` model registers successfully. A model with `Unknown` classification registers successfully. Classification is metadata, not a gate.
- **Does not deeply analyze file contents.** Classification is extension-based and header-based. It does not decompile pickle files, run static analysis on Python code, or disassemble native binaries. Deep analysis may be added as future signals.
- **Does not persist scan artifacts.** The classification produces `category` and `signals` only. No intermediate scan logs, reports, or file-level results are stored on the model object.

---

## Part 3: Validation and Early Feedback

### The Experience Problem to Solve

Anthony identified the worst-case journey:

> User uploads model → registration succeeds → user tries to deploy → deployment fails because artifact type is incompatible with available infrastructure.

This is a late failure. The user invested time uploading (potentially GBs of data), only to discover the problem at deployment time. artifact profile solves this by giving the user the classification immediately at registration — but the question is **what the platform does with it**.

### Design: Inform Early, Enforce at Deployment

Registration always succeeds regardless of classification. But the registration response includes actionable information when the classification may cause downstream issues.

#### Registration Response — Advisory Warning

When the model is classified as `RuntimeDependent` or `Unknown`, the response includes an advisory `warning` alongside the `artifactProfile`:

**Response (201 Created) — RuntimeDependent example:**

```json
{
  "name": "my-fine-tuned-model",
  "version": "1",
  "weightType": "FullWeight",
  "baseModel": "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
  "artifactProfile": {
    "category": "RuntimeDependent",
    "signals": ["pickleDeserialization"]
  },
  "warnings": [
    {
      "code": "RuntimeDependentArtifact",
      "message": "This model contains artifacts that require code execution during loading (pickle deserialization). Only SafeTensors-based models (artifact profile 'DataOnly') are currently deployable. Convert to SafeTensors format before deploying."
    }
  ]
}
```

**Response (201 Created) — DataOnly example:**

```json
{
  "name": "my-lora-adapter",
  "version": "1",
  "weightType": "LoRA",
  "baseModel": "azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2",
  "artifactProfile": {
    "category": "DataOnly",
    "signals": []
  }
}
```

No `warnings` array when the model is `DataOnly`. The field is simply absent.

#### Deployment — Hard Enforcement

At deployment time, the RP reads `artifactProfile.category` to decide whether deployment is allowed:

> **Build constraint:** Only `DataOnly` models can be deployed. No isolated runtime path is available at Build. The `Isolated` handling mode is reserved for future iterations when sandboxed execution infrastructure is available.

| `artifactProfile.category` | Build | Future (isolated path available) |
|---|---|---|
| `DataOnly` | ✅ `runtimeHandling: "Standard"`. Deploy normally. | ✅ `runtimeHandling: "Standard"`. Deploy normally. |
| `RuntimeDependent` | ❌ **Deployment rejected.** Error: `IncompatibleArtifact`. | `runtimeHandling: "Isolated"`. Deploy in sandbox. |
| `Unknown` | ❌ **Deployment rejected.** Error: `IncompatibleArtifact`. | `runtimeHandling: "Isolated"`. Treated as RuntimeDependent. |

**Deployment failure error (Build):**

```json
{
  "error": {
    "code": "IncompatibleArtifact",
    "message": "Model 'my-fine-tuned-model' v1 cannot be deployed. Only models with SafeTensors weights (artifact profile 'DataOnly') are deployable. This model's artifact profile is 'RuntimeDependent' (signals: pickleDeserialization). Convert model weights to SafeTensors format before deploying.",
    "details": [
      {
        "code": "artifactProfileCategory",
        "message": "RuntimeDependent"
      },
      {
        "code": "artifactProfileSignals",
        "message": "pickleDeserialization"
      }
    ]
  }
}
```

This error tells the user:
1. **What** is wrong (only DataOnly models are deployable)
2. **Why** this model is not DataOnly (pickle deserialization detected)
3. **What to do** (convert to SafeTensors)

### Why Not Block at Registration?

Three reasons:

1. **Format support may expand.** A model that cannot be deployed today may be deployable tomorrow when a new backend or SKU adds isolated execution support. Blocking at registration permanently prevents that model from existing in the registry. At Build, only `DataOnly` models are deployable — but this restriction will be relaxed as isolated runtime paths become available.

2. **The registry is backend-agnostic.** Whether a model can be deployed depends on the deployment target, not the model itself. A pickle model might deploy fine on a future backend that supports sandboxing. Registration shouldn't make deployment-target-specific decisions.

3. **Users need a place to store and organize models regardless.** A model in the registry can be inspected, tagged, versioned, and shared — even before a compatible deployment path exists. Blocking registration removes that utility.

---

## Part 4: User Experience by Surface

### Portal

**Model details page:**

A small badge next to the model name:

| Category | Badge | Color |
|---|---|---|
| `DataOnly` | `Data only` | Neutral / gray |
| `RuntimeDependent` | `Not deployable` | Red / error |
| `Unknown` | `Unclassified` | Red / error |

The badge is **not prominent**. It sits in the metadata section alongside `weightType`, `baseModel`, and `tags`. It does not dominate the page.

**Expandable detail (click badge):**

> **artifact profile: Requires isolation**
>
> This model contains artifacts that require code execution during loading.
>
> **Signals detected:**
> - Pickle deserialization — `.bin` files use Python pickle format
>
> **What this means for deployment:**
> This model cannot be deployed. Only SafeTensors-based models (artifact profile: Data only) are currently deployable. Convert weights to SafeTensors format to enable deployment.

**Model list page:**

No artifact profile column by default. Available as an optional column in the table. The filter sidebar includes an `Artifact profile` filter with three checkboxes: `Data only`, `Not deployable`, `Unclassified`.

**Deployment creation flow:**

If the user selects a model with `RuntimeDependent` or `Unknown` classification:

> ⛔ This model cannot be deployed. Only SafeTensors-based models (artifact profile: Data only) are deployable. Convert the model weights to SafeTensors format to enable deployment. [Learn more]

The "Deploy" button is **disabled** for models that are not `DataOnly`. The user cannot proceed to SKU/template selection.

### CLI (`azd`)

**`azd ai models show`:**

artifact profile is **not shown by default** in the formatted output. It appears in `--output json` and `--output table --all-fields`.

Default output:
```
Name:        my-fine-tuned-model
Version:     1
Weight type: FullWeight
Base model:  azureml://registries/azureml-meta/models/Llama-3.3-70B-Instruct/versions/2
Created:     2026-04-15T10:30:00Z
```

With `--output json` (artifact profile is present):
```json
{
  "name": "my-fine-tuned-model",
  "version": "1",
  "weightType": "FullWeight",
  "artifactProfile": {
    "category": "RuntimeDependent",
    "signals": ["pickleDeserialization"]
  }
}
```

**`azd ai models create` (registration):**

If the model is classified as `RuntimeDependent` or `Unknown`, the CLI prints a single-line warning after the success message:

```
✓ Model 'my-fine-tuned-model' v1 registered successfully.
⚠ Not deployable: artifact profile is 'RuntimeDependent' (pickle deserialization detected). Only SafeTensors models are deployable. Convert to SafeTensors format.
```

For `DataOnly` models, no extra output.

**`azd ai models list`:**

No artifact profile column by default. Available with `--output table --all-fields`:

```
NAME                    VERSION  TYPE        PROFILE       STATUS
my-safetensors-model    1        FullWeight  Data only     Registered
my-pickle-model         1        FullWeight  Not deploy.   Registered
my-lora-adapter         1        LoRA        Data only     Registered
```

### SDK

**Model object:**

```python
model = client.models.get(name="my-fine-tuned-model", version="1")

# artifact profile is always available
print(model.content_profile.category)    # "RuntimeDependent"
print(model.content_profile.signals)     # ["pickleDeserialization"]
```

**Registration response:**

```python
model = client.models.create_or_update(Model(...))

# Check artifact profile immediately after registration
if model.content_profile.category != "DataOnly":
    print(f"Warning: model classified as {model.content_profile.category}")
    print(f"Signals: {', '.join(model.content_profile.signals)}")
    print("Deployment may use isolated runtime path.")
```

**Filtering (client-side until Index Service integration):**

```python
# Find all RuntimeDependent models
risky_models = [
    m for m in client.models.list()
    if m.content_profile.category == "RuntimeDependent"
]
```

**Deployment response:**

```python
# After creating deployment via control-plane SDK
deployment = mgmt_client.accelerator_deployments.begin_create_or_update(
    resource_group, account_name, deployment_name, deployment_body
).result()

print(deployment.properties.runtime_handling)  # "Standard" or "Isolated"
```

### Error Messages — Consistency Across Surfaces

The same classification uses consistent terminology everywhere:

| Surface | `DataOnly` display | `RuntimeDependent` display | `Unknown` display |
|---|---|---|---|
| REST API | `"DataOnly"` | `"RuntimeDependent"` | `"Unknown"` |
| Portal badge | `Data only` | `Not deployable` | `Unclassified` |
| CLI warning | `Data only` | `Not deployable` | `Not deployable` |
| Deployment error | N/A (deploys normally) | `IncompatibleArtifact` + signals | `IncompatibleArtifact` |
| Policy definition | `artifactProfile.category == "DataOnly"` | `artifactProfile.category == "RuntimeDependent"` | `artifactProfile.category == "Unknown"` |

Note: The REST wire value is PascalCase (`RuntimeDependent`). User-facing surfaces translate to natural language (`Not deployable`). Policy uses the wire value. At Build, only `DataOnly` models are deployable — `RuntimeDependent` and `Unknown` models are registered but cannot be deployed.

---

## Part 5: Governance and Policy Integration

### Azure Policy

`artifactProfile.category` and `artifactProfile.signals` are available as policy fields. Example policies:

**Block RuntimeDependent models from deploying in production:**

```json
{
  "if": {
    "allOf": [
      {
        "field": "Microsoft.CognitiveServices/accounts/acceleratorDeployments/model.artifactProfile.category",
        "notEquals": "DataOnly"
      },
      {
        "field": "Microsoft.CognitiveServices/accounts/tags['environment']",
        "equals": "production"
      }
    ]
  },
  "then": {
    "effect": "deny"
  }
}
```

**Block models with native binaries everywhere:**

```json
{
  "if": {
    "field": "Microsoft.CognitiveServices/accounts/acceleratorDeployments/model.artifactProfile.signals[*]",
    "equals": "nativeBinary"
  },
  "then": {
    "effect": "deny"
  }
}
```

**Allow pickle in dev, block in prod:**

```json
{
  "if": {
    "allOf": [
      {
        "field": "Microsoft.CognitiveServices/accounts/acceleratorDeployments/model.artifactProfile.signals[*]",
        "equals": "pickleDeserialization"
      },
      {
        "field": "Microsoft.CognitiveServices/accounts/tags['environment']",
        "notEquals": "development"
      }
    ]
  },
  "then": {
    "effect": "deny"
  }
}
```

### Why This Structure Works for Governance

- `category` gives broad, simple policy: "only DataOnly in prod."
- `signals` gives granular policy: "pickle OK in dev, but native binaries blocked everywhere."
- `runtimeHandling` (on deployment) gives audit evidence: "this deployment is running in isolated mode."

All three use the same vocabulary, so a policy written against `artifactProfile.category` reads consistently whether the admin sees it in the policy JSON, the portal, or a compliance report.

---

## Part 6: Extensibility

### Additive-Only Contract

- **New `category` values** can be introduced in future API versions. Existing values (`DataOnly`, `RuntimeDependent`, `Unknown`) are stable and will not be removed or renamed.
- **New `signals`** can be added at any time. They are additive-only. A new signal never changes the meaning of existing signals.
- **New `runtimeHandling` values** can be introduced (e.g., `Restricted`, `VMIsolated`) if the platform adds finer-grained infrastructure paths.

### Why `category` + `signals` Is Future-Proof

If the platform later needs to distinguish between "needs a Python sandbox" and "needs full VM isolation," that distinction lives in `runtimeHandling` on the deployment side — not in `category`. The model's classification stays the same (it's still `RuntimeDependent` with `nativeBinary`). Only the deployment behavior changes.

If the classifier improves and can deeply analyze pickle files to determine some are safe (no `REDUCE` opcodes, no dangerous imports), a new category like `SafePickle` could be introduced. Or a signal like `pickleDeserialization` could be refined into `pickleDeserialization.safe` and `pickleDeserialization.unsafe`. Both are additive changes.

### Example: Future Category Addition

```
category: "DataOnly" | "RuntimeDependent" | "ContainerRequired" | "Unknown"
```

`ContainerRequired` could mean "the model needs a custom container but doesn't require sandboxing for the artifacts themselves" — a BYOC scenario. This slots in without breaking any existing policy, code, or UX that checks for `DataOnly` or `RuntimeDependent`.

### What Will NOT Change

- `artifactProfile` will always be service-computed, never user-writable.
- `category` will always be a single enum value, never an array.
- `signals` will always be an array of strings.
- `runtimeHandling` will always live on the deployment, never on the model.

---

## Part 7: Worked Examples

### Example 1: Clean SafeTensors LoRA Adapter

User registers a LoRA adapter with `adapter_model.safetensors` and `adapter_config.json`.

**Registration response:**
```json
{
  "name": "my-lora-medical",
  "version": "1",
  "weightType": "LoRA",
  "artifactProfile": {
    "category": "DataOnly",
    "signals": []
  }
}
```

**Portal:** Badge says `Data only` in gray. No warning.
**CLI:** No extra output after success message.
**Deployment:** `runtimeHandling: "Standard"`. Normal path.
**Policy:** Passes all policies. No governance action.

### Example 2: PyTorch Pickle Model

User registers a full-weight model with `pytorch_model.bin` files.

**Registration response:**
```json
{
  "name": "my-pytorch-model",
  "version": "1",
  "weightType": "FullWeight",
  "artifactProfile": {
    "category": "RuntimeDependent",
    "signals": ["pickleDeserialization"]
  },
  "warnings": [
    {
      "code": "RuntimeDependentArtifact",
      "message": "This model contains artifacts that require code execution during loading (pickle deserialization). Only SafeTensors-based models (artifact profile 'DataOnly') are currently deployable. Convert to SafeTensors format before deploying."
    }
  ]
}
```

**Portal:** Badge says `Not deployable` in yellow. Expandable detail explains pickle.
**CLI:** Warning line printed after success: model is not deployable.
**Deployment:** **Rejected.** Error: `IncompatibleArtifact`. Only `DataOnly` models are deployable at Build.
**Policy:** Also blocked by default — non-DataOnly models cannot deploy regardless of policy.

### Example 3: HuggingFace Model with Custom Code

User imports a HF model that includes `modeling_custom.py` and pickle weights.

**Registration response:**
```json
{
  "name": "my-custom-arch",
  "version": "1",
  "weightType": "FullWeight",
  "artifactProfile": {
    "category": "RuntimeDependent",
    "signals": ["pickleDeserialization", "customPythonCode"]
  },
  "warnings": [
    {
      "code": "RuntimeDependentArtifact",
      "message": "This model contains artifacts that require code execution during loading (pickle deserialization, custom Python code). Only SafeTensors-based models (artifact profile 'DataOnly') are currently deployable. Convert to SafeTensors format and remove custom code before deploying."
    }
  ]
}
```

**Deployment:** **Rejected.** Only `DataOnly` models are deployable at Build. Both signals contribute to the RuntimeDependent classification.

### Example 4: Unknown Format

User uploads a model with `.onnx` files and a format the classifier doesn't recognize.

**Registration response:**
```json
{
  "name": "my-experimental-model",
  "version": "1",
  "weightType": "FullWeight",
  "artifactProfile": {
    "category": "Unknown",
    "signals": ["unknownFormat"]
  },
  "warnings": [
    {
      "code": "UnclassifiedArtifact",
      "message": "Foundry could not confidently classify this artifact package. Only SafeTensors-based models (artifact profile 'DataOnly') are currently deployable. This model cannot be deployed until it is reclassified or converted to a supported format."
    }
  ]
}
```

**Portal:** Badge says `Unclassified` in yellow.
**Deployment:** **Rejected.** Treated as non-DataOnly. Only `DataOnly` models are deployable at Build.

---

## Appendix A: Classification Rules

The classifier runs at registration commit time. It operates on file extensions and file headers only. It does not execute or deeply analyze file contents.

### Decision Logic

```
signals = []

for each file in uploaded artifacts:
    ext = file.extension.lower()
    if ext in [".bin", ".pkl", ".pt", ".pth"]:
        signals.add("pickleDeserialization")
    if ext == ".py":
        signals.add("customPythonCode")
    if ext in [".so", ".dll", ".dylib", ".wasm"]:
        signals.add("nativeBinary")
    if ext not in KNOWN_SAFE_EXTENSIONS and ext not in KNOWN_RISKY_EXTENSIONS:
        signals.add("unknownFormat")

# Dynamic ops detection (future — requires config.json inspection)
# if config.json references custom op registries or dynamic architectures:
#     signals.add("dynamicOps")

if len(signals) == 0:
    category = "DataOnly"
elif signals == {"unknownFormat"}:
    category = "Unknown"
else:
    category = "RuntimeDependent"
```

### Known Safe Extensions

Extensions that never trigger a signal:

```
.safetensors, .json, .txt, .md, .yaml, .yml,
.model (sentencepiece), .vocab, .tiktoken, .gguf,
.msgpack
```

### Known Risky Extensions

Extensions that trigger a specific signal:

```
.bin, .pkl, .pt, .pth     → pickleDeserialization
.py                        → customPythonCode
.so, .dll, .dylib, .wasm  → nativeBinary
```

### Edge Cases

| Scenario | Classification | Rationale |
|---|---|---|
| Model has only `.safetensors` and `.json` | `DataOnly`, `[]` | All known-safe formats. |
| Model has `.safetensors` AND `.bin` | `RuntimeDependent`, `["pickleDeserialization"]` | Presence of any risky file triggers classification. |
| Model has `.safetensors` AND `.py` | `RuntimeDependent`, `["customPythonCode"]` | Custom code detected. |
| Model has only `.bin` files | `RuntimeDependent`, `["pickleDeserialization"]` | All weights are pickle. |
| Model has `.gguf` only | `DataOnly`, `[]` | GGUF is a known-safe binary format. |
| Model has `.onnx` only | Check KNOWN_SAFE list | ONNX should be added to known-safe (static graph format). |
| Model has files with no extension | `Unknown`, `["unknownFormat"]` | Cannot classify. |
| Model has `.safetensors` AND `.whl` | `Unknown`, `["unknownFormat"]` | `.whl` is not in either list. |
| Empty upload (no files) | Caught by existing validation | `NoWeightFiles` error before classification runs. |

---

## Appendix B: Model Object Schema Addition

This extends the schema from [spec-models-deploy.md](spec-models-deploy.md#appendix-model-object-schema).

| Property | Type | In Response | Description |
|---|---|---|---|
| `artifactProfile` | `artifactProfile` | Read-only | Service-computed artifact classification. Present after registration commit. |

### artifactProfile

| Property | Type | Description |
|---|---|---|
| `category` | `string` (enum) | `"DataOnly"` \| `"RuntimeDependent"` \| `"Unknown"` |
| `signals` | `string[]` | Zero or more signal identifiers. Additive-only across API versions. |

### Deployment Schema Addition

| Property | Type | In Response | Description |
|---|---|---|---|
| `runtimeHandling` | `string` (enum) | Read-only | `"Standard"` \| `"Isolated"`. Present on accelerator deployment response. |

---

## Open Decisions

| # | Question | Options | Impact |
|---|---|---|---|
| 1 | Should `.onnx` be classified as known-safe? | Yes (static graph) / No (can contain custom ops) | Affects ONNX import experience |
| 2 | Should `dynamicOps` signal be V1 or deferred? | V1 (requires config.json parsing) / Deferred (extension-only for V1) | Affects classifier complexity |
| 3 | What is the latency budget for synchronous classification? | Target <100ms / Allow up to 500ms | Affects whether deep header inspection is feasible |
| 4 | Should `artifactProfile` be backfilled for existing models? | Yes (background migration) / No (null for pre-existing) | Affects policy rollout |
| 5 | Does the isolated path have user-visible cost/latency impact? | Yes (document difference) / No (transparent) | Determines how prominently to surface classification |

---

## Appendix C: Naming Decision — Why `artifactProfile`

We evaluated four field names across every user-facing surface: portal badge, portal filter sidebar, portal deployment warning, CLI output, SDK property access, REST JSON key, Azure Policy field, deployment error message, catalog metadata label, and documentation heading.

### Option 1: `contentProfile`

**Pros:**
- Intuitive phrase — "profile of the content" reads naturally.
- Short (14 characters).

**Cons:**
- **High confusion with content safety.** On an AI platform that has content filtering, content moderation, and content safety as first-class features, a field named `contentProfile` on a model object will be read as "the model's content moderation profile" — what topics it's allowed to generate, what guardrails are applied. That is fundamentally different from what this field represents.
- **Ambiguous between file contents and model output.** "Content" can mean "what's in the files" or "what the model produces." Both interpretations are valid, but only one is correct.
- Portal filter "Filter by content profile" sounds like a content moderation filter.
- Deployment error "content profile is RuntimeDependent" reads as if the model's output is the problem, not its file format.

**Verdict:** Rejected. The naming collision with content safety/moderation is a real usability problem on an AI platform.

### Option 2: `artifactScan`

**Pros:**
- Industry precedent. HuggingFace calls their equivalent "Pickle Scanning" and "Malware Scanning." ML engineers understand "scan" in this context.
- Clearly communicates an action: the platform scanned the files.
- Short (12 characters).

**Cons:**
- "Scan" is a verb/action, not a property. `model.artifactScan` sounds like a method call or an ongoing process, not a piece of metadata.
- Portal filter "Filter by artifact scan" sounds like "show me models currently being scanned."
- Users may expect to trigger a re-scan. "Scan" implies something that can be re-run.
- Deployment error "artifact scan is RuntimeDependent" implies the scan found a problem, introducing a pass/fail mental model that doesn't match the three-state classification.

**Verdict:** Close second. The verb/action connotation is the main weakness. Would work well for a CI/CD-style scan result object, but less well for durable model metadata.

### Option 3: `artifactType`

**Pros:**
- Most natural for non-technical users. "What type of artifact is this?" is immediately understandable.
- Best catalog/portal experience. "Filter by artifact type" reads perfectly. "Artifact type: Data only" is the clearest badge.
- Deployment error "artifact type is RuntimeDependent" reads naturally.

**Cons:**
- **Sub-field redundancy.** `artifactType.category` reads as "the type's category" — two classification words stacked. Could rename sub-field to `.kind`, but then you have `artifactType.kind` which is similarly redundant.
- **Proximity to `weightType`.** The model object already has `weightType` (`FullWeight`, `LoRA`, `DraftModel`). Adding `artifactType` creates two `*Type` fields on the same object that answer different questions. Users will ask "what's the difference between weightType and artifactType?"
- `type` is a reserved or overloaded term in many languages (TypeScript, Python) and API design systems.

**Verdict:** Best portal/UX experience, but the `weightType` proximity and sub-field redundancy are real schema-level problems.

### Option 4: `artifactProfile` ✅ (Selected)

**Pros:**
- **Zero ambiguity about what it refers to.** "Artifact" in ML platforms always means "the files you uploaded." No confusion with model output, content safety, or model behavior.
- **"Profile" is a neutral, durable noun.** It means "a collection of characteristics about X." It doesn't imply an action (unlike "scan"), a judgment (unlike "assessment"), or a binary result (unlike "check").
- **Reads well on every surface:**
  - Portal badge: "Artifact profile: Data only" ✓
  - Portal filter: "Filter by artifact profile" ✓
  - Deployment warning: "This model's artifact profile requires isolation" ✓
  - CLI: "Artifact profile: Requires isolation" ✓
  - SDK: `model.artifact_profile.category` ✓
  - Policy: `model.artifactProfile.category == "DataOnly"` ✓
  - Deployment error: "Model artifact profile is RuntimeDependent" ✓
- **Established vocabulary.** "Artifact" is already used throughout this spec set ("Required Artifacts" table, "artifact files," "uploaded artifacts").
- **No collision with existing model fields.** Unlike `artifactType` (which sits next to `weightType`), `artifactProfile` is distinct from any existing field.
- Sub-fields `category` and `signals` read naturally under `profile`: "the profile's category" and "the profile's signals."

**Cons:**
- "Profile" is mildly overloaded in Azure broadly (CDN profiles, Traffic Manager profiles), but not in the AI/ML domain specifically.
- Slightly generic — "profile of what?" — but the parent context (`model.artifactProfile`) makes it unambiguous.

**Verdict:** Selected. Best balance of clarity, neutrality, surface readability, and schema compatibility. No collisions with existing platform terminology or model fields.

### Summary

| Name | Portal/UX | Schema fit | Clarity | Risk |
|---|---|---|---|---|
| `contentProfile` | Good | Good | ⚠️ Content safety confusion | Rejected |
| `artifactScan` | ⚠️ Sounds like action | Good | Good | Runner-up |
| `artifactType` | Best | ⚠️ `weightType` proximity | Best for non-technical | Runner-up |
| **`artifactProfile`** | **Good** | **Best** | **Good** | **Selected** |
