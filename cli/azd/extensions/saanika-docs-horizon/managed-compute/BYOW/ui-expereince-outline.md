# UX Guidance — Register Your Own Models

> **Audience:** Designer
> **Specs:** [spec-models-deploy.md](spec-models-deploy.md) (full registration API + validation rules), [spec-artifact-classification.md](spec-artifact-classification.md) (what happens when non-SafeTensors files are uploaded), [spec-models-register-local.md](spec-models-register-local.md) (upload-first flow mechanics), [spec-models-register-lora.md](spec-models-register-lora.md) (LoRA adapter registration)

---

## What this is

Users upload their own model weights into a Foundry project and deploy them on GPUs. This document covers the **registration flow only** — from clicking "+ Register" to seeing the model in the project.

### What's in scope at Build

- **Local upload** is the only manual registration path. User uploads files from their machine (or pastes a blob storage SAS URI).
- **LoRA adapters** register through the same flow, with `weightType: "LoRA"`. Applicable for Firewokrs integrations only. 
- **Training job outputs** auto-register when the job completes — no UI registration flow needed. These models simply appear in the models list. ([spec-models-register-training-job.md](spec-models-register-training-job.md))
- **Hugging Face import** is out of scope for Build. ([spec-models-register-hf.md](spec-models-register-hf.md))

---

## Guiding principles for registration

**Registration always succeeds regardless of file format.** The platform never rejects `.bin`, `.pt`, or any other file type at registration time. It accepts everything, classifies the artifacts, and surfaces advisory warnings. Enforcement happens later at deployment time. This is a deliberate design choice — see [spec-artifact-classification.md](spec-artifact-classification.md) §"Why Not Block at Registration?" for the full rationale.

**Show consequences inline.** When the user picks a base model, immediately show what that unlocks (frameworks, GPU types). When they upload files that will limit deployability, tell them right away — but don't block them.

**Every error tells the user what to do.** Not just what went wrong.

---

## Entry points

The user can start registration from:
- Build Tab -> Models 

---

## Registration wizard

### Step 1 — Tell us about your model

Collect everything about the model before uploading files.

**Base model** (required) — this is the most important field. It determines which serving frameworks, GPU types, and deployment templates are available downstream.
- Search-as-you-type picker against the Foundry Model Catalog.
- Each result row should show: model name, publisher, version, supported frameworks (vLLM / SGLang), and GPU types. This previews what the user will get at deployment time.
- Only show catalog models with approved deployment templates — models without templates can't serve as a base. (See [spec-models-deploy.md](spec-models-deploy.md) §"Part B: Bridge" for how templates are resolved from the base model.)

**Model name** (required) — text input. Constraint: `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,253}$`. Show the rule as a hint below the field.

**Version** (optional) — text input, defaults to auto-increment. Hint: "Leave blank to auto-assign the next version."


**Weight type** (required) — two radio options:
- **Full weight model** — "A complete model checkpoint with all parameters."
- **LoRA adapter** — "A lightweight delta-weight file that modifies specific layers of the base model."

When LoRA is selected, reveal three additional fields:
- **Rank** and **Alpha** (required unless `adapter_config.json` provides them during upload)
- **Target modules** (optional)
- **Dropout** (optional)
- Show a hint: "These fields auto-populate from `adapter_config.json` if included in your upload." ([spec-models-register-lora.md](spec-models-register-lora.md) §"Auto-population")

**Description** (optional) — text area.

**Tags** (optional) — key:value chips.

Block "Next" only when required fields are missing (name, base model, weight type).

---

### Step 2 — Upload your files

Behind the scenes, this step calls `startPendingUpload` → uploads to blob storage via SAS URI → then commits with `PutModel`. The user doesn't see these API steps — they see a file upload screen. ([spec-models-register-local.md](spec-models-register-local.md) for the three-step mechanics.)

**Upload area** — drag-and-drop zone + Browse button. Below it, an alternative: "Or paste an Azure Blob Storage SAS URI."

**File list** — as files are selected or uploading, show a table: file name, size, status (queued / uploading / done). Show a total progress bar with estimated time remaining.

**Large model hint** — for uploads > 100 GB, show: "For very large models, please use the SDK/CLI with `azcopy` is significantly faster. [Learn more →](spec-models-register-local.md)"

**Do not close warning** — persistent banner: "Do not close this tab while uploading."

#### What files to expect and how to respond

The platform accepts any file. The artifact classification system ([spec-artifact-classification.md](spec-artifact-classification.md)) inspects files at registration commit and sets `artifactProfile.category`. The UI's job during upload is to give the user early, honest feedback about what will happen:

**SafeTensors files** (`.safetensors`) — no message needed. These are the happy path. Model will be classified `DataOnly` and will be deployable.

**PyTorch / pickle files** (`.bin`, `.pt`, `.pkl`, `.pth`) — **accept them.** Show an advisory (yellow, not red): "These files use pickle/PyTorch format. The model will register successfully but won't be deployable until converted to SafeTensors. [How to convert →]" The model will be classified `RuntimeDependent`.

**Code or binary files** (`.py`, `.so`, `.dll`) — **accept them.** Advisory: "This model contains executable code. It will register but won't be deployable on managed compute."

**`config.json` missing** — advisory (not blocking): "Recommended: include `config.json`. Inference frameworks need it at serving time." (The spec says `config.json` validation is advisory — [spec-models-deploy.md](spec-models-deploy.md) §"Validation Rules".)

**`config.json` mismatches base model** — advisory: "The architecture in `config.json` differs from the selected base model. This may cause deployment issues." (Advisory only, does not block.)

**`adapter_config.json` found** (LoRA flow) — auto-populate the rank, alpha, and target modules fields from Step 1. Show: "LoRA configuration detected and auto-populated."

**LoRA adapter weight formats** — both `.safetensors` and `.bin` are fully accepted for LoRA adapters. The Fireworks backend supports both. No advisory needed for `.bin` in the LoRA case. ([spec-models-register-lora.md](spec-models-register-lora.md) §"Fireworks integration note")

**Key rule: never prevent registration based on file contents.** The user may want to store and version a model even if it's not deployable today. Format support may expand in the future.

---

### Step 3 — Review and register

Read-only summary of everything from Steps 1–2. Each section has an "Edit" link back to the relevant step.

Show a brief "What happens next":
1. Platform validates and classifies your uploaded files.
2. Model is registered in this project.
3. You can then deploy it on GPU infrastructure.

**"Register model" button:**
- Calls `PUT /models/{name}/versions/{version}`.
- Spinner while in progress.
- On success → navigate to the model detail page. If the API response includes a `warnings` array (e.g., `RuntimeDependentArtifact`), show it as an info banner on the detail page — not as an error.
- On failure → show an inline error banner at the top of the review page.

**Registration errors** (from [spec-models-deploy.md](spec-models-deploy.md) §"Validation Rules"):

| Error | User-facing message |
|---|---|
| `BaseModelNotFound` | "The selected base model was not found in the catalog. It may have been removed." |
| `BaseModelNoDTs` | "The selected base model has no deployment templates. Choose a different base model." |
| `ModelVersionConflict` | "Version '{ver}' already exists with different properties. Use a different version or model name." |
| `UploadIncomplete` | "Some files are missing. Go back and re-upload." |
| `BaseModelRequired` | "A base model is required. Go back and select one." |
| `BaseModelDeprecated` | "The selected base model has been deprecated. Choose a different base model." |

---

## After registration — model detail page

This is where the user lands after successful registration. It's also the page for any model clicked from the models list.

**Success banner** (shown once) — "Model registered successfully." If deployable: prominent "Deploy" button. If not deployable: advisory explaining why, with conversion guidance.

**Overview** — name, version, weight type, status, created date, base model (linked to catalog), description, tags.

**Artifact profile badge** — small, not prominent. Sits alongside the other metadata. ([spec-artifact-classification.md](spec-artifact-classification.md) §"Part 4: User Experience by Surface" for the exact display rules.)

| Category | Badge | Color | Deploy button |
|---|---|---|---|
| `DataOnly` | "Data only" | Gray | Enabled |
| `RuntimeDependent` | "Not deployable" | Red | Disabled — tooltip explains SafeTensors conversion needed |
| `Unknown` | "Unclassified" | Red | Disabled — same tooltip |

Clicking the badge expands to show signals (e.g., "Pickle deserialization — `.bin` files detected").

**Deployment templates** — table showing templates available from the base model: name, framework, GPU options, "best for" hint. This previews the deployment configuration step.

**LoRA variant** — shows LoRA config (rank, alpha, target modules, dropout). "Deploy" becomes "Attach to deployment →" since adapters can't deploy standalone. No deployment templates table (adapters inherit from the base deployment). ([spec-models-register-lora.md](spec-models-register-lora.md))

**Training job lineage** — models auto-created from training jobs show: "Created from training job: `{jobName}`" with a link. ([spec-models-register-training-job.md](spec-models-register-training-job.md) §"Lineage")

---

## Models list page

Columns: name, type (Full weight / LoRA), base model, status.

- **Full weight (DataOnly)** → primary action: "Deploy"
- **LoRA adapters** → primary action: "Attach"
- **RuntimeDependent / Unknown** → red "Not deployable" badge, action buttons disabled. Model is still visible, inspectable, deletable.
- **Training job models** → look identical to manually registered models.

Context menu (⋯): View details, Delete, Copy model URI.

Filter: All types / Full weight / LoRA adapter.

**Empty state** — centered: "No models registered yet. Register your own fine-tuned model to deploy it on GPU infrastructure." CTA: "Register a model." Secondary: "Or deploy from the Model Catalog →."


